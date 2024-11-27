(ns jepsen.tigerbeetle.explainer
  "The model checker said something was illegal. Great, but *why*? This
  namespace helps you figure out what writes *could* have led to a specific
  read.

  Fun story: this is the subset sum problem, and feels well-suited to
  constraint programming, but most solvers use small integer domains--either
  32-bit or ~50-bit ints, and we need 128-bit ints. Clojure's core.logic can do
  bigints, but blows up the compiler when given a few hundred lvars."
  (:require [bifurcan-clj [map :as bm]]
            [clojure [datafy :refer [datafy]]
                     [pprint :refer [pprint]]]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [history :as h]]
            [tesser.core :as t])
  (:import (jepsen.tigerbeetle SubsetSum)))

(defn possible-transfers
  "Finds all transfers which were invoked prior to the given index,
  matching the given op types, whose key k was equal to value v."
  [history transfer-id->timestamp max-index types k v]
  (h/ensure-pair-index history)
  (->> (t/mapcat (fn [op]
                   (when (and (identical? :create-transfers (:f op))
                              (h/invoke? op)
                              (< (:index op) max-index))
                     (let [op' (h/completion history op)]
                       (when (contains? types (:type op'))
                         (->> (:value op)
                              ; Augment transfers to have an index
                              (map-indexed (fn index [i transfer]
                                             (assoc transfer :i i)))
                              ; Find just those of interest
                              (filter (fn restrict [transfer]
                                        (= v (get transfer k))))
                              (map (fn augment [t]
                                     ; One thing to keep in mind here: it's
                                     ; normal for OK operations to contain
                                     ; transfers that did not actually succeed.
                                     ; We... actually *want* to have those as
                                     ; candidates in case the state machine is
                                     ; doing something unexpected, but it gets
                                     ; weird! We assign timestamps based either
                                     ; on transfer or op timestamp, if known.
                                     {:id           (:id t)
                                      :amount       (:amount t)
                                      :op-index     (:index op)
                                      :type         (:type op')
                                      :result       (nth (:value op') (:i t))
                                      :timestamp    (bm/get
                                                      transfer-id->timestamp
                                                      (:id t)
                                                      (:timestamp op'))}))
                              (into [])))))))
       (t/into [])
       (h/tesser history)))

(defn applied-transfers
  "Takes a list of amounts (bigintegers) and a list of transfer maps (e.g. with
  bigint :amount), in the same order, but where the amounts are drawn from a
  subset of the transfers. Returns the transfers which contributed those
  amounts, with :applied? true or false, depending on whether they appeared in
  amounts."
  ([amounts transfers]
   (applied-transfers amounts transfers true))
  ([amounts transfers last-applied?]
   (cond ; No more amounts to find
         (nil? (seq amounts))
         []

         ; Out of transfers, uh oh
         (nil? (seq transfers))
         (throw (IllegalStateException.
                  (str "Ran out of transfers, still looking for"
                       amounts)))

         true
         (let [[a & amounts'] amounts
               [t & transfers'] transfers]
           (lazy-seq
             (cond ; Match!
                   (= (bigint a) (:amount t))
                   (cons (assoc t :applied? true)
                         (applied-transfers amounts' transfers' true))

                   ; 0 transfers are special--the solver doesn't take them, so
                   ; we fill in their applied status based on whatever happened
                   ; last. Either is fine, they don't change the balance.
                   (= 0N (:amount t))
                   (cons (assoc t :applied? last-applied?)
                         (applied-transfers amounts transfers' last-applied?))

                   ; No dice, try next transfer
                   true
                   (cons (assoc t :applied? false)
                         (applied-transfers amounts transfers' false))))))))

  (defn explain
  "Tries to explain how you got a specific read. Takes:

    - A history
    - A transfer-id->timestamp map
    - An OK read operation
    - An account ID in that read that you'd like to explain
    - The field in that account--either :credits-posted or :debits-posted--which
      was bad.
    - The value for that field we want to reach
    - The set of op types (e.g. #{:ok, :info} we're considering
    - Additional data to merge into the solution, if one is found.

  Attempts to find a set of transfers that produce that specific read,
  returning a map of the form:

    :considering    The set of operation types (e.g. :ok, :info) we considered
    :solution       A vector of transfer IDs executed"
  [history transfer-id->timestamp read-op' account-id field value considering additional-data]
  (assert (#{:credits-posted :debits-posted} field))
  (let [transfer-field (case field
                         :credits-posted :credit-account-id
                         :debits-posted  :debit-account-id)
        possible-transfers (possible-transfers history
                                               transfer-id->timestamp
                                               (:index read-op')
                                               considering
                                               transfer-field
                                               account-id)
        ; Temporary: filter to just those which were :ok or we didn't know the
        ; result. KYLE NEXT WEEK START HERE: it looks like this is unsolvable
        ; if we do this?
        possible-transfers (vec (filter (comp #{nil :ok} :result)
                                        possible-transfers))

        n (count possible-transfers)
        ; Order the transfers such that OKs are at the start, and infos, then
        ; failures, at the end--this should help us explore the state space
        ; faster.
        possible-transfers (vec
                             (sort-by (fn likelihood [transfer]
                                        [; First, place those which we know
                                         ; were :ok up front
                                         (:timestamp transfer)

                                         #_(case (:result transfer)
                                           :ok 0
                                           1)
                                         ; Then, choose those whose ops
                                         ; succeeded
                                         (case (:type transfer)
                                          :ok   0
                                          :info 1
                                          :fail 2)
                                         ; And finally by timestamp
                                         (:timestamp transfer)])
                                    possible-transfers))
        ; Ask the solver for a solution
        solution (SubsetSum/solve (biginteger value)
                                  (mapv (comp biginteger :amount)
                                        possible-transfers))]
    (when solution
      ; Turn that solution back into a data structure explaining what
      ; transfers we did and didn't use.
      (let [solution (vec (applied-transfers solution possible-transfers))]
        (merge additional-data
               {:considering considering
                :solution    solution})))))
