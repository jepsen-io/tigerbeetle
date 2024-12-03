(ns jepsen.tigerbeetle.explainer
  "The model checker said something was illegal. Great, but *why*? This
  namespace helps you figure out what writes *could* have led to a specific
  read.

  Fun story: this is the subset sum problem, and feels well-suited to
  constraint programming, but most solvers use small integer domains--either
  32-bit or ~50-bit ints, and we need 128-bit ints. Clojure's core.logic can do
  bigints, but blows up the compiler when given a few hundred lvars."
  (:require [bifurcan-clj [map :as bm]
                          [set :as bs]]
            [clojure [datafy :refer [datafy]]
                     [pprint :refer [pprint]]]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [history :as h]]
            [tesser.core :as t])
  (:import (jepsen.tigerbeetle SubsetSum)))

(defn possible-transfers
  "Finds all transfers in the given gistory which were invoked prior to the
  given index, which have not already been applied to obtain the last valid
  read, matching the given op types, whose `transfer-field` was equal to
  `account-id`."
  [{:keys [history
           transfer-id->timestamp
           op'
           transfer-field
           account-id
           op-types
           last-valid-read]}]
  (h/ensure-pair-index history)
  (let [applied-indices (:applied-indices last-valid-read bs/empty)
        max-index       (:index op')]
    (->> (t/mapcat (fn [op]
                     (when (and (identical? :create-transfers (:f op))
                                (h/invoke? op)
                                (< (:index op) max-index)
                                (not (bs/contains? applied-indices
                                                   (:index op))))
                       (let [op' (h/completion history op)]
                         (when (contains? op-types (:type op'))
                           (->> (:value op)
                                ; Augment transfers to have an index
                                (map-indexed (fn index [i transfer]
                                               (assoc transfer :i i)))
                                ; Find just those of interest
                                (filter (fn restrict [transfer]
                                          (= account-id
                                             (get transfer transfer-field))))
                                (map (fn augment [t]
                                       ; One thing to keep in mind here: it's
                                       ; normal for OK operations to contain
                                       ; transfers that did not actually
                                       ; succeed. We... actually *want* to have
                                       ; those as candidates in case the state
                                       ; machine is doing something unexpected,
                                       ; but it gets weird! We assign
                                       ; timestamps based either on transfer or
                                       ; op timestamp, if known.
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
         (h/tesser history))))

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
  "Tries to explain how you got a specific read. Takes a map of:

  {:history                 The history we're going to use
   :transfer-id->timestamp  The map of transfer IDs to timestamps
   :op'                     An OK account read operation which you want to
                            reach
   :account-id              An account ID in that read that you want to validate
   :field                   The field, either :credits-posted or
                            :debits-posted, that you want to explain
   :value                   The value of that field you want to reach
   :op-types                The set of operation types we're considering
   :additional-data         Additional data to merge into the solution map, if
                            found
   :last-valid-read         The map constructed by
                            checker/model-check-last-valid-read

  Attempts to find a set of transfers that produce that specific read,
  returning a map of the form:

    :considering    The set of operation types (e.g. :ok, :info) we considered
    :solution       A vector of transfer IDs executed"
  [{:keys [history
           transfer-id->timestamp
           op'
           account-id
           field
           value
           op-types
           additional-data
           last-valid-read] :as opts}]
  (assert (#{:credits-posted :debits-posted} field))
  (let [transfer-field (case field
                         :credits-posted :credit-account-id
                         :debits-posted  :debit-account-id)
        possible-transfers (possible-transfers
                             (assoc opts :transfer-field transfer-field))
        ; Exploratory: filter to just those which were :ok or we didn't know the
        ; result.
        ;possible-transfers (vec (filter (comp #{nil :ok} :result)
        ;                                possible-transfers))

        n (count possible-transfers)
        ; Order the transfers such that OKs are at the start, and infos, then
        ; failures, at the end--this should help us explore the state space
        ; faster.
        possible-transfers (vec
                             (sort-by (fn likelihood [transfer]
                                        [; Place those which we know
                                         ; were :ok up front
                                         (case (:result transfer)
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
        ; We're starting from the last valid read, so we actually want to find
        ; transfers that sum to the difference between our invalid read and the
        ; last valid one.
        last-valid-value (-> last-valid-read
                             :account
                             (get field 0))

        ; Ask the solver for a solution
        solution (SubsetSum/solve (biginteger
                                    (- value last-valid-value))
                                  (mapv (comp biginteger :amount)
                                        possible-transfers))]
    (when solution
      ; Turn that solution back into a data structure explaining what
      ; transfers we did and didn't use.
      (let [solution (vec (applied-transfers solution possible-transfers))]
        (merge additional-data
               {:op-types    op-types
                :solution    solution})))))
