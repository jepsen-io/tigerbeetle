(ns jepsen.tigerbeetle.explainer
  "The model checker said something was illegal. Great, but *why*? This
  namespace helps you figure out what writes *could* have led to a specific
  read.

  Fun story: this is the subset sum problem, and feels well-suited to
  constraint programming, but most solvers use small integer domains--either
  32-bit or ~50-bit ints, and we need 128-bit ints. Clojure's core.logic can do
  bigints, but blows up the compiler when given a few hundred lvars."
  (:require [clojure [pprint :refer [pprint]]]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [history :as h]]
            [tesser.core :as t])
  (:import (jepsen.tigerbeetle SubsetSum)))

(defn possible-transfers
  "Finds all transfers which were invoked prior to the given index,
  matching the given op types, whose key k was equal to value v."
  [history max-index types k v]
  (h/ensure-pair-index history)
  (->> (t/mapcat (fn [op]
                   (when (and (identical? :create-transfers (:f op))
                              (h/invoke? op)
                              (< (:index op) max-index))
                     (let [op' (h/completion history op)]
                       (when (contains? types (:type op'))
                         (->> (:value op)
                              (filter (fn [transfer]
                                        (= v (get transfer k))))
                              (map (fn restrict [t]
                                     {:id     (:id t)
                                      :amount (:amount t)
                                      :type   (:type op')}))
                              (into [])))))))
       (t/into [])
       (h/tesser history)))

(defn applied-transfers
  "Takes a list of amounts (bigintegers) and a list of transfer maps (e.g. with
  bigint :amount), in the same order, but where the amounts are drawn from a
  subset of the transfers. Returns the transfers which contributed those
  amounts, with :applied? true or false, depending on whether they appeared in
  amounts."
  [amounts transfers]
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
            (if (= (bigint a) (:amount t))
              ; Match!
              (cons (assoc t :applied? true)
                    (applied-transfers amounts' transfers'))
              ; No dice, try next transfer
              (cons (assoc t :applied? false)
                    (applied-transfers amounts transfers')))))))

(defn explain
  "Tries to explain how you got a specific read. Takes:

    - A history
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
  [history read-op' account-id field value considering additional-data]
  (assert (#{:credits-posted :debits-posted} field))
  (let [transfer-field (case field
                         :credits-posted :credit-account-id
                         :debits-posted  :debit-account-id)
        possible-transfers (possible-transfers history
                                               (:index read-op')
                                               considering
                                               transfer-field
                                               account-id)
        n (count possible-transfers)
        ; Order the transfers such that OKs are at the start, and infos, then
        ; failures, at the end--this should help us explore the state space
        ; faster.
        possible-transfers (vec
                             (sort-by (fn likelihood [transfer]
                                      (case (:type transfer)
                                        :ok   0
                                        :info 1
                                        :fail 2))
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
