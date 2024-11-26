(ns jepsen.tigerbeetle.explainer
  "The model checker said something was illegal. Great, but *why*? This
  namespace helps you figure out what writes *could* have led to a specific
  read."
  (:require [clojure [pprint :refer [pprint]]]
            [clojure.core.logic :as l]
            [clojure.core.logic.fd :as lfd]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [history :as h]]
            [tesser.core :as t]))

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
        n (count possible-transfers)]
    (when (pos? n)
      (let [; Compile a core.logic program where each transfer contributes
            ; either 0 or its `:amount`, trying to reach `value`. I'd love to
            ; do this functionally but the finite-domain library has no concept
            ; of a sum over a variable number of lvars. We bash together a
            ; program expression.

            ; Names of our logic variables
            lvars (mapv (fn [i]
                          (symbol (str "a" i)))
                        (range n))
            ; Every var can contribute either 0 or the amount of its transfer
            domains (mapv (fn [lvar transfer]
                            `(lfd/in ~lvar (lfd/domain 0 ~(:amount transfer))))
                          lvars
                          possible-transfers)
            ; The vars have to sum to `value`. We introduce an extra 0 here
            ; because (+ a) throws an unbound error--I don't think they
            ; implemented unary +.
            sum `(lfd/eq (~'= ~value (~'+ 0N ~@lvars)))
            program
            `(l/run 1 [~'q]
                    (l/fresh [~@lvars]
                             ; Let q be the lvars
                             (l/== ~'q ~lvars)
                             ~@domains
                             ~sum))
            [solution] (eval program)]
        (when solution
          ; Turn that solution back into a data structure explaining what
          ; transfers we did and didn't use.
          (let [solution (mapv (fn [transfer contribution]
                                 (assoc transfer
                                        :applied? (< 0 contribution)))
                                 possible-transfers
                                 solution)]
            (merge additional-data
                   {:considering considering
                    :solution    solution})))))))
