(ns jepsen.tigerbeetle.explainer
  "The model checker said something was illegal. Great, but *why*? This
  namespace helps you figure out what writes *could* have led to a specific
  read.

  Fun story: this is the subset sum problem, and feels well-suited to
  constraint programming, but most solvers use small integer domains--either
  32-bit or ~50-bit ints, and we need 128-bit ints. Clojure's core.logic can do
  bigints, but blows up the compiler when given a few hundred lvars."
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

(defn split-vec
  "Splits a vector into left and right halves, approximately at the midpoint."
  [v]
  (let [n (count v)]
    (case n
      0 [[] []]
      1 [v []]
      (let [half (long (/ n 2))]
        [(subvec v 0 half) (subvec v half n)]))))

(defn sum-tree
  "Takes a target value (either an lvar or a literal) and a vector of lvars, as
  symbols. Returns a map of:

   {:constraints    A collection of logic constraints which ensure the sum holds
    :new-lvars      A collection of logic variables you must declare.}"
  ([target lvars]
   (let [new-lvars   (atom [])
         constraints (vec (sum-tree target lvars (atom 0) new-lvars))]
     {:constraints constraints
      :new-lvars   @new-lvars}))
  ([target lvars next-lvar-num new-lvars]
   (info "Considering" (count lvars) "lvars" lvars "summing to" target)
   (case (count lvars)
     0 []
     1 [`(l/== ~target ~(first lvars))]
     2 [`(lfd/+ ~(first lvars) ~(second lvars) ~target)]
     ; Introduce new logic variables for the left and right halves. We're not
     ; even going to try worrying about anaphoric hygiene here. Also I am
     ; *lazy* and am not thinking about doing this purely functional.
     (let [left  (symbol (str "s" (swap! next-lvar-num inc)))
           right (symbol (str "s" (swap! next-lvar-num inc)))
           [left-lvars right-lvars] (split-vec lvars)]
       (swap! new-lvars conj left right)
       (cons `(lfd/+ ~left ~right ~target)
             (concat (sum-tree left left-lvars next-lvar-num new-lvars)
                     (sum-tree right right-lvars next-lvar-num new-lvars)))))))

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
            ; Fun story: the implementation of n-ary + causes the compiler to
            ; blow up. We build our own by assembling lvars into a tree of
            ; binary + operations.
            sum (sum-tree value lvars)

            program
            `(l/run 1 [~'q]
                    (l/fresh [~@lvars ~@(:new-lvars sum)]
                             ; Let q be the lvars
                             (l/== ~'q ~lvars)
                             ~@domains
                             ~@(:constraints sum)))
            _ (pprint program)
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
