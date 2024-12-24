(ns jepsen.tigerbeetle.repl
  "The REPL drops you here."
  (:require [bifurcan-clj [core :as b]
                          [list :as bl]
                          [map :as bm]
                          [set :as bs]]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [loopr]]
            [jepsen [generator :as gen]
                    [history :as h]
                    [store :as store]]
            [jepsen.generator.context :as ctx]
            [jepsen.tigerbeetle [core :refer :all]
                                [lifecycle-map :as lm]]
            [jepsen.tigerbeetle.workload [generator :as tgen]]
            [tesser.core :as t]))

(defn reads-of
  "Takes an ID. Returns [[invoke, complete]] op pairs which tried to or
  successfully read that ID, with values restricted to just that specific ID,
  rather than vectors of all IDs."
  [id history]
  (assert (instance? clojure.lang.BigInt id)
          (str "Expected bigint ID, got " (pr-str id)))
  (h/ensure-pair-index history)
  (->> (t/filter h/invoke?)
       (t/keep
         (fn per-op [op]
                 (let [op' (h/completion history op)]
                   (cond
                     ; Look at query
                     (read-by-id-fs (:f op))
                     (let [i (loopr [i 0]
                                    [id2 (:value op)]
                                    (if (= id id2)
                                      i
                                      (recur (inc i)))
                                    nil)]
                           (when i
                             [(update op :value nth i)
                              (update op' :value nth i)]))

                     ; Look at results
                     (read-by-predicate-fs (:f op))
                     (let [i (loopr [i 0]
                                    [event (:value op')]
                                    (if (= id (:id event))
                                      i
                                      (recur (inc i)))
                                    nil)]
                       (when i
                         [op
                          (update op' :value nth i)]))

                     true
                     nil))))
       (t/into [])
       (h/tesser history)))

(defn writes-with
  "Filters a history to just those writes where (f event result) is true.
  Returns [invoke, ok] pairs, where values are filtered to just those specific
  writes."
  [f history]
  (h/ensure-pair-index history)
  (->> (t/filter h/invoke?)
       (t/filter (h/has-f? write-fs))
       (t/keep
         (fn per-op [op]
           (let [op' (h/completion history op)
                 ; Build up filtered values
                 [v v'] (bireduce (fn [[v v' :as pair] event res]
                                    (if (f event res)
                                      [(conj v  event)
                                       (conj v' res)]
                                      pair))
                                  [[] []]
                                  (:value op)
                                  (:value op'))]
             (when (seq v)
               [(assoc op :value v)
                (assoc op' :value v')]))))
       (t/into [])
       (h/tesser history)))

(defn writes-with-result
  "Filters a history to just those ops with a specific result code. Filters
  events to just those events too. Returns [invoke, ok] pairs."
  [result history]
  (writes-with (fn [event res] (= result res)) history))

(defn writes-finishing
  "Filters a history to just those writes which posted or voided the given ID.
  Returns [invoke, ok] pairs."
  [id history]
  (assert (instance? clojure.lang.BigInt id))
  (writes-with (fn [event res] (= id (:pending-id event)))
               history))

(defn find-by
  "Finds a single account or transfer by a given predicate."
  [pred history]
  (->> (t/filter h/ok?)
       (t/filter (h/has-f? #{:lookup-accounts
                             :lookup-transfers
                             :get-account-transfers}))
       (t/mapcat :value)
       (t/filter pred)
       t/first
       (h/tesser history)))

(defn find-by-id
  "Finds a single account or transfer by ID."
  [id history]
  (assert (instance? clojure.lang.BigInt id)
          (str "Expected bigint ID, got " (pr-str id)))
  (find-by #(= id (:id %)) history))

(defn find-by-ts
  "Finds a single account or transfer by timestamp."
  [ts history]
  (find-by #(= ts (:timestamp %)) history))

(defn find-create
  "Finds the [invocation completion] operation that created a given ID or
  account map or transfer map."
  [id-or-map history]
  (let [id (if (map? id-or-map)
             (:id id-or-map)
             id-or-map)]
    (assert (instance? clojure.lang.BigInt id)
            (str "Expected bigint ID, got " (pr-str id)))
    (->> (t/filter h/invoke?)
         (t/filter (h/has-f? #{:create-accounts
                               :create-transfers}))
         (t/keep (fn [op]
                   (when (some #(= id (:id %))
                               (:value op))
                     [op (h/completion history op)])))
         ;(t/first)
         (t/into [])
         (h/tesser history))))

(defn transfers-debiting
  "Given a history, returns all invoked transfers which debited a specific
  account, partitioned by type: {:ok [t1 t2 ...], :info [...], :fail [...]}."
  [account-id history]
  (h/ensure-pair-index history)
  (->> (t/filter (h/has-f? :create-transfers))
       (t/filter h/invoke?)
       (t/keep (fn [op]
                 (let [transfers (->> (:value op)
                                      (filter #(= account-id
                                                  (:debit-account-id %)))
                                      (into []))]
                   (when (seq transfers)
                     (let [complete (h/completion history op)]
                       {:type      (:type complete)
                        :transfers transfers})))))
       (t/group-by :type)
       (t/mapcat :transfers)
       (t/into [])
       (h/tesser history)))

(defn result-times
  "Returns a vector of times, in seconds, of a specific kind of result code
  (e.g. :debit-account-not-found)"
  [result-code history]
  :TODO
  )

(defn reconstruct-generator
  "Reconstructs the generator state from a test and history."
  ([test]
   (reconstruct-generator test (:history test)))
  ([test history]
   (let [ctx (ctx/context test)]
     (reduce (fn [gen op]
               (gen/update gen test ctx op))
             (tgen/wrap-gen (tgen/gen test))
             history))))
