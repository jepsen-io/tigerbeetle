(ns jepsen.tigerbeetle.repl
  "The REPL drops you here."
  (:require [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [loopr]]
            [jepsen [history :as h]
                    [store :as store]]
            [jepsen.tigerbeetle.core :refer :all]
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
  "Finds the invocation operation that created a given ID or account map or
  transfer map."
  [id-or-map history]
  (let [id (if (map? id-or-map)
             (:id id-or-map)
             id-or-map)]
    (assert (instance? clojure.lang.BigInt id)
            (str "Expected bigint ID, got " (pr-str id)))
    (->> (t/filter h/invoke?)
         (t/filter (h/has-f? #{:create-accounts
                               :create-transfers}))
         (t/filter (fn [op]
                     (some #(= id (:id %))
                           (:value op))))
         (t/first)
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
