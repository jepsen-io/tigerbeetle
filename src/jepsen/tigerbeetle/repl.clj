(ns jepsen.tigerbeetle.repl
  "The REPL drops you here."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen [history :as h]
                    [store :as store]]
            [tesser.core :as t]))

(defn find-by
  "Finds a single account or transfer by a given predicate."
  [pred history]
  (->> (t/filter h/ok?)
       (t/filter (h/has-f? #{:lookup-accounts
                             :lookup-transfers}))
       (t/mapcat :value)
       (t/filter pred)
       t/first
       (h/tesser history)))

(defn find-by-id
  "Finds a single account or transfer by ID."
  [id history]
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
