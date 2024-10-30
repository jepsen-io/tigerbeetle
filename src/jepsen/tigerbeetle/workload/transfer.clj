(ns jepsen.tigerbeetle.workload.transfer
  "A general-purpose TigerBeetle workload. Performs random transfers between
  accounts. Each operation is a TigerBeetle request; see
  https://docs.tigerbeetle.com/reference/requests/ for details.

  We represent accounts and transfers as maps; see j.t.client for the mapping.

  Our operations are:

  {:type  :invoke
   :f     :create-accounts
   :value [account1 account2 ...]}

  {:type :ok
   :f    :create-accounts
   :value [:ok :linked-event-failed ...]"
  (:require [clojure.core.match :refer [match]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]]
            [jepsen.tigerbeetle.client :as c]
            [potemkin :refer [definterface+]]))

(defn gen-account
  "Generates a single random account with the given ID"
  [id]
  {:id        id
   :user-data (rand-int 10)
   :ledger    (rand-int 10)
   :code      (rand-int 10)
   :flags     #{}})

(defn create-accounts-gen
  "Generates a new create-accounts operation"
  [test context]
  (let [id (:next-account-id context)]
    {:type      :invoke
     :f         :create-accounts
     :value     (mapv gen-account (range id (+ id (rand-int 4))))}))

(definterface+ IGenContext
  (context [this ctx]
           "Constructs a new context map passed on to our wrapped generator"))

; Tracks generator context required to generate operations.
(defrecord GenContext [gen ^long next-account-id]
  IGenContext
  (context [this ctx]
    (assoc ctx :next-account-id next-account-id))

  gen/Generator
  (op [this test ctx]
      (when-let [[op gen'] (gen/op gen test (context this ctx))]
        [op (GenContext. gen' next-account-id)]))

  (update [this test ctx event]
    (let [ctx (context this ctx)]
      (match [(:type event) (:f event)]

             ; When we create new accounts, advance our next account ID
             [:invoke :create-accounts]
             (GenContext. (gen/update gen test ctx event)
                          (inc (reduce max next-account-id
                                       (mapv :id (:value event)))))

             [_ _]
             this))))

(defn gen
  "Generator of all events"
  []
  (map->GenContext
    {:gen (gen/mix
            [create-accounts-gen])
     :next-account-id 0}))

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (Client. (c/open node)))

  (setup! [this test])

  (invoke! [this test op]
    (case (:f op)
      :create-accounts
      (assoc op :type :ok, :value (c/create-accounts! conn (:value op)))))

  (teardown! [this test])

  (close! [this test]
    (c/close! conn)))

(defn workload
  "Takes CLI opts and constructs a partial test map."
  [opts]
  {:client    (Client. nil)
   :generator (gen)
   :checker   checker/unbridled-optimism})
