(ns jepsen.tigerbeetle.workload.transfer
  "A general-purpose TigerBeetle workload. Performs random transfers between
  accounts. Each operation is a TigerBeetle request; see
  https://docs.tigerbeetle.com/reference/requests/ for details.

  We represent accounts and transfers as maps; see j.t.client for the mapping.

  Our operations are:

  ## Create an account

    {:type  :invoke
     :f     :create-accounts
     :value [account1 account2 ...]}

    {:type      :ok
     :f         :create-accounts
     :value     [:ok :linked-event-failed ...]
     :timestamp 123}

  ## Create a transfer

    {:type  :invoke
     :f     :create-transfers
     :value [transfer1 transfer2 ...]}

    {:type  :ok
     :f     :create-transfers
     :value [:ok :linked-event-failed ...]
     :timestamp 123}

  ## Fetch several transfers matching an account filter

    {:type :invoke
     :f    :get-account-transfers
     :value account-filter1}

    {:type  :ok
     :f     :get-account-transfers
     :value [transfer1 transfer2 ...]}

  ## Look up a accounts by IDs

    {:type    :invoke
     :f       :lookup-accounts
     :value   [id1 id2 id3 ...]}

    {:type      :ok
     :f         :lookup-accounts
     :value     [account1, nil, account3, ...]
     :timestamp 123}

  ## Look up transfers by IDs

    {:type    :invoke
     :f       :lookup-transfers
     :value   [id1 id2 ...]}

    {:type    :ok
     :f       :lookup-transfers
     :value   [transfer1 transfer2 ...]}

  ## Query accounts matching a QueryFilter

    {:type :invoke
     :f    :query-accounts
     :value query-filter}

    {:type :ok
     :f    :query-accounts
     :value [account1 account2 ...]}

  ## Query transfers matching a QueryFilter

    {:type  :invoke
     :f     :query-transfers
     :value query-filter}

    {:type :ok
     :f    :query-transfers
     :value [transfer1 transfer2 ...]}

  During the final read phase, we switch exclusively to :f
  :final-lookup-accounts and :f :final-lookup-transfers, whose semantics are
  otherwise identical to lookup-accounts/lookup-transfers."
  (:require [clojure.core.match :refer [match]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [util :refer [timeout]]]
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
    (Client. (c/open test node)))

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
