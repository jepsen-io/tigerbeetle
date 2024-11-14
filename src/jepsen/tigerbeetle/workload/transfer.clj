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
            [jepsen.tigerbeetle.workload [generator :refer [final-gen
                                                            gen
                                                            wrap-gen]]]
            [potemkin :refer [definterface+]]
            [slingshot.slingshot :refer [try+ throw+]]))

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (Client. (c/open test node)))

  (setup! [this test])

  (invoke! [this test op]
    (try+
      (case (:f op)
        :create-accounts
        (assoc op :type :ok, :value (c/create-accounts! conn (:value op)))

        :lookup-accounts
        (assoc op :type :ok, :value (c/lookup-accounts conn (:value op)))

        :create-transfers
        (assoc op :type :ok, :value (c/create-transfers! conn (:value op))))
      (catch [:type :timeout] e
        (assoc op :type :info, :value nil, :error :timeout))))

  (teardown! [this test])

  (close! [this test]
    (c/close! conn)))

(defn workload
  "Takes CLI opts and constructs a partial test map."
  [opts]
  {:client          (Client. nil)
   :generator       (gen)
   :final-generator (final-gen)
   :wrap-generator  wrap-gen
   :checker         (checker/unbridled-optimism)})
