(ns jepsen.tigerbeetle.workload.client
  "A general-purpose TigerBeetle Jepsen client. Wraps j.t.client. We represent
  accounts and transfers as maps; see j.t.client for the mapping. Our
  operations are:

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

  ## Look up accounts by IDs

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
     :value [transfer1 transfer2 ...]}"
  (:require [clojure.core.match :refer [match]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [util :refer [timeout]]]
            [jepsen.tigerbeetle [client :as c]
                                [checker :as tigerbeetle.checker]]
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

  (invoke! [this test {:keys [f value] :as op}]
    (try+
      (merge op
             {:type :ok}
             (case f
               :create-accounts       (c/create-accounts! conn value)
               :lookup-accounts       (c/lookup-accounts conn value)
               :create-transfers      (c/create-transfers! conn value)
               :lookup-transfers      (c/lookup-transfers conn value)
               :query-accounts        (c/query-accounts conn value)
               :query-transfers       (c/query-transfers conn value)
               :get-account-transfers (c/get-account-transfers conn value)
               :get-account-balances  (c/get-account-balances conn value)
               :final-reads-done      {:type :info, :value nil}
               :debug-gen             {:type :info, :value nil}))
      (catch [:type :timeout] e
        (assoc op :type :info, :value nil, :error :timeout))))

  (teardown! [this test])

  (close! [this test]
    (c/close! conn)))

(defn client
  "A basic client capable of handling the standard TigerBeetle API operations."
  []
  (Client. nil))
