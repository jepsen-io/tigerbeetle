(ns jepsen.tigerbeetle.checker-test
  (:require [bifurcan-clj [map :as bm]]
            [clojure [pprint :refer [pprint]]
                     [test :refer :all]]
            [jepsen [history :as h]]
            [jepsen.tigerbeetle [checker :refer :all]]))

; Some example data

(defn a
  "Build a simple account."
  [i]
  {:id (bigint i)
   :user-data i
   :ledger i
   :code i
   :flags #{}})

; A few simple events for playing around
(def a1 (a 1))
(def a2 (a 2))
(def a3 (a 3))
(def a4 (a 4))

(def tsm
  "A simple map of ID->timestamp"
  (bm/from (zipmap (map bigint (range 0N 10N))
                   (range 100 110))))

; Events with their timestamps
(defn ats
  "Account timestamp. Stamps an account with its timestamp from the timestamp map, and initial balances."
  [a]
  (assoc a
         :timestamp       (bm/get tsm (:id a))
         :credits-pending 0N
         :credits-posted  0N
         :debits-pending  0N
         :debits-posted   0N))

(def a1' (ats a1))
(def a2' (ats a2))
(def a3' (ats a3))
(def a4' (ats a4))

(defn o
  "Shorthand for creating an op."
  ([time-index process type f value]
   {:index   time-index
    :time    time-index
    :process process
    :type    type
    :f       f
    :value   value})
  ([time-index process type f value timestamp]
   {:index     time-index
    :time      time-index
    :process   process
    :type      type
    :f         f
    :value     value
    :timestamp timestamp}))

;; Tests

(deftest create-accounts-basic-test
  (testing "empty"
    (let [h (h/history [])
          r (check h)]
      (is (= {:valid? :unknown
              :not #{}
              :also-not #{}
              :stats {:create-account-results {}
                      :create-transfer-results {}
                      :get-account-transfers-lengths
                      {0.0 0.0, 0.3 0.0, 0.5 0.0, 0.9 0.0, 0.99 0.0, 1.0 0.0}}
              :error-types #{:empty-transaction-graph}
              :empty-transaction-graph true}
             r))))

  (testing "correct"
    (let [h (h/history
              [(o 0 0 :invoke :create-accounts [a1 a2])
               (o 1 0 :ok     :create-accounts [:ok :ok] 101)
               (o 2 1 :invoke :lookup-accounts [1N 2N])
               (o 3 1 :ok     :lookup-accounts [a1' a2'] 200)])
          r (check h)]
      (is (= {:valid? true
              :stats {:create-account-results {:ok 2}
                      :create-transfer-results {}
                      :get-account-transfers-lengths
                      {0.0 0.0, 0.3 0.0, 0.5 0.0, 0.9 0.0, 0.99 0.0, 1.0 0.0}}
              :error-types #{}}
             r)))))

(deftest create-accounts-model-test
  (let [h (h/history
            [(o 0 0 :invoke :create-accounts [a1 a2])
             ; Should be :ok, :ok
             (o 1 0 :ok     :create-accounts [:ok :ledger-must-not-be-zero] 101)
             (o 2 1 :invoke :lookup-accounts [1N 2N])
             (o 3 1 :ok     :lookup-accounts [a1' a2'] 200)])
        r (check h)]
    (is (= {:valid? false
            :error-types #{:model}
            :stats {:create-account-results {:ok 1
                                             :ledger-must-not-be-zero 1}
                    :create-transfer-results {}
                    :get-account-transfers-lengths
                    {0.0 0.0, 0.3 0.0, 0.5 0.0, 0.9 0.0, 0.99 0.0, 1.0 0.0}}
            :model {:op-count     0
                    :event-count  1
                    :op       (h 0)
                    :op'      (h 1)
                    :account  a2
                    :expected :ok
                    :actual   :ledger-must-not-be-zero}}
           r))))

(deftest lookup-accounts-model-test
  (let [h (h/history
            [(o 0 0 :invoke :create-accounts [a1 a2])
             (o 1 0 :ok     :create-accounts [:ok :ok] 101)
             (o 2 1 :invoke :lookup-accounts [1N 2N])
             ; An improper read!
             (o 3 1 :ok     :lookup-accounts [a1' (assoc a2' :ledger 1)] 200)])
        r (check h)]
    (is (= {:valid? false
            :error-types #{:model}
            :stats {:create-account-results {:ok 2}
                    :create-transfer-results {}
                    :get-account-transfers-lengths
                    {0.0 0.0, 0.3 0.0, 0.5 0.0, 0.9 0.0, 0.99 0.0, 1.0 0.0}}
            :model {:op-count     1
                    :event-count 3
                    :op          (h 2)
                    :op'         (h 3)
                    :id          2N
                    :expected    a2'
                    :actual      (assoc a2' :ledger 1)
                    :diff
                    {:expected   {:ledger 2}
                     :actual     {:ledger 1}}}}
           r))))

(deftest realtime-test
  (testing "consistent"
    ; Timestamps here advance sequentially.
    (let [h (h/history
              [(o 0 0 :invoke :lookup-accounts [])
               (o 1 0 :ok     :lookup-accounts [] 100)
               (o 2 1 :invoke :lookup-transfers [])
               (o 3 1 :ok     :lookup-transfers [] 101)])]
      (is (:valid? (check h)))))
  (testing "inconsistent"
    ; But here, they're nonlinearizable
    (let [h (h/history
              [(o 0 0 :invoke :lookup-accounts [])
               (o 1 0 :ok     :lookup-accounts [] 101)
               (o 2 1 :invoke :lookup-transfers [])
               (o 3 1 :ok     :lookup-transfers [] 100)])
          r (check h)]
      (is (= {:valid? false
              :not #{:strong-read-uncommitted}
              :also-not #{:strong-read-committed
                          :strong-snapshot-isolation
                          :strong-serializable}
              :error-types #{:G0-realtime}
              :stats {:create-account-results {}
                      :create-transfer-results {}
                      :get-account-transfers-lengths
                      {0.0 0.0, 0.3 0.0, 0.5 0.0, 0.9 0.0, 0.99 0.0, 1.0 0.0}}
              :G0-realtime
              [{:type  :G0-realtime
                :cycle [(h 1) (h 3) (h 1)]
                :steps [{:type :realtime,
                         :a' (h 1)
                         :b  (h 2)}
                        {:type       :ww
                         :timestamp  100
                         :timestamp' 101}]}]}
             r)))))
