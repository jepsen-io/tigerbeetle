(ns jepsen.tigerbeetle.checker-test
  (:require [bifurcan-clj [map :as bm]]
            [clojure [pprint :refer [pprint]]
                     [test :refer :all]]
            [jepsen [history :as h]]
            [jepsen.tigerbeetle [checker :refer :all]
                                [model-test
                                 :refer [a ats
                                         a1 a2 a3 a4
                                         a1' a2' a3' a4'
                                         t tts
                                         ]]]))

; Some example data

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
                      {0.0 0.0, 0.3 0.0, 0.5 0.0, 0.9 0.0, 0.99 0.0, 1.0 0.0}
                      :chain-lengths
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
                      {0.0 0.0, 0.3 0.0, 0.5 0.0, 0.9 0.0, 0.99 0.0, 1.0 0.0}
                      :chain-lengths
                      {0.0 1.0, 0.3 1.0, 0.5 1.0, 0.9 1.0, 0.99 1.0, 1.0 1.0}}
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
                    {0.0 0.0, 0.3 0.0, 0.5 0.0, 0.9 0.0, 0.99 0.0, 1.0 0.0}
                    :chain-lengths
                    {0.0 1.0, 0.3 1.0, 0.5 1.0, 0.9 1.0, 0.99 1.0, 1.0 1.0}}
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
             (o 3 1 :ok     :lookup-accounts [a1' (assoc a2' :ledger 5)] 200)])
        r (check h)]
    (is (= {:valid? false
            :error-types #{:model}
            :stats {:create-account-results {:ok 2}
                    :create-transfer-results {}
                    :get-account-transfers-lengths
                    {0.0 0.0, 0.3 0.0, 0.5 0.0, 0.9 0.0, 0.99 0.0, 1.0 0.0}
                    :chain-lengths
                    {0.0 1.0, 0.3 1.0, 0.5 1.0, 0.9 1.0, 0.99 1.0, 1.0 1.0}}
            :model {:op-count     1
                    :event-count 3
                    :op          (h 2)
                    :op'         (h 3)
                    :id          2N
                    :expected    a2'
                    :actual      (assoc a2' :ledger 5)
                    :diff
                    {:expected   {:ledger 1}
                     :actual     {:ledger 5}}}}
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
                      {0.0 0.0, 0.3 0.0, 0.5 0.0, 0.9 0.0, 0.99 0.0, 1.0 0.0}
                      :chain-lengths
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

(deftest indefinite-test
  (let [h (h/history
            ; We perform two writes that crash. Note that we have no explicit
            ; timestamps here.
            [(o 0 0 :invoke :create-accounts [a1 a2])
             (o 1 0 :info   :create-accounts nil)
             (o 2 1 :invoke :create-transfers [(t 10N a1 a2 5N)])
             (o 3 1 :info   :create-transfers nil)
             ; Reads observe the accounts, but not the transfer.
             (o 4 2 :invoke :lookup-accounts [1N 2N])
             (o 5 2 :ok     :lookup-accounts [a1' a2'] 500)
             (o 6 3 :invoke :lookup-transfers [10N])
             (o 7 3 :ok     :lookup-transfers [nil] 501)])
        r (check h)]
    (is (:valid? r))))

(deftest explainer-test
  (let [h (h/history
            ; We perform two writes that crash. Note that we have no explicit
            ; timestamps here.
            [(o 0 0 :invoke :create-accounts [a1 a2])
             (o 1 0 :info   :create-accounts nil)
             ; A crashed op with two transfers, only one of which is applied
             (o 2 1 :invoke :create-transfers [(t 10N a1 a2 5N)
                                               (t 11N a1 a2 3N)])
             (o 3 1 :info   :create-transfers nil)
             ; A failed transfer
             (o 4 2 :invoke :create-transfers [(t 12N a1 a2 2N)])
             (o 5 2 :fail   :create-transfers nil)
             ; An OK transfer
             (o 6 3 :invoke :create-transfers [(t 13N a1 a2 10N)])
             (o 7 3 :ok     :create-transfers [:ok] 213)
             ; Transfer reads see just t13
             (o 10 3 :invoke :lookup-transfers [10N 11N 12N 13N])
             (o 11 3 :ok     :lookup-transfers [nil nil nil (tts (t 13N a1 a2 10N))] 501)
             ; But an *account* read sees 10, 12, and 13
             (o 12 4 :invoke :lookup-accounts [1N])
             (o 13 4 :ok     :lookup-accounts [(assoc a1' :debits-posted 17N)] 502)])
        r (check h)]
    (is (not (:valid? r)))
    ; We have a model error
    (is (= {:expected (assoc a1' :debits-posted 10N)
            :actual (assoc a1' :debits-posted 17N)
            :diff {:expected {:debits-posted 10N}
                   :actual   {:debits-posted 17N}}
            :id          1N
            :op          (h 10)
            :op'         (h 11)
            :op-count    3
            :event-count 7}
           (:model r)))
    ; And also an explanation: we could have gotten here by applying the
    ; crashed and failed transfers.
    (is (= {:history      :original
            :considering  #{:ok :info :fail}
            :solution     [{:id 13N, :op-index 6, :amount 10N, :type :ok, :applied? true, :timestamp 213}
                           {:id 10N, :op-index 2, :amount 5N, :type :info, :applied? true, :timestamp nil}
                           {:id 11N, :op-index 2, :amount 3N, :type :info, :applied? false, :timestamp nil}
                           {:id 12N, :op-index 4, :amount 2N, :type :fail, :applied? true, :timestamp nil}]}
           (:explanation r)))))
