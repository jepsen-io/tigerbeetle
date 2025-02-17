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


(defn ab
  "Account with initial balances. Takes an account, adds four zero balance fields."
  [a]
  (assoc a
         :credits-pending 0N
         :credits-posted  0N
         :debits-pending  0N
         :debits-posted   0N))

;; Tests

(deftest create-accounts-basic-test
  (testing "empty"
    (let [h (h/history [])
          r (check h)]
      (is (= {:valid? :unknown
              :final-reads-incomplete? true}
             (dissoc r :stats)))))

  (testing "correct"
    (let [h (h/history
              [(o 0 0 :invoke :create-accounts [a1 a2])
               (o 1 0 :ok     :create-accounts [:ok :ok] 101)
               (o 2 1 :invoke :lookup-accounts [1N 2N])
               (o 3 1 :ok     :lookup-accounts [a1' a2'] 200)
               (o 1000 100 :invoke :final-reads-done nil)
               (o 1001 100 :info   :final-reads-done nil)])
          r (check h)]
      (is (= {:valid? true
              :error-types #{}}
             (dissoc r :stats))))))

(deftest create-accounts-model-test
  (let [h (h/history
            [(o 0 0 :invoke :create-accounts [a1 a2])
             ; Should be :ok, :ok
             (o 1 0 :ok     :create-accounts [:ok :ledger-must-not-be-zero] 101)
             (o 2 1 :invoke :lookup-accounts [1N 2N])
             (o 3 1 :ok     :lookup-accounts [a1' a2'] 200)
             (o 1000 100 :invoke :final-reads-done nil)
             (o 1001 100 :info   :final-reads-done nil)])
        r (check h)]
    (is (= {:valid? false
            :error-types #{:model}
            :model {:op-count     0
                    :event-count  1
                    :op       (h 0)
                    :op'      (h 1)
                    :account  a2
                    :expected :ok
                    :actual   :ledger-must-not-be-zero}}
           (dissoc r :stats)))))

(deftest lookup-accounts-model-test
  (let [h (h/history
            [(o 0 0 :invoke :create-accounts [a1 a2])
             (o 1 0 :ok     :create-accounts [:ok :ok] 101)
             (o 2 1 :invoke :lookup-accounts [1N 2N])
             ; An improper read!
             (o 3 1 :ok     :lookup-accounts [a1' (assoc a2' :ledger 5)] 200)

             ; Dummy write to finish main phase
             (o 100 100 :invoke :create-accounts [])
             (o 101 100 :ok     :create-accounts [] 1000)
             (o 1000 100 :invoke :final-reads-done nil)
             (o 1001 100 :info   :final-reads-done nil)])
        r (check h)]
    (is (= {:valid? false
            :error-types #{:model}
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
           (dissoc r :stats)))))

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
               (o 3 1 :ok     :lookup-transfers [] 100)
               (o 1000 100 :invoke :final-reads-done nil)
               (o 1001 100 :info   :final-reads-done nil)])
          r (check h)]
      (is (= {:valid? false
              :not #{:strong-read-uncommitted}
              :also-not #{:strong-read-committed
                          :strong-snapshot-isolation
                          :strong-serializable}
              :error-types #{:G0-realtime}
              :G0-realtime
              [{:type  :G0-realtime
                :cycle [(h 1) (h 3) (h 1)]
                :steps [{:type :realtime,
                         :a' (h 1)
                         :b  (h 2)}
                        {:type       :ww
                         :timestamp  100
                         :timestamp' 101}]}]}
             (dissoc r :stats))))))

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

(deftest indefinite-first-event-failed-test
  ; If the first event in an info op fails, we should still be able to infer
  ; its success from a later event.
  (let [h (h/history
            ; We perform two writes that crash. Note that we have no explicit
            ; timestamps here.
            [(o 0 0 :invoke :create-accounts [a1 a2])
             (o 1 0 :info   :create-accounts nil)
             ; First transfer fails, but later transfer succeeds
             (o 2 1 :invoke :create-transfers [(t 10N a3 a2 5N)
                                               (t 11N a1 a2 3N)])
             (o 3 1 :info   :create-transfers nil)
             ; Read observes the second transfer
             (o 4 2 :invoke :lookup-accounts [1N 2N])
             (o 5 2 :ok     :lookup-accounts [(assoc a1' :debits-posted 3N)
                                              (assoc a2' :credits-posted 3N)]
                500)
             (o 6 3 :invoke :lookup-transfers [10N 11N])
             (o 7 3 :ok     :lookup-transfers [nil (tts (t 11N a1 a2 3N))]
                                               501)])
        r (check h)]
    (is (:valid? r))))


(deftest explainer-test
  (let [h (h/history
            [; This crashes, but that's fine: we read a1 and a2 later
             (o 0 0 :invoke :create-accounts [a1 a2])
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
             (o 13 4 :ok     :lookup-accounts [(assoc a1' :debits-posted 17N)] 502)
             ; A read of A2, just so we know it exists
             (o 14 5 :invoke :lookup-accounts [2N])
             (o 15 5 :ok     :lookup-accounts [(assoc a2' :credits-posted 17N)] 503)
             ; Dummy write to finish main phase
             (o 100 100 :invoke :create-accounts [])
             (o 101 100 :ok     :create-accounts [] 1000)
             (o 1000 100 :invoke :final-reads-done nil)
             (o 1001 100 :info   :final-reads-done nil)])
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
            :event-count 7
            ; An explanation: we could have gotten here by applying the crashed and
            ; failed transfers.
            :explanation
            {:history      :original
             :op-types     #{:ok :info :fail}
             :solution     [{:id 13N, :op-index 6, :amount 10N, :type :ok, :result :ok, :applied? true, :timestamp 213}
                            {:id 10N, :op-index 2, :amount 5N, :type :info, :result nil, :applied? true, :timestamp nil}
                            {:id 11N, :op-index 2, :amount 3N, :type :info, :result nil, :applied? false, :timestamp nil}
                            {:id 12N, :op-index 4, :amount 2N, :type :fail, :result nil, :applied? true, :timestamp nil}]}}
           (:model r)))))

(deftest explainer-last-valid-read-test
  (let [h (h/history
            ; We create accounts and do a transfer
            [(o 0 0 :invoke :create-accounts [a1 a2])
             (o 1 0 :ok     :create-accounts [:ok :ok] 102)
             (o 2 1 :invoke :create-transfers [(t 10N a1 a2 5N)])
             (o 3 1 :ok     :create-transfers [:ok] 210)
             ; That transfer is visible to a balance read.
             (o 4 2 :invoke :lookup-accounts [1N])
             (o 5 2 :ok     :lookup-accounts [(assoc a1' :debits-posted 5N)] 211)
             ; Now we do a failed transfer
             (o 6 2 :invoke :create-transfers [(t 11N a1 a2 2N)])
             (o 7 2 :fail   :create-transfers nil)
             ; And an account read illegally reflects it!
             (o 8 4 :invoke :lookup-accounts [1N 2N])
             (o 9 4 :ok     :lookup-accounts [(assoc a1' :debits-posted 7N)
                                              (assoc a2' :credits-posted 7N)] 213)
             ; We need a transfer read to get the timestamp for t10
             (o 10 5 :invoke :lookup-transfers [10N])
             (o 11 5 :ok     :lookup-transfers [(tts (t 10N a1 a2 5N))] 500)

             ; Dummy write to finish main phase
             (o 100 100 :invoke :create-accounts [])
             (o 101 100 :ok     :create-accounts [] 1000)
             (o 1000 100 :invoke :final-reads-done nil)
             (o 1001 100 :info   :final-reads-done nil)])
        r (check h)]
    (is (not (:valid? r)))
    ; We have a model error
    (is (= {:expected (assoc a1' :debits-posted 5N)
            :actual   (assoc a1' :debits-posted 7N)
            :diff     {:expected {:debits-posted 5N}
                       :actual   {:debits-posted 7N}}
            :id          1N
            :op          (h 8)
            :op'         (h 9)
            :op-count    3
            :event-count 4
            ; Our last valid read of ID 1 was...
            :last-valid-read {:op (h 4)
                              :op' (h 5)
                              :account (assoc a1' :debits-posted 5N)}
            ; An explanation: we could have gotten here by applying the crashed and
            ; failed transfers.
            :explanation
            {:history      :original
             :op-types     #{:ok :info :fail}
             ; We start from the last valid read
             :solution     [{:id 11N, :op-index 6, :amount 2N, :type :fail, :result nil, :applied? true, :timestamp nil}]}}
           (:model r)))))

(deftest trailing-indefinite-write-test
  ; If there are pending unapplied writes at the end of the main phase, the
  ; final read phase might observe only some of their effects. We need to make
  ; sure we always infer a contiguous order by clipping off any ops that
  ; execute at timestamps after the first final read.
  (let [h (h/history
            [; Two crashed writes, one of which depends on the other
             (o 0 0 :invoke :create-accounts [a1 a2])
             (o 1 0 :info   :create-accounts nil)
             (o 2 1 :invoke :create-transfers [(t 10N a1 a2 5N)])
             (o 3 1 :info   :create-transfers nil)
             ; Now we start final reads. We read create-accounts and miss it;
             ; it's still pending
             (o 4 2 :invoke :lookup-accounts [1N 2N])
             (o 5 2 :ok     :lookup-accounts [nil nil] 100)
             ; Create-accounts applies, as does create-transfers, at timestamp
             ; 101, 102, and 110. Now we read the transfer
             (o 6 2 :invoke :lookup-transfers [10N])
             (o 7 2 :ok     :lookup-transfers
                [(assoc (tts (t 10N a1 a2 5N)) :timestamp 110)] 120)
             (o 1000 100 :invoke :final-reads-done nil)
             (o 1001 100 :info   :final-reads-done nil)])]
    (is (true? (:valid? (check h))))))

; The more I think about this test, the less certain I am that there's any
; reasonable way to fix it. :-(
#_(deftest indefinite-import-timestamp-test
  ; We have to be very careful when inferring timestamps for import writes,
  ; because the import timestamps may not necessarily align with the actual
  ; execution timestamps. In particular, it is possible to create:
  ;
  ; TB ts   Imported ts    Record
  ; 100     10             Account a1
  ; 200     20             Transfer t3
  ;
  ; But because the account returns :ok, and the transfer returns :info, we use
  ; account ts 100 and transfer ts 20, incorrectly inferring that the transfer
  ; executed *first*.
  (let [a1  (assoc a1 :timestamp 10 :flags #{:imported})
        a2  (assoc a2 :timestamp 20 :flags #{:imported})
        a1' (ab a1)
        a2' (ab a2)
        t10 (assoc (t 10N a1 a2 5N) :timestamp 30, :flags #{:imported})
        h (h/history
            [; Create accounts
             (o 0 0 :invoke :create-accounts [a1 a2])
             (o 1 0 :ok     :create-accounts [:ok :ok] 100)
             ; Now create a transfer
             (o 2 0 :invoke :create-transfers [t10])
             (o 3 0 :info   :create-transfers nil)
             ; Which we go on to read
             (o 10 0 :invoke :lookup-accounts  [1N 2N])
             (o 11 0 :ok     :lookup-accounts  [a1' a2'] 1000)
             (o 12 0 :invoke :lookup-transfers [10N])
             (o 13 0 :ok     :lookup-transfers [t10] 1001)
             ; To ensure we check consistency, do an extra write here to push
             ; up the bounds of the model checker
             (o 20 0 :invoke :create-accounts [a3])
             (o 21 0 :ok     :create-accounts [:ok] 2000)
             (o 1000 100 :invoke :final-reads-done nil)
             (o 1001 100 :info   :final-reads-done nil)])
        r (check h)]
    (is (true? (:valid? r)))))

; When an acknowledged write goes missing, let's clearly call that out.
(deftest lost-write-test
  (let [h (h/history
            [(o 0 0 :invoke :create-accounts [a1])
             (o 1 0 :ok     :create-accounts [:ok] 10)
             (o 2 0 :invoke :lookup-accounts [1N])
             (o 3 0 :ok     :lookup-accounts [nil] 11)
             (o 1000 100 :invoke :final-reads-done nil)
             (o 1001 100 :info   :final-reads-done nil)])
        r (check h)]
    (is (= [1N] (:lost-accounts r)))))
