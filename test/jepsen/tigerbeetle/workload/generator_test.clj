(ns jepsen.tigerbeetle.workload.generator-test
  (:refer-clojure :exclude [test])
  (:require [bifurcan-clj [map :as bm]]
            [clojure [datafy :refer [datafy]]
                     [pprint :refer [pprint]]
                     [test :refer :all]]
            [jepsen [generator :as gen]
                    [history :as h]
                    [util :as util]]
            [jepsen.generator [test :as gen.test]
                              [context :as ctx]]
            [jepsen.tigerbeetle [lifecycle-map :as lm]
                                [model-test :as mt :refer [a ats t tts]]]
            [jepsen.tigerbeetle.workload.generator :as g]))

; We have our own ideas about ledgers
(defn reledger
  [account]
  (assoc account :ledger (g/account-id->ledger (:id account))))
(def a1  (reledger mt/a1))
(def a1' (reledger mt/a1'))
(def a2  (reledger mt/a2))
(def a2' (reledger mt/a2'))
(def a3  (reledger mt/a3))
(def a3' (reledger mt/a3'))

(deftest long-weights-test
  (are [x y] (= x (g/long-weights y))
       []           []
       [1]          [1]
       [2]          [2]
       [1 2]        [1 2]
       [1 2]        [1/2 1]
       [15 20 24]   [1/2 2/3 4/5]
       [1 2]        [0.25 0.5]
       [2 3]        [0.3333333333333 0.5]
       ))

(deftest writer-concurrency-test
  ; Never less than nodes
  (is (= 1 (g/rw-threads 2 1)))
  (is (= 2 (g/rw-threads 2 2)))
  ; Roughly half
  (is (= 2 (g/rw-threads 2 3))) ; 1 reader
  (is (= 2 (g/rw-threads 2 4))) ; 2 reader
  (is (= 2 (g/rw-threads 2 5))) ; 3 reader
  (is (= 4 (g/rw-threads 2 6))) ; 2 reader
  (is (= 4 (g/rw-threads 2 7))) ; 3 reader
  (is (= 4 (g/rw-threads 2 8)))) ; 4 reader

; Some basic defaults for a generator
(def test {:concurrency 2})
(def ctx (ctx/context test))
(def g (g/wrap-gen (g/gen {:ta-ratio 1, :rw-ratio 1})))
(def l1 (g/account-id->ledger 1N))
(def l2 (g/account-id->ledger 2N))

(deftest test-hash-ledger-check
  ; Note: a1 and a3 both live in ledger 1, just by their hash. The rest of our
  ; tests depend on this.
  (is (= 1 (g/account-id->ledger 1N)))
  (is (= 3 (g/account-id->ledger 2N)))
  (is (= 1 (g/account-id->ledger 3N))))

(deftest transfer-account-id-test
  ; As soon as we create an account, transfers should start using it.
  (let [g (-> g
              (gen/update test ctx
                          (h/op {:index 0
                                 :process 0,
                                 :type :invoke,
                                 :f :create-accounts,
                                 :value [a1 a3]})))
        as (-> g :state :ledger->accounts (bm/get l1))]
    (testing "on invoke"
      ; At this point, we should know that a1 and a2 are unlikely.
      (is (= {1N a1, 3N a3} (datafy (lm/unlikely as))))
      ; If we generate a first-stage transfer, it should almost always use these
      ; keys.
      (let [n  1000
            ts (map (fn [_] (g/gen-new-transfer-1 (:state g) 4N))
                    (range n))
            id-freqs (frequencies (mapcat (juxt :debit-account-id :credit-account-id) ts))]
        ; Most should use 1N/3N.
        (is (< 0.4 (/ (id-freqs 1N 0) n)))
        (is (< 0.4 (/ (id-freqs 3N 0) n)))
        ; Most should have different debit/credit IDs
        (is (< 0.9 (/ (->> ts
                           (filter #(not= (:debit-account-id %)
                                          (:credit-account-id %)))
                           count)
                      n
                      1.0)))))

    (testing "on ok"
      (let [g (gen/update g test ctx
                          (h/op {:index 1
                                 :process 0
                                 :type :ok
                                 :f :create-accounts
                                 :value [:ok :timestamp-must-be-zero]}))
            s  (:state g)
            as (-> s :ledger->accounts (bm/get l1))]
        ; 1 is now likely, but we know 3 is unlikely
        (is (= {1N a1} (datafy (lm/likely as))))
        (is (= {3N a3} (datafy (lm/unlikely as))))

        ; Generating IDs biases strongly towards 1, rather than 2.
        (let [n 1000
              freqs (fn [f]
                      (update-vals
                        (frequencies (take n (repeatedly f)))
                        (partial * (float (/ n)))))
              ; Globally
              ids  (freqs #(g/rand-account-id s))
              ; In ledger 1
              ids1 (freqs #(g/rand-account-id s 1))
              ; In ledger 2
              ids2 (freqs #(g/rand-account-id s 2))]
          ; In ledger 1 we almost always generate 1, since it's likely and 3
          ; isn't
          (is (<= 0.90 (ids1 1N) 1))
          (is (<= 0    (ids1 3N) 0.1))
          ; In ledger 2, there's nothing, so we get zipfian randoms
          (is (<= 0.6 (ids 1N) 0.75))
          (is (<= 0.05 (ids 2N) 0.15))
          (is (<= 0.004 (ids 3N) 0.1))
          )))

    (testing "on info"
      (let [g (gen/update g test ctx
                          (h/op {:index 1
                                 :process 0
                                 :type :info
                                 :f :create-accounts
                                 :value nil}))
            as (-> g :state :ledger->accounts (bm/get l1))]
        ; Both are now unlikely
        (is (= {} (datafy (lm/likely as ))))
        (is (= {1N a1, 3N a3} (datafy (lm/unlikely as))))))))
