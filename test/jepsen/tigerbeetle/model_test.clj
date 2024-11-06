(ns jepsen.tigerbeetle.model-test
  (:require [bifurcan-clj [core :as b]
                          [map :as bm]]
            [clojure [datafy :refer [datafy]]
                     [pprint :refer [pprint]]
                     [test :refer :all]]
            [jepsen [history :as h]]
            [jepsen.tigerbeetle [model :refer :all]]))

(defn a
  "Build a simple account."
  [i]
  {:id (bigint i)
   :user-data i
   :ledger 1
   :code i
   :flags #{}})

; A few simple events for playing around
(def a1 (a 1))
(def a2 (a 2))
(def a3 (a 3))
(def a4 (a 4))

(def atsm
  "A simple map of ID->timestamp"
  (bm/from (zipmap (map bigint (range 0N 10N))
                   (range 100 110))))

; Events with their timestamps
(defn ats
  "Account timestamp. Stamps an account with its timestamp from the timestamp map, as well as initial balances."
  [a]
  (assoc a
         :timestamp       (bm/get atsm (:id a))
         :credits-pending 0N
         :credits-posted  0N
         :debits-pending  0N
         :debits-posted   0N))

(def a1' (ats a1))
(def a2' (ats a2))
(def a3' (ats a3))
(def a4' (ats a4))

; And some transfers

(defn t
  "Build a simple transfer. From and to can be IDs or account maps."
  [i from to amount]
  {:id                (bigint i)
   :debit-account-id  (if (map? from) (:id from) from)
   :credit-account-id (if (map? to) (:id to) to)
   :amount            (bigint amount)
   :user-data         i
   :ledger            1
   :code              i
   :flags             #{}})

(def ttsm
  "A simple map of ID->timestamp for transfers"
  (bm/from (zipmap (map bigint (range 0N 10N))
                   (range 200 210))))

(defn tts
  "Transfer with timestamp. Stamps a transfer with its corresponding timestamp from the timestamp map."
  [t]
  (assoc t :timestamp (bm/get ttsm (:id t))))

(def init0
  "An initial state with the default timestamp map"
  (init {:account-id->timestamp  atsm
         :transfer-id->timestamp ttsm}))

(defn chain
  "Takes a vector of accounts/transfers and returns them with the :linked flag set on all but the last."
  [events]
  (let [n (dec (count events))]
    (loop [i 0
           events (transient events)]
      (if (<= n i)
        (persistent! events)
        (let [event (-> (nth events i)
                        (update :flags conj :linked))]
          (recur (inc i)
                 (assoc! events i event)))))))

(defn ca-step
  "Shorthand for a create-accounts step."
  [model invoke-val ok-val]
  (step model
        {:f :create-accounts, :value invoke-val}
        {:f :create-accounts, :value ok-val}))

(defn ct-step
  "Shorthand for a create-transfers step"
  [model invoke-val ok-val]
  (step model
        {:f :create-transfers, :value invoke-val}
        {:f :create-transfers, :value ok-val}))

(deftest create-accounts-test
  (testing "empty"
    (is (= init0
           (ca-step init0 [] []))))

  (testing "basic"
    (let [model (ca-step init0 [a1] [:ok])
          _ (testing "first"
              (is (not (inconsistent? model)))
              (is (= 101 (:timestamp model)))
              (is (= {1N a1'}
                     (datafy (:accounts model))))
              (is (= {} (datafy (:transfers model)))))
          model (ca-step model
                         [a2 a1 a4]
                         [:ok :exists :ok])
          _ (testing "second"
              (is (not (inconsistent? model)))
              (is (= 104 (:timestamp model)))
              (is (= {1N a1' 2N a2' 4N a4'}
                     (datafy (:accounts model))))
              (is (= {} (datafy (:transfers model)))))]))

  (testing "nonmonotonic"
    (is (= (inconsistent {:type :nonmonotonic-timestamp
                          :timestamp 102
                          :timestamp' 101})
           (ca-step init0 [a2 a1] [:ok :ok]))))

  (testing "inconsistent results"
    (let [a1  (update a1 :flags conj :imported)
          ; We mix imported and non-imported accounts, and the first imported
          ; account has no timestamp. Both should fail.
          op  {:f :create-accounts, :value [a1 a2]}
          ; But we insist both were OK
          op' {:f :create-accounts, :value [:ok :ok]}]
      (is (= (inconsistent {:type     :model
                            :op       op
                            :op'      op'
                            :account  a1
                            :expected :imported-event-timestamp-out-of-range
                            :actual   :ok})
             (step init0 op op')))))

  (testing "linked chains"
    ; This a2 fails, so a1 and a3 should also
    (let [model (ca-step init0
                         (vec (concat (chain [a1 (assoc a2 :ledger 0) a3])
                                      ; But a4 is fine
                                      (chain [a4])))
                         [:linked-event-failed
                          :ledger-must-not-be-zero
                          :linked-event-failed
                          :ok])]
      (is (= {4N a4'} (datafy (:accounts model))))))

  (testing "diff flags"
    (let [model (ca-step init0
                         [a1 (update a1 :flags conj :closed)]
                         [:ok :exists-with-different-flags])]
      (is (= {1N a1'} (datafy (:accounts model))))))

  (testing "diff user-data-128"
    (let [model (ca-step init0
                         [a1 (assoc a1 :user-data 2N)]
                         [:ok :exists-with-different-user-data-128])]
      (is (= {1N a1'} (datafy (:accounts model))))))

  (testing "diff user-ledger"
    (let [model (ca-step init0
                         [a1 (assoc a1 :ledger 2)]
                         [:ok :exists-with-different-ledger])]
      (is (= {1N a1'} (datafy (:accounts model))))))

  (testing "diff code"
    (let [model (ca-step init0
                         [a1 (assoc a1 :code 2)]
                         [:ok :exists-with-different-code])]
      (is (= {1N a1'} (datafy (:accounts model)))))))

(deftest basic-transfer-test
  ; We transfer 5 from 1->2, and 15 from 2->1. No special flags.
  (let [t1 (t 1N a1 a2 5N)
        t2 (t 2N a2 a1 15N)
        model (-> init0
                  (ca-step [a1 a2] [:ok :ok])
                  (ct-step [t1 t2] [:ok :ok]))]
    (is (not (inconsistent? model)))
    (is (= {1N (assoc a1'
                      :credits-posted 15N
                      :debits-posted 5N)
            2N (assoc a2'
                      :credits-posted 5N
                      :debits-posted 15N)}
           (datafy (:accounts model))))
    (is (= {1N (tts t1), 2N (tts t2)}
           (datafy (:transfers model))))))

(deftest create-transfer-missing-acct-test
  ; We try to transfer without a debit or credit account.
  (let [t1 (t 1N a1 a2 5N)
        t2 (t 2N a2 a3 10N)
        ; Only account 2 exists
        model (ca-step init0 [a2] [:ok])]
    (is (= model
           (ct-step model [t1 t2]
                    [:debit-account-not-found
                     :credit-account-not-found])))))

(deftest create-transfer-import-test
  ; Ensure all events are imports or none are
  (let [t1 (update (t 1N a1 a2 5N) :flags conj :imported)
        t2 (t 2N a2 a1 5N)
        model (ca-step init0 [a1 a2] [:ok :ok])]
    (testing "import first"
      (let [model' (ct-step model [t1 t2] [:ok :imported-event-expected])]
        (is (not (inconsistent? model')))
        ; t1 should have executed, but t2 nope
        (is (= #{1N} (set (bm/keys (:transfers model')))))))
    (testing "import second"
      (let [model' (ct-step model [t2 t1] [:ok :imported-event-not-expected])]
        (is (not (inconsistent? model')))
        ; t2 should have executed, but not t1
        (is (= #{2N} (set (bm/keys (:transfers model')))))))))

