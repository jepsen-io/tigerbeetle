(ns jepsen.tigerbeetle.model-test
  (:require [bifurcan-clj [core :as b]
                          [map :as bm]]
            [clojure [datafy :refer [datafy]]
                     [pprint :refer [pprint]]
                     [test :refer :all]]
            [jepsen [history :as h]]
            [jepsen.tigerbeetle [model :as m :refer :all]]))

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
(def a5 (a 5))

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
(def a5' (ats a5))

; And some transfers

(defn t
  "Build a simple transfer. From and to can be IDs or account maps."
  ([i from to amount]
   (t i from to amount #{}))
  ([i from to amount flags]
   {:id                (bigint i)
    :debit-account-id  (if (map? from) (:id from) from)
    :credit-account-id (if (map? to) (:id to) to)
    :amount            (bigint amount)
    :user-data         i
    :ledger            1
    :code              i
    :flags             flags}))

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

(defn step*
  "Shorthand for any kind of step."
  [f model invoke-val ok-val]
  (step model
        {:f f :value invoke-val}
        {:f f :value ok-val}))

(defn ca-step
  "Shorthand for a create-accounts step."
  [model invoke-val ok-val]
  (step* :create-accounts model invoke-val ok-val))

(defn ct-step
  "Shorthand for a create-transfers step"
  [model invoke-val ok-val]
  (step* :create-transfers model invoke-val ok-val))

(defn la-step
  "Shorthand for a lookup-accounts step."
  [model invoke-val ok-val]
  (step* :lookup-accounts model invoke-val ok-val))

(defn consistent?
  "Not inconsistent?"
  [model]
  (not (inconsistent? model)))

(deftest chains-test
  (testing "empty"
    (is (= [] (chains []))))
  (testing "normal"
    (is (= [[1N] [2N 3N] [4N]]
           (map (partial map :id)
                (chains
                  [a1
                   (assoc a2 :flags #{:linked})
                   a3
                   a4])))))
  (testing "trailing"
    (is (= [[1N] [2N 3N]]
           (map (partial map :id)
                (chains [a1
                         (assoc a2 :flags #{:linked})
                         (assoc a3 :flags #{:linked})]))))))

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
    (is (= (inconsistent {:type :nonmonotonic-account-timestamp
                          :account-timestamp 102
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

(deftest create-account-linked-event-chain-open-test
  ; If we leave an account chain open, it should explode.
  (is (consistent?
        (ca-step init0
                 [(assoc a1 :flags #{:linked})
                  (assoc a2 :flags #{:linked})]
                 [:linked-event-failed
                  :linked-event-chain-open]))))

(deftest create-account-timestamp-must-be-zero-test
  ; Creating an event with a nonzero timestamp is bad
  (is (consistent?
        (ca-step init0
                 [(assoc a1 :timestamp 5)]
                 [:timestamp-must-be-zero]))))

(deftest create-account-imported-event-timestamp-out-of-range-test
  ; Imported timestamps need to be between 0 and 2^63
  (is (consistent?
        (ca-step ; We need a far-future clock for this one
                 (assoc init0 :timestamp Long/MAX_VALUE)
                 [(assoc a1 :timestamp 0 :flags #{:imported})
                  (assoc a2 :timestamp 1 :flags #{:imported})
                  (assoc a3 :timestamp Long/MAX_VALUE :flags #{:imported})
                  (assoc a4 :timestamp (inc (bigint Long/MAX_VALUE)):flags #{:imported})]
                 [:imported-event-timestamp-out-of-range
                  :ok
                  :ok
                  :imported-event-timestamp-out-of-range]))))

(deftest create-account-imported-event-timestamp-must-not-advance-test
  ; Imported timestamps have to rise
  (let [m (ca-step (assoc init0 :timestamp 5)
                   [(assoc a1 :timestamp 3 :flags #{:imported})
                    ; Should you be allowed to do this? Feels a little weird
                    (assoc a2 :timestamp 5 :flags #{:imported})
                    (assoc a2 :timestamp 6 :flags #{:imported})]
                   [:ok
                    :ok
                    :imported-event-timestamp-must-not-advance])]
    (is (consistent? m))
    (is (= 5 (:timestamp m)))
    (is (= 5 (:account-timestamp m)))))

(deftest basic-transfer-test
  ; We transfer 5 from 1->2, and 15 from 2->1. No special flags.
  (let [t1 (t 1N a1 a2 5N)
        t2 (t 2N a2 a1 15N)
        model (-> init0
                  (ca-step [a1 a2] [:ok :ok])
                  (ct-step [t1 t2] [:ok :ok])
                  ; And a read of each
                  (la-step [1N 2N]
                           [(assoc a1'
                                   :credits-posted 15N
                                   :debits-posted 5N)
                            (assoc a2'
                                   :credits-posted 5N
                                   :debits-posted 15N)]))]
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

(deftest transfer-import-test
  ; Ensure all events are imports or none are
  (let [t1 (assoc (t 1N a1 a2 5N)
                  :timestamp 1N
                  :flags #{:imported})
        t2 (t 2N a2 a1 5N)
        model (ca-step init0 [a1 a2] [:ok :ok])]
    (testing "import first"
      (let [model' (ct-step model [t1 t2] [:ok :imported-event-expected])]
        (is (consistent? model'))
        ; t1 should have executed, but t2 nope
        (is (= #{1N} (set (bm/keys (:transfers model')))))))
    (testing "import second"
      (let [model' (ct-step model [t2 t1] [:ok :imported-event-not-expected])]
        (is (consistent? model'))
        ; t2 should have executed, but not t1
        (is (= #{2N} (set (bm/keys (:transfers model')))))))))

(deftest two-phase-transfer-test
  ; Perform a pending transfer, then post or void it.
  (let [pending (t 1N a1 a2 10N #{:pending})
        ; We only post 5 of the 10 pending
        post    (assoc (t 2N 0N 0N 5N #{:post-pending-transfer})
                       :pending-id 1N)
        void    (assoc (t 3N 0N 0N 0N #{:void-pending-transfer})
                       :pending-id 1N)
        model   (ca-step init0 [a1 a2] [:ok :ok])]
    (testing "pending"
      (let [m (ct-step model [pending] [:ok])]
        ; Both should have pending but not posted balances
        (is (consistent? m))
        (is (= {1N (assoc a1' :debits-pending 10N)
                2N (assoc a2' :credits-pending 10N)}
               (datafy (:accounts m))))))
    (testing "post"
      (let [m (ct-step model [pending post] [:ok :ok])]
        ; Balances should now have moved to posted
        (is (consistent? m))
        (is (= {1N (assoc a1' :debits-posted 5N)
                2N (assoc a2' :credits-posted 5N)}
               (datafy (:accounts m))))))
    (testing "void"
      (let [m (ct-step model [pending void] [:ok :ok])]
        ; Balances should remain at 0
        (is (consistent? m))
        (is (= {1N a1'
                2N a2'}
               (datafy (:accounts m))))
        (is (= {1N (tts pending)
                3N (tts void)}
               (datafy (:transfers m))))))))

(deftest exceeds-credits-test
  (let [a1 (update a1 :flags conj :debits-must-not-exceed-credits)]
    (is (consistent?
          (-> init0
              (ca-step [a1 a2] [:ok :ok])
              ; We can credit a1, and debit it up to posted+pending, but no further
              (ct-step [(t 1N a2 a1 10N)
                        (t 2N a2 a1 5N #{:pending})
                        ; a1 now has 10 posted, 5 pending
                        (t 3N a1 a2 5N)
                        (t 4N a1 a2 5N #{:pending})
                        (t 5N a1 a2 5N)]
                       [:ok :ok :ok :ok :exceeds-credits]))))))

(deftest exceeds-debits-test
  (let [a1 (update a1 :flags conj :credits-must-not-exceed-debits)]
    (is (consistent?
          (-> init0
              (ca-step [a1 a2] [:ok :ok])
              ; We can debit a1, and credit it up to posted+pending, but no further
              (ct-step [(t 1N a1 a2 10N)
                        (t 2N a1 a2 5N #{:pending})
                        ; a1 now has 10 posted, 5 pending
                        (t 3N a2 a1 5N)
                        (t 4N a2 a1 5N #{:pending})
                        (t 5N a2 a1 5N)]
                       [:ok :ok :ok :ok :exceeds-debits]))))))

(deftest balancing-credit-test
  ; Start off debiting a2 by 10, so we have a limit to reach
  (let [debit (t 1N a2 a1 10N)
        m (-> init0
              (ca-step [a1 a2] [:ok :ok])
              (ct-step [debit] [:ok]))
        pending (t 2N a1 a2 6N #{:balancing-credit :pending})
        posted  (t 3N a1 a2 6N #{:balancing-credit})]
    (testing "pending, posted"
      (let [m (ct-step m [pending posted] [:ok :ok])]
        (is (consistent? m))
        (is (= {1N (assoc a1'
                          :credits-posted 10N
                          :debits-posted  4N
                          :debits-pending 6N)
                2N (assoc a2'
                          :credits-posted  4N
                          :credits-pending 6N
                          :debits-posted 10N)}
               (datafy (:accounts m))))
        (is (= {1N (tts debit)
                2N (tts pending)
                ; Transfer 3 only moves 4, not 6
                3N (assoc (tts posted)
                          :amount 4N)}
               (datafy (:transfers m))))))
    (testing "posted, pending"
      (let [; Have to flip IDs so they get ordered timestamps
            posted  (assoc posted :id 2N)
            pending (assoc pending :id 3N)
            m       (ct-step m [posted pending] [:ok :ok])]
        (is (consistent? m))
        (is (= {1N (assoc a1'
                          :credits-posted 10N
                          :debits-posted  6N
                          :debits-pending 4N)
                2N (assoc a2'
                          :credits-posted  6N
                          :credits-pending 4N
                          :debits-posted 10N)}
               (datafy (:accounts m))))
        (is (= {1N (tts debit)
                2N (tts posted)
                ; Transfer 3 only moves 4, not 6
                3N (assoc (tts pending)
                          :amount 4N)}
               (datafy (:transfers m))))))))

(deftest balancing-debit-test
  ; Start off crediting a2 by 10, so we have a limit to reach
  (let [credit (t 1N a1 a2 10N)
        m (-> init0
              (ca-step [a1 a2] [:ok :ok])
              (ct-step [credit] [:ok]))
        pending (t 2N a2 a1 6N #{:balancing-debit :pending})
        posted  (t 3N a2 a1 6N #{:balancing-debit})]
    (testing "pending, posted"
      (let [m (ct-step m [pending posted] [:ok :ok])]
        (is (consistent? m))
        (is (= {1N (assoc a1'
                          :debits-posted   10N
                          :credits-posted  4N
                          :credits-pending 6N)
                2N (assoc a2'
                          :debits-posted  4N
                          :debits-pending 6N
                          :credits-posted 10N)}
               (datafy (:accounts m))))
        (is (= {1N (tts credit)
                2N (tts pending)
                ; Transfer 3 only moves 4, not 6
                3N (assoc (tts posted)
                          :amount 4N)}
               (datafy (:transfers m))))))
    (testing "posted, pending"
      (let [; Have to flip IDs so they get ordered timestamps
            posted  (assoc posted :id 2N)
            pending (assoc pending :id 3N)
            m       (ct-step m [posted pending] [:ok :ok])]
        (is (consistent? m))
        (is (= {1N (assoc a1'
                          :debits-posted 10N
                          :credits-posted  6N
                          :credits-pending 4N)
                2N (assoc a2'
                          :debits-posted  6N
                          :debits-pending 4N
                          :credits-posted 10N)}
               (datafy (:accounts m))))
        (is (= {1N (tts credit)
                2N (tts posted)
                ; Transfer 3 only moves 4, not 6
                3N (assoc (tts pending)
                          :amount 4N)}
               (datafy (:transfers m))))))))

(deftest create-transfer-imported-event-timestamp-must-not-advance-test
  ; Imported timestamps have to rise
  (let [t1 (assoc (t 1N a1 a2 5N #{:imported}) :timestamp 3)
        t2 (assoc (t 1N a1 a2 5N #{:imported}) :timestamp 1000)
        m (-> init0
              (ca-step [a1 a2] [:ok :ok])
              (ct-step [t1 t2]
                       [:ok :imported-event-timestamp-must-not-advance]))]
    (is (consistent? m))
    (is (= 102 (:timestamp m))) ; From the accounts
    (is (= 102 (:account-timestamp m)))
    (is (= 3   (:transfer-timestamp m)))))
