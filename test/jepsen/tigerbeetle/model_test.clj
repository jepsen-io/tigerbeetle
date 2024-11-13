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

(def init1
  "A lot of tests just need two accounts."
  (ca-step init0 [a1 a2] [:ok :ok]))

(defn la-step
  "Shorthand for a lookup-accounts step."
  [model invoke-val ok-val]
  (step* :lookup-accounts model invoke-val ok-val))

(defn lt-step
  "Shorthand for a lookup-transfers step."
  [model invoke-val ok-val]
  (step* :lookup-transfers model invoke-val ok-val))

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

(deftest create-transfer-flags-error-test
  (testing "nonconflicting"
    (are [flags] (nil? (create-transfer-flags-error flags))
         nil
         #{}
         #{:pending}
         #{:balancing-credit :balancing-debit}
         #{:imported :post-pending-transfer}))
  (testing "conflicting"
    (are [flags] (create-transfer-flags-error flags)
         #{:pending :post-pending-transfer}
         #{:post-pending-transfer :closing-credit :balancing-credit}
         #{:balancing-debit :void-pending-transfer})))

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

(deftest create-account-id-must-not-be-zero-test
  (is (consistent?
        (ca-step init0
                 [(assoc a1 :id 0)]
                 [:id-must-not-be-zero]))))

(deftest create-account-id-must-not-be-int-max
  (is (consistent?
        (ca-step init0
                 [(assoc a1 :id (-> 2N .toBigInteger (.pow 128) bigint dec))]
                 [:id-must-not-be-int-max]))))

(deftest create-account-exists-with-different-flags-test
  (is (consistent?
        (-> init0
            (ca-step [a1] [:ok])
            (ca-step [(assoc a1 :flags #{:foo})]
                     [:exists-with-different-flags])))))

(deftest create-account-exists-with-different-user-data
  (is (consistent?
        (-> init0
            (ca-step [a1 (assoc a1 :user-data 5)]
                     [:ok :exists-with-different-user-data-128])))))

(deftest create-account-exists-with-different-ledger-test
  (is (consistent?
        (-> init0
            (ca-step [a1 (assoc a1 :ledger 2)]
                     [:ok :exists-with-different-ledger])))))

(deftest create-account-exists-with-different-code-test
  (is (consistent?
        (-> init0
            (ca-step [a1 (assoc a1 :code 2)]
                     [:ok :exists-with-different-code])))))

(deftest create-account-exists-test
  (is (consistent?
        (-> init0
            (ca-step [a1 a1]
                     [:ok :exists])))))

(deftest create-account-flags-are-mutually-exclusive-test
  (is (consistent?
        (ca-step init0
                 [(assoc a1 :flags #{:debits-must-not-exceed-credits
                                     :credits-must-not-exceed-debits})]
                 [:flags-are-mutually-exclusive]))))

(deftest create-account-balance-must-be-zero-test
  (is (consistent?
        (ca-step init0
                 [(assoc a1 :credits-pending 1)
                  (assoc a2 :credits-posted 1)
                  (assoc a3 :debits-pending 1)
                  (assoc a4 :debits-posted 1)]
                 [:credits-pending-must-be-zero
                  :credits-posted-must-be-zero
                  :debits-pending-must-be-zero
                  :debits-posted-must-be-zero]))))

(deftest create-account-ledger-must-not-be-zero-test
  (is (consistent?
        (ca-step init0
                 [(assoc a1 :ledger 0)]
                 [:ledger-must-not-be-zero]))))

(deftest create-account-code-must-not-be-zero-test
  (is (consistent?
        (ca-step init0
                 [(assoc a1 :code 0)]
                 [:code-must-not-be-zero]))))

(deftest create-account-imported-event-timestamp-must-not-regress-test
  (is (consistent?
        (-> init0
            (ca-step [a2] [:ok])
            (ca-step [(assoc a1
                             :timestamp 101
                             :flags #{:imported})]
                     [:imported-event-timestamp-must-not-regress])))))

;; Transfers

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
  (let [t1 (t 3N a1 a2 5N)
        t2 (t 4N a2 a3 10N)
        ; Only account 2 exists
        model (ca-step init0 [a2] [:ok])]
    (is (= (assoc model
                  :errors (bm/from {3N :debit-account-not-found
                                    4N :credit-account-not-found}))
           (ct-step model [t1 t2]
                    [:debit-account-not-found
                     :credit-account-not-found])))))

(deftest transfer-import-test
  ; Ensure all events are imports or none are
  (let [t1 (assoc (t 3N a1 a2 5N)
                  :timestamp 103N
                  :flags #{:imported})
        t2 (t 4N a2 a1 5N)
        model (ca-step init0 [a1 a2 a5] [:ok :ok :ok])]
    (testing "import first"
      (let [model' (ct-step model [t1 t2] [:ok :imported-event-expected])]
        (is (consistent? model'))
        ; t1 should have executed, but t2 nope
        (is (= #{3N} (set (bm/keys (:transfers model')))))))
    (testing "import second"
      (let [model' (ct-step model [t2 t1] [:ok :imported-event-not-expected])]
        (is (consistent? model'))
        ; t2 should have executed, but not t1
        (is (= #{4N} (set (bm/keys (:transfers model')))))))))

(deftest two-phase-transfer-test
  ; Perform a pending transfer, then post or void it.
  (let [pending (t 1N a1 a2 10N #{:pending})
        ; We only post 5 of the 10 pending
        post    (assoc (t 2N 0N 0N 5N #{:post-pending-transfer})
                       :pending-id 1N
                       :code 1)
        void    (assoc (t 3N 0N 0N 0N #{:void-pending-transfer})
                       :pending-id 1N
                       :code 1)
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
               (datafy (:accounts m))))
        (is (= {1N (assoc (tts pending) :state :posted)
                2N (tts post)}
               (datafy (:transfers m))))))
    (testing "void"
      (let [m (ct-step model [pending void] [:ok :ok])]
        ; Balances should remain at 0
        (is (consistent? m))
        (is (= {1N a1'
                2N a2'}
               (datafy (:accounts m))))
        (is (= {1N (assoc (tts pending) :state :voided)
                3N (tts void)}
               (datafy (:transfers m))))))))

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

(deftest create-transfer-linked-event-failed-test
  ; This a2 fails, so a1 and a3 should also
  (is (consistent?
        (-> init0
            (ca-step [a1 a2] [:ok :ok])
            (ct-step (chain [(t 3N a1 a2 5N)
                             (t 4N a1 a1 5N)
                             (t 5N a2 a1 5N)])
                     [:linked-event-failed
                      :accounts-must-be-different
                      :linked-event-failed])))))

(deftest create-transfer-linked-event-chain-open-test
  (is (consistent?
        (-> init0
            (ca-step [a1 a2] [:ok :ok])
            (ct-step [(t 3N a1 a2 5N)
                      (t 4N a1 a2 5N #{:linked})]
                     [:ok :linked-event-chain-open])))))

(deftest create-transfer-imported-event-expected-test
  (is (consistent?
        (-> init0
            (ca-step [a1 a2 a5] [:ok :ok :ok])
            (ct-step [(assoc (t 3N a1 a2 10N #{:imported}) :timestamp 103)
                      (t 4N a1 a2 10N)]
                     [:ok :imported-event-expected])))))

(deftest create-transfer-imported-event-not-expected-test
  (is (consistent?
        (-> init0
            (ca-step [a1 a2] [:ok :ok])
            (ct-step [(t 3N a1 a2 10N)
                      (t 4N a1 a2 10N #{:imported})]
                     [:ok :imported-event-not-expected])))))

(deftest create-transfer-timestamp-must-be-zero-test
  (is (consistent?
        (-> init0
            (ca-step [a1 a2] [:ok :ok])
            (ct-step [(assoc (t 3N a1 a2 5N) :timestamp 5)]
                     [:timestamp-must-be-zero])))))

(deftest create-transfer-imported-event-timestamp-out-of-range-test
  (is (consistent?
        (-> init0
            (ca-step [a1 a2] [:ok :ok])
            (ct-step [(assoc (t 3N a1 a2 5N #{:imported}) :timestamp 0)]
                     [:imported-event-timestamp-out-of-range])))))

(deftest create-transfer-imported-event-timestamp-must-not-advance-test
  (is (consistent?
        (-> init1
            (ct-step [(assoc (t 3N a1 a2 1N #{:imported})
                             :timestamp 201)]
                     [:imported-event-timestamp-must-not-advance])))))

(deftest create-transfer-id-must-not-be-zero-test
  (is (consistent?
        (-> init1
            (ct-step [(t 0N a1 a2 1N)]
                     [:id-must-not-be-zero])))))

(deftest create-transfer-id-must-not-be-int-max-test
  (let [id (-> 2N
               .toBigInteger
               (.pow 128)
               bigint
               dec)]
    (is (consistent?
          (-> init1
              (ct-step [(t id a1 a2 1N)]
                       [:id-must-not-be-int-max]))))))

(deftest create-transfer-exists-with-different-*-test
  (is (consistent?
        (-> init1
            (ct-step [(t 3N a1 a2 1N)
                      (t 3N a1 a2 1N #{:balancing-credit})
                      (assoc (t 3N a1 a2 1N) :pending-id 4N)
                      (assoc (t 3N a1 a2 1N) :timeout 1)
                      (t 3N a3 a2 1N)
                      (t 3N a1 a3 1N)
                      (t 3N a1 a2 100N)
                      (assoc (t 3N a1 a2 1N) :user-data 5)
                      (assoc (t 3N a1 a2 1N) :ledger 5)
                      (assoc (t 3N a1 a2 1N) :code 5)
                      (t 3N a1 a2 1N)]
                     [:ok
                      :exists-with-different-flags
                      :exists-with-different-pending-id
                      :exists-with-different-timeout
                      :exists-with-different-debit-account-id
                      :exists-with-different-credit-account-id
                      :exists-with-different-amount
                      :exists-with-different-user-data-128
                      :exists-with-different-ledger
                      :exists-with-different-code
                      :exists])))))

(deftest create-transfer-id-already-failed-test
  (let [transfers [(t 4N a3 a2 1N)
                   (t 5N a1 a3 1N)
                   (assoc (t 6N a1 a2 0N #{:post-pending-transfer}) :pending-id 4N)]]
    ; Punting on exceeds-credits/debits, already-closed
    (is (consistent?
          (-> init1
              (ct-step transfers
                       [:debit-account-not-found
                        :credit-account-not-found
                        :pending-transfer-not-found])
              (ct-step transfers
                       [:id-already-failed
                        :id-already-failed
                        :id-already-failed]))))))

(deftest create-transfer-flags-are-mutually-exclusive-test
  (is (consistent?
        (-> init1
            (ct-step [(t 3N a1 a2 1N #{:pending :post-pending-transfer})]
                     [:flags-are-mutually-exclusive])))))

(deftest create-transfer-*-account-id-must-not-be-*-test
  (is (consistent?
        (-> init1
            (ct-step [(t 3N 0N a2 1N)
                      (t 3N a1 0N 1N)
                      (t 3N uint128-max a2 1N)
                      (t 3N a1 uint128-max 1N)]
                     [:debit-account-id-must-not-be-zero
                      :credit-account-id-must-not-be-zero
                      :debit-account-id-must-not-be-int-max
                      :credit-account-id-must-not-be-int-max])))))

(deftest create-transfer-accounts-must-be-different-test
  (is (consistent?
        (-> init1
            (ct-step [(t 3N a2 a2 1N)]
                     [:accounts-must-be-different])))))

(deftest create-transfer-pending-id-must-be-zero-test
  (is (consistent?
        (-> init1
            (ct-step [(assoc (t 3N a1 a2 1N) :pending-id 4N)]
                     [:pending-id-must-be-zero])))))

(deftest create-transfer-pending-id-must-not-be-*-test
  (is (consistent?
        (-> init1
            (ct-step [(assoc (t 3N a1 a2 1N #{:void-pending-transfer}) :pending-id 0N)
                      (assoc (t 3N a1 a2 1N #{:post-pending-transfer}) :pending-id uint128-max)]
                     [:pending-id-must-not-be-zero
                      :pending-id-must-not-be-int-max])))))

(deftest create-transfer-pending-id-must-be-different-test
  (is (consistent?
        (ct-step init1
                 [(assoc (t 3N a1 a2 1N #{:post-pending-transfer}) :pending-id 3N)]
                 [:pending-id-must-be-different]))))

(deftest create-transfer-timeout-reserved-for-pending-transfer-test
  (is (consistent?
        (ct-step init1
                 [(assoc (t 3N a1 a2 1N) :timeout 1)]
                 [:timeout-reserved-for-pending-transfer]))))

(deftest create-transfer-closing-transfer-must-be-pending-test
  (is (consistent?
        (ct-step init1
                 [(t 3N a1 a2 1N #{:closing-credit})
                  (t 4N a1 a2 1N #{:closing-debit})]
                 [:closing-transfer-must-be-pending
                  :closing-transfer-must-be-pending]))))

(deftest create-transfer-code-must-not-be-zero-test
  (is (consistent?
        (ct-step init1
                 [(assoc (t 3N a1 a2 1N) :code 0)]
                 [:code-must-not-be-zero]))))

(deftest create-transfer-*-account-not-found-test
  (is (consistent?
        (ct-step init1
                 [(t 3N a1 a3 1N)
                  (t 4N a3 a2 1N)]
                 [:credit-account-not-found
                  :debit-account-not-found]))))

(deftest create-transfer-same-ledger-test
  (is (consistent?
        (-> init0
            (ca-step [a1
                      a2
                      (assoc a3 :ledger 2)]
                     [:ok :ok :ok])
            (ct-step [(t 4N a1 a3 1N)
                      (assoc (t 5N a1 a2 1N) :ledger 2)]
                     [:accounts-must-have-the-same-ledger
                      :transfer-must-have-the-same-ledger-as-accounts])))))

(deftest create-transfer-pending-transfer-not-found-test
  (is (consistent?
        (ct-step init1
                 [(assoc (t 4N a1 a2 1N #{:post-pending-transfer})
                         :pending-id 3N)]
                 [:pending-transfer-not-found]))))

(deftest create-transfer-pending-transfer-not-pending-test
  (is (consistent?
        (ct-step init1
                 [(t 3N a1 a2 1N)
                  (assoc (t 4N a1 a2 1N #{:post-pending-transfer})
                         :pending-id 3N)]
                 [:ok :pending-transfer-not-pending]))))

(deftest create-transfer-pending-transfer-has-different-*-test
  (is (consistent?
        (-> init0
            (ca-step [a1 a2 a3] [:ok :ok :ok])
            (ct-step [(t 5N a1 a2 1N #{:pending})
                      (assoc (t 6N a3 a2 1N #{:post-pending-transfer}) :pending-id 5N)
                      (assoc (t 7N a1 a3 1N #{:post-pending-transfer}) :pending-id 5N)
                      (assoc (t 8N a1 a2 1N #{:post-pending-transfer}) :pending-id 5N :ledger 2)
                      (assoc (t 9N a1 a2 1N #{:post-pending-transfer}) :pending-id 5N :code 123)]
                     [:ok
                      :pending-transfer-has-different-debit-account-id
                      :pending-transfer-has-different-credit-account-id
                      :pending-transfer-has-different-ledger
                      :pending-transfer-has-different-code])))))

(deftest create-transfer-exceeds-pending-transfer-amount-test
  (is (consistent?
        (ct-step init1
                 [(t 3N a1 a2 5N #{:pending})
                  (assoc (t 4N a1 a2 6N #{:post-pending-transfer})
                         :code 0
                         :pending-id 3N)]
                 [:ok :exceeds-pending-transfer-amount]))))

(deftest create-transfer-pending-transfer-has-different-amount-test
  ; You can post for less
  (testing "post"
    (is (consistent?
          (ct-step init1
                   [(t 3N a1 a2 5N #{:pending})
                    (assoc (t 4N a1 a2 3N #{:post-pending-transfer})
                           :code 0
                           :pending-id 3N)]
                   [:ok :ok]))))
  ; But you can't void for less
  (testing "void"
    (is (consistent?
          (ct-step init1
                   [(t 3N a1 a2 5N #{:pending})
                    (assoc (t 4N a1 a2 3N #{:void-pending-transfer})
                           :code 0
                           :pending-id 3N)]
                   [:ok :pending-transfer-has-different-amount])))))

(deftest create-transfer-pending-transfer-already-posted-test
  (is (consistent?
        (ct-step init1
                 [(t 3N a1 a2 5N #{:pending})
                  (assoc (t 4N a1 a2 0 #{:post-pending-transfer}) :code 0 :pending-id 3N)
                  (assoc (t 5N a1 a2 0 #{:post-pending-transfer}) :code 0 :pending-id 3N)
                  (assoc (t 6N a1 a2 0 #{:void-pending-transfer}) :code 0 :pending-id 3N)]
                 [:ok :ok
                  :pending-transfer-already-posted
                  :pending-transfer-already-posted]))))

(deftest create-transfer-pending-transfer-already-voided-test
  (is (consistent?
        (ct-step init1
                 [(t 3N a1 a2 5N #{:pending})
                  (assoc (t 4N a1 a2 0 #{:void-pending-transfer}) :code 0 :pending-id 3N)
                  (assoc (t 5N a1 a2 0 #{:post-pending-transfer}) :code 0 :pending-id 3N)
                  (assoc (t 6N a1 a2 0 #{:void-pending-transfer}) :code 0 :pending-id 3N)]
                 [:ok :ok
                  :pending-transfer-already-voided
                  :pending-transfer-already-voided]))))

(deftest create-transfer-imported-event-timestamp-must-not-regress-test
  ; TODO: we're punting on the restriction that you can't duplicate the
  ; timestamp of an account. We *can*, it's just Even More Maps to keep track
  ; of.
  (is (consistent?
        (-> init0
            (ca-step [a1 a2 a5] [:ok :ok :ok]) ; This gets us timestamps 101, 102, 105
            (ct-step [(assoc (t 3N a1 a2 5N #{:imported}) :timestamp 104)
                      (assoc (t 4N a1 a2 5N #{:imported}) :timestamp 103)]
                     [:ok :imported-event-timestamp-must-not-regress])))))

(deftest create-transfer-imported-event-timestamp-must-postdate-*-account-test
  (is (consistent?
        (-> init0
            (ca-step [a1 a3] [:ok :ok])
            ; a1 at timestamp 101, a3 at timestamp 103. We try to sneak in
            ; between.
            (ct-step [(assoc (t 5N a3 a1 1N #{:imported}) :timestamp 102)]
                     [:imported-event-timestamp-must-postdate-debit-account])))))

(deftest create-transfer-imported-event-timeout-must-be-zero-test
  (is (consistent?
        (-> init0
            (ca-step [a1 a2 a5] [:ok :ok :ok])
            (ct-step [(assoc (t 3N a1 a2 1N #{:pending :imported})
                             :timestamp 103
                             :timeout 1)]
                     [:imported-event-timeout-must-be-zero])))))

(deftest create-transfer-*-account-already-closed-test
  ; TODO: punting on account closure for now
  )

(deftest create-transfer-overflows-*-test
  (let [half (inc (bigint (/ uint128-max 2)))
        third (inc (bigint (/ uint128-max 3)))
        init  (-> init0
                  (ca-step [a1 a2 a3] [:ok :ok :ok]))]
    (testing "debits-pending"
      (is (consistent?
            (ct-step init
                     [(t 3N a1 a2 half #{:pending})
                      (t 4N a1 a3 half #{:pending})]
                     [:ok
                      :overflows-debits-pending]))))
    (testing "debits-posted"
      (is (consistent?
            (ct-step init
                     [(t 3N a1 a2 half)
                      (t 4N a1 a3 half)]
                     [:ok
                      :overflows-debits-posted]))))
    (testing "credits-pending"
      (is (consistent?
            (ct-step init
                     [(t 3N a1 a3 half #{:pending})
                      (t 4N a2 a3 half #{:pending})]
                     [:ok
                      :overflows-credits-pending]))))
    (testing "credits-posted"
      (is (consistent?
            (ct-step init
                     [(t 3N a1 a3 half)
                      (t 4N a2 a3 half)]
                     [:ok
                      :overflows-credits-posted]))))
    (testing "debits"
      (is (consistent?
            (ct-step init
                     [(t 3N a1 a2 half #{:pending})
                      (t 4N a1 a3 half)]
                     [:ok
                      :overflows-debits]))))
    (testing "credits"
      (is (consistent?
            (ct-step init
                     [(t 3N a1 a3 half #{:pending})
                      (t 4N a2 a3 half)]
                     [:ok
                      :overflows-credits]))))
    ))

(deftest create-transfer-overflows-timeout-test
  (let [timeout (-> Long/MAX_VALUE
                    (- 103) ; timestamp for this txfr
                    ; nanos->secs
                    (/ 1000000000)
                    long
                    inc)]
    (is (consistent?
          (ct-step init1
                   [(assoc (t 3N a1 a2 1N #{:pending})
                           :timeout timeout)]
                   [:overflows-timeout])))))

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

(deftest lookup-transfers-test
  (let [t3 (t 3N a1 a2 10N #{:pending})
        t4 (t 4N a2 a1 5N)]
    (is (consistent?
          (-> init1
              (ct-step [t3 t4]
                       [:ok :ok])
              (lt-step [3N 4N]
                       [(assoc t3
                               :timestamp 203
                               )
                        (assoc t4
                               :timestamp 204
                               )]))))))
