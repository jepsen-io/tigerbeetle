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
  "Account timestamp. Stamps an account with its timestamp from the timestamp map."
  [a]
  (assoc a :timestamp (bm/get tsm (:id a))))

(def a1' (ats a1))
(def a2' (ats a2))
(def a3' (ats a3))
(def a4' (ats a4))

(def init0
  "An initial state with the default timestamp map"
  (init {:account-id->timestamp tsm
         :transfer-id->timestamp tsm}))

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
      (is (= {1N a1'} (datafy (:accounts model))))))

  )
