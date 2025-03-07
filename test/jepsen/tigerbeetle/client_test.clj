(ns jepsen.tigerbeetle.client-test
  (:require [clojure [datafy :refer [datafy]]
                     [pprint :refer [pprint]]
                     [test :refer :all]]
            [clj-commons.byte-streams :as bs]
            [jepsen.tigerbeetle.client :as c]))

(deftest account-batch-test
  ; Simple round-trip test to test our coercions
  (let [; On the read path, we're going to have balances and timestamps as well.
        blank {:debits-posted 0
               :debits-pending 0
               :credits-posted 0
               :credits-pending 0
               :timestamp 0}
        a1 {:id 0, :user-data 4, :ledger 9, :code 7, :flags #{}}
        a2 {:id 1, :user-data 5, :ledger 0, :code 5, :flags #{:linked}}
        rt (fn [batch]
             (let [b (c/account-batch batch)]
               ; Batches are mutable buffers; flip so we can read it
               (.beforeFirst b)
               (datafy b)))]
    (testing "empty"
      (is (= [] (rt []))))

    (testing "single"
      (is (= [(merge blank a1)] (rt [a1])))
      (is (= [(merge blank a2)] (rt [a2]))))

    (testing "double"
      (is (= [(merge blank a1) (merge blank a2)]
             (rt [a1 a2]))))

    (testing "triple"
      (is (= [(merge blank a1) (merge blank a2) (merge blank a1)]
              (rt [a1 a2 a1]))))

    (testing "IDs"
      (let [x (inc (bigint Long/MAX_VALUE))]
        (is (= [x]
               (mapv :id (rt [(assoc a1 :id x)]))))))))
