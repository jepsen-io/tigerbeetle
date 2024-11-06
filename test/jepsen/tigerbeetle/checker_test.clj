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
  "Account timestamp. Stamps an account with its timestamp from the timestamp map."
  [a]
  (assoc a :timestamp (bm/get tsm (:id a))))

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
      (is (= {:valid? true
              :error-types #{}}
             r))))

  (testing "correct"
    (let [h (h/history
              [(o 0 0 :invoke :create-accounts [a1 a2])
               (o 1 0 :ok     :create-accounts [:ok :ok] 101)
               (o 2 1 :invoke :lookup-accounts [1N 2N])
               (o 3 1 :ok     :lookup-accounts [a1' a2'] 200)])
          r (check h)]
      (is (= {:valid? true
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
            :model {:op       (h 0)
                    :op'      (h 1)
                    :account  a2
                    :expected :ok
                    :actual   :ledger-must-not-be-zero}}
           r))))
