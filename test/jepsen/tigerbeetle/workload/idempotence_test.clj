(ns jepsen.tigerbeetle.workload.idempotence-test
  (:require [bifurcan-clj [map :as bm]]
            [clojure [pprint :refer [pprint]]
                     [test :refer :all]]
            [jepsen [checker :as checker]
                    [history :as h]]
            [jepsen.tigerbeetle [model-test
                                 :refer [a ats
                                         a1 a2 a3 a4
                                         a1' a2' a3' a4'
                                         t tts]]]
            [jepsen.tigerbeetle.workload.idempotence :refer :all]))

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

(deftest write-compatible-with-read?-test
  (is (write-compatible-with-read?
        {:amount 33,
         :ledger 1,
         :debit-account-id 15173N,
         :credit-account-id 29130N,
         :user-data 3,
         :id 70406N,
         :code 6,
         :timeout 0,
         :flags #{:balancing-credit}}
        {:amount 0N,
         :ledger 1,
         :debit-account-id 15173N,
         :pending-id 0N,
         :credit-account-id 29130N,
         :user-data 3,
         :id 70406N,
         :code 6,
         :timeout 0,
         :timestamp 1738187650464272022,
         :flags #{:balancing-credit}})))

(deftest duplicate-test
  (let [h (h/history
            ; We insert two copies of the same account that succeed in the same op.
            [(o 0 0 :invoke :create-accounts [a1 a2 a1])
             (o 1 0 :ok     :create-accounts [:ok :ok :ok])
             ; Here are two versions of the same transfer ID, in two different ops.
             (o 2 0 :invoke :create-transfers [(t 10N a1 a2 1N)])
             (o 3 0 :ok     :create-transfers [:ok])
             (o 4 0 :invoke :create-transfers [(t 10N a2 a1 2N)])
             (o 5 0 :ok     :create-transfers [:ok])])
        r (checker/check (checker) nil h nil)]
    ;(pprint r)
    (is (not (:valid? r)))
    (is (= {:valid? false
            :accounts {:duplicates [[a1 a1]]
                       :divergences nil}
            :transfers {:duplicates [[(t 10N a1 a2 1N)
                                      (t 10N a2 a1 2N)]]
                        :divergences nil}}
           r))))

(deftest divergence-test
  (let [h (h/history
            ; We insert account a1
            [(o 0 0 :invoke :create-accounts [a1])
             (o 1 0 :ok     :create-accounts [:ok])
             ; However, we read a different copy of a1, and two distinct copies of a2 (out of nowhere)
             (o 2 0 :invoke :lookup-accounts [1N 2N])
             (o 3 0 :ok     :lookup-accounts [(assoc a1 :ledger 5) (assoc a2 :code 5)])
             (o 4 0 :invoke :query-accounts  :whatever)
             (o 5 0 :ok     :query-accounts  [(assoc a2 :code 6)])
             ; We create a transfer from a1 to a2.
             (o 6 0 :invoke :create-transfers [(t 10N a1 a2 1N)])
             (o 7 0 :ok     :create-transfers [:ok])
             ; And read a different version of transfer 10, and two versions of transfer 11.
             (o 8 0 :invoke  :lookup-transfers [10N 11N])
             (o 9 0 :ok      :lookup-transfers [(t 10N a2 a1 1N) (t 11N a1 a2 2N)])
             (o 10 0 :invoke :query-transfers  :whatever)
             (o 11 0 :ok     :query-transfers  [(t 11N a1 a2 3N)])])
        r (checker/check (checker) nil h nil)]
    ;(pprint r)
    (is (not (:valid? r)))
    (is (= {:valid? false
            :accounts {:duplicates  nil
                       :divergences [[a1
                                      (assoc a1 :ledger 5)]
                                     [(assoc a2 :code 6)
                                      (assoc a2 :code 5)]]}
            :transfers {:duplicates nil
                        :divergences [[(t 10N a1 a2 1N)
                                       (t 10N a2 a1 1N)]
                                      [(t 11N a1 a2 2N)
                                       (t 11N a1 a2 3N)]]}}
           r))))
