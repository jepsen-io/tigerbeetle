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

(deftest ^:focus duplicate-test
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
            :accounts
            {:duplicates [[(assoc a1
                                  :index 0
                                  :index' 1
                                  :type :ok
                                  :result :ok)
                           (assoc a1
                                  :index 0
                                  :index' 1
                                  :type :ok
                                  :result :ok)]]}
            :transfers
            {:duplicates [[(assoc (t 10N a1 a2 1N) :index 2 :index' 3 :type :ok :result :ok)
                           (assoc (t 10N a2 a1 2N) :index 4 :index' 5 :type :ok :result :ok)]]}}
           r))))
