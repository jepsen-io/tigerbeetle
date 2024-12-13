(ns jepsen.tigerbeetle.workload.generator-test
  (:require [clojure [datafy :refer [datafy]]
                     [pprint :refer [pprint]]
                     [test :refer :all]]
            [jepsen [generator :as gen]
                    [history :as h]
                    [util :as util]]
            [jepsen.generator [test :as gen.test]
                              [context :as ctx]]
            [jepsen.tigerbeetle [lifecycle-map :as lm]
                                [model-test :refer [a ats t tts
                                                    a1 a2 a3 a4
                                                    a1' a2' a3' a4']]]
            [jepsen.tigerbeetle.workload.generator :as g]))

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

(deftest transfer-account-id-test
  ; As soon as we create an account, transfers should start using it.
  (let [test {:concurrency 2}
        ctx  (ctx/context test)
        g (-> (g/wrap-gen (g/gen {:ta-ratio 1, :rw-ratio 1}))
              (gen/update test ctx
                          (h/op {:index 0
                                 :process 0,
                                 :type :invoke,
                                 :f :create-accounts,
                                 :value [a1 a2]})))]
    ; At this point, we should know that a1 and a2 are likely.
    (is (= {1N a1, 2N a2}
           (-> (:state g)
               :accounts
               lm/likely
               datafy
               )))
    ; If we generate a first-stage transfer, it should almost always use these
    ; keys.
    (let [n  1000
          ts (map (fn [_] (g/gen-new-transfer-1 (:state g) 3N))
                  (range n))
          id-freqs (frequencies (mapcat (juxt :debit-account-id :credit-account-id) ts))]
      ; Most should use 1N/2N.
      (pprint id-freqs)
      (is (< 0.4 (/ (id-freqs 1N 0) n)))
      (is (< 0.4 (/ (id-freqs 1N 0) n)))
      ; Most should have different debit/credit IDs
      (is (< 0.9 (/ (count (filter #(not= (:debit-account-id %) (:credit-account-id %)) ts)) n)))
    )))
