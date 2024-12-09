(ns jepsen.tigerbeetle.workload.generator-test
  (:require [clojure [pprint :refer [pprint]]
                     [test :refer :all]]
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
