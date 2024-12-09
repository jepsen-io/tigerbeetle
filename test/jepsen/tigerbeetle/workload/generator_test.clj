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
