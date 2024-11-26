(ns jepsen.tigerbeetle.subset-sum-test
  (:require [clojure [pprint :refer [pprint]]
                     [test :refer :all]]
            [clojure.test.check [clojure-test :refer [defspec]]
                                [generators :as gen]
                                [properties :as prop]
                                [results :as results :refer [Result]]
                                [rose-tree :as rose]]
            [com.gfredericks.test.chuck.clojure-test :as chuck :refer [checking for-all]]
            [dom-top.core :refer [loopr]])
  (:import (jepsen.tigerbeetle SubsetSum)))

(def uint128
  "A generator of 128-bit BigIntegers"
  (gen/let [;low  gen/nat
            ;high gen/nat
            low  gen/large-integer
            high gen/large-integer
            ]
    (SubsetSum/bigInteger high low)))

(defn draw
  "Takes a vector of booleans and a vector of xs. Draws just those xs where
  the corresponding choice is true."
  [choices xs]
  (->> (map (fn [choice x] (when choice x)) choices xs)
       (remove nil?)
       vec))

(defn drawn-from?
  "Is the collection `smaller` drawn from the collection `bigger`, in order?"
  [smaller bigger]
  (cond (nil? (seq smaller))
        ; Base case: nothing left to check
        true

        ; Base case: need smaller, out of bigger
        (nil? (seq bigger))
        false

        true
        (let [[s & smaller'] smaller
              [b & bigger'] bigger]
          (if (= s b)
            (recur smaller' bigger')
            (recur smaller bigger')))))

(def test-n 100000)

(def int-max (biginteger (dec (.pow (biginteger 2) 128))))

(deftest ^:focus bigint-roundtrip-test
  (testing "max"
    (is (= int-max
           (SubsetSum/bigInteger -1 -1))))

  (testing "overflow"
    (is (thrown-with-msg? ArithmeticException #"larger than 2\^128 - 1"
                          (SubsetSum/high (biginteger (inc int-max))))))

  (checking "roundtrip" (* test-n 10)
            [low  gen/large-integer
             high gen/large-integer]
            (let [b (SubsetSum/bigInteger high low)]
              (is (<= 0 b))
              (is (<= b int-max))
              (is (= high (SubsetSum/high b)))
              (is (= low  (SubsetSum/low  b))))))

(deftest binary-ops-test
  (checking "binary ops" (* test-n 10)
            [a uint128
             b uint128]
            (let [a-high (SubsetSum/high a)
                  a-low  (SubsetSum/low a)
                  b-high (SubsetSum/high b)
                  b-low  (SubsetSum/low b)]
              (is (= (compare a b)
                     (SubsetSum/compare a-high a-low b-high b-low)))
              (is (= (= a b)
                     (SubsetSum/equals a-high a-low b-high b-low)))
              (is (= (< a b)
                     (SubsetSum/lt a-high a-low b-high b-low))))))

(def xs-gen
  "Generator of a vector of bigints whose sum is below INT_MAX"
  (gen/let [total uint128
            n    gen/small-integer
            bits (gen/vector uint128 n)]
    (loopr [sum 0
            xs  (transient [])]
           [x bits]
           (let [sum' (+ sum x)]
             (if (< total sum')
               ; Can't go bigger
               (persistent!
                 (conj! xs (biginteger (- total sum))))
               (recur sum' (conj! xs (biginteger x)))))
             ; Tack on whatever it takes to get to total
           (persistent!
             (conj! xs (biginteger (- total sum)))))))

(deftest subset-sum-test
  (checking "draw" test-n
            [xs       xs-gen
             choices  (gen/vector gen/boolean (count xs))]
            (let [chosen    (draw choices xs)
                  target    (biginteger (reduce + 0 chosen))
                  ; _ (println "\n\n")
                  ;_ (prn :xs xs :chosen chosen :target target)
                  ; Our solution may not be unique
                  solution  (SubsetSum/solve target xs)]
              ; (prn :solution solution)
              (is (not (nil? solution)))
              ; But it should sum to the target
              (is (= target (biginteger (reduce + solution))))
              ; And be drawn from the original xs
              (is (drawn-from? solution xs)))))
