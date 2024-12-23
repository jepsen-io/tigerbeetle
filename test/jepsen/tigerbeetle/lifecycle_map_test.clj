(ns jepsen.tigerbeetle.lifecycle-map-test
  (:require [bifurcan-clj [core :as b]
                          [list :as bl]
                          [map :as bm]
                          [set :as bs]]
            [clojure [datafy :refer [datafy]]
                     [pprint :refer [pprint]]
                     [test :refer :all]]
            [clojure.test.check [clojure-test :refer [defspec]]
                                [generators :as gen]
                                [properties :as prop]
                                [results :as results :refer [Result]]
                                [rose-tree :as rose]]
            [com.gfredericks.test.chuck.clojure-test :as chuck :refer [checking for-all]]
            [dom-top.core :refer [loopr]]
            [jepsen.tigerbeetle [lifecycle-map :refer :all]]))

(def test-n 100)

(def list-gen
  "Generator of Lists."
  (gen/let [xs (gen/vector gen/nat)]
    (bl/from xs)))

(def set-gen
  "Generator of Sets."
  (gen/let [xs (gen/set gen/nat)]
    (bs/from xs)))

(def disjoint-sets-gen
  "Generates a vector of disjoint sets."
  (gen/let [sets (gen/vector set-gen)]
    (loopr [used (b/linear bs/empty)
            sets' []]
           [set sets]
           (recur (bs/union used set)
                  (conj sets' (bs/difference set used)))
           sets')))

(defn cut
  "Cuts a vector xs into subvectors from [0,i0), [i0,i1) ... [in-1, size(xs))"
  [xs is]
  (assert (vector? is))
  (assert (vector? xs))
  (->> (conj is (count xs))
       (cons 0)
       (partition 2 1)
       (mapv (fn [[i i']]
               (subvec xs i i')))))

(def disjoint-maps-gen
  "Generates a vector of maps with disjoint keys."
  (gen/let [n    gen/nat
            ks   (gen/vector-distinct gen/nat {:num-elements n
                                               :max-tries 100})
            vs   (gen/vector gen/nat n)
            cuts (if (pos? n)
                   (gen/vector-distinct
                     (gen/large-integer* {:min 0 :max (dec n)})
                     {:max-elements (long (/ n 3))})
                   (gen/return []))]
    (let [cuts (vec (sort cuts))]
      (mapv (comp bm/from zipmap)
            (cut ks cuts)
            (cut vs cuts)))))

(deftest lazy-list-concat-test
  (checking "concat" test-n
            [lists (gen/vector list-gen)]
            (is (= (bl/concat-all lists)
                   (lazy-list-concat lists)))))

(defn set-union
  "Reducer form of bifurcan set union"
  ([] bs/empty)
  ([x] x)
  ([x y] (bs/union x y)))

(deftest lazy-set-union-test
  (checking "union" test-n
            [sets disjoint-sets-gen]
            (is (= (reduce set-union sets)
                   (lazy-set-union sets)))))

(defn map-union
  "Reducer form of bifurcan map union"
  ([] bm/empty)
  ([x] x)
  ([x y] (bm/union x y)))

(deftest lazy-map-union-test
  (checking "union" test-n
            [maps disjoint-maps-gen]
            (is (= (reduce map-union maps)
                   (lazy-map-union maps)))))

(defn d=
  "Equals, with datafy on the second arg."
  [expected actual]
  (= expected (datafy actual)))

(deftest lifecycle-map-test
  (testing "empty"
    (let [m (lifecycle-map)]
      (is (d= {} (possible m)))
      (is (d= {} (seen m)))
      (is (d= {} (unseen m)))
      (is (d= {} (likely m)))
      (is (d= {} (unlikely m)))))

  (testing "possible"
    (let [m (-> (lifecycle-map)
                (add-likely :x 0))]
      (is (d= {:x 0} (possible m)))
      (is (d= {} (seen m)))
      (is (d= {:x 0} (unseen m)))
      (is (d= {:x 0} (likely m)))
      (is (d= {} (unlikely m)))))

  (testing "unseen"
    (let [m (-> (lifecycle-map)
                (add-likely :x 0)
                (add-likely :y 1)
                (is-unseen 0 :x)  ; 0 probability
                (is-unseen 1 :y))] ; 1 probability
      (is (d= {:x 0, :y 1} (possible m)))
      (is (d= {} (seen m)))
      (is (d= {:x 0, :y 1} (unseen m)))
      (is (d= {:x 0} (likely m)))
      (is (d= {:y 1} (unlikely m)))))

  (testing "seen"
    (let [m (-> (lifecycle-map)
                (add-likely :x 0)
                (add-likely :y 1)
                (is-seen :y)
                (is-unseen 0 :x)
                (is-unseen 1 :y)
                (is-seen :x))]
      (is (d= {:x 0, :y 1} (possible m)))
      (is (d= {:x 0, :y 1} (seen m)))
      (is (d= {} (unseen m)))
      (is (d= {} (likely m)))
      (is (d= {} (unlikely m))))))
