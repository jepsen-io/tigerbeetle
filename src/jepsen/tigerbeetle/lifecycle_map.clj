(ns jepsen.tigerbeetle.lifecycle-map
  "Transfers and accounts go through several phases during the course of a
  test. When we attempt to create something, we call it *possible*. When not
  yet observed, we call it *unseen*. Once observed, we call it *seen*. There is
  a very good chance that many of our possible, unseen records do not actually
  exist--for instance, if the op returns :fail, or the result of that
  individual record is an error like :linked-event-failed, or we try to read it
  and do not see it. We call these records *unlikely*, and other unseen records
  *likely*. When trying to read and update objects, we want to focus our
  efforts mainly on the seen and likely ones, not the unlikely ones."
  (:require [bifurcan-clj [core :as b]
                          [list :as bl]
                          [map :as bm]
                          [set :as bs]]
            [clojure.data.generators :as dg]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [loopr]]
            [potemkin :refer [definterface+]])
  (:import (io.lacuna.bifurcan ISet
                               Lists
                               Sets
                               Maps)
           (java.util OptionalLong)
           (java.util.function Function
                               LongFunction)))

;; Lazy, virtual data structures formed by gluing together n disjoint
;; structures.

(defn lazy-list-concat
  "Lazy list concat for Bifurcan."
  [lists]
  (case (count lists)
    0 bl/empty
    1 (first lists)
    (let [size (reduce + (map b/size lists))
          nth  (reify LongFunction
                 (apply [_ i]
                   (loopr [i i]
                          [list lists]
                          (let [size (b/size list)]
                            (if (< i size)
                              ; It's in this list
                              (b/nth list i)
                              ; Try the next one
                              (recur (- i size)))))))]
          (Lists/from size nth))))

(defn lazy-set-union
  "Lazy set union for Bifurcan. ONLY works if the sets are disjoint--for our
  purposes, they always are."
  [sets]
  (case (count sets)
    0 bs/empty
    1 (first sets)
    (let [; List of elements
          size (reduce + (map b/size sets))
          nth  (reify LongFunction
                 (apply [_ i]
                   (loopr [i i]
                          [set sets]
                          (let [size (b/size set)]
                            (if (< i size)
                              ; It's in this set
                              (b/nth set i)
                              ; Try the next one
                              (recur (- i size)))))))
          elements (Lists/from size nth)
          ; Function for the index of an element
          index-of (reify Function
                     (apply [_ x]
                       (loopr [i 0] ; Index of first element in set
                              [set sets]
                              (if-let [j (bs/index-of set x)]
                                (OptionalLong/of (+ i j))
                                (recur (+ i (b/size set))))
                              (OptionalLong/empty))))]
      (Sets/from elements, index-of))))

(defn lazy-map-union
  "Lazy map union for Bifurcan. ONLY works if the maps are disjoint--for our
  purposes, they always are."
  [maps]
  (let [ks (lazy-set-union (mapv bm/keys maps))
        to-v (reify Function
               (apply [_ k]
                 (loopr []
                        [m maps]
                        (let [v (bm/get m k ::not-found)]
                          (if (identical? ::not-found v)
                            (recur)
                            v))
                        (throw (IllegalStateException.
                                 (str "Expected a value for key " k))))))]
    (Maps/from ks to-v)))

;; Lifecycle maps

(definterface+ ILifecycleMap
  (is-possible [m x]
               [m id x]
                 "Adds a possible record to the map. In the two-arity form,
                 expects x to have a key (:id x).")

  (is-seen [m id]
           "Indicates that we positively observed the given id.")

  (is-unseen [m p id]
             "Indicates that we failed to observe the given ID. If seen, no
             effect. If unseen, With probability p, moves id from likely to
             unlikely.")

  (possible [m]
            "A map of ids to possible records.")

  (seen [m]
        "A map of ids to seen records.")

  (unseen [m]
          "A map of ids to unseen records.")

  (likely [m]
            "A map of ids to likely records.")

  (unlikely [m]
            "A map of ids to unlikely records.")

  (seen? [m id] "Have we seen this ID?")

  (unseen? [m id] "Have we not yet seen this ID?")

  (likely? [m id] "Is this ID likely?")

  (unlikely? [m id] "Is this ID unlikely?"))

; We divide records into disjoint maps of id->record:
; seen      - Definitely seen
; likely    - Unseen and likely
; unlikely  - Unseen and unlikely
(defrecord LifecycleMap [seen likely unlikely]
  ILifecycleMap
  (is-possible [m x]
    (is-possible m (:id x) x))

  (is-possible [m id x]
    (LifecycleMap. seen (bm/put likely id x) unlikely))

  (is-seen [this id]
    (if (bm/contains? seen id)
      ; Already saw this
      this
      (if-let [x (or (bm/get likely id)
                     (bm/get unlikely id))]
        (do ; (info id "now seen")
            (LifecycleMap. (bm/put seen id x)
                           (bm/remove likely id)
                           (bm/remove unlikely id)))
        (throw (IllegalStateException.
                 (str "Can't see id that was never added: " (pr-str id)))))))

  (is-unseen [this p id]
    (if (or (bm/contains? seen id)
            (< p (dg/double)))
      ; No change
      this
      ; Swap to unlikely
      (if-let [x (bm/get likely id)]
        (do ;(info id "now unlikely")
            (LifecycleMap. seen
                           (bm/remove likely id)
                           (bm/put unlikely id x)))
        ; Already unlikely
        this)))

  (possible [this]
    (lazy-map-union [seen likely unlikely]))

  (seen [this]
    seen)

  (unseen [this]
    (lazy-map-union [likely unlikely]))

  (likely [this]
    likely)

  (unlikely [this]
    unlikely)

  (seen?      [this id] (bm/contains? seen id))
  (unseen?    [this id] (or (bm/contains? likely id)
                            (bm/contains? unlikely id)))
  (likely?    [this id] (bm/contains? likely id))
  (unlikely?  [this id] (bm/contains? unlikely id)))

(defn lifecycle-map
  "Constructs an empty lifecycle map."
  []
  (LifecycleMap. bm/empty bm/empty bm/empty))
