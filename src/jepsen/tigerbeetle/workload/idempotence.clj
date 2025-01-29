(ns jepsen.tigerbeetle.workload.idempotence
  "A workload which only checks for idempotent writes. We work a lot like the
  general-purpose transfer workload, but intentionally repeat write operations,
  both with identical values and with random ones. The checker verifies that
  writes for an ID never succeed twice, and that reads always observe a single
  version of a given ID."
  (:require [bifurcan-clj [core :as b]
                          [map :as bm]
                          [list :as bl]
                          [set :as bs]]
            [clojure.core.match :refer [match]]
            [clojure.data.generators :as dg]
            [dom-top.core :refer [loopr]]
            [jepsen [checker :as checker]
                    [generator :as gen]
                    [util :refer [timeout zipf zipf-default-skew]]]
            [jepsen.tigerbeetle [core :refer :all]
                                [checker :as tigerbeetle.checker]
                                [lifecycle-map :as lm]]
            [jepsen.tigerbeetle.workload [generator
                                          :as tb-gen
                                          :refer [final-gen
                                                  gen
                                                  zipf-nth]]
                                         [client :refer [client]]]
            [potemkin :refer [definterface+]]
            [slingshot.slingshot :refer [try+ throw+]]))

(defn poison
  "Takes a nice healthy vector of account or transfer maps and replaces some of
  them (with probability `p`) with values and/or IDs drawn from the given
  Bifurcan list."
  [events p memory]
  (let [n (count events)]
    (loop [i      0
           events (transient events)]
      (if (= i n)
        (persistent! events)
        (recur (inc i)
               (if (< (dg/double) p)
                 ; Replace. We can either swap just the ID, or the entire value.
                 (if-let [replacement (zipf-nth memory)]
                   (assoc! events i
                           (condp < (dg/double)
                             ; Just replace ID+ledger. Why ledger? Because the
                             ; underlying generator keeps track of accounts
                             ; organized by ledger, and if we mess up that
                             ; relationship it's going to get VERY confused.
                             1/2 (assoc (nth events i)
                                        :id (:id replacement)
                                        :ledger (:ledger replacement))
                             ; Replace entire event
                             replacement))
                   ; Nothing to replace with
                   events)
                 ; Not replacing
                 events))))))

(defn remember
  "Takes a Bifurcan list, a maximum size `max-size`, a probaility `p`, and a
  vector of account or transfer maps. Saves some of those maps into the list,
  with probability `p`, so we can repeat them later."
  [memory max-size p events]
  (loopr [memory (b/linear memory)]
         [event events]
         (recur
           (cond ; Initial fill
                 (< (b/size memory) max-size)
                 (bl/add-last memory event)

                 ; Replacement
                 (< (dg/double) p)
                 (bl/set memory (zipf max-size) event)

                 true
                 memory))
         (b/forked memory)))

(defrecord Repeater
  [gen        ; The underlying TigerBeetle generator
   max-size   ; Maximum size of our memory pools
   dup-p      ; Probability we replace an account/transfer with a duplicate
   accounts   ; A Bifurcan list of accounts we can repeat
   transfers] ; A Bifurcan list of transfers we can repeat

  gen/Generator
  (op [this test ctx]
    ; Ask the generator for an operation.
    (let [[{:keys [f value] :as op} gen'] (gen/op gen test ctx)]
      (cond ; Exhausted
            (nil? op)
            nil

            ; Pending
            (identical? :pending op)
            [op this]

            ; Create accounts
            (identical? :create-accounts f)
            (let [accounts (remember accounts max-size dup-p value)
                  op (assoc op :value (poison value dup-p accounts))]
              [op (assoc this :gen gen' :accounts accounts)])

            ; Some other f
            true
            [op (assoc this :gen gen')])))

  (update [this test ctx event]
    (assoc this :gen (gen/update gen test ctx event))))

(defn repeater
  "Constructs a new generator that repeats writes, wrapping a standard transfer
  generator."
  [gen]
  (Repeater. gen 1024 1/2 bl/empty bl/empty))

(defrecord UnsafeLM [gen]
  gen/Generator
  (op [this test ctx]
    (binding [lm/*safe* false]
      (when-let [[op gen'] (gen/op gen test ctx)]
        [op (UnsafeLM. gen')])))

  (update [this test ctx event]
    (binding [lm/*safe* false]
      (UnsafeLM. (gen/update gen test ctx event)))))

(defn wrap-gen
  "Wraps the final and standard generator. We turn off LifecycleMap safety
  checks because we'll be introducing duplicate events on purpose."
  [gen]
  (UnsafeLM. (tb-gen/wrap-gen gen)))

(defn workload
  "Takes CLI opts and constructs a partial test map."
  [opts]
  {:client          (client)
   :generator       (repeater (gen opts))
   :final-generator (final-gen)
   :wrap-generator  wrap-gen
   :checker         (tigerbeetle.checker/checker)})
