(ns jepsen.tigerbeetle.workload.idempotence
  "A workload which only checks for idempotent writes. We work a lot like the
  general-purpose transfer workload, but intentionally repeat write operations,
  both with identical values and with random ones. The checker verifies that
  writes for an ID never succeed twice (a 'duplicate'), and that reads always
  observe a single version of a given ID ('divergence')."
  (:require [bifurcan-clj [core :as b]
                          [map :as bm]
                          [list :as bl]
                          [set :as bs]]
            [clojure [datafy :refer [datafy]]
                     [pprint :refer [pprint]]]
            [clojure.core.match :refer [match]]
            [clojure.data.generators :as dg]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [letr loopr]]
            [jepsen [checker :as checker]
                    [generator :as gen]
                    [history :as h]
                    [util :refer [timeout zipf zipf-default-skew
                                  nil-if-empty]]]
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
            [slingshot.slingshot :refer [try+ throw+]]
            [tesser.core :as t]))

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
               (if (and (< (dg/double) p)
                        (< 0 (b/size memory)))
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

            ; Create transfers
            (identical? :create-transfers f)
            (let [accounts (remember transfers max-size dup-p value)
                  op (assoc op :value (poison value dup-p transfers))]
              [op (assoc this :gen gen' :transfers transfers)])

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

;; Checker

(defn merge-cat
  "Merge Bifurcan maps with list concat"
  [m1 m2]
  (bm/merge m1 m2 bl/concat))

(defn merge-union
  "Merge Bifurcan maps with set union"
  [m1 m2]
  (bm/merge m1 m2 bs/union))

(t/deftransform bmap-merge-cat
  "A Tesser transform that merges maps of lists with list concat."
  []
  (assert (nil? downstream))
  {:reducer-identity  (comp b/linear bm/map)
   :reducer           merge-cat
   :post-reducer      identity
   :combiner-identity (comp b/linear bm/map)
   :combiner          merge-cat
   :post-combiner     b/forked})

(t/deftransform bmap-merge-union
  "A Tesser transform that merges maps of sets with union."
  []
  (assert (nil? downstream))
  {:reducer-identity  (comp b/linear bm/map)
   :reducer           merge-union
   :post-reducer      identity
   :combiner-identity (comp b/linear bm/map)
   :combiner          merge-union
   :post-combiner     b/forked})

(defn writes-by-id
  "Takes a history and constructs a Bifurcan map of ID -> [w1, w2, ...], where
  each w is an event map (e.g. an Account or Transfer), augmented with the
  following metadata:

    :index   The history index of the invocation of the write
    :index'  The history index of the completion of the write
    :type    The type of the completion--:ok, :info, or :fail
    :result  The result keyword (e.g. :ok, :credits-must-not-exceed-debits,
              ...), or nil if unknown."
  [history]
  (h/ensure-pair-index history)
  (->> (t/filter h/invoke?)
       (t/map (fn per-op [op]
                (let [op'   (h/completion history op)
                      index (:index op)
                      index' (:index op')
                      type (:type op')]
                  (bireduce (fn per-event [writes event result]
                              (bm/put writes
                                      (:id event)
                                      (bl/list (vary-meta event assoc
                                                          :index   index
                                                          :index'  index'
                                                          :type    type
                                                          :result  result))
                                      bl/concat))
                            (b/linear bm/empty)
                            (:value op)
                            (:value op')))))
       bmap-merge-cat
       (h/tesser history)))

(defn immutable-part
  "Projects a transfer or account map into its immutable part--e.g. without
  :debits-pending etc."
  [event]
  ; Detect type
  (if (contains? event :amount)
    ; Transfers are all immutable
    event
    ; Accounts
    (-> event
        ; Drop four derived fields
        (dissoc :credits-pending :credits-posted :debits-pending :debits-posted)
        ; And the closed flag
        (update :flags disj :closed))))

(defn immutable-reads
  "Takes an OK read. Extracts a Bifurcan map of IDs to Bifurcan sets of
  immutable values."
  [{:keys [value] :as op'}]
  (let [index' (:index op')]
    (loopr [reads (b/linear bm/empty)]
           [event value]
           (let [event (vary-meta (immutable-part event)
                                  assoc :index' index')]
             (recur (bm/put reads
                            (:id event)
                            (bs/add bs/empty event)
                            bs/union))))))

(defn reads-by-id
  "Takes a history and constructs a Bifurcan map of ID -> #{r1, r2 ...} (a
  Bifurcan Set), where each r is an event map (e.g. an Account or Transfer)
  restricted to their purely immutable parts. Also adds a piece of metadata:

    :index'  The history index of the completion of the write"
  [history]
  (h/ensure-pair-index history)
  (->> (t/filter h/ok?)
       (t/map immutable-reads)
       bmap-merge-union
       (h/tesser history)))

(defn ok-write?
  "Is this write (from writes-by-id) acknowledged as OK?"
  [write]
  (= :ok (:result (meta write))))

(defn duplicates
  "Takes writes by ID, looks for duplicates. Returns a sequence of vectors,
  each vector with duplicate events."
  [writes-by-id]
  (keep (fn dups [writes]
          (let [oks (vec (filter ok-write? writes))]
            (when (< 1 (count oks))
              oks)))
        (bm/values writes-by-id)))

(defn divergences
  "Takes writes by ID and reads by ID. Looks for divergences. Returns a
  seqeunce of vectors, each vector with divergent events for a single ID."
  [writes-by-id reads-by-id]
  (keep (fn divs [pair]
          (letr [id     (bm/key pair)
                reads  (set (bm/value pair))

                ; If there are no reads, we're done.
                _ (when (= 0 (count reads))
                    (return nil))

                ; If we have multiple reads, we've got divergence
                _ (when (< 1 (count reads))
                    (return reads))

                ; We have exactly one read. Let's make sure it aligns with
                ; every write.
                expected (dissoc (first reads) :timestamp)
                writes (->> (bm/get writes-by-id id)
                            (filter ok-write?)
                            (map immutable-part)
                            (remove #{expected}))]
            (if (seq writes)
              ; Some conflicting writes
              (into reads writes)
              ; Only one read, no conflicting writes
              nil)))
        reads-by-id))

(defn check-type
  "Takes either :accounts or :transfers and checks for errors."
  [type history]
  (let [f (case type
            :accounts  :create-accounts
            :transfers :create-transfers)
        writes-by-id (->> history
                          (h/filter (h/has-f? f))
                          writes-by-id)
        reads-by-id  (->> history
                          (h/filter (h/has-f?
                                      (case type
                                        :accounts  read-account-fs
                                        :transfers read-transfer-fs)))
                          reads-by-id)
        dups (nil-if-empty (duplicates writes-by-id))
        divs (nil-if-empty (divergences writes-by-id reads-by-id))]
    {:duplicates  dups
     :divergences divs}))

(defrecord Checker []
  checker/Checker
  (check [this test history opts]
    (let [accounts  (check-type :accounts  history)
          transfers (check-type :transfers history)
          ]
      {:valid? (not (or (:duplicates accounts)
                        (:duplicates transfers)
                        (:divergences accounts)
                        (:divergences transfers)))
       :accounts  accounts
       :transfers transfers})))

(defn checker
  "Constructs a new checker for idempotence histories.

  Our main checker is predicated on the idea that we only attempt a write of a
  given ID once. That won't fly here--we try writing IDs multiple times and
  can't tell which one will win. It breaks our inference of which writes
  succeeded: if we read ID 4, we don't know which of the three operations that
  wrote 4 actually executed it.

  Instead, we're just here to verify:

  1. No duplicates. A write of an ID succeeds at most once.
  2. No divergence. All reads of an ID are identical."
  []
  (Checker.))

(defn workload
  "Takes CLI opts and constructs a partial test map."
  [opts]
  {:client          (client)
   :generator       (repeater (gen opts))
   :final-generator (final-gen)
   :wrap-generator  wrap-gen
   :checker         (Checker.)})
