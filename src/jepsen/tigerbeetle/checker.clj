(ns jepsen.tigerbeetle.checker
  "Checker for TigerBeetle's data model. We build a single-threaded state
  machine model of TigerBeetle which accepts the abstract operations from
  Jepsen histories.

  As in Elle, this analysis hinges on recoverability: we must be able to
  uniquely determine the write that generated a given version of an object.
  This implies we cannot test idempotence in this checker; this will have to be
  a separate analysis.

  Our histories are divided into two phases: the *main phase*, and the *final
  read phase*. In the main phase, we perform arbitrary reads and writes. In the
  final read phase, we perform reads of all indefinite (e.g.) info operations
  until all their effects have been definitively resolved. Since TigerBeetle is
  Strong Serializable, we can rely on the real-time ordering property to ensure
  that for every operation, either a.) we know that the operation definitely
  succeeded or failed, or b.), it will be applied *after* the main phase.
  Having a complete view of the history of the main phase allows us to check
  the main phase's consistency in ~n log n time.

  To check a history, we proceed as follows:

  1. Event resolution. Based on the final reads, resolve all main phase :info
     events to :ok or :fail.

  2. Operation resolution. Using the resolved events, resolve each main phase
     operation to :ok or :fail. Check atomicity: either all events for an
     operation are :ok, or all are :fail.

  3. Event timestamp order. Order all ok events by timestamp.

  4. Operation timestamp order. Using the event timestamp order, construct a
     total order over all operations. Check timestamp atomicity: operation
     timestamps should never interleave. We can now assume operations applied
     atomically (in timestamp space).

  5. Model consistency. Play forward all OK main-phase operations in timestamp
     order, and compare their effects to a model of the system. This validates
     the Serializability & semantic correctness of the history.

  6. Real-time consistency. TigerBeetle is strong serializable. To validate
     real-time ordering, we build a degenerate model of the history in which the
     only state is the timestamp, and every operation has ww, wr, or rw
     dependencies on it. We unify this with the real-time order across all
     processes, and pass it to Elle, which identifies cycles.

  Why not solve with Elle directly? We could (and might opt to in the future),
  but there are some tricky challenges. For instance, account balances are
  'written' by creating new transfers. A read of an account's 'balance = 25'
  has a write-read dependency on the 'last' transfer that produced that state,
  and a read-write dependencies on the 'next' transfer to produce that state.
  To solve this directly, we could do the same trick we did for RadixDLT and
  choose transfer amounts of 1, 2, 4, 8, etc; this limits us to 127 writes per
  account, but allows us to uniquely extract the set of transfers reflected in
  any balance. Having timestamps in the read makes this significantly easier.

  Note that this approach hinges on real-time reads in the final read phase. A
  stale read could manifest as what looks like a more serious anomaly--say, a
  torn transaction violating model consistency. We may want to add more
  provable Elle dependencies to get better locality."
  (:require [clojure [pprint :refer [pprint]]
                     [set :as set]]
            [clojure.core [match :refer [match]]]
            [clojure.tools.logging :refer [info warn]]
            [bifurcan-clj [core :as b]
                          [int-map :as bim]
                          [graph :as bg]
                          [map :as bm]
                          [set :as bs]]
            [dom-top.core :refer [loopr]]
            [elle [core :as elle]
                  [graph]
                  [rels :refer [ww wr rw]]
                  [txn]
                  [util :refer [nanos->secs]]]
            [jepsen [checker :as checker]
                    [history :as h]]
            [jepsen.tigerbeetle [core :refer [write-fs
                                              read-fs
                                              read-account-fs
                                              read-transfer-fs]]
                                [explainer :as e]
                                [model :as model]]
            [tesser [core :as t]
                    [math :as tm]
                    [quantiles :as tq]])
  (:import (jepsen.history Op)))


(t/deftransform bset
  "A Tesser transform that builds a Bifurcan set of all elements."
  []
  (assert (nil? downstream))
  {:reducer-identity  (comp b/linear bs/set)
   :reducer           bs/add
   :post-reducer      identity
   :combiner-identity (comp b/linear bs/set)
   :combiner          bs/union
   :post-combiner     b/forked})

(t/deftransform bset-union
  "A Tesser transform that builds a Bifurcan set which is the union of all
  Bifurcan set inputs."
  []
  (assert (nil? downstream))
  {:reducer-identity  (comp b/linear bs/set)
   :reducer           bs/union
   :post-reducer      identity
   :combiner-identity (comp b/linear bs/set)
   :combiner          bs/union
   :post-combiner     b/forked})

(t/deftransform bmap-merge
  "A Tesser transform that builds a Bifurcan map which is merged from all input
  maps."
  []
  (assert (nil? downstream))
  {:reducer-identity (comp b/linear bm/map)
   ; TODO: confirm all values are identical
   :reducer          bm/merge
   :post-reducer     identity
   :combiner-identity (comp b/linear bm/map)
   :combiner         bm/merge
   :post-combiner    b/forked})

(t/deftransform bsorted-map
  "Turns [k v] pairs into a Bifurcan sorted map."
  []
  (assert (nil? downstream))
  (let [r (fn reducer
            ([] (b/linear (bm/sorted-map)))
            ([m] m)
            ([m [k v]]
             (bm/put m k v)))]
    {:reducer-identity  r
     :reducer           r
     :post-reducer      r
     :combiner-identity r
     :combiner          bm/merge
     :post-combiner     b/forked}))

(defn op->id->timestamp-map
  "Takes a single Operation whose :value is a collection of things (e.g.
  accounts) with :id and :timestamp fields. Returns a map of id->timestamp."
  [op]
  (loopr [m (b/linear bm/empty)]
         [event (:value op)]
         (recur (bm/put m (:id event) (:timestamp event)))
         (b/forked m)))

(defn account-id->timestamp
  "Takes a history. Computes a map of account IDs to timestamps for all
  observed accounts."
  [history]
  (->> (t/filter h/ok?)
       (t/filter (h/has-f? read-account-fs))
       (t/map op->id->timestamp-map)
       bmap-merge
       (h/tesser history)))

(defn transfer-id->timestamp
  "Takes a history. Computes a map of transfer IDs to timestamps for all
  observed transfers."
  [history]
  (->> (t/filter h/ok?)
       (t/filter (h/has-f? read-transfer-fs))
       (t/map op->id->timestamp-map)
       bmap-merge
       (h/tesser history)))

(defn op->ids
  "Takes an Operation and returns a Bifurcan Set of all IDs in it."
  [^Op op]
  (loopr [ids (b/linear (bs/set))]
         [event (.value op)]
         (recur (bs/add ids (:id event)))
         (b/forked ids)))

(defn seen-transfers
  "Runs through the entire history and builds a Bifurcan set of all read
  transfer IDs."
  [history]
  (->> (t/filter h/ok?)
       (t/filter (h/has-f? read-transfer-fs))
       (t/map op->ids)
       bset-union
       (h/tesser history)))

(defn seen-accounts
  "Runs through the entire history and builds a Bifurcan set of all read
  account IDs."
  [history]
  (->> (t/filter h/ok?)
       (t/filter (h/has-f? read-account-fs))
       (t/map op->ids)
       bset-union
       (h/tesser history)))

(defn create-ok-ids
  "Takes an invoke and a corresponding :ok op for either :create-transfers or
  :create-accounts. Returns a Bifurcan set of all IDs in the invoke which were
  :ok in the completion."
  [invoke ok]
  (let [events  (:value invoke)
        results (:value ok)
        n       (count events)]
    (loop [i    0
           ids (b/linear (bs/set))]
      (if (= i n)
        (b/forked ids)
        (recur (inc i)
               (if (identical? :ok (nth results i))
                 (bs/add ids (:id (nth events i)))
                 ids))))))

(defn created-accounts
  "Runs through the history and builds a Bifurcan set of all account IDs we
  know were acknowledged as created."
  [history]
  (->> (t/filter h/ok?)
       (t/filter (h/has-f? :create-accounts))
       (t/map (fn [op]
                (create-ok-ids (h/invocation history op) op)))
       bset-union
       (h/tesser history)))

(defn created-transfers
  "Runs through the history and builds a Bifurcan set of all transfer IDs we
  know were acknowledged as created."
  [history]
  (->> (t/filter h/ok?)
       (t/filter (h/has-f? :create-transfers))
       (t/map (fn [op]
                (create-ok-ids (h/invocation history op) op)))
       bset-union
       (h/tesser history)))

(defn resolve-ops-infer-timestamp
  "Takes a history, a map of account IDs to timestamps, one for transfer IDs to
  timestamps, and an :info operation. Returns nil if no written event was ever
  read. If one was read, returns the timestamp of the first observed event.
  Empty writes and all reads return nil."
  [history account-id->timestamp transfer-id->timestamp op]
  (when-let [; What map are we going to look in?
             id->ts (case (:f op)
                      :create-accounts  account-id->timestamp
                      :create-transfers transfer-id->timestamp
                      nil)]
    (->> (h/invocation history op)
         :value
         (keep (fn get-ts [event]
                 (bm/get id->ts (:id event))))
         first)))

(defn resolve-ops
  "Takes a map of:

    :history                  The history
    :account-id->timestamp    A map of observed account IDs to timestamps
    :transfer-id->timestamp   A map of observed transfer IDs to timestamps

  Returns a new history, like the given history, but where any info operations
  are resolved to either :ok or :fail based on their events being present in
  the id->timestamp maps. Associates actually-ok ops with a :timestamp and
  :value :unknown."
  [{:keys [history account-id->timestamp transfer-id->timestamp]}]
  (h/map (fn resolve [op]
           (if (and (h/client-op? op)
                    (h/info? op))
             (if-let [ts (resolve-ops-infer-timestamp
                           history
                           account-id->timestamp
                           transfer-id->timestamp
                           op)]
               (assoc op :type :ok, :value :unknown, :timestamp ts)
               ; No observed timestamp; must have failed
               (assoc op :type :fail))
             ; :ok or :info op
             op))
         history))

(defn timestamp-sorted
  "Takes a history. Returns a sequence of all OK operations in the history
  sorted by :timestamp ascending."
  [history]
  (assert (every? :timestamp (h/oks history))
          (str "History missing an OK :timestamp "
               (pr-str (first (remove :timestamp (h/oks history))))))
  (->> history
       h/oks
       (sort-by :timestamp)))

(defn model-check-last-valid-read
  "When we find an error, it's nice to backtrack to the last known-valid
  operation. Takes a map of:

    {:history                A history
     :oks                    Timestamp-sorted OK operations to apply
     :account-id->timestamp  The timestamp map for accounts
     :transfer-id->timestamp The timestamp map for transfers
     :type                   Either :account or :transfer
     :id                     The ID we want to find.}

  Runs the model forward on OK operations until it crashes, and returns a map
  from the most recent valid read of that ID:

    {:op               The invocation of the read
     :op'              The completion of the read
     :account          The state of the account at that time
     :model            The entire model state just before the read.
     :applied-indices  A Bifurcan set of the indices of invoke ops we applied,
                       up to but not including this op.}
  "
  [{:keys [history oks type id] :as opts}]
  (assert id)
  (let [; A predicate of an op that tells us if it was a read of our desired
        ; type
        read-of-type?
        (h/has-f? (case type
                    :account  read-account-fs
                    :transfer read-transfer-fs))
        ; Takes a completion op and returns a read in it of a specific
        ; account/transfer matching our ID, or nil if none is there, or it's of
        ; the wrong type.
        ; TODO: maybe we WANT nil reads? Like before an account exists?
        read-of-id (fn read-of-id [op']
                     (when (read-of-type? op')
                       (first (filter (fn find-id [event]
                                        (= id (:id event)))
                                      (:value op')))))]
    (loopr [model           (model/init (select-keys opts
                                                     [:account-id->timestamp
                                                      :transfer-id->timestamp]))
            ; Indices applied since last good state
            applied-indices (b/linear bs/empty)
            ; Last good state
            good            {:model           model
                             :applied-indices (b/linear bs/empty)}]
           [op' oks]
           (let [op               (h/invocation history op')
                 model'           (model/step model op op')
                 applied-indices' (bs/add applied-indices (:index op))]
             (if (model/inconsistent? model')
               ; Abort!
               good
               ; This was valid. Is it of interest?
               (if-let [read (read-of-id op')]
                 (recur model'
                        (b/linear bs/empty)
                        {:op op
                         :op' op'
                         type read
                         :model model
                         :applied-indices (bs/union
                                            (:applied-indices good)
                                            applied-indices')})
                 (recur model' applied-indices' good))))
           ; Never invalid
           (assoc good
                  :applied-indices applied-indices))))

(defn explain
  "Explains how a model checker error might have occurred.

  Takes a map of options for model-check, an error map from model-check, and a
  last valid read map. For some kinds of error map, tries to produce an
  :explanation for why. Returns an explanation structure, or nil if no
  explanation is required or possible."
  [{:keys [history resolved-history transfer-id->timestamp model-check]
    :as opts}
   {:keys [op' id diff actual] :as err}
   last-valid-read]
  (when err
    ; We have a model-checker error
    (when (read-account-fs (:f op'))
      ; Which got stuck on a read of an account
      (when (map? (:actual diff))
        ; And the diff is on a single account, rather than a vector
        (when-let [field (first (set/intersection
                                  (set (keys (:actual diff)))
                                  #{:credits-posted
                                    :debits-posted}))]
          ; And it's an explicable field! Let's ask the explainer using either
          ; the resolved or original history
          (let [value (get actual field)
                opts  (assoc opts
                             :op'             op'
                             :account-id      id
                             :field           field
                             :value           value
                             :last-valid-read last-valid-read)]
            (or (e/explain (assoc opts
                                  :history resolved-history
                                  :op-types #{:ok :info}
                                  :additional-data {:history :resolved}))
                (e/explain (assoc opts
                                  :op-types #{:ok :info}
                                  :additional-data {:history :original}))
                (e/explain (assoc opts
                                  :op-types #{:ok :info :fail}
                                  :additional-data {:history :original})))))))))

(defn model-check
  "Checks a sequence of OK operations from a history using our model checker.
  Returns an error map, or nil if no errors were found."
  [{:keys [history
           resolved-history
           account-id->timestamp
           transfer-id->timestamp]
    :as opts}]
  (let [oks (timestamp-sorted resolved-history)
        n   (count oks)]
    (loopr [model (model/init
                    (select-keys opts [:account-id->timestamp
                                       :transfer-id->timestamp]))]
           [op' oks]
           (let [op     (h/invocation history op')
                 model  (model/step model op op')]
             (if (model/inconsistent? model)
               (let [err (model/error-map model)
                     ; If we had a bad read of a specific ID, try and trace it
                     ; to the last valid read.
                     id  (:id err)
                     type (condp contains? (:f op)
                            read-account-fs :account
                            read-transfer-fs :transfer
                            nil)
                     last-valid-read
                     (when (and id type)
                       (model-check-last-valid-read
                         (assoc opts :oks oks, :type type, :id id)))

                     ; Try to explain the error.
                     explanation (explain opts err last-valid-read)
                     err (cond-> err
                           explanation
                           (assoc :explanation explanation)

                           (:op last-valid-read)
                           (assoc :last-valid-read
                                  ; These are helpful for processing but also
                                  ; enormous
                                  (dissoc last-valid-read
                                          :applied-indices
                                          :model)))]
                 {(:type err) (dissoc err :type)})
               (recur model)))
           ; OK
           nil)))

(defn stats
  "Folds over the history, gathering basic statistics. We return:

    {:create-transfer-results     A map of result frequencies for
                                  create-transfers
     :create-account-results      A map of result frequencies for
                                  create-accounts
     :get-account-transfer-lengths A quantile distribution of the lengths of
                                   get-account-transfers. Helpful for tuning
                                   generators so we actually do queries that
                                   return something.
     :chain-lengths                A quantile distribution of the lengths
                                   of chains across create-account and
                                   -transfer ops.
     }"
  [history]
  (let [quantiles (fn [d]
                    (sorted-map
                      0.0  (tq/quantile d 0)
                      0.3  (tq/quantile d 0.3)
                      0.5  (tq/quantile d 0.5)
                      0.9  (tq/quantile d 0.9)
                      0.99 (tq/quantile d 0.99)
                      1.0  (tq/quantile d 1)))

        create-account-results
        (->> (t/filter (h/has-f? :create-accounts))
             (t/mapcat :value)
             t/frequencies)

        create-transfer-results
        (->> (t/filter (h/has-f? :create-transfers))
             (t/mapcat :value)
             t/frequencies)

        query-accounts-lengths
        (->> (t/filter (h/has-f? :query-accounts))
             (t/map (comp count :value))
             (tm/digest tq/hdr-histogram)
             (t/post-combine quantiles))

        query-transfers-lengths
        (->> (t/filter (h/has-f? :query-transfers))
             (t/map (comp count :value))
             (tm/digest tq/hdr-histogram)
             (t/post-combine quantiles))

        get-account-transfers-lengths
        (->> (t/filter (h/has-f? :get-account-transfers))
             (t/map (comp count :value))
             (tm/digest tq/hdr-histogram)
             (t/post-combine quantiles))

        ok-fold
        (->> (t/filter h/ok?)
             (t/fuse
               {:create-account-results        create-account-results
                :create-transfer-results       create-transfer-results
                :query-accounts-lengths        query-accounts-lengths
                :query-transfers-lengths       query-transfers-lengths
                :get-account-transfers-lengths get-account-transfers-lengths}))

        chain-lengths
        (->> (t/filter (h/has-f? write-fs))
             (t/mapcat (fn [op]
                         (loopr [size  1
                                 sizes (transient [])]
                                [event (:value op)]
                                (if (:linked (:flags event))
                                  (recur (inc size) sizes)
                                  (recur 1 (conj! sizes size)))
                                (persistent! sizes))))
             (tm/digest tq/hdr-histogram)
             (t/post-combine quantiles))

        invoke-fold
        (->> (t/filter h/invoke?)
             (t/fuse
               {:chain-lengths chain-lengths}))]
    (->> (t/fuse
           {:ok     ok-fold
            :invoke invoke-fold})
         (t/post-combine (fn [fused]
                           (reduce merge (vals fused))))
         (h/tesser history))))

(defn check-duplicate-timestamps
  "Checks a history to ensure all timestamps are distinct. Returns a map like:

  {:duplicate-timestamps
   {:count       The number of timestamps with duplicates
    :frequencies A vector of [timestamp, count] pairs, sorted by decreasing
                 count.
    :dups        A vector of vectors, each a group of OK operations with
                 identical timestamps."
  [history]
  (let [dups (->> (t/filter h/ok?)
                  (t/filter :timestamp)
                  (t/group-by :timestamp)
                  (t/into [])
                  (h/tesser history)
                  (remove #(= 1 (count (val %))))
                  (into {}))]
    (when (seq dups)
      {:duplicate-timestamps
       {:count        (count dups)
        :frequencies  (sort-by val (update-vals dups count))
        :dups         dups}})))

;; Realtime graph analysis

(defrecord TimestampExplainer []
  elle/DataExplainer
  (explain-pair-data [_ a b]
    (let [t (:timestamp a)
          t' (:timestamp b)]
      (when (and t t' (< t t'))
        {:type       :ww
         :timestamp  t
         :timestamp' t'})))

  (render-explanation [_ {:keys [type timestamp timestamp']} a-name b-name]
    (str a-name " executed at logical timestamp " timestamp ", and "
         b-name " executed at logical timestamp " timestamp' " ("
         (float (nanos->secs (- timestamp' timestamp))) " seconds later.")))

(defn timestamp-graph
  "Takes a history, and constructs an Elle dependency graph based on the
  timestamps of each operation."
  [history]
  (let [; A sorted map of timestamp to operation index.
        timestamp->index (->> (t/keep (fn ts->index-keep [op]
                                        (when-let [ts (:timestamp op)]
                                          [ts (:index op)])))
                              (bsorted-map)
                              (h/tesser history))
        ; Zip through timestamps, linking each op to the next.
        graph (loopr [prev-op nil
                      g       (b/linear (elle.graph/op-digraph))]
                     [ti timestamp->index]
                     (let [t  (bm/key ti)
                           i  (bm/value ti)
                           op (h/get-index history i)]
                       (if (nil? prev-op)
                         ; First step; immediately jump forward
                         (recur op g)
                         ; Link prev-op to op
                         (recur op (elle.graph/link g prev-op op ww))))
                     ; Done
                     (b/forked g))]
    {:graph graph
     :explainer (TimestampExplainer.)}))

(defn check-realtime
  "Checks a history for realtime consistency. We model the system as a single
  object, the timestamp, which every :ok operation advances. We infer a
  write-write dependency between every pair of operations on the basis of those
  timestamps, then ask Elle to check that the resulting graph is Strict
  Serializable.

  Takes a map with the full history."
  [{:keys [history] :as opts}]
  (let [analyzer (elle/combine
                   timestamp-graph
                   elle/realtime-graph)
        cycles (:anomalies (elle.txn/cycles! opts analyzer history))]
    (elle.txn/result-map opts cycles)))

;; Integrating various checks

(defn analysis
  "Analyzes a history, gluing together all the various data structures we
  need."
  [history]
  (let [account-id->timestamp (h/task history account-id->timestamp []
                                      (account-id->timestamp history))
        transfer-id->timestamp (h/task history transfer-id->timestamp []
                                      (transfer-id->timestamp history))
        resolved-history (h/task history resolve-ops
                                 [ait account-id->timestamp
                                  tit transfer-id->timestamp]
                                 (resolve-ops {:history history
                                               :account-id->timestamp ait
                                               :transfer-id->timestamp tit}))
        model-check (h/task history model-check [rh  resolved-history
                                                 ait account-id->timestamp
                                                 tit transfer-id->timestamp]
                            (model-check {:history history
                                          :resolved-history rh
                                          :account-id->timestamp ait
                                          :transfer-id->timestamp tit}))
        check-realtime (h/task history check-realtime []
                               (check-realtime {:history history}))
        check-duplicate-timestamps (h/task history duplicate-timestamps []
                                           (check-duplicate-timestamps history))
        stats (h/task history stats []
                      (stats history))
        ; Build error map
        errors (merge (sorted-map)
                      @model-check
                      (:anomalies @check-realtime)
                      @check-duplicate-timestamps)]
    (merge errors
           {:stats @stats}
           (select-keys @check-realtime [:not :also-not])
           {:error-types (into (sorted-set) (keys errors))})))

(def unknown-error-types
  "These errors only cause us to be :valid? :unknown, rather than false"
  #{:empty-transaction-graph})

(defn check
  "Checks a history, returning a map with statistics and errors."
  [history]
  (let [a           (analysis history)
        error-types (:error-types a)
        valid? (cond (seq (remove unknown-error-types error-types))
                     false

                     (seq error-types)
                     :unknown

                     true
                     true)]
    (assoc a :valid? valid?)))

(defrecord Checker []
  checker/Checker
  (check [this test history opts]
    (check history)))

(defn checker
  "Constructs a new TigerBeetle checker."
  []
  (Checker.))
