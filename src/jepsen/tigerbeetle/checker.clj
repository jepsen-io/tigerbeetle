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
  (:require [clojure.core [match :refer [match]]]
            [bifurcan-clj [core :as b]
                          [int-map :as bim]
                          [graph :as bg]
                          [map :as bm]
                          [set :as bs]]
            [dom-top.core :refer [loopr]]
            [jepsen [history :as h]]
            [tesser.core :as t]))

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
  (assert (nil? downstream))
  {:reducer-identity  (comp b/linear bs/set)
   :reducer           bs/union
   :post-reducer      identity
   :combiner-identity (comp b/linear bs/set)
   :combiner          bs/union
   :post-combiner     b/forked})

(defn first-final-read
  "Takes a history. Returns the offset of the first final read."
  [history]
  (loopr [i 0]
         [op history]
         (let [f (:f op)]
           (if (or (identical? f :final-lookup-accounts)
                   (identical? f :final-lookup-transfers))
             i
             (recur (inc i))))))

(defn resolve-events-into-set
  "Takes a Bifurcan set, a vector of ids, and a vector of values. Adds ids to
  set if values are present."
  [s ids values]
  (loop [i 0
         s s]
    (if (= i (count ids))
      s
      (recur (inc i)
             (if (nil? (nth values i))
               s
               (bs/add s (nth ids i)))))))

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
       (t/filter (h/has-f? #{:get-account-transfers
                             :lookup-transfers
                             :query-transfers}))
       (t/map ids-in)
       bset-union
       (h/tesser history)))

(defn seen-accounts
  "Runs through the entire history and builds a Bifurcan set of all read
  account IDs."
  [history]
  (->> (t/filter h/ok?)
       (t/filter (h/has-f? #{:lookup-accounts :query-accounts}))
       (t/map ids-in)
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
        (recur (if (identical? :ok (nth results i))
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

(defn op-actually-ok?
  "Takes a history, a set of OK account ids and OK transfer ids, and a single
  operation of type :info. Returns true if any of the operation's effects were
  in the OK accounts/transfers."
  [ok-accounts ok-transfers op]
  (assert (h/info? op))
  (if-let [; What set are we going to look in?
           ok-ids (case (:f op)
                    :create-accounts ok-accounts
                    :create-transfers ok-transfers
                    nil)]
    (let [invoke (h/invocation history op)]
      ; Look for an ID we read later
      (loopr []
             [event (:value invoke)]
             (if (bs/contains? ok-ids (:id event))
               true
               (recur))
             false))
    ; This is not a create op; must be a read. We can safely fail this.
    false))

(defn resolve-ops
  "Takes a map of:

    :history              The history
    :seen-accounts        The set of all accounts IDs we read
    :seen-transfers       The set of all transfer IDs we read

  Runs through the history and resolves any info operations to either :ok or
  :fail based on presence in ok-accounts or ok-transfers. We construct a new
  history in which no writes are :info, and also a collection of atomicity
  errors when some but not all of an operation's writes occur."
  {:keys [history ok-accounts ok-transfers]}
  ; TODO: precompile this as a bitset
  (h/map (fn [op]
           (if (identical? (:type op) :info)
             (assoc op :type (if (op-actually-ok? ok-accounts ok-transfers op)
                               :ok
                               :fail))
             op))))

(defn timestamp-sorted
  "Takes a history. Returns a sequence of all OK operations in the history
  sorted by :timestamp ascending."
  [history]
  (->> history
       (h/filter op/ok)
       (sort-by :timestamp)))

(defn 

(defn analysis
  "Analyzes a history, gluing together all the various data structures we
  need."
  [history]
  (let [;created-accounts (h/task history created-accounts []
        ;                         (created-accounts history))
        ;created-transfers (h/task history created-transfers []
        ;                          (created-transfers history))
        seen-accounts  (h/task history seen-accounts []
                               (seen-accounts history))
        seen-transfers (h/task history seen-transfers []
                               (seen-transfers history))
        resolved-history (h/task history resolve-ops [sa seen-accounts
                                                      st seen-transfers])
        ]))
