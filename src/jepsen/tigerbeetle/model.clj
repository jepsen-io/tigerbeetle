(ns jepsen.tigerbeetle.model
  "A state machine model of TigerBeetle's semantics.

  ## Timestamps

  We have to keep advancing timestamps in the model itself, and record those
  timestamps in the records we store. However, the timestamps computed by
  TigerBeetle are impure--they are driven not only by monotonic advancing
  indices, but also by the leader's wall clock. We do not, in general, have
  access to these timestamps--they are not returned in the response to (e.g.)
  create-accounts, and even if they were, we may not *have* that response.

  We could try to infer lower bounds on timestamps and advance them once
  observed, but we can actually be more precise by punting. We require that our
  caller provide us with a map of account (transfer) ID to timestamp. We can do
  this because our histories *always* require a complete read of every account
  and transfer."
  (:require [bifurcan-clj [core :as b]
             [int-map :as bim]
             [list :as bl]
             [map :as bm]
             [set :as bs]
             [util :refer [iterable=]]]
            [clojure
             [data :refer [diff]]
             [datafy :refer [datafy]]
             [pprint :as pprint :refer [pprint]]
             [set :as set]]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [letr loopr]]
            [jepsen.history :as h]
            [potemkin :refer [definterface+ def-derived-map]])
  (:import (java.util Arrays)
           (jepsen.history Op)
           (io.lacuna.bifurcan IMap)))

;; Utilities

(defn ^long error-map-priority
  "Gives the preferred order of keys in error maps. Higher means later."
  [k]
  (case k
    :type        0
    :id          1
    :account     2
    :transfer    3
    :filter      20
    :expected    51
    :actual      52
    :diff        53
    :op-count    80
    :event-count 81
    :op          100
    :op'         101
    1000))

(defn error-map-compare
  "Comparator for error maps."
  [a b]
  (let [c (compare (error-map-priority a) (error-map-priority b))]
    (if (zero? c)
      (compare a b)
      c)))

(defn error-map
  "It's nice to have our error maps keep their keys in a particular order.
  Turns any map into a sorted map by our preferred printing order."
  [m]
  (into (sorted-map-by error-map-compare) m))

(defn first-not=-index
  "Takes two iterables and returns the index of the first mismatch between
  them, or nil if no mismatch."
  [^Iterable a, ^Iterable b]
  (let [a (.iterator a)
        b (.iterator b)]
    (loop [i 0]
      (if (.hasNext a)
        (if (.hasNext b)
          (if (= (.next a) (.next b))
            (recur (inc i))
            i)
          ; More as, no more bs
          i)
        ; Done
        (if (.hasNext b)
          ; No more as, more bs
          i
          ; Equal all the way
          nil)))))

(defn diff-map
  "A little map with :expected and :actual keys."
  [expected actual]
  (let [[- +] (diff expected actual)]
    {:expected -, :actual +}))

(defn fill-list
  "Constructs a Bifurcan list of n elements of x."
  [n x]
  (let [a (object-array n)]
    (Arrays/fill a x)
    (bl/from-array a)))

(defn bm-put-set
  "Takes a map, key `k`, and value `v`. Updates the map so that `k` points to a
  set containing `v` (plus whatever is already present)."
  [m k v]
  (bm/update m k
             (fn add [extant]
               (bs/add (or extant bs/empty) v))))

(defn bm-union
  "Takes two Bifurcan maps and computes their union. `nil` symbolizes the empty
  set."
  [a b]
  (cond (nil? a) b
        (nil? b) a
        true     (bm/union a b)))

(defn bm-intersection
  "Takes two Bifurcan maps and computes their intersection. `nil`
  symbolizes the universal set."
  [a b]
  (cond (nil? a) b
        (nil? b) a
        true     (bm/intersection a b)))

(defn bim-slice
  "Takes an intmap, a minimum (possibly nil), maximum (also possibly nil), and
  an integer map. Slices m from min to max, inclusive. A nil min or max means
  unbounded in that direction."
  [m min max]
  (let [n   (b/size m)
        min (or min
                (when (= 0 n) 0)
                (bm/key (b/nth m 0)))
        max (or max
                (when (= 0 n) 0)
                (bm/key (b/nth m (dec n))))]
    (bim/slice m min max)))

;; Models

(definterface+ IModel
  (step [this invoke ok]
        "Takes a model, an invoke op, and a corresponding OK op. Returns a new
        model.")

  (debug [this]
         "A compact debugging representation of the model. Helpful for REPL
         analysis."))

(defrecord Inconsistent [op op' type op-count event-count]
  IModel
  (step [this invoke ok] this)

  (debug [this]
    (error-map this)))

(def inconsistent
  "Represents inconsistent termination of a model."
  map->Inconsistent)

(defn inconsistent?
  "Is this model inconsistent?"
  [model]
  (instance? Inconsistent model))

; The indentation is getting ridiculous
(defmethod pprint/simple-dispatch jepsen.tigerbeetle.model.Inconsistent
  [i]
  (.write ^java.io.Writer *out* (str (.getName (class i)) "{"))
  (pprint/pprint-newline :mandatory)
  (let [prefix "  "
        suffix "}"]
    (pprint/pprint-logical-block
      :prefix prefix :suffix suffix
      (pprint/print-length-loop [aseq (seq i)]
                                (when aseq
                                  (pprint/pprint-logical-block
                                    (pprint/write-out (key (first aseq)))
                                    (.write ^java.io.Writer *out* " ")
                                    (pprint/pprint-newline :miser)
                                    (pprint/write-out (fnext (first aseq))))
                                  (when (next aseq)
                                    (.write ^java.io.Writer *out* ", ")
                                    (pprint/pprint-newline :linear)
                                    (recur (next aseq))))))))

(prefer-method pprint/simple-dispatch
               jepsen.tigerbeetle.model.Inconsistent
               clojure.lang.IPersistentMap)

(def permanent-errors
  "A set of errors that we remember and return for subsequent attempts. See
  https://docs.tigerbeetle.com/reference/requests/create_transfers/#id_already_failed."
  #{:debit-account-not-found
    :credit-account-not-found
    :pending-transfer-not-found
    :exceeds-credits
    :exceeds-debits
    :debit-account-already-closed
    :credit-account-already-closed})

(definterface+ ITB
  (advance-timestamp [model ^long timestamp]
                     "Advances the internal timestamp of this model to
                     `timestamp`. Constructs an invalid state if the timestamp
                     is not strictly monotonic.")

  (advance-account-timestamp [model ^long timestamp account]
                             "Advances the account-specific timestamp.")

  (advance-transfer-timestamp [model ^long timestamp transfer]
                              "Advances the transfer-specific timestamp.")

  (create-account [model account import?]
                  "Adds an account to a model, returning the new model if ok,
                  or an error keyword.")

  (create-transfer [model account import?]
                   "Adds a transfer to a model, returning the new
                   model if ok, or an error keyword.")

  (create-accounts-chain [model accounts import?]
                         "Creates a series of accounts in a single chain. All
                         operations succeed or fail as a unit. Returns
                         [model', results].")

  (create-transfers-chain [model transfers import?]
                          "Creates a series of transfers in a single chain. All
                          operations succeed or fail as a unit. Returns
                          [model', results']")

  (read-account [model id]
                "Reads an account by ID, returning account or nil.")

  (read-transfer [model id]
                 "Reads a transfer by ID, returning transfer or nil.")

  (get-account-transfers- [model account-filter]
                          "Returns a vector of transfers matching the given
                          account filter.")

  (get-account-balances- [model account-filter]
                         "Returns a vector of account balances matching the
                         given account filter.")

  (query-accounts- [model query-filter]
                  "Returns a vector of accounts matching the given query
                  filter.")

  (query-transfers- [model query-filter]
                    "Returns a vector of transfers matching the given query
                    filter.")

  ; API operations
  (create-accounts [model invoke ok]
                   "Applies a single create-accounts operation to the model,
                   returning model'")

  (create-transfers [model invoke ok]
                    "Applies a single create-transfers operation to the model,
                    returning model'")

  (lookup-accounts [model invoke ok]
                   "Applies a single lookup-accounts operation to the model,
                   returning model'")

  (lookup-transfers [model invoke ok]
                    "Applies a single lookup-transfers operation to the model,
                    returning model'")

  (get-account-transfers [model invoke op]
                         "Applies a single get-account-transfers operation to
                         the model, returning model'")

  (get-account-balances [model invoke op]
                        "Applies a single get-account-balances operation to the
                        model, returning model'")

  (query-accounts [model invoke op]
                  "Applies a single query-accounts operation to the model,
                  returning model'")

  (query-transfers [model invoke op]
                   "Applies a single query-transfers operation to the model,
                   returning model'"))

(def timestamp-upper-bound
  "One bigger than the biggest timestamp for an imported event (2^63)"
  9223372036854775808)

(def int64-max
  "2^63 - 1"
  Long/MAX_VALUE)

(def uint128-max
  "2^128 - 1"
  340282366920938463463374607431768211455N)

(defn chains
  "Takes a vector of events and partitions it into a vector of linked chains,
  each chain a vector of events."
  [events]
  (let [n (count events)]
    (loop [i       0 ; Starting index of the chain
           j       0 ; Ending index of the chain]
           chains (transient [])]
      (if (= j n)
        ; Done
        (if (< i j)
          ; Unfinished chain. We'll catch this later.
          (persistent! (conj! chains (subvec events i j)))
          ; All set.
          (persistent! chains))
        (let [j' (inc j)]
          (if (:linked (:flags (nth events j)))
            (recur i  j' chains)
            (recur j' j' (conj! chains (subvec events i j')))))))))

(defn remember-error
  "Some 'transient' errors are remembered for later. Takes a model, an event
  with an ID, and an error keyword. Returns a model."
  [model event error]
  (if (contains? permanent-errors error)
    (update model :errors bm/put (:id event) error)
    model))

(defn create-chain-check-open!
  "There is a special kind of error which is layered on *top* of the normal
  create-chain logic. If a linked event chain is left open, we flag that error
  *in addition* to any other errors in the chain.

  Takes:

  - A vector of events from a single chain
  - An array (mutated in-place) of results from applying those events.
  - An OK model, resulting from applying those events OK. This is `nil` if we
    were unable to apply all events.
  - An error model, either the original, or the original plus remembered
    errors.

  Returns [model' expected-results], aborting the chain if it ends with a
  :linked flag. `model'` is the OK model, if given, or the error model.
  Expected-results is a Bifurcan list."
  [events ^objects results ok-model error-model]
  (let [n (alength results)
        n- (dec n)]
    (if (and (< 0 n) (:linked (:flags (peek events))))
      ; Linked chain open.
      (do ; Go back and flip any OK results to failures.
          (loop [i 0]
            (when (< i n-)
              (when (identical? :ok (aget results i))
                (aset results i :linked-event-failed))
              (recur (inc i))))
          ; Set last result to linked-event-chain-open
          (aset results (dec n) :linked-event-chain-open)
          ; Construct vector and return
          [error-model (bl/from-array results)])
      ; Not open
      [(or ok-model error-model) (bl/from-array results)])))

(defn create-chain
  "Shared logic for creating a single chain of accounts or transfers. Takes a
  model, a function to create a single event in a model, a list of events,
  and whether this is an import or not. The function `(create this event
  import?)` returns either a new model state (either inconsistent, or
  implicitly an :ok result), or an error keyword.

  Returns a tuple of [model' expected-results] from applying this particular
  chain."
  [model create events import?]
  ; Note that *every* response to creating a chain of accounts is either
  ; (entirely :ok, or entirely :linked-event-failed with the exception of a
  ; single error), with the exception of :linked-event-chain-open, which takes
  ; place regardless.
  (let [n           (count events)
        results     (object-array n)
        event-count (:event-count model)]
    (loopr [i        0
            model'   model]
           [event events]
           (let [result (create model' event import?)]
             (cond
               ; Inconsistent; abort here. No point computing results.
               (inconsistent? result)
               [result nil]

               ; Logical error; fail chain immediately.
               (keyword? result)
               (do (Arrays/fill results :linked-event-failed)
                   (aset results i result)
                   (create-chain-check-open!
                     events
                     results
                     nil
                     (-> model
                         (remember-error event result)
                         (assoc :event-count (+ event-count n)))))

               ; Good, move on
               true
               (recur (inc i) result)))
           ; Completed successfully
           (do (Arrays/fill results :ok)
               (create-chain-check-open!
                 events
                 results
                 (assoc model' :event-count (+ event-count n))
                 model)))))

(defn validate
  "Takes a model, an invoke, an OK, an event name (e.g. :transfer or :event), a
  vector of events, a Bifurcan list of expected results, and a vector of actual
  results. Returns model if the results match, or an inconsistent state.

  If the model has a speculative error, returns an invalid state.

  The special `actual` value :unknown indicates that we don't know what the
  client returned, and that we should allow any results."
  [model invoke ok event-name events expected actual]
  ; First, check integrity of results
  (or (when (not (identical? :unknown actual))
        ; We know what actually happened; did it differ?
        (when-let [i (first-not=-index expected actual)]
          ; Unexpected result
          (if (not= (b/size expected) (count actual))
            ; Wrong number of results
            (inconsistent
              {:type           :model
               :op-count       (:op-count model)
               :event-count    (- (:event-count model) (- (count events) i))
               :expected       (datafy expected)
               :actual         actual
               :expected-count (b/size expected)
               :actual-count   (count actual)})
            ; Same number, wrong element
            (inconsistent
              ; We have to unwind the event-count here, because our
              ; apply-chain logic advances it either way.
              {:type        :model
               :op-count    (:op-count model)
               :event-count (- (:event-count model) (- (count events) i))
               event-name   (nth events i)
               :expected    (b/nth expected i)
               :actual      (nth actual i)}))))
      ; What about a speculative error?
      (when-let [err (:speculative-error model)]
        (let [; Find the corresponding result
              event (:event err)
              id (:id event)
              i (loopr [i 0]
                       [event events]
                       (if (= id (:id event))
                         i
                         (recur (inc i)))
                       nil)
              _ (assert i)
              result (b/nth expected i)]
          (inconsistent
            (-> err
                ; We rename :event to :transfer or :account
                (dissoc :event)
                (assoc event-name event
                       :result      result
                       :op-count    (:op-count model)
                       :event-count (:event-count model))))))
      ; All good!
      model))

(defn create-helper
  "Common logic for create-transfers and create-accounts. Takes a model, an
  invoke, an OK, a function (create-chain model chain import?)
  which applies a single chain of events to the model, and a name (e.g.
  :account) for what we call a single event, used in the error map. Returns
  model'."
  [this invoke ok create-chain event-name]
  (let [events (:value invoke)
        actual (:value ok)
        n      (count events)
        ; Are we doing a batch import?
        import? (when-let [t (first events)]
                  (contains? (:flags t) :imported))]
    ; Zip through chains, applying each
    (loopr [this     this                 ; The model
            expected (b/linear bl/empty)] ; Our expected results list
           [chain (chains events)]
           ; Apply this chain
           (let [[this chain-results] (create-chain this chain import?)]
             (if (inconsistent? this)
               this
               (recur this (bl/concat expected chain-results))))
           ; Done applying chains.
           (let [this (validate this invoke ok event-name events
                                expected actual)]
             (if (inconsistent? this)
               this
               (update this :op-count inc))))))

(def conflicting-transfer-flags
  "A map of transfer flags to sets of conflicting flags."
  {:pending #{:post-pending-transfer
              :void-pending-transfer}
   :post-pending-transfer #{:pending
                            :void-pending-transfer
                            :balancing-debit
                            :balancing-credit
                            :closing-debit
                            :closing-credit}
   :void-pending-transfer #{:pending
                            :post-pending-transfer
                            :balancing-debit
                            :balancing-credit
                            :closing-debit
                            :closing-credit}
   :balancing-debit #{:void-pending-transfer
                      :post-pending-transfer}
   :balancing-credit #{:void-pending-transfer
                       :post-pending-transfer}
   :closing-debit #{:post-pending-transfer
                    :void-pending-transfer}
   :closing-credit #{:post-pending-transfer
                     :void-pending-transfer}})

(defn create-transfer-flags-error
  "Takes transfer flags. Returns :flag-are-mutually-exclusive if it has
  mutually exclusive flags, else nil."
  [flags]
  ; See https://docs.tigerbeetle.com/reference/requests/create_transfers/#flags_are_mutually_exclusive
  (loopr []
         [flag flags]
         (let [conflicts (set/intersection flags (conflicting-transfer-flags flag))]
           (if (empty? conflicts)
             (recur)
             :flags-are-mutually-exclusive))))

(defn create-transfer-extant-error
  "Checks for several possible errors involving an extant copy of a transfer.
  Returns an error keyword or nil."
  [{:keys [flags pending-id timeout credit-account-id debit-account-id amount
           user-data ledger code] :as transfer}
   extant]
  (cond
    (nil? extant)
    nil

    ; See
    ; https://docs.tigerbeetle.com/reference/requests/create_transfers/#exists
    ; -- we probably have to be more particular about
    ; balancing/post-pending transfers
    (not= flags (:flags extant))
    :exists-with-different-flags

    (not= (or pending-id 0) (:pending-id extant 0))
    :exists-with-different-pending-id

    (not= timeout (:timeout extant))
    :exists-with-different-timeout

    (not= credit-account-id (:credit-account-id extant))
    :exists-with-different-credit-account-id

    (not= debit-account-id (:debit-account-id extant))
    :exists-with-different-debit-account-id

    (not= amount (:amount extant))
    :exists-with-different-amount

    (not= user-data (:user-data extant))
    :exists-with-different-user-data-64

    (not= ledger (:ledger extant))
    :exists-with-different-ledger

    (not= code (:code extant))
    :exists-with-different-code

    true
    :exists))

(defn create-transfer-*-account-id-error
  "Validates debit and credit account IDs on a transfer."
  [credit-account-id debit-account-id]
  (cond
    (= 0N credit-account-id)
    :credit-account-id-must-not-be-zero

    (= 0N debit-account-id)
    :debit-account-id-must-not-be-zero

    (= uint128-max credit-account-id)
    :credit-account-id-must-not-be-int-max

    (= uint128-max debit-account-id)
    :debit-account-id-must-not-be-int-max

    (= credit-account-id debit-account-id)
    :accounts-must-be-different))

(defn create-transfer-basic-error
  "A few basic errors that can occur when creating a transfer"
  [errors import? id flags]
  (cond ; Already failed
        (bm/contains? errors id)
        :id-already-failed

        ; ID range
        (= 0N id)
        :id-must-not-be-zero

        (= uint128-max id)
        :id-must-not-be-int-max

        ; Import mismatch
        (and import? (not (:imported flags)))
        :imported-event-expected

        (and (not import?) (:imported flags))
        :imported-event-not-expected

        (and (not (:pending flags))
             (or (:closing-credit flags)
                 (:closing-debit flags)))
        :closing-transfer-must-be-pending))

(defn create-transfer-code-error
  "Checking for code = 0 happens after pending transfer stuff"
  [code]
  (cond (= 0 code)
        :code-must-not-be-zero))

(defn create-transfer-timestamp-error
  "Errors related to timestamps during transfer creation"
  [timestamp transfer-timestamp import? credit-acct debit-acct transfer]
  (let [ts (:timestamp transfer 0)]
    (if import?
      (cond (not (< 0 ts timestamp-upper-bound))
            :imported-event-timestamp-out-of-range

            (and (< timestamp ts) (not= -1 timestamp))
            :imported-event-timestamp-must-not-advance

            (<= ts transfer-timestamp)
            :imported-event-timestamp-must-not-regress

            (<= ts (:timestamp credit-acct -1))
            :imported-event-timestamp-must-postdate-credit-account

            (<= ts (:timestamp debit-acct -1))
            :imported-event-timestamp-must-postdate-debit-account

            (< 0 (:timeout transfer 0))
            :imported-event-timeout-must-be-zero)
      ; Not importing
      (when (not (= 0 ts))
        :timestamp-must-be-zero))))

(defn create-transfer-account-exists-error
  "Errors when accounts don't exist"
  [flags debit-account credit-account]
  (cond ; Accounts must exist
        (nil? debit-account)
        :debit-account-not-found

        (nil? credit-account)
        :credit-account-not-found))

(defn create-transfer-account-error
  "Errors involving the accounts of a transfer."
  [flags debit-account credit-account]
  (cond ; Same ledger
        (not= (:ledger credit-account)
              (:ledger debit-account))
        :accounts-must-have-the-same-ledger

        ; You can't do anything except void a closed account.
        (and (not (:void-pending-transfer flags))
             (:closed (:flags debit-account)))
        :debit-account-already-closed

        (and (not (:void-pending-transfer flags))
             (:closed (:flags credit-account)))
        :credit-account-already-closed))

(defn create-transfer-pending-error
  "Errors involving pending transfers"
  [{:keys [id code ledger amount debit-account-id credit-account-id flags]
    :as transfer}
   pending-id
   pending]
  (let [post? (:post-pending-transfer flags)
        void? (:void-pending-transfer flags)]
    ; Constraints on the pending ID
    (or (if (or post? void?)
          (condp = pending-id
            0N          :pending-id-must-not-be-zero
            uint128-max :pending-id-must-not-be-int-max
            id          :pending-id-must-be-different
            nil)
          (when (not= 0N pending-id)
            :pending-id-must-be-zero))
        ; Constraints on the transfer
        (if (nil? pending)
          ; No pending transfer known
          (cond
            (not= 0N pending-id)
            :pending-transfer-not-found)

          ; OK, we're doing a pending transfer and we found it.
          (cond (not (:pending (:flags pending)))
                :pending-transfer-not-pending

                (and (not= debit-account-id 0)
                     (not= debit-account-id (:debit-account-id pending)))
                :pending-transfer-has-different-debit-account-id

                (and (not= credit-account-id 0)
                     (not= credit-account-id (:credit-account-id pending)))
                :pending-transfer-has-different-credit-account-id

                (and (not= ledger 0) (not= ledger (:ledger pending)))
                :pending-transfer-has-different-ledger

                (and (not= code 0) (not= code (:code pending)))
                :pending-transfer-has-different-code

                (< (:amount pending) amount)
                :exceeds-pending-transfer-amount

                (and void? (not= amount 0N) (not= amount (:amount pending)))
                :pending-transfer-has-different-amount

                (= :posted (:state pending))
                :pending-transfer-already-posted

                (= :voided (:state pending))
                :pending-transfer-already-voided)))))


(defn transfer-update-account
  "Called to update a single account for a transfer. Take an account, a mode
  (:debit or :credit), the pending transfer (if any), an amount, and the
  transfer flags."
  [account mode pending amount flags]
  (assert account)
  (case mode
    :debit
    (let [; First, close/reopen account
          account (cond (:closing-debit flags)
                        (update account :flags conj :closed)

                        (and (:void-pending-transfer flags)
                             (:closing-debit (:flags pending)))
                        (update account :flags disj :closed)

                        true
                        account)]
      ; Balance changes
      (cond
        (:pending flags)
        (update account :debits-pending + amount)

        (:post-pending-transfer flags)
        (-> account
            (update :debits-pending - (:amount pending))
            (update :debits-posted + amount))

        (:void-pending-transfer flags)
        (update account :debits-pending - (:amount pending))

        true
        (update account :debits-posted + amount)))

    :credit
    (let [; First, close/reopen account
          account (cond (:closing-credit flags)
                        (update account :flags conj :closed)

                        (and (:void-pending-transfer flags)
                             (:closing-credit (:flags pending)))
                        (update account :flags disj :closed)

                        true
                        account)]
      (cond
        (:pending flags)
        (update account :credits-pending + amount)

        (:post-pending-transfer flags)
        (-> account
            (update :credits-pending - (:amount pending))
            (update :credits-posted + amount))

        (:void-pending-transfer flags)
        (update account :credits-pending - (:amount pending))

        true
        (update account :credits-posted + amount)))))

(defn historical-balance
  "Constructs a historical balance from an account and a timestamp."
  [timestamp {:keys [debits-pending credits-pending
                     debits-posted  credits-posted]}]
  {:timestamp timestamp
   :debits-pending debits-pending
   :debits-posted debits-posted
   :credits-pending credits-pending
   :credits-posted credits-posted})

(defn transfer-update-historical-balances-update-account
  "Helper for transfer-update-historical-balances--updates a single account
  sub-map within the historical balances map."
  [balances timestamp account]
  (bim/put (or balances (bim/int-map))
           timestamp
           (historical-balance timestamp account)))

(defn transfer-update-historical-balances
  "Called to update the historical balances when we perform a transfer. Takes
  the historical-balances map, a timestamp, and the debit and credit accounts.
  Returns a new historical balances map."
  [historical-balances timestamp debit-account credit-account]
  (cond-> historical-balances
    ; Debit account
    (:history (:flags debit-account))
    (bm/update (:id debit-account)
               transfer-update-historical-balances-update-account
               timestamp debit-account)

    ; Credit account
    (:history (:flags credit-account))
    (bm/update (:id credit-account)
               transfer-update-historical-balances-update-account
               timestamp credit-account)))

(defn update-secondary-index
  "Updates a secondary index to fold in a new account or transfer. Takes the
  index, the value for the key `k-val` we want to index on (e.g. the code), and
  the account or transfer `v`. Returns the new index.

  Two facts:

  1. We have to do range queries & ordering on timestamps
  2. Each timestamp uniquely identifies a single transfer or account.

  This lets us do something cute. We store our secondary indices using
  Bifurcan integer maps of timestamp-id. These maps give us efficient
  union, intersection, slice, and ordered traversal by timestamp. Moreover,
  integer comparison is much faster than comparing the full transfer or
  account objects. So each index is a map of, say, `debit account id` ->
  `timestamp` -> `id`.

  Why store ids instead of transfers or account directly? Accounts are partly
  mutable--their balances change. We have to go look them up when constructing
  responses."
  [index k-val v]
  (bm/update index k-val
             (fn [timestamp-int-map]
               (bim/put (or timestamp-int-map (bim/int-map))
                        (:timestamp v)
                        (:id v)))))

(defn has-user-data?
  "Takes a user-data value. Nil signifies any user data. Returns a predicate of
  any x which returns true iff (:user-data x) matches user-data."
  [user-data]
  (if user-data
    (fn pred [x] (= user-data (:user-data x)))
    any?))

(defn query-scan
  "Takes an intmap of timestamps to ids (e.g. of accounts or transfers), a
  function `(read id) -> value`, a predicate which each value must satisfy, a
  limit to the number of results, and whether we traverse in reversed order.
  Returns a vector of resulting values."
  [pool read pred? limit reversed?]
  (let [n     (b/size pool)
        dir   (if reversed? -1 1)
        start (if reversed? (dec n) 0)]
    (loop [i       start
           results (transient [])]
      (cond ; Full
            (= (count results) limit)
            (persistent! results)

            ; Out of bounds
            (not (< -1 i n))
            (persistent! results)

            ; Check this value
            true
            (let [i'    (+ i dir)
                  id    (bm/value (b/nth pool i))
                  value (read id)]
              (if (pred? value)
                (recur i' (conj! results value))
                (recur i' results)))))))

(declare ->DebugTB)

(defrecord TB
  [; A pair of maps of account/transfer ID to timestamp, derived from the
   ; observed history. We use these to advance time synthetically throughout
   ; the simulation.
   account-id->timestamp
   transfer-id->timestamp
   ^long op-count           ; Number of ops applied
   ^long event-count        ; Number of events applied
   ^long timestamp          ; Our internal timestamp
   ^long account-timestamp  ; The last account timestamp created
   ^long transfer-timestamp ; The last transfer timestamp created
   accounts  ; A map of account IDs to accounts
   transfers ; A map of transfer IDs to transfers
   ; A map of account-id -> timestamp -> account-balance.
   historical-balances

   ; A map of permanent errors (TigerBeetle calls these *transient*
   ; errors, but they persist forever)
   errors

   ; A partial invalid map representing the first speculative event that was
   ; applied to a model. Speculative events occur if we believe at least one
   ; event in an op occurred, but others are unobserved and have no timestamp.
   ; We execute those unobserved events anyway, assuming that they must fail by
   ; the end of the operation. If this is present after an operation, the DB is
   ; invalid.
   speculative-error

   ; Indices for accounts
   account-index
   account-ledger-index
   account-code-index
   account-user-data-index

   ; Indices for transfers
   transfer-index
   transfer-debit-index   ; by debit account id
   transfer-credit-index  ; by credit account id
   transfer-ledger-index
   transfer-code-index
   transfer-user-data-index
   ]

  ITB
  (advance-timestamp [this ts]
    (if (< timestamp ts)
      (assoc this :timestamp ts)
      (inconsistent
        {:type        :nonmonotonic-timestamp
         :op-count    op-count
         :event-count event-count
         :timestamp   timestamp
         :timestamp'  ts})))

  (advance-account-timestamp [this ts account]
    (if (< account-timestamp ts)
      (assoc this :account-timestamp ts)
      (inconsistent
        {:type               :nonmonotonic-account-timestamp
         :op-count           op-count
         :event-count        event-count
         :account            account
         :account-timestamp  account-timestamp
         :account-timestamp' ts})))

  (advance-transfer-timestamp [this ts transfer]
    (if (< transfer-timestamp ts)
      (assoc this :transfer-timestamp ts)
      (inconsistent
        {:type                :nonmonotonic-transfer-timestamp
         :op-count            op-count
         :event-count         event-count
         :transfer            transfer
         :transfer-timestamp  transfer-timestamp
         :transfer-timestamp' ts})))

  (create-account [this account import?]
    (let [id         (:id account)
          extant     (bm/get accounts id)
          flags      (:flags account)
          imported?  (:imported flags)
          atimestamp (:timestamp account 0)
          ledger     (:ledger account 0)
          code       (:code account 0)
          user-data  (:user-data account 0)]
      (cond
        ; See https://docs.tigerbeetle.com/reference/requests/create_accounts#result
        (and import? (not imported?))
        :imported-event-expected

        (and (not import?) imported?)
        :imported-event-not-expected

        (and (not import?) (not (zero? atimestamp)))
        :timestamp-must-be-zero

        (and (:debits-must-not-exceed-credits flags)
             (:credits-must-not-exceed-debits flags))
        :flags-are-mutually-exclusive

        (and import? (not (< 0 atimestamp timestamp-upper-bound)))
        :imported-event-timestamp-out-of-range

        (and import? (< timestamp atimestamp) (not= timestamp -1))
        :imported-event-timestamp-must-not-advance

        (and import? (<= atimestamp account-timestamp))
        :imported-event-timestamp-must-not-regress

        ; TODO: we don't presently represent these
        ; :reserved-field
        ; :reserved-flag

        (= 0 (:id account))
        :id-must-not-be-zero

        (= uint128-max (:id account))
        :id-must-not-be-int-max

        extant
        (cond
          (not= (:flags extant) flags)
          :exists-with-different-flags

          ; We don't do 128/32, just 64
          (not= (:user-data extant) user-data)
          :exists-with-different-user-data-64

          (not= (:ledger extant) (:ledger account))
          :exists-with-different-ledger

          (not= (:code extant) (:code account))
          :exists-with-different-code

          true
          :exists)

        (when-let [p (:debits-pending account)] (not= 0 p))
        :debits-pending-must-be-zero

        (when-let [p (:debits-posted account)] (not= 0 p))
        :debits-posted-must-be-zero

        (when-let [p (:credits-pending account)] (not= 0 p))
        :credits-pending-must-be-zero

        (when-let [p (:credits-posted account)] (not= 0 p))
        :credits-posted-must-be-zero

        (= 0 ledger)
        :ledger-must-not-be-zero

        (= 0 code)
        :code-must-not-be-zero

        true
        ; OK, go! What timestamp did the actual system assign this account?
        (letr [ts      (if imported?
                         atimestamp
                         (bm/get account-id->timestamp id 0))
               ; If we *don't* have a timestamp, it's very likely that this
               ; event is going to succeed now, but be rolled back due to a
               ; later failed event in the chain. We assign it a speculative
               ; timestamp and mark the transfer as :speculative?.
               speculative? (zero? ts)
               ts           (if speculative?
                              (inc (max timestamp account-timestamp))
                              ts)
               account      (if speculative?
                              (assoc account :speculative? true)
                              account)
               ; Advance clocks to the new timestamp
               this' (advance-account-timestamp this ts account)
               _     (when (inconsistent? this') (return this'))
               this' (if imported?
                       this'
                       (advance-timestamp this' ts))
               _     (when (inconsistent? this') (return this'))
               ; Record account
               account (assoc account
                              :credits-pending 0N
                              :credits-posted 0N
                              :debits-pending 0N
                              :debits-posted 0N
                              :timestamp ts)
               ; Remember the first speculative event
               speculative-error' (or speculative-error
                                      (when speculative?
                                        {:type    :speculative-account
                                         :event   account}))]
          (assoc this'
                 :accounts              (bm/put accounts id account)
                 :speculative-error     speculative-error'
                 :account-index         (update-secondary-index
                                          account-index true account)
                 :account-code-index    (update-secondary-index
                                          account-code-index code account)
                 :account-ledger-index  (update-secondary-index
                                          account-ledger-index ledger account)
                 :account-user-data-index
                 (update-secondary-index
                   account-user-data-index user-data account))))))

  (create-transfer [this transfer import?]
    ; See https://docs.tigerbeetle.com/reference/requests/create_transfers
    (letr [{:keys [id
                   flags
                   credit-account-id
                   debit-account-id
                   amount
                   ledger
                   code]} transfer
           extant         (bm/get transfers id)
           user-data      (:user-data transfer 0)
           pending-id     (:pending-id transfer 0N)
           imported?      (:imported flags)
           post?          (:post-pending-transfer flags)
           void?          (:void-pending-transfer flags)
           credit-account (bm/get accounts credit-account-id nil)
           debit-account  (bm/get accounts debit-account-id nil)
           ts             (:timestamp transfer 0)
           pending    (when (not= 0N pending-id)
                        (bm/get transfers pending-id))
           ; Some values come from the pending transfer
           amount (if void?
                    (:amount pending)
                    amount)
           code (or (:code pending) code)
           credit-account-id (or (:credit-account-id pending)
                                 credit-account-id)
           debit-account-id (or (:debit-account-id pending)
                                debit-account-id)
           debit-account  (bm/get accounts debit-account-id)
           credit-account (bm/get accounts credit-account-id)

           _ (when-let [err (or (create-transfer-basic-error
                                  errors import? id flags)
                                (create-transfer-flags-error flags)
                                (create-transfer-extant-error transfer extant)
                                (create-transfer-pending-error
                                  transfer pending-id pending)
                                (create-transfer-*-account-id-error
                                  credit-account-id debit-account-id)
                                (create-transfer-account-exists-error
                                  flags debit-account credit-account)
                                (create-transfer-timestamp-error
                                  timestamp transfer-timestamp import?
                                  credit-account debit-account transfer)
                                (create-transfer-account-error
                                  flags debit-account credit-account)
                                (create-transfer-code-error code)
                                )]

               (return err))

           _ (when (and (not (:pending flags))
                        (< 0 (:timeout transfer 0)))
                 (return :timeout-reserved-for-pending-transfer))

           ; Same-value constraints
           _ (when (not= ledger (:ledger credit-account))
               (return :transfer-must-have-the-same-ledger-as-accounts))

           ; Balancing transfer
           credit-debits (:debits-posted credit-account)
           debit-credits (:credits-posted debit-account)
           credit-credits+ (+ (:credits-posted credit-account)
                              (:credits-pending credit-account))
           debit-debits+   (+ (:debits-posted debit-account)
                              (:debits-pending debit-account))
           amount' (cond-> amount
                     (:balancing-credit flags)
                     (min (max 0 (- credit-debits credit-credits+)))

                     (:balancing-debit flags)
                     (min (max 0 (- debit-credits debit-debits+))))

           ; Account balance constraints. Note that post/void constraints are
           ; enforced on pending txns--we don't check them again, because
           ; post/void can never increase pending + posted, whereas the formula
           ; from the docs, which we use here, would double-count the transfer
           ; amount: once at pending, and again at post/void.
           credit-flags (:flags credit-account)
           debit-flags  (:flags debit-account)
           _ (when (and (not post?)
                        (not void?)
                        (:debits-must-not-exceed-credits debit-flags)
                        (< debit-credits (+ debit-debits+ amount')))
               (return :exceeds-credits))
           _ (when (and (not post?)
                        (not void?)
                        (:credits-must-not-exceed-debits credit-flags)
                        (< credit-debits (+ credit-credits+ amount')))
               (return :exceeds-debits))

           ; Balance overflows
           _ (cond
               (:pending flags)
               (cond
                 (< uint128-max (+ (:debits-pending debit-account) amount'))
                 (return :overflows-debits-pending)

                 (< uint128-max (+ (:credits-pending credit-account) amount'))
                 (return :overflows-credits-pending))

               ; Single-phase or post-pending transfer
               (not void?)
               (cond
                 (< uint128-max (+ (:debits-posted debit-account) amount'))
                 (return :overflows-debits-posted)

                 (< uint128-max (+ (:credits-posted credit-account) amount'))
                 (return :overflows-credits-posted)))
           _ (when-not void?
               (cond
                 (< uint128-max (+ (:debits-pending debit-account)
                                   (:debits-posted debit-account)
                                   amount'))
                 (return :overflows-debits)

                 (< uint128-max (+ (:credits-pending credit-account)
                                   (:credits-posted credit-account)
                                   amount'))
                 (return :overflows-credits)))

           ; OK, we're good to go.
           ; What timestamp did the actual system assign this transfer?
           {:keys [id amount flags]} transfer
           ts (if imported?
                ts
                (bm/get transfer-id->timestamp id 0))
           ; If we don't have a timestamp, it's very likely that this event
           ; succeeds now, but is rolled back due to a later failure in a
           ; chain. We assign it a speculative timestamp and mark the transfer
           ; as :speculative?.
           speculative? (zero? ts)
           ;_  (when (= 1208471N (:id transfer))
           ;     (prn :ts ts
           ;          :imported? imported?
           ;          :speculative? speculative?
           ;          :timestamp timestamp
           ;          :this-timestamp (:timestamp this)
           ;          :transfer-timestamp transfer-timestamp))
           ts           (if speculative?
                          (inc (max timestamp transfer-timestamp))
                          ts)
           transfer     (if speculative?
                          (assoc transfer :speculative? true)
                          transfer)

           ; Timeout overflow
           _ (when (< Long/MAX_VALUE
                      (+ ts (* (:timeout transfer 0) 1000000000N)))
               (return :overflows-timeout))

           ; Advance our clocks to the new timestamp
           this' (advance-transfer-timestamp this ts transfer)
           _     (when (inconsistent? this') (return this'))
           this' (if imported?
                   this'
                   (advance-timestamp this' ts))
           _     (when (inconsistent? this') (return this'))

           ; Fill in transfer
           transfer (assoc transfer
                           :timestamp         ts
                           :code              code
                           :amount            amount'
                           :debit-account-id  debit-account-id
                           :credit-account-id credit-account-id
                           :pending-id        pending-id
                           :timeout           (:timeout transfer 0))
           ; Record transfer
           transfers' (bm/put transfers id transfer)
           ; And if we updated a pending transfer...
           transfers' (if pending
                        (let [pending'
                              (assoc pending :state
                                     (cond void? :voided
                                           post? :posted
                                           true  (assert false "unreachable")))]
                          (bm/put transfers' pending-id pending'))
                        ; Nothing pending
                        transfers')

           ; Record speculative error
           speculative-error' (or speculative-error
                                  (when     speculative?
                                    {:type  :speculative-transfer
                                     :event transfer}))

           ; Update secondary indices
           transfer-index'
           (update-secondary-index transfer-index true transfer)
           transfer-credit-index'
           (update-secondary-index transfer-credit-index
                                   credit-account-id transfer)
           transfer-debit-index'
           (update-secondary-index transfer-debit-index
                                   debit-account-id transfer)
           transfer-code-index'
           (update-secondary-index transfer-code-index code transfer)
           transfer-ledger-index'
           (update-secondary-index transfer-ledger-index ledger transfer)
           transfer-user-data-index'
           (update-secondary-index transfer-user-data-index user-data transfer)

           ; Update accounts
           accounts'
           (-> accounts
               (bm/update debit-account-id
                          transfer-update-account
                          :debit
                          pending
                          amount'
                          flags)
               (bm/update credit-account-id
                          transfer-update-account
                          :credit
                          pending
                          amount'
                          flags))

           ; And historical balances
           historical-balances' (transfer-update-historical-balances
                                  historical-balances ts
                                  (bm/get accounts' debit-account-id)
                                  (bm/get accounts' credit-account-id))]
      (assoc this'
             :accounts                 accounts'
             :transfers                transfers'
             :historical-balances      historical-balances'
             :speculative-error        speculative-error'
             :transfer-index           transfer-index'
             :transfer-debit-index     transfer-debit-index'
             :transfer-credit-index    transfer-credit-index'
             :transfer-code-index      transfer-code-index'
             :transfer-ledger-index    transfer-ledger-index'
             :transfer-user-data-index transfer-user-data-index')))

  (create-accounts-chain [this accounts import?]
    (create-chain this create-account accounts import?))

  (create-transfers-chain [this transfers import?]
    (create-chain this create-transfer transfers import?))

  (create-accounts [this invoke ok]
    (create-helper this invoke ok create-accounts-chain :account))

  (create-transfers [this invoke ok]
    (create-helper this invoke ok create-transfers-chain :transfer))

  (read-account [this id]
    (bm/get accounts id))

  (read-transfer [this id]
    (-> (bm/get transfers id)
        (dissoc :state)))

  (lookup-accounts [this invoke ok]
    (let [ids    (:value invoke)
          actual (:value ok)
          n      (count ids)]
      (assert (= n (count actual)))
      (loop [i 0
             this this]
        (if (= i n)
          (assoc this :op-count (inc op-count))
          (let [id       (nth ids i)
                actual   (nth actual i)
                expected (read-account this id)]
            (if (= expected actual)
              (recur (inc i) (update this :event-count inc))
              (inconsistent
                {:type        :model
                 :op-count    (:op-count this)
                 :event-count (:event-count this)
                 :id          id
                 :expected    expected
                 :actual      actual
                 :diff        (diff-map expected actual)})))))))

  (lookup-transfers [this invoke ok]
    (let [ids     (:value invoke)
          actual  (:value ok)
          n       (count ids)]
      (assert (= n (count actual)))
      (loop [i 0
             this this]
        (if (= i n)
          (assoc this :op-count (inc op-count))
          (let [id      (nth ids i)
                actual  (nth actual i)
                expected (read-transfer this id)]
            (if (= expected actual)
              (recur (inc i) (update this :event-count inc))
              (inconsistent
                {:type        :model
                 :op-count    (:op-count this)
                 :event-count (:event-count this)
                 :id          id
                 :expected    expected
                 :actual      actual
                 :diff        (diff-map expected actual)})))))))

  (query-accounts- [this {:keys [user-data
                                 ledger
                                 code
                                 timestamp-min
                                 timestamp-max
                                 limit
                                 flags]}]
    (-> nil
        (bm-intersection (when user-data
                           (bm/get account-user-data-index
                                   user-data
                                   (bim/int-map))))
        (bm-intersection (when code
                           (bm/get account-code-index code (bim/int-map))))
        (bm-intersection (when ledger
                           (bm/get account-ledger-index ledger (bim/int-map))))
        ; If these constraints left us with the universe, fall back on the
        ; global index
        (or (bm/get account-index true (bim/int-map)))
        ; Timestamp constraints
        (bim-slice timestamp-min timestamp-max)
        ; Linear scan
        (query-scan (partial read-account this)
                    any?
                    limit
                    (:reversed flags))))

  (query-accounts [this invoke ok]
    (let [filter (:value invoke)
          actual (:value ok)
          expected (query-accounts- this filter)]
      (if (= expected actual)
        ; Good
        (assoc this
               :op-count (inc op-count)
               :event-count (inc event-count))
        ; Uh oh
        (inconsistent
          {:type        :model
           :op-count    op-count
           :event-count event-count
           :filter      filter
           :expected    expected
           :actual      actual
           :diff        (diff-map expected actual)}))))

  (query-transfers- [this {:keys [user-data
                                  ledger
                                  code
                                  timestamp-min
                                  timestamp-max
                                  limit
                                  flags]}]
    (-> nil
        (bm-intersection (when user-data
                           (bm/get transfer-user-data-index
                                   user-data
                                   (bim/int-map))))
        (bm-intersection (when code
                           (bm/get transfer-code-index code (bim/int-map))))
        (bm-intersection (when ledger
                           (bm/get transfer-ledger-index ledger (bim/int-map))))
        ; If these constraints left us with the universe, fall back on the
        ; global index.
        (or (bm/get transfer-index true (bim/int-map)))
        ; Timestamp constraints
        (bim-slice timestamp-min timestamp-max)
        ; Linear scan
        (query-scan (partial read-transfer this)
                    any?
                    limit
                    (:reversed flags))))

  (query-transfers [this invoke ok]
    (let [filter   (:value invoke)
          actual   (:value ok)
          expected (query-transfers- this filter)]
      (if (= expected actual)
        ; Good
        (assoc this
               :op-count (inc op-count)
               ; Call this a single event, I guess/
               :event-count (inc event-count))
        ; Uh oh
        (inconsistent
          {:type        :model
           :op-count    op-count
           :event-count event-count
           :filter      filter
           :expected    expected
           :actual      actual
           :diff        (diff-map expected actual)}))))

  (get-account-transfers- [this {:keys [account-id
                                        user-data
                                        code
                                        timestamp-min
                                        timestamp-max
                                        limit
                                        flags]}]
    ; Begin our search with the involved accounts
    (-> (cond-> (bim/int-map)
          (:debits flags)
          (bm-union (bm/get transfer-debit-index account-id))
          (:credits flags)
          (bm-union (bm/get transfer-credit-index account-id)))
        ; Restrict by user-data
        (bm-intersection
          (when user-data
            (bm/get transfer-user-data-index user-data (bim/int-map))))
        ; Restrict by code
        (bm-intersection
          (when code (bm/get transfer-code-index code (bim/int-map))))
        ; Restrict by timestamp
        (bim-slice timestamp-min timestamp-max)
        ; Linear scan
        (query-scan (partial read-transfer this)
                    any?
                    limit
                    (:reversed flags))))

  (get-account-transfers [this invoke ok]
    (let [account-filter (:value invoke)
          expected       (get-account-transfers- this account-filter)
          actual         (:value ok)
          n              (count expected)]
      ; Compare expected to actual
      (if (= expected actual)
        ; Good
        (assoc this
                :op-count (inc op-count)
                ; Call this a single event, I guess?
                :event-count (inc event-count))
        ; Uh oh
        (inconsistent
          {:type        :model
           :op-count    op-count
           :event-count event-count
           :filter      account-filter
           :expected    expected
           :actual      actual
           :diff        (diff-map expected actual)}))))

  (get-account-balances- [this {:keys [account-id
                                       user-data
                                       code
                                       timestamp-min
                                       timestamp-max
                                       limit
                                       flags]
                                :as account-filter}]
    ; This looks almost exactly like get-account-transfers, but we can't use
    ; that directly because the linear scan where we limit results would limit
    ; *transfers*, not *balances*.
    ;
    ; Begin our search with the involved accounts
    (let [pool
          (-> (cond-> (bim/int-map)
                (:debits flags)
                (bm-union (bm/get transfer-debit-index account-id))
                (:credits flags)
                (bm-union (bm/get transfer-credit-index account-id)))
              ; Restrict by user-data
              (bm-intersection
                (when user-data
                  (bm/get transfer-user-data-index user-data (bim/int-map))))
              ; Restrict by code
              (bm-intersection
                (when code
                  (bm/get transfer-code-index code (bim/int-map))))
              ; Restrict by timestamp
              (bim-slice timestamp-min timestamp-max))
          ; Linear scan. This is pretty close to query-scan, but that uses the
          ; IDs and we actually need the timestamps.
          n     (b/size pool)
          reversed (:reversed flags)
          dir   (if reversed -1 1)
          start (if reversed (dec n) 0)
          ; We're only interested in balances for this single account
          balances (bm/get historical-balances account-id (bim/int-map))]
      (loop [i       start
             results (transient [])]
        (cond ; Full
              (= (count results) limit)
              (persistent! results)

              ; Out of bounds
              (not (< -1 i n))
              (persistent! results)

              ; Check this timestamp
              true
              (let [i'    (+ i dir)
                    ts    (bm/key (b/nth pool i))
                    ; Find the corresponding balance
                    balance (bim/get balances ts)]
                (if balance
                  (recur i' (conj! results balance))
                  (recur i' results)))))))

  (get-account-balances [this invoke ok]
    (let [account-filter (:value invoke)
          expected (get-account-balances- this account-filter)
          actual    (:value ok)
          n         (count expected)]
      ; Compare expected to actual
      (if (= expected actual)
        ; Good
        (assoc this
               :op-count (inc op-count)
               :event-count (inc event-count))
        ; Uh oh
        (inconsistent
          {:type        :get-account-balances
           :op-count    op-count
           :event-count event-count
           :filter      account-filter
           :expected    expected
           :actual      actual
           :diff        (diff-map expected actual)}))))

  IModel
  (step [this invoke ok]
    ;(info "Model applying" (:f invoke) (:value invoke) (:value ok))
    ;(info "Model is" (pr-str this))
    (let [this' (case (:f invoke)
                  :create-accounts       (create-accounts this invoke ok)
                  :create-transfers      (create-transfers this invoke ok)
                  :lookup-accounts       (lookup-accounts this invoke ok)
                  :lookup-transfers      (lookup-transfers this invoke ok)
                  :query-accounts        (query-accounts this invoke ok)
                  :query-transfers       (query-transfers this invoke ok)
                  :get-account-transfers (get-account-transfers this invoke ok)
                  :get-account-balances  (get-account-balances this invoke ok)
                  )]
      (if (inconsistent? this')
        (assoc this'
               :op invoke
               :op' ok)
        this')))

  (debug [this]
    (->DebugTB this)))

(def-derived-map DebugTB [^TB m]
  :op-count           (.op-count m)
  :event-count        (.event-count m)
  :timestamp          (.timestamp m)
  :account-timestamp  (.account-timestamp m)
  :transfer-timestamp (.transfer-timestamp m)
  :accounts           (into (sorted-map) (datafy (.accounts m)))
  :transfers          (into (sorted-map) (datafy (.transfers m))))

(defn init
  "Constructs an initial model state. Takes two Bifurcan maps of account ID ->
  timestamp and transfer ID -> timestamp."
  [{:keys [account-id->timestamp
           transfer-id->timestamp]}]
  (assert (instance? IMap account-id->timestamp))
  (assert (instance? IMap transfer-id->timestamp))
  (map->TB {:op-count                      0
            :event-count                   0
            :account-id->timestamp         account-id->timestamp
            :transfer-id->timestamp        transfer-id->timestamp
            :timestamp                     -1
            :account-timestamp             -1
            :transfer-timestamp            -1
            :accounts                      bm/empty
            :transfers                     bm/empty
            :historical-balances           bm/empty
            :errors                        bm/empty
            :speculative-error             nil
            :account-index                 bm/empty
            :account-code-index            bm/empty
            :account-ledger-index          bm/empty
            :account-user-data-index       bm/empty
            :transfer-index                bm/empty
            :transfer-code-index           bm/empty
            :transfer-ledger-index         bm/empty
            :transfer-debit-index          bm/empty
            :transfer-credit-index         bm/empty
            :transfer-user-data-index      bm/empty
            }))
