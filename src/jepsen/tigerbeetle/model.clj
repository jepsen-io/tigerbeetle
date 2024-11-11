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
                          [list :as bl]
                          [map :as bm]
                          [set :as bs]
                          [util :refer [iterable=]]]
            [clojure [datafy :refer [datafy]]
                     [pprint :refer [pprint]]
                     [set :as set]]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [letr loopr]]
            [jepsen.history :as h]
            [potemkin :refer [definterface+]])
  (:import (java.util Arrays)
           (jepsen.history Op)
           (io.lacuna.bifurcan IMap)))

(definterface+ IModel
  (step [this invoke ok]
        "Takes a model, an invoke op, and a corresponding OK op. Returns a new
        model."))

(defrecord Inconsistent [message]
  IModel
  (step [this invoke ok] this))

(defn inconsistent
  "Represents inconsistent termination of a model."
  [message]
  (Inconsistent. message))

(defn inconsistent?
  "Is this model inconsistent?"
  [model]
  (instance? Inconsistent model))

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

  (advance-account-timestamp [model ^long timestamp]
                             "Advances the account-specific timestamp.")

  (advance-transfer-timestamp [model ^long timestamp]
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

  (create-accounts [model invoke ok]
                   "Applies a single create-accounts operation to the model,
                   returning model'")

  (create-transfers [model invoke ok]
                    "Applies a single create-transfers operation to the model,
                    returning model'")

  (read-account [model id]
                "Reads an account by ID, returning account or nil.")

  (lookup-accounts [model invoke ok]
                   "Applies a single lookup-accounts operation to the model,
                   returning model'"))

(def timestamp-upper-bound
  "One bigger than the biggest timestamp for an imported event (2^63)"
  9223372036854775808)

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

(defn fill-list
  "Constructs a Bifurcan list of n elements of x."
  [n x]
  (let [a (object-array n)]
    (Arrays/fill a x)
    (bl/from-array a)))

(defn remember-error
  "Some 'transient' errors are remembered for later. Takes a model, an event
  with an ID, and an error keyword. Returns a model."
  [model event error]
  (if (contains? permanent-errors error)
    (update model :errors bm/put (:id event) error)
    model))

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
  ; entirely :ok, or entirely :linked-event-failed with the exception of a
  ; single error.
  (let [n       (count events)
        results (object-array n)]
    (loopr [i       0
            model'   model]
           [event events]
           (let [result (if (and (= i (dec n))
                                 (:linked (:flags event)))
                          :linked-event-chain-open
                          (create model' event import?))]
             (cond
               ; Logical error; fail chain and return model without having
               ; applied ops.
               (keyword? result)
               (do (Arrays/fill results :linked-event-failed)
                   (aset results i result)
                   [(remember-error model event result)
                    (bl/from-array results)])

               ; Inconsistent; abort here. No point computing results.
               (inconsistent? result)
               [result nil]

               ; Good, move on
               true
               (recur (inc i) result)))
           ; Completed successfully
           (do (Arrays/fill results :ok)
               [model' (bl/from-array results)]))))

(defn validate
  "Takes a model, an invoke, an OK, an event name (e.g. :transfer or :event), a
  vector of events, a Bifurcan list of expected results, and a vector of actual
  results. Returns model if the results match, or an inconsistent state."
  [model invoke ok event-name events expected actual]
  (if-let [i (first-not=-index expected actual)]
    (inconsistent
      {:type      :model
       :op        invoke
       :op'       ok
       event-name (nth events i)
       :expected  (b/nth expected i)
       :actual    (nth actual i)})
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
           (validate this invoke ok event-name events expected actual))))

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

(defn transfer-flags-error
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

(defn transfer-update-account
  "Called to update a single account for a transfer. Take an account, a mode
  (:debit or :credit), an amount, and the transfer flags."
  [account mode pending-amount amount flags]
  (assert account)
  (case mode
    :debit
    (cond
      (:pending flags)
      (update account :debits-pending + amount)

      (:post-pending-transfer flags)
      (-> account
          (update :debits-pending - pending-amount)
          (update :debits-posted + amount))

      (:void-pending-transfer flags)
      (update account :debits-pending - pending-amount)

      true
      (update account :debits-posted + amount))

    :credit
    (cond
      (:pending flags)
      (update account :credits-pending + amount)

      (:post-pending-transfer flags)
      (-> account
          (update :credits-pending - pending-amount)
          (update :credits-posted + amount))

      (:void-pending-transfer flags)
      (update account :credits-pending - pending-amount)

      true
      (update account :credits-posted + amount))))

(defrecord TB
  [; A pair of maps of account/transfer ID to timestamp, derived from the
   ; observed history. We use these to advance time synthetically throughout
   ; the simulation.
   account-id->timestamp
   transfer-id->timestamp
   ^long timestamp          ; Our internal timestamp
   ^long account-timestamp  ; The last account timestamp created
   ^long transfer-timestamp ; The last transfer timestamp created
   accounts  ; A map of account IDs to accounts
   transfers ; A map of transfer IDs to transfers
   errors    ; A map of permanent errors (TigerBeetle calls these *transient*
             ; errors, but they persist forever
   ]

  ITB
  (advance-timestamp [this ts]
    (if (< timestamp ts)
      (assoc this :timestamp ts)
      (inconsistent {:type :nonmonotonic-timestamp
                     :timestamp  timestamp
                     :timestamp' ts})))

  (advance-account-timestamp [this ts]
    (if (< account-timestamp ts)
      (assoc this :account-timestamp ts)
      (inconsistent {:type :nonmonotonic-account-timestamp
                     :account-timestamp account-timestamp
                     :timestamp' ts})))

  (advance-transfer-timestamp [this ts]
    (if (< transfer-timestamp ts)
      (assoc this :transfer-timestamp ts)
      (inconsistent {:type :nonmonotonic-transfer-timestamp
                     :transfer-timestamp transfer-timestamp
                     :timestamp' ts})))

  (create-account [this account import?]
    (let [id        (:id account)
          extant    (bm/get accounts id)
          flags     (:flags account)
          imported? (:imported flags)
          atimestamp (:timestamp account 0)]
      (cond
        ; See https://docs.tigerbeetle.com/reference/requests/create_accounts#result
        (and import? (not imported?))
        :imported-event-expected

        (and (not import?) imported?)
        :imported-event-not-expected

        (and (not import?) (not (zero? atimestamp)))
        :timestamp-must-be-zero

        (and import? (not (< 0 atimestamp timestamp-upper-bound)))
        :imported-event-timestamp-out-of-range

        (and import? (< timestamp atimestamp))
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

          ; We don't do 64/32, just 128
          (not= (:user-data extant) (:user-data account))
          :exists-with-different-user-data-128

          (not= (:ledger extant) (:ledger account))
          :exists-with-different-ledger

          (not= (:code extant) (:code account))
          :exists-with-different-code

          true
          :exists)

        (and (:debits-must-not-exceed-credits flags)
             (:credits-must-not-exceed-debits flags))
        :flags-are-mutually-exclusive

        (when-let [p (:debits-pending account)] (not= 0 p))
        :debits-pending-must-be-zero

        (when-let [p (:debits-posted account)] (not= 0 p))
        :debits-posted-must-be-zero

        (when-let [p (:credits-pending account)] (not= 0 p))
        :credits-pending-must-be-zero

        (when-let [p (:credits-posted account)] (not= 0 p))
        :credits-posted-must-be-zero

        (= 0 (:ledger account))
        :ledger-must-not-be-zero

        (= 0 (:code account))
        :code-must-not-be-zero

        true
        ; OK, go! What timestamp did the actual system assign this account?
        (letr [ts      (if imported?
                         (:timestamp account)
                         (bm/get account-id->timestamp id))
               _       (assert ts (str "No timestamp known for account " id))
               ; Advance clocks to the new timestamp
               this' (advance-account-timestamp this ts)
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
                              :timestamp ts)]
          (assoc this' :accounts (bm/put accounts id account))))))

  (create-transfer [this transfer import?]
    ; See https://docs.tigerbeetle.com/reference/requests/create_transfers
    ; TODO: this is incomplete; we have to go through the whole list of errors
    ; and test them all
    (letr [{:keys [id
                   flags
                   credit-account-id
                   debit-account-id
                   pending-id
                   amount
                   user-data
                   ledger
                   code]} transfer
           extant (bm/get transfers id)

           ; Did this already fail?
           _ (when-let [error (bm/get errors id)]
               (return :id-already-failed))

           ; Basic checks
           _ (when (= 0N id)
               (return :id-must-not-be-zero))
           _ (when (= uint128-max id)
               (return :id-must-not-be-int-max))
           _ (when-let [err (transfer-flags-error flags)]
               (return err))

           ; Extant checks
          _ (when extant
              (return
                (cond
                  ; See
                  ; https://docs.tigerbeetle.com/reference/requests/create_transfers/#exists
                  ; -- we probably have to be more particular about
                  ; balancing/post-pending transfers
                  (not= flags (:flags extant))
                  :exists-with-different-flags

                  (not= pending-id (:pending-id extant))
                  :exists-with-different-pending-id

                  (not= (:timeout transfer) (:timeout extant))
                  :exists-with-different-timeout

                  (not= credit-account-id (:credit-account-id extant))
                  :exists-with-different-credit-account-id

                  (not= debit-account-id (:debit-account-id extant))
                  :exists-with-different-debit-account-id

                  (not= amount (:amount extant))
                  :exists-with-different-amount

                  (not= user-data (:user-data extant))
                  :exists-with-different-user-data-128

                  (not= ledger (:ledger extant))
                  :exists-with-different-ledger

                  (not= code (:code extant))
                  :exists-with-different-code

                  true
                  :exists)))

           ; Import checks
           imported? (:imported flags)
           _ (when (and import? (not imported?))
               (return :imported-event-expected))
           _ (when (and (not import?) imported?)
               (return :imported-event-not-expected))

           ; Timestamp checks
           ts (:timestamp transfer 0)
           _ (when (and (not import?) (not (zero? ts)))
               (return :timestamp-must-be-zero))
           _ (when (and import? (not (< 0 ts timestamp-upper-bound)))
               (return :imported-event-timestamp-out-of-range))
           _ (when (and import? (< timestamp ts))
               (return :imported-event-timestamp-must-not-advance))
           _ (when (and import? (<= ts transfer-timestamp))
               (return :imported-event-timestamp-must-not-regress))

           ; The whole post/void dance
           pending    (when pending-id
                        (or (bm/get transfers pending-id)
                            (return :pending-transfer-not-found)))
           _ (when pending
               (letr [_ (when (not (:pending (:flags pending)))
                          (return :pending-transfer-not-pending))
                      _ (when (and (not= credit-account-id 0)
                                   (not= credit-account-id
                                         (:credit-account-id pending)))
                          (return :pending-transfer-has-different-credit-account-id))
                      _ (when (and (not= debit-account-id 0)
                                   (not= debit-account-id
                                         (:debit-account-id pending)))
                          (return :pending-transfer-has-different-debit-account-id))
                      _ (when (and (not= ledger 0)
                                   (not= ledger (:ledger pending)))
                          (return :pending-transfer-has-different-ledger))
                      _ (when (and (not= code 0)
                                   (not= code (:code pending)))
                          (return :pending-transfer-has-different-code))
                      _ (when (< (:amount pending) amount)
                          (return :exceeds-pending-transfer-amount))
                      void? (:void-pending-transfer flags)
                      _ (when (and void?
                                   (not= amount 0N)
                                   (not= amount (:amount pending)))
                          (return :pending-transfer-has-different-amount))]))

           ; Fetch accounts
           credit-account-id (or (:credit-account-id pending)
                                 credit-account-id)
           debit-account-id (or (:debit-account-id pending)
                                debit-account-id)
           credit-account (or (bm/get accounts credit-account-id)
                              (return :credit-account-not-found))
           debit-account (or (bm/get accounts debit-account-id)
                             (return :debit-account-not-found))

           _ (when (= credit-account-id debit-account-id)
               (return :accounts-must-be-different))

           ; Same-value constraints
           ledger (:ledger credit-account)
           _ (when (not= ledger (:ledger debit-account))
               (return :accounts-must-have-the-same-ledger))
           _ (when (not= ledger (:ledger transfer))
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

           ; Account balance constraints
           credit-flags (:flags credit-account)
           debit-flags  (:flags debit-account)
           _ (when (and (:credits-must-not-exceed-debits credit-flags)
                        (< credit-debits (+ credit-credits+ amount')))
               (return :exceeds-debits))
           _ (when (and (:debits-must-not-exceed-credits debit-flags)
                        (< debit-credits (+ debit-debits+ amount')))
               (return :exceeds-credits))

          ; OK, we're good to go.
          ; What timestamp did the actual system assign this transfer?
          {:keys [id amount flags]} transfer
          ts (if imported?
               ts
               (bm/get transfer-id->timestamp id))
          _  (assert ts (str "No timestamp known for transfer " id))
          ; Advance our clocks to the new timestamp
          this' (advance-transfer-timestamp this ts)
          _     (when (inconsistent? this') (return this'))
          this' (if imported?
                  this'
                  (advance-timestamp this' ts))
          _     (when (inconsistent? this') (return this'))

          ; Fill in transfer
          transfer (assoc transfer
                          :timestamp ts
                          :amount amount')
          ; Record transfer
          transfers' (bm/put transfers id transfer)

          ; Update accounts
          accounts'
          (-> accounts
              (bm/update debit-account-id
                         transfer-update-account
                         :debit
                         (:amount pending)
                         amount'
                         flags)
              (bm/update credit-account-id
                         transfer-update-account
                         :credit
                         (:amount pending)
                         amount'
                         flags))]
      (assoc this'
             :accounts accounts'
             :transfers transfers')))

  (create-accounts-chain [this accounts import?]
    (create-chain this create-account accounts import?))

  (create-transfers-chain [this transfers import?]
    (create-chain this create-transfer transfers import?))

  (create-accounts [this invoke ok]
    (create-helper this invoke ok create-accounts-chain :account))

  (create-transfers [this invoke ok]
    (create-helper this invoke ok create-transfers-chain :transfer))

  (read-account [this id]
    ; TODO: compute balances
    (bm/get accounts id))

  (lookup-accounts [this invoke ok]
    (let [ids    (:value invoke)
          actual (:value ok)
          n      (count ids)]
      (loop [i 0]
        (if (= i n)
          this
          (let [id       (nth ids i)
                actual   (nth actual i)
                expected (read-account this id)]
            (if (= expected actual)
              (recur (inc i))
              (inconsistent
                {:type :model
                 :op        invoke
                 :op'       ok
                 :id        id
                 :expected  expected
                 :actual    actual})))))))

  IModel
  (step [this invoke ok]
        (case (:f invoke)
          :create-accounts  (create-accounts this invoke ok)
          :create-transfers (create-transfers this invoke ok)
          :lookup-accounts  (lookup-accounts this invoke ok)
        )))

(defn init
  "Constructs an initial model state. Takes two Bifurcan maps of account ID ->
  timestamp and transfer ID -> timestamp."
  [{:keys [account-id->timestamp
           transfer-id->timestamp]}]
  (assert (instance? IMap account-id->timestamp))
  (assert (instance? IMap transfer-id->timestamp))
  (map->TB {:account-id->timestamp account-id->timestamp
            :transfer-id->timestamp transfer-id->timestamp
            :timestamp -1
            :account-timestamp -1
            :transfer-timestamp -1
            :accounts  bm/empty
            :transfers bm/empty
            :errors    bm/empty}))
