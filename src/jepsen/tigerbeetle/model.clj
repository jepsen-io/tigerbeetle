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
                     [pprint :refer [pprint]]]
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

(definterface+ ITB
  (advance-timestamp [model timestamp]
                     "Advances the internal timestamp of this model to
                     `timestamp`. Constructs an invalid state if the timestamp
                     is not strictly monotonic.")

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
      (if (= i n)
        (persistent! chains)
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

(defn create-chain
  "Shared logic for creating a single chain of accounts or transfers. Takes a
  model, a list of events, whether this is an import or not, and a function
  `(create this event)` which returns either a new model state (either
  inconsistent, or implicitly an :ok result), or an error keyword.

  Returns a tuple of [model' expected-results] from applying this particular
  chain."
  [model events import? create]
  ; Note that *every* response to creating a chain of accounts is either
  ; entirely :ok, or entirely :linked-event-failed with the exception of a
  ; single error.
  (let [results (object-array (count events))]
    (loopr [i       0
            model'   model]
           [event events]
           (let [result (create model' event import?)]
             (cond
               ; Logical error; fail chain and return model unchanged.
               (keyword? result)
               (do (Arrays/fill results :linked-event-failed)
                   (aset results i result)
                   [model (bl/from-array results)])

               ; Inconsistent; abort here. No point computing results.
               (inconsistent? result)
               [result nil]

               ; Good, move on
               true
               (recur (inc i) result)))
           ; Completed successfully
           (do (Arrays/fill results :ok)
               [model' (bl/from-array results)]))))

(defn create-helper
  "Common logic for create-transfers and create-accounts. Takes a model, an
  invoke, an OK, a function (create-chain model chain import?) which applies
  a single chain of events to the model, and a name (e.g. :account) for what
  we call a single event, used in the error map. Returns model'."
  [this invoke ok create-chain event-name]
  (let [events (:value invoke)
        actual (:value ok)
        ; Are we doing a batch import?
        import? (when-let [t (first events)]
                  (contains? (:flags t) :imported))]
    ; Zip through chains, applying each
    (loopr [this    this                 ; The model
            results (b/linear bl/empty)] ; Our expected results list
           ; TODO: we have to figure out how to handle linked-event-chain-open
           [chain (chains events)]
           ; Apply this chain
           (let [[this chain-results] (create-chain
                                        this chain import?)]
             (if (inconsistent? this)
               this
               (recur this (bl/concat results chain-results))))
           ; Done applying chains. Validate.
           (if-let [i (first-not=-index results actual)]
             (inconsistent
               {:type      :model
                :op        invoke
                :op'       ok
                event-name (nth events i)
                :expected  (b/nth results i)
                :actual    (nth actual i)})
             ; OK!
             this))))

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
   ^long timestamp ; Our internal timestamp
   accounts  ; A map of account IDs to accounts
   transfers ; A map of transfer IDs to transfers
   ]

  ITB
  (advance-timestamp [this ts]
    (if (< timestamp ts)
      (assoc this :timestamp ts)
      (inconsistent {:type :nonmonotonic-timestamp
                     :timestamp  timestamp
                     :timestamp' ts})))

  (create-account [this account import?]
    (let [id     (:id account)
          extant (bm/get accounts id)]
      (cond
        ; See https://docs.tigerbeetle.com/reference/requests/create_accounts#result
        (and import? (not (:imported (:flags account))))
        :imported-event-expected

        (and (not import?) (:imported (:flags account)))
        :imported-event-not-expected

        (and (not import?) (not (zero? (:timestamp account 0))))
        :timestamp-must-be-zero

        (and import? (not (< 0 (:timestamp account 0) timestamp-upper-bound)))
        :imported-event-timestamp-out-of-range

        (and import? (< timestamp (:timestamp account)))
        :imported-event-timestamp-must-not-advance

        ; TODO: we don't presently represent these
        ; :reserved-field
        ; :reserved-flag

        (= 0 (:id account))
        :id-must-not-be-zero

        (= uint128-max (:id account))
        :id-must-not-be-int-max

        extant
        (cond
          (not= (:flags extant) (:flags account))
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

        (and (:debits-must-not-exceed-credits (:flags account))
             (:credits-must-not-exceed-debits (:flags account)))
        :flags-are-mutually-exclusive

        (when-let [p (:debits-pending account)] (not = 0 p))
        :debits-pending-must-be-zero

        (when-let [p (:debits-posted account)] (not= 0 p))
        :debits-posted-must-be-zero

        (when-let [p (:credits-pending account)] (not= 0 p))
        :credits-pending-must-be-zero

        (when-let [p (:credits-posted account)] (not= 0 p))
        :credits-posted-must-be-zero

        (= 0 (:ledger account))
        :ledger-must-not-be-zero

        true
        ; OK, go! What timestamp did the actual system assign this account?
        (letr [ts      (bm/get account-id->timestamp id)
               _       (assert ts (str "No timestamp known for account " id))
               ; Advance our clock to the new timestamp
               this' (advance-timestamp this ts)
               _ (when (inconsistent? this')
                   ; Timestamp error
                   (return this'))
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
                   debit-account-id]} transfer
           extant (bm/get transfers id)
           _ (when (and import? (not (:imported flags)))
               (return :imported-event-expected))
           _ (when (and (not import?) (:imported flags))
               (return :imported-event-not-expected))
           _ (when (and (not import?) (not (zero? (:timestamp transfer 0))))
               (return :timestamp-must-be-zero))

           ledger (:ledger transfer)
           amount (:amount transfer)
           code   (:code transfer)

           ; The whole post/void dance
           pending-id (:pending-id transfer)
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
          ts (bm/get transfer-id->timestamp id)
          _  (assert ts (str "No timestamp known for transfer " id))
          ; Advance our clock to the new timestamp
          this' (advance-timestamp this ts)
          _ (when (inconsistent? this')
              ; Clock nonmonotonic!
              (return this'))

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
    (create-chain this accounts import? create-account))

  (create-transfers-chain [this transfers import?]
    (create-chain this transfers import? create-transfer))

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
            :accounts  bm/empty
            :transfers bm/empty}))
