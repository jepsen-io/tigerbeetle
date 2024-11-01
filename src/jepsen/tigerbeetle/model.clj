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
                          [set :as bs]]
            [dom-top.core :refer [loopr]]
            [jepsen.history :as h]
            [potemkin :refer [definterface+]]))

(definterface+ IModel
  (step [this invoke ok]
        "Takes a model, an invoke op, and a corresponding OK op. Returns a new
        model."))

(defrecord Inconsistent [message]
  IModel
  (step [this invoke ok] this)

  Object
  (toString [this] (pr-str message)))

(defn inconsistent
  "Represents inconsistent termination of a model."
  [message]
  (Inconsistent. message))

(definterface+ ITB
  (advance-timestamp [model timestamp]
                     "Advances the internal timestamp of this model to
                     `timestamp`. Constructs an invalid state if the timestamp
                     is not strictly monotonic.")

  (create-account-result [model account import?]
                         "Computes the result code of adding this account to
                         the given model. Does not actually add it.")

  (create-account [model account]
                  "Actually adds an account to a model, returning the new
                  model.")

  (create-accounts [model invoke ok]
                   "Applies a single create-accounts operation to the model,
                   returning model'"))

(def timestamp-upper-bound
  "One bigger than the biggest timestamp for an imported event (2^63)"
  9223372036854775808)

(def uint128-max
  "2^128 - 1"
  340282366920938463463374607431768211455N)

(defrecord TB
  [; A pair of maps of account/transfer ID to timestamp, derived from the
   ; observed history. We use these to advance time synthetically throughout
   ; the simulation.
   account-id->timestamp
   transfer-id->timestamp
   timestamp ; Our internal timestamp
   accounts  ; A map of account IDs to accounts
   transfers ; A map of transfer IDs to transfers
   ]

  ITB
  (advance-timestamp [this ts]
    (if (< timestamp ts)
      (assoc this :timestamp ts)
      (invalid {:type :nonmonotonic-timestamp
                :timestamp  timestamp
                :timestamp' ts})))

  (create-account-result [this account import?]
    (let [id     (:id account)
          extant (bm/get accounts id)]
      (cond
        (and import? (not (:import (:flags account))))
        :imported-event-expected

        (and (not import?) (:import (:flags account)))
        :imported-event-not-expected

        (and (not import?) (not (zero? (:timestamp account))))
        :timestamp-must-be-zero

        (and import? (not (< 0 (:timestamp account) timestamp-upper-bound)))
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
        :ok)))

  (create-account [this account]
    ; What timestamp did the actual system assign this account?
    (let [ts      (bm/get account-id->timestamp (:id account))
          account (assoc account :timestamp ts)]
      ; Advance our clock and store the account
      (-> this
          (advance-timestamp ts)
          (assoc :accounts (bm/put accounts (:id account) account)))))

  (create-accounts [this invoke ok]
    (let [accounts (:value invoke)
          results  (:value ok)
          ; Are we doing a batch import?
          import? (when-let [a (first accounts)]
                    (contains? (:flags a) :imported))]
      ; Zip through accounts/results, adding each in turn
      (loop [this this      ; The model
             i    0         ; The index of the event we're processing
             ; The index of the first event in this chain. Nil if not in chain.
             first-chain-i nil
             ; Has this chain already failed?
             chain-failed? false]
        (if (= i n)
          this ; Done
          ; Process account
          (let [account  (nth accounts i)
                result   (nth results i)
                ; What result do we expect from adding this account?
                expected (if chain-failed?
                           :linked-event-failed
                           (create-account-result this account import?))
                ; Did we start a chain?
                linked?    (:linked (:flags account))
                new-chain? (and linked? (nil? first-chain-i))
                ; If we did start a new chain, record where we started
                first-chain-i (if new-chain? i first-chain-i)
                ; Chain failure state resets every time a chain is completed
                chain-failed? (if linked? chain-failed? false)]
            (cond
              ; Our chain has failed.
              chain-failed?
              ; HERE I GUESS

              ; This is fine. Go ahead and add it.
              (= expected match)
              (recur (create-account this account)
                     (inc i)



  IModel
  (step [this, ^Op invoke, ^Op ok]
        (case (:f invoke)
          :create-accounts
          (create-accounts this invoke ok)))))
