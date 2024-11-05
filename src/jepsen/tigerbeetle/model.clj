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
            [clojure [datafy :refer [datafy]]]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [loopr]]
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

  (create-account-result [model account import?]
                         "Computes the result code of adding this account to
                         the given model. Does not actually add it.")

  (create-account [model account]
                  "Actually adds an account to a model, returning the new
                  model.")

  (create-accounts-chain [model accounts import?]
                         "Creates a series of accounts in a single chain. All
                         operations succeed or fail as a unit. Returns
                         [model', results].")

  (create-accounts [model invoke ok]
                   "Applies a single create-accounts operation to the model,
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

  (create-account-result [this account import?]
    (let [id     (:id account)
          extant (bm/get accounts id)]
      (cond
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
        :ok)))

  (create-account [this account]
    ; What timestamp did the actual system assign this account?
    (let [ts      (bm/get account-id->timestamp (:id account))
          account (assoc account :timestamp ts)
          ; Advance our clock to the new account
          this' (advance-timestamp this ts)]
      (if (inconsistent? this')
        this'
        ; Good, we were able to advance to this time. Add the account.
        (assoc this' :accounts (bm/put accounts (:id account) account)))))

  (create-accounts-chain [this accounts import?]
    ; Note that *every* response to creating a chain of accounts is either
    ; entirely :ok, or entirely :linked-event-failed with the exception of a
    ; single error.
    (let [results (object-array (count accounts))]
      (loopr [i       0
              this'   this]
             [account accounts]
             (let [result (create-account-result this account import?)]
               (if (= :ok result)
                 (recur (inc i)
                        (create-account this' account))
                 ; Failed! Early return time
                 (do (Arrays/fill results :linked-event-failed)
                     (aset results i result)
                     [this (bl/from-array results)])))
             ; Completed successfully
             (do (Arrays/fill results :ok)
                 [this' (bl/from-array results)]))))

  (create-accounts [this invoke ok]
    (let [accounts (:value invoke)
          actual   (:value ok)
          ; Are we doing a batch import?
          import? (when-let [a (first accounts)]
                    (contains? (:flags a) :imported))]
      ; Zip through chains, applying each
      (loopr [this      this                 ; The model
              results   (b/linear bl/empty)] ; Our results List
             [chain (chains accounts)]
             ; Apply this chain
             (let [[this chain-results] (create-accounts-chain
                                          this chain import?)]
               (recur this (bl/concat results chain-results)))
             ; Done applying chains. Validate.
             (if (iterable= results actual)
               this
               (inconsistent
                 {:type     :results-mismatch
                  :op       invoke
                  :op'      ok
                  :expected (datafy results)
                  :actual   actual
                  ;:model    this
                  })))))

  IModel
  (step [this invoke ok]
        (case (:f invoke)
          :create-accounts
          (create-accounts this invoke ok))))

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
