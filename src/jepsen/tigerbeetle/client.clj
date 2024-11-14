(ns jepsen.tigerbeetle.client
  "Wrapper for the Java TigerBeetle client."
  (:require [bifurcan-clj [core :as b]
                          [map :as bm]]
            [camel-snake-kebab.core :refer [->kebab-case-keyword
                                           ->PascalCase
                                           ->SCREAMING_SNAKE_CASE]]
            [clojure [datafy :refer [datafy]]]
            [clojure.core.protocols :refer [Datafiable]]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [loopr]]
            [jepsen [util :as util :refer [timeout]]]
            [jepsen.control.net :as cn]
            [jepsen.tigerbeetle [core :refer [cluster-id port]]]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (com.tigerbeetle AccountFlags
                            AccountBatch
                            Client
                            CreateAccountResult
                            CreateAccountResultBatch
                            CreateTransferResult
                            CreateTransferResultBatch
                            IdBatch
                            UInt128
                            TransferBatch
                            TransferFlags)
           (java.util Arrays)))

; Serialization

(defmacro defenum
  "Takes a prefix like 'transfer-result' and a class like CreateTransferResult.
  Defines a pair of functions: transfer-result->keyword and
  transfer-result->enum which map back and forth between enum and
  keyword representations."
  [prefix enum-class]
  (let [; this will surely not come back to bite me later at, say,
        ; AOT compilation
        enums (eval `(. ~enum-class values))]
    `(do
       (defn ~(symbol (str prefix "->keyword"))
         [enum#]
         (case (.ordinal enum#)
           ~@(mapcat (fn [enum]
                       [(.ordinal enum)
                        (->kebab-case-keyword (.name enum))])
                     enums)))
       (defn ~(symbol (str prefix "->enum"))
         [kw#]
         (case kw#
           ~@(mapcat (fn [enum]
                       [(->kebab-case-keyword (.name enum))
                        (symbol
                          (str enum-class "/"
                               (->PascalCase (.name enum))))])
                     enums))))))

(defenum create-account-result  com.tigerbeetle.CreateAccountResult)
(defenum create-transfer-result com.tigerbeetle.CreateTransferResult)

(defn ^int account-flags->int
  "Turns a collection of keyword flags into a bitmask"
  [flags]
  (loopr [packed 0]
         [flag flags]
         (recur
           (bit-or
             packed
             (case flag
               :closed                         AccountFlags/CLOSED
               :credits-must-not-exceed-debits AccountFlags/CREDITS_MUST_NOT_EXCEED_DEBITS
               :debits-must-not-exceed-credits AccountFlags/DEBITS_MUST_NOT_EXCEED_CREDITS
               :history                        AccountFlags/HISTORY
               :imported                       AccountFlags/IMPORTED
               :linked                         AccountFlags/LINKED)))))

(defn int->account-flags
  "Turns an int into a set of flags"
  [^long flags]
  (persistent!
    (cond-> (transient (hash-set))
      (AccountFlags/hasClosed                     flags) (conj! :closed)
      (AccountFlags/hasCreditsMustNotExceedDebits flags) (conj! :credits-must-not-exceed-debits)
      (AccountFlags/hasDebitsMustNotExceedCredits flags) (conj! :debits-must-not-exceed-credits)
      (AccountFlags/hasHistory                    flags) (conj! :history)
      (AccountFlags/hasImported                   flags) (conj! :imported)
      (AccountFlags/hasLinked                     flags) (conj! :linked))))

(def transfer-flag->int
  "A map that turns a single transfer flag into an int (and vice-versa)"
  {:balancing-credit      TransferFlags/BALANCING_CREDIT
   :balancing-debit       TransferFlags/BALANCING_DEBIT
   :closing-credit        TransferFlags/CLOSING_CREDIT
   :closing-debit         TransferFlags/CLOSING_DEBIT
   :imported              TransferFlags/IMPORTED
   :linked                TransferFlags/LINKED
   :pending               TransferFlags/PENDING
   :post-pending-transfer TransferFlags/POST_PENDING_TRANSFER
   :void-pending-transfer TransferFlags/VOID_PENDING_TRANSFER})

(defn transfer-flags->int
  "Turns a collection of keyword transfer flags into a packed int"
  [flags]
  (loopr [packed 0]
         [flag flags]
         (recur
           (bit-or packed (transfer-flag->int flag)))))

(defn int->transfer-flags
  "Turns a packed int into a set of transfer flags."
  [^long flags]
  (loopr [s (transient #{})]
         [[kw-flag int-flag] transfer-flags->int]
         (recur
           (if (= 0 (bit-and flags int-flag))
             s
             (conj! s kw-flag)))
         (persistent! s)))

(defn bigint->bytes
  "Turns a Clojure bigint into a byte array."
  [x]
  (assert (not (nil? x)) "Expected a bigint, but got nil")
  (UInt128/asBytes (biginteger x)))

(defn account-batch
  "Constructs a new batch of accounts from a vector of maps of the form:

  {:id         1 ; BigInt
  :user-data  2 ; Always a long, stored in UserData64
  :ledger     3
  :code       4
  :flags      #{:closed :imported}
  :timestamp  5

  "
  [accounts]
  (loopr [b (AccountBatch. (count accounts))]
         [{:keys [id
                  user-data
                  ledger
                  code
                  flags
                  timestamp] :as account} accounts]
         (recur
           (doto b
             (.add)
             (.setId (bigint->bytes id))
             (.setUserData64 user-data)
             (.setLedger ledger)
             (.setCode code)
             (.setFlags (account-flags->int flags))
             ;(.setTimestamp timestamp))
             ))))

(defn transfer-batch
  "Constructs a new batch of transfers froma vector of maps."
  [transfers]
  (loopr [b (TransferBatch. (count transfers))]
         [{:keys [id
                  debit-account-id
                  credit-account-id
                  pending-id
                  amount
                  ledger
                  code
                  user-data
                  flags
                  timeout
                  timestamp]}
          transfers]
         (do (doto b
               (.add)
               (.setId (bigint->bytes id))
               (.setDebitAccountId (bigint->bytes debit-account-id))
               (.setCreditAccountId (bigint->bytes credit-account-id))
               (.setAmount (biginteger amount))
               (.setLedger ledger)
               (.setCode code)
               (.setUserData64 user-data)
               (.setFlags (transfer-flags->int flags)))
             (when pending-id
               (.setPendingId b (bigint->bytes pending-id)))
             (when timeout (.setTimeout b timeout))
             (when timestamp  (.setTimestamp b timestamp))
             (recur b))))

(defn id-batch
  "Constructs a new batch of IDs from a vector."
  [ids]
  (loopr [b (IdBatch. (count ids))]
         [id ids]
         (recur
           (doto b
             (.add (UInt128/asBytes (biginteger id)))))))

; Deserialization

(defn account-batch-current->clj
  "Reads a single Accounts off an AccountBatch, as a Clojure map."
  [^AccountBatch b]
  {:id              (bigint (UInt128/asBigInteger (.getId b)))
   :user-data       (.getUserData64 b)
   :ledger          (.getLedger b)
   :code            (.getCode b)
   :flags           (int->account-flags (.getFlags b))
   :timestamp       (.getTimestamp b)
   :credits-pending (.getCreditsPending b)
   :credits-posted  (.getCreditsPosted b)
   :debits-pending  (.getDebitsPending b)
   :debits-posted   (.getDebitsPosted b)})

(extend-protocol Datafiable
  AccountBatch
  (datafy [b]
    (.beforeFirst b)
    (loop [accounts (transient [])]
      (if-not (.next b)
        ; End of batch
        (persistent! accounts)
        (recur (conj! accounts (account-batch-current->clj b)))))))

; Helpers

(defn ^bytes uuid
  "Generates a new UUID using the TigerBeetle Java helper"
  []
  (UInt128/id))

; Client

(defn open
  "Opens a client to the given node."
  [test node]
  (Client. (UInt128/asBytes cluster-id)
           ; Client can only take IP addresses, not hostnames. Clients also
           ; *must* receive the full list of nodes. However, we want to ensure
           ; that clients talk to a specific node, so we have a chance to see
           ; when two nodes disagree. We give them fake ports for all but the
           ; target node.
           (->> (:nodes test)
                (map (fn [some-node]
                       (str (cn/ip some-node) ":" (if (= node some-node)
                                                    port
                                                    1))))
                into-array)))

(defn close!
  "Close a client"
  [^Client client]
  (.close client))

(defn create-account-result-batch->clj
  "Converts a CreateAccountResultBatch to a Clojure vector. Also takes the
  request responsible for producing this response. Since result batches only
  contain errors, this fills in OK results for those operations that succeeded
  OK."
  [^AccountBatch req, ^CreateAccountResultBatch res]
  ; Rewind just in case
  (.beforeFirst res)
  ; So the response batch is not, as the request docs say, 1:1 with the request
  ; batch. It only includes errors! We construct a vector with OK elements for
  ; anything in the request *not* in the results. We'll use an array cuz it has
  ; a convenient fill method.
  (let [n   (.getLength req)
        ary (object-array n)]
    (Arrays/fill ary :ok)
    (loop []
      (if-not (.next res)
        ; End of batch; return vector
        (vec ary)
        (do (aset ary
                  (.getIndex res)
                  (create-account-result->keyword (.getResult res)))
            (recur))))))

(defn create-transfer-result-batch->clj
  "Converts a CreateAccountResultBatch to a Clojure vector. Also takes the
  request responsible for producing this response. Fills in OK results for
  those operations that succeeded."
  [^TransferBatch req, ^CreateTransferResultBatch res]
  (.beforeFirst res)
  (let [n (.getLength req)
        ary (object-array n)]
    (Arrays/fill ary :ok)
    (loop []
      (if-not (.next req)
        ; End of batch, return vector
        (vec ary)
        (do (aset ary
                  (.getIndex res)
                  (create-transfer-result->keyword (.getResult res)))
            (recur))))))

(defn deref+
  "Takes a client and future. Dereferences the future. Throws and closes client
  on timeout."
  [client fut]
  (let [res (deref fut 5000 ::timeout)]
    (when (identical? res ::timeout)
      (close! client)
      (throw+ {:type :timeout}))
    res))

(defn create-accounts!
  "Takes a client and a vector of account maps. Returns a vector of result
  keywords."
  [^Client c, accounts]
  (let [req (account-batch accounts)
        res (deref+ c (.createAccountsAsync c req))]
    (create-account-result-batch->clj req res)))

(defn create-transfers!
  "Takes a client and a vector of transfer maps. Returns a vector of result
  keywords."
  [^Client c, transfers]
  (let [req (transfer-batch transfers)
        res (deref+ c (.createTransfersAsync c req))]
    (create-transfer-result-batch->clj req res)))

(defn account-batch->clj
  "Converts an account batch to a Clojure vector. Takes the vector of IDs
  responsible for producing this response: an IDBatch. Guarantees the result
  vector is 1:1 with the ID vector."
  [ids ^AccountBatch res]
  ; Rewind
  (.beforeFirst res)
  ; Convert results to a map of id->account
  (let [res (loop [out (b/linear bm/empty)]
              (if-not (.next res)
                out
                ; TODO: check to ensure all are identical
                (let [a (account-batch-current->clj res)]
                  (recur (bm/put out (:id a) a)))))]
    (mapv (partial bm/get res) ids)))

(defn lookup-accounts
  "Takes a client and a vector of IDs. Returns a vector of account maps."
   [^Client c, ids]
   (let [req (id-batch ids)
         res  (deref+ c (.lookupAccountsAsync c req))]
     (account-batch->clj ids res)))
