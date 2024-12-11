(ns jepsen.tigerbeetle.client
  "Wrapper for the Java TigerBeetle client."
  (:require [bifurcan-clj [core :as b]
                          [map :as bm]]
            [camel-snake-kebab.core :refer [->kebab-case-keyword
                                           ->PascalCase
                                           ->SCREAMING_SNAKE_CASE]]
            [clojure [datafy :refer [datafy]]
                     [pprint :refer [pprint]]]
            [clojure.core.protocols :refer [Datafiable]]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [loopr]]
            [jepsen [util :as util :refer [timeout
                                           secs->nanos]]]
            [jepsen.control.net :as cn]
            [jepsen.tigerbeetle [core :refer [cluster-id port]]]
            [potemkin :refer [definterface+]]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (com.tigerbeetle AccountBatch
                            AccountFilter
                            AccountFlags
                            Batch
                            Client
                            CreateAccountResult
                            CreateAccountResultBatch
                            CreateTransferResult
                            CreateTransferResultBatch
                            IdBatch
                            QueryFilter
                            TransferBatch
                            TransferFlags
                            UInt128)
           (java.util Arrays)))

; Helpers

(defn ^bytes uuid
  "Generates a new UUID using the TigerBeetle Java helper"
  []
  (UInt128/id))

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
         [[kw-flag int-flag] transfer-flag->int]
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
             (.setTimestamp (or timestamp 0))
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
               (.setDebitAccountId  (bigint->bytes debit-account-id))
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

(defn account-filter
  "Constructs an AccountFilter from a map."
  [{:keys [account-id
           flags
           code
           user-data
           timestamp-min
           timestamp-max
           limit]}]
  (let [f (doto (AccountFilter.)
            (.setAccountId (bigint->bytes account-id))
            (.setCredits   (contains? flags :credits))
            (.setDebits    (contains? flags :debits))
            (.setReversed  (contains? flags :reversed)))]
    (when code                (.setCode f code))
    (when user-data           (.setUserData64 f user-data))
    (when timestamp-min       (.setTimestampMin f timestamp-min))
    (when timestamp-max       (.setTimestampMax f timestamp-max))
    (when limit               (.setLimit f limit))
    f))

(defn query-filter
  "Constructs a QueryFIlter from a map."
  [{:keys [code ledger limit flags timestamp-min timestamp-max user-data]
    :as filter}]
  (let [f (doto (QueryFilter.)
            (.setReversed (contains? flags :reversed)))]
    (when code          (.setCode f code))
    (when ledger        (.setLedger f ledger))
    (when user-data     (.setUserData64 f user-data))
    (when timestamp-min (.setTimestampMin f timestamp-min))
    (when timestamp-max (.setTimestampMax f timestamp-max))
    (when limit         (.setLimit f limit))
    ;(info :filter (datafy f))
    ;(assert (= filter (select-keys (datafy f) (keys filter))))
    f))

; Deserialization

(defn account-batch-current->clj
  "Reads a single account map off an AccountBatch"
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

(defn transfer-batch-current->clj
  "Reads a single transfer map off a TransferBatch"
  [^TransferBatch b]
  {:id                 (bigint (UInt128/asBigInteger (.getId b)))
   :debit-account-id   (bigint (UInt128/asBigInteger (.getDebitAccountId b)))
   :credit-account-id  (bigint (UInt128/asBigInteger (.getCreditAccountId b)))
   :pending-id         (bigint (UInt128/asBigInteger (.getPendingId b)))
   :amount             (bigint (.getAmount b))
   :user-data          (.getUserData64 b)
   :ledger             (.getLedger b)
   :code               (.getCode b)
   :flags              (int->transfer-flags (.getFlags b))
   :timeout            (.getTimeout b)
   :timestamp          (.getTimestamp b)})

(extend-protocol Datafiable
  AccountBatch
  (datafy [b]
    (.beforeFirst b)
    (loop [accounts (transient [])]
      (if-not (.next b)
        ; End of batch
        (persistent! accounts)
        (recur (conj! accounts (account-batch-current->clj b))))))

  QueryFilter
  (datafy [f]
    {:code          (.getCode f)
     :ledger        (.getLedger f)
     :limit         (.getLimit f)
     :timestamp-min (.getTimestampMin f)
     :timestamp-max (.getTimestampMax f)
     :user-data     (.getUserData64 f)
     :flags         (cond-> #{}
                      (.getReversed f) (conj :reversed))}))

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
  (let [n   (.getLength req)
        ary (object-array n)]
    (Arrays/fill ary :ok)
    (loop []
      (if-not (.next res)
        ; End of batch, return vector
        (vec ary)
        (do (aset ary
                  (.getIndex res)
                  (create-transfer-result->keyword (.getResult res)))
            (recur))))))

(defn read-batch
  "Takes a batch and a function which reads off the current element of the
  batch as a map. Returns a vector of those elements."
  [read-element ^Batch b]
  (.beforeFirst b)
  (loop [out (transient [])]
    (if-not (.next b)
      (persistent! out)
      (recur (conj! out (read-element b))))))

(defn index-batch
  "Takes a batch and a function which reads off the current element of the
  batch as a map with an :id field. Returns a Bifurcan map of id -> element"
  [read-element ^Batch b]
  ; Rewind
  (.beforeFirst b)
  ; Convert results to a map of id->account
  (loop [index (b/linear bm/empty)]
    (if-not (.next b)
      index
      ; TODO: check to ensure all are identical
      (let [e (read-element b)]
        (recur (bm/put index (:id e) e))))))

(defn account-batch->clj
  "Converts an account batch to a Clojure vector. Takes the vector of IDs
  responsible for producing this response. Guarantees the result vector is 1:1
  with the ID vector. With no IDs, just returns the response vector."
  ([res]
   (read-batch account-batch-current->clj res))
  ([ids ^AccountBatch res]
   (mapv (partial bm/get (index-batch account-batch-current->clj res))
         ids)))

(defn transfer-batch->clj
  "Converts a transfer batch to a CLojure vector. Optionally takes the vector
  of IDs responsible for producing this response. Guarantees the result vector
  is 1:1 with the ID vector. With no IDs, just returns the response vector."
  ([res]
   (read-batch transfer-batch-current->clj res))
  ([ids res]
   (mapv (partial bm/get (index-batch transfer-batch-current->clj res))
         ids)))

; Primary tracking

(defn primary-tracker
  "TigerBeetle doesn't provide a way to figure out which nodes currently think
  they're the leader. However, it's useful for fault injection to target
  leaders more often. To track this, we store information about which nodes are
  currently accepting writes in a `primary tracker` atom. The test has a single
  one of these atoms.

  The atom stores a map of node names to the time we most recently completed a
  write against that node."
  []
  (atom {}))

(defn primary-tracker-write-completed!
  "Updates a primary tracker to indicate that we completed a write against the
  given node."
  [primary-tracker node]
  (let [time (System/nanoTime)]
    (swap! primary-tracker
           (fn [tracker]
             (let [t (get tracker node 0)]
               (assoc tracker node (max t time)))))))

(defn primary-tracker-primaries
  "Returns all nodes which have performed a write in the last five seconds."
  [primary-tracker]
  (let [cutoff (- (System/nanoTime) (secs->nanos 5))]
    (vec
      (keep (fn [[node time]]
              (if (<= cutoff time)
                node))
            @primary-tracker))))

; Client

(definterface+ IClient
  (create-accounts! [this accounts]
                    "Takes a client and a vector of account maps. Returns

                      {:timestamp The server timestamp for this write
                       :value     A vector of result keywords.}")

  (create-transfers! [this transfers]
                     "Takes a client and a vector of transfer maps. Returns

                       {:timestamp The server timestamp for this write
                        :value     A vector of result keywords.}")

  (lookup-accounts [this ids]
                   "Takes a client and a vector of IDs. Returns

                     {:timestamp The server timestamp for the operation
                      :value     A vector of account maps.}")

  (lookup-transfers [this ids]
                   "Takes a client and a vector of IDs. Returns

                     {:timestamp The server timestamp for this operation
                      :value     A vector of transfer maps.}")

  (query-accounts [this filter]
                  "Takes a client and a map representing a query filter.
                  Returns

                    {:timestamp The server timestamp for this operation
                     :value     A vector of matching accounts}")

  (query-transfers [this filter]
                   "Takes a client and a map representing a query filter.
                   Returns

                     {:timestamp The server timestamp for this operation
                      :value     A vector of matching transfers}")

  (get-account-transfers [this account-filter]
                         "Takes a client and a map representing an account
                         filter. Returns

                           {:timestamp   The server timestamp for this operation
                            :value       A vector of matching transfers}")

  (deref+ [this fut]
          "Takes a future. Dereferences the future, throwing and closing on
          timeout.")

  (close! [this]
          "Closes the client."))

(defn with-timestamp
  "Takes a response and a value extracted from that response. Returns a map of
  the form

    {:timestamp response-timestamp
     :value     value}"
  [^Batch response value]
  {:timestamp (.getTimestamp (.getHeader response))
   :value     value})

(defrecord TrackingClient [^Client client, node, primary-tracker, ^long timeout]
  IClient
  (create-accounts! [this accounts]
    (let [req (account-batch accounts)
          res (deref+ this (.createAccountsAsync client req))]
      (primary-tracker-write-completed! primary-tracker node)
      (with-timestamp res (create-account-result-batch->clj req res))))

  (create-transfers! [this transfers]
    (let [req (transfer-batch transfers)
          res (deref+ this (.createTransfersAsync client req))]
      (primary-tracker-write-completed! primary-tracker node)
      (with-timestamp res (create-transfer-result-batch->clj req res))))

  (lookup-accounts [this ids]
    (let [req (id-batch ids)
          res  (deref+ this (.lookupAccountsAsync client req))]
      (with-timestamp res (account-batch->clj ids res))))

  (lookup-transfers [this ids]
    (let [req (id-batch ids)
          res (deref+ this (.lookupTransfersAsync client req))]
      (with-timestamp res (transfer-batch->clj ids res))))

  (query-accounts [this filter]
    (let [req (query-filter filter)
          res (deref+ this(.queryAccountsAsync client req))]
      (with-timestamp res (account-batch->clj res))))

  (query-transfers [this filter]
    (let [req (query-filter filter)
          res (deref+ this (.queryTransfersAsync client req))]
      (with-timestamp res (transfer-batch->clj res))))

  (get-account-transfers [this filter]
    (let [req (account-filter filter)
          res (deref+ this (.getAccountTransfersAsync client req))]
      (with-timestamp res
        (transfer-batch->clj res))))

  (deref+ [this fut]
    (let [res (deref fut timeout ::timeout)]
      (when (identical? res ::timeout)
        (.close client)
        (throw+ {:type :timeout}))
      res))

  (close! [this]
          (.close client))

  java.lang.AutoCloseable
  (close [this]
    (close! this)))

(defn open
  "Opens a client to the given node."
  [test node]
  (TrackingClient.
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
                into-array))
    node
    (:primary-tracker test)
    (:timeout test)))
