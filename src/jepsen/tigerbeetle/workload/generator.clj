(ns jepsen.tigerbeetle.workload.generator
  "Generates operations for the transfer workload. This is a giant mess of
  monads, so we're breaking it out into its own namespace.

  General goals:

  - Generate *mostly* logically :ok events. Logical errors like
  :exceeds-credits are fine and tell us something, but they don't change state,
  and changing state is the name of the game.

  - Heavy concurrency on some accounts. Not sure if we actually want low
  concurrency or not.

  - Reads of mostly recent accounts/transfers. Occasional reads of older
  accounts.

  - Continually adding accounts.

  The name of the game, as usual, is exponential distributions. We're going to
  create accounts continuously throughout the test and maintain an ordered list
  of them. When selecting accounts for transfers/reads, we'll take an
  exponential distribution over accounts. We'll order that distribution by
  *hashmap* order, which means that some accounts get hot access, and others
  are cooler, and newly created accounts can be either hot or cool."
  (:require [bifurcan-clj [core :as b]
                          [map :as bm]
                          [list :as bl]
                          [set :as bs]]
            [clojure.core.match :refer [match]]
            [clojure.data.generators :as dg]
            [jepsen [generator :as gen]
                    [util :as util]]
            [potemkin :refer [definterface+]]))

; The State encapsulates the information we need to know about the current
; state of the database in order to generate new invocations. Mutations are
; split into pairs: gen-*, which generates data for an invocation, and a
; corresponding update function add-*, which folds that invocation into the
; state when it is actually performed.
(definterface+ IState
  (rand-account-id [state]
                   "Generates a random account ID.")

  (gen-new-accounts [state n]
               "Generates a series of n new accounts.")

  (gen-new-transfers [state n]
                     "Generates a series of n transfers between accounts.")

  (add-new-accounts [state accounts]
                    "Called when we invoke create-accounts, to track that these
                    accounts may now exist.")

  (saw-accounts [state accounts]
                 "Updates the state to acknowledge that we have positively
                 observed a series of accounts.")

  (gen-lookup-accounts [state n]
                       "Draws a vector of n account IDs that we might want to look up."))

(defrecord State
  [
   next-id        ; The next ID we'll hand out
   accounts       ; A Bifurcan map of id->account
   transfers      ; A Bifurcan map of id->transfer
   ; A Bifurcan set of account IDs we may have created, but haven't seen yet.
   unseen-accounts
   ; Ditto, transfers
   unseen-transfers
   ]

  IState
  (rand-account-id [this]
    (let [n (b/size accounts)]
      (if (pos? n)
        (let [i (dg/long 0 n)]
          (bm/key (b/nth accounts i)))
        ; No accounts yet but we can always make one up! A small number has a
        ; chance to be created soon.
        (dg/bigint 10))))

  (gen-new-accounts [this n]
    (let [ids (range next-id (+ next-id n))]
       (mapv (fn [id]
               {:id        id
                :ledger    (dg/long 1 3)
                :code      (dg/long 1 10)
                :user-data (dg/long 0 10)
                :flags     #{}})
             ids)))

  (gen-new-transfers [this n]
    (let [ids (range next-id (+ next-id n))]
      (mapv (fn [id]
              {:id                id
               :debit-account-id  (rand-account-id this)
               :credit-account-id (rand-account-id this)
               :amount            (dg/long 0 10)
               :ledger            (dg/long 1 3)
               :code              (dg/long 1 10)
               :user-data         (dg/long 0 10)
               :flags             #{}})
            ids)))

  (add-new-accounts [this new-accounts]
    (let [ids (mapv :id new-accounts)]
      (assoc this
             :next-id (inc (reduce max next-id ids))
             :unseen-accounts (b/forked
                                (reduce bs/add
                                        (b/linear unseen-accounts)
                                        ids))
             :accounts (b/forked
                                (reduce (fn [m a]
                                          (bm/put m (:id a) a))
                                       (b/linear accounts)
                                       new-accounts)))))

  (saw-accounts [this accounts]
    (assoc this
           :unseen-accounts
           (b/forked
             (reduce bs/remove
                     (b/linear unseen-accounts)
                     (mapv :id accounts)))))

  (gen-lookup-accounts [this n]
    (when (pos? (b/size accounts))
           (->> (repeatedly (partial rand-account-id this))
             (take n)
             vec))))

(defn state
  "A fresh state."
  []
  (map->State
    {:next-id          1N
     :accounts         bm/empty
     :transfers        bm/empty
     :unseen-accounts  bs/empty
     :unseen-transfers bs/empty}))

; A generator which maintains the state and ensures its wrapped generator has
; access to it via the context map.
(defrecord GenContext [gen state]
  gen/Generator
  (op [this test ctx]
      (when-let [[op gen'] (gen/op gen test (assoc ctx :state state))]
        [op (GenContext. gen' state)]))

  (update [this test ctx event]
    (let [ctx (assoc ctx :state state)]
      (match [(:type event) (:f event)]
             ; When we create new accounts, advance our next account ID
             [:invoke :create-accounts]
             (GenContext. (gen/update gen test ctx event)
                          (add-new-accounts state (:value event)))

             [_ _]
             this))))

(defn create-accounts-gen
  "A generator for create-accounts operations."
  [test ctx]
  {:f     :create-accounts
   :value (gen-new-accounts (:state ctx) (dg/long 1 4))})

(defn lookup-accounts-gen
  "A generator for lookup-accounts operations."
  [test ctx]
  {:f      :lookup-accounts
   :value  (gen-lookup-accounts (:state ctx) (dg/long 1 4))})

(defn create-transfers-gen
  "A generator for create-transfers operations."
  [test ctx]
  {:f     :create-transfers
   :value (gen-new-transfers (:state ctx) (dg/long 1 4))})

(defn wrap-gen
  "Wraps a generator in one that maintains our state."
  [gen]
  (map->GenContext
    {:gen gen
     :state (state)}))

(defn gen
  "Generator of events during the main phase"
  []
  (gen/mix
    [create-accounts-gen
     create-transfers-gen
     lookup-accounts-gen]))

(def final-gen-chunk-size
  "Roughly how many things do we try to read per final read?"
  128)

(defn final-gen
  "Final generator. Makes sure we try to observe every unseen account and
  transfer."
  []
  (reify gen/Generator
    (op [this test ctx]
      ; Look up every unseen account.
      (let [state           (:state ctx)
            unseen-accounts (:unseen-accounts state)
            n               (b/size unseen-accounts)
            chunks          (b/split unseen-accounts
                                     (/ n final-gen-chunk-size))]
        (gen/op
          (map (fn [ids]
                 {:f :lookup-accounts, :value (vec ids)})
               chunks)
          test
          ctx)))

    (update [this test ctx event]
      this)))
