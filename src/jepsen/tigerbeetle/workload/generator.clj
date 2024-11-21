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
            [clojure [datafy :refer [datafy]]]
            [clojure.core.match :refer [match]]
            [clojure.data.generators :as dg]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [generator :as gen]
                    [util :as util]]
            [potemkin :refer [definterface+]]))

(defn b-inverse-cdf
  "Inverse cumulative distribution function for the zipfian bounding function
  used in `zipf`."
  (^double [^double skew ^double t ^double p]
           (let [tp (* t p)]
             (if (<= tp 1)
               ; Clamp so we don't fly off to infinity
               tp
               (Math/pow (+ (* tp (- 1 skew))
                            skew)
                         (/ (- 1 skew)))))))

(defn zipf
  "Selects a Zipfian-distributed integer in [0, n) with a given skew
  factor. Adapted from the rejection sampling technique in
  https://jasoncrease.medium.com/rejection-sampling-the-zipf-distribution-6b359792cffa."
  ([^long n]
   (zipf 1.000001 n))
  ([^double skew ^long n]
   (if (= n 0)
     0
     (do (assert (not= 1.0001 skew)
                 "Sorry, our approximation can't do skew = 1.0! Try a small epsilon, like 1.0001")
         (let [t (/ (- (Math/pow n (- 1 skew)) skew)
                    (- 1 skew))]
           (loop []
             (let [inv-b         (b-inverse-cdf skew t (dg/double))
                   sample-x      (long (+ 1 inv-b))
                   y-rand        (dg/double)
                   ratio-top     (Math/pow sample-x (- skew))
                   ratio-bottom  (/ (if (<= sample-x 1)
                                      1
                                      (Math/pow inv-b (- skew)))
                                    t)
                   rat (/ ratio-top (* t ratio-bottom))]
               (if (< y-rand rat)
                 (dec sample-x)
                 (recur)))))))))

(defn zipf-nth
  "Selects a random element from a Bifurcan collection with a Zipfian
  distribution."
  ([xs]
   (zipf-nth 1.00001 xs))
  ([skew xs]
   (b/nth xs (zipf skew (b/size xs)))))

(defn binto
  "Generic `into` for Bifurcan collections. Takes a conj function (e.g.
  bs/add), a collection to add to, and a reducible of things to add. Returns
  new collection."
  [conj coll xs]
  (b/forked
    (reduce conj
            (b/linear coll)
            xs)))

(defn into-by-id
  "Takes a Bifurcan map and a collection of maps with :id fields. Puts them
  into the map, using :id as the key."
  [m xs]
  (binto (fn conj [m x]
           (bm/put m (:id x) x))
         m
         xs))

; The State encapsulates the information we need to know about the current
; state of the database in order to generate new invocations. Mutations are
; split into pairs: gen-*, which generates data for an invocation, and a
; corresponding update function add-*, which folds that invocation into the
; state when it is actually performed.
(definterface+ IState
  (rand-ledger [state]
               "Generates a random ledger.")

  (rand-code [state]
             "Generates a random code.")

  (rand-user-data [state]
                  "Generates random user data.")

  (rand-account-id [state]
                   [state ledger]
                   "Generates a random, likely extant, account ID. Optionally
                   constrained to a single ledger.")

  (rand-transfer-id [state]
                    "Generates a random, likely extant, transfer ID.")

  (rand-timestamp [state]
                  "Generates a random timestamp likely to be within the range
                  of timestamps for the database.")

  (gen-new-accounts [state n]
               "Generates a series of n new accounts.")

  (gen-new-transfers [state n]
                     "Generates a series of n transfers between accounts.")

  (add-new-accounts [state accounts]
                    "Called when we invoke create-accounts, to track that these
                    accounts may now exist.")

  (add-new-transfers [state transfers]
                     "Called when we invoke create-transfers, to track that
                     those transfers may now exist.")

  (saw-accounts [state accounts]
                 "Updates the state to acknowledge that we have positively
                 observed a series of accounts.")

  (saw-transfers [state transfers]
                 "Updates the state to acknowldge that we have positively
                 observed a series of transfers.")

  (gen-lookup-accounts [state n]
                       "Draws a vector of n account IDs that we might want to
                       look up.")

  (gen-lookup-transfers [state n]
                        "Draws a vector of n transfers IDs that we might want
                        to look up.")

  (gen-get-account-transfers [state]
                             "Generates an account filter map for a get-account-transfers operation."))

(defrecord State
  [
   next-id             ; The next ID we'll hand out
   ^long timestamp-min ; The smallest timestamp observed
   ^long timestamp-max ; The largest timestamp observed
   accounts            ; A Bifurcan map of id->account
   transfers           ; A Bifurcan map of id->transfer
   ledger->account-ids ; A Bifurcan map of ledger to lists of account IDs.
   ; A Bifurcan set of account IDs we may have created, but haven't seen yet.
   unseen-accounts
   ; Ditto, transfers
   unseen-transfers
   ]

  IState
  (rand-account-id [this]
    (let [n (b/size accounts)]
      (if (pos? n)
        (bm/key (zipf-nth accounts))
        ; No accounts yet but we can always make one up!
        (bigint (inc (zipf 100))))))

  (rand-account-id [this ledger]
    (let [ids (bm/get ledger->account-ids ledger bl/empty)
          n   (b/size ids)]
      (if (pos? n)
        (zipf-nth ids)
        ; No accounts yet, but we can always make one up!
        (bigint (inc (zipf 100))))))

  (rand-transfer-id [this]
    (let [n (b/size transfers)]
      (if (pos? n)
        (bm/key (zipf-nth transfers))
        ; No transfers yet but we can always make one up!
        (bigint (inc (zipf 100))))))

  (rand-ledger [this]
    (inc (zipf 3)))

  (rand-user-data [this]
    (inc (zipf 1000)))

  (rand-code [this]
    (inc (zipf 1000)))

  (rand-timestamp [this]
    (cond (< timestamp-min timestamp-max)
          (dg/uniform timestamp-min timestamp-max)

          (= timestamp-min timestamp-max)
          timestamp-min

          ; We haven't seen anything yet; might as well guess
          true
          (System/nanoTime)))

  (gen-new-accounts [this n]
    (let [ids (range next-id (+ next-id n))]
       (mapv (fn [id]
               {:id        id
                :ledger    (rand-ledger this)
                :code      (rand-code this)
                :user-data (rand-user-data this)
                :flags     #{}})
             ids)))

  (gen-new-transfers [this n]
    (let [ids (range next-id (+ next-id n))
          ; TODO: occasionally generate incompatible ledgers
          ledger (rand-ledger this)
          debit-account-id (rand-account-id this ledger)
          ; Mostly generate distinct debit/credit accounts
          credit-account-id (loop [tries 3]
                              ; TODO: sometimes a diff ledger
                              (let [id (rand-account-id this ledger)]
                                (if (and (pos? tries) (= id debit-account-id))
                                  (recur (dec tries))
                                  id)))]
      (mapv (fn [id]
              {:id                id
               :debit-account-id  debit-account-id
               :credit-account-id credit-account-id
               :amount            (if (< (dg/double) 0.01)
                                    ; Sometimes we generate zero transfers
                                    0
                                    ; But mostly, zipf-distributed ones
                                    (inc (zipf 1000)))
               :ledger            ledger
               :code              (rand-code this)
               :user-data         (rand-user-data this)
               :flags             #{}})
            ids)))

  (add-new-accounts [this new-accounts]
    (let [ids (mapv :id new-accounts)
          ledger->account-ids
          (binto (fn track-ledger [m account]
                   (bm/update m
                              (:ledger account)
                              (fn append [account-ids]
                                (if account-ids
                                  (bl/add-last account-ids
                                               (:id account))
                                  (bl/list (:id account))))))
                 ledger->account-ids
                 new-accounts)]
      (assoc this
             :next-id             (inc (reduce max next-id ids))
             :accounts            (into-by-id accounts new-accounts)
             :ledger->account-ids ledger->account-ids
             :unseen-accounts     (binto bs/add unseen-accounts ids))))

  (add-new-transfers [this new-transfers]
    (let [ids (mapv :id new-transfers)]
      (assoc this
             :next-id          (inc (reduce max next-id ids))
             :transfers        (into-by-id transfers new-transfers)
             :unseen-transfers (binto bs/add unseen-transfers ids))))

  (gen-lookup-accounts [this n]
    (->> (repeatedly (partial rand-account-id this))
         (take n)
         vec))

  (gen-lookup-transfers [this n]
    (->> (repeatedly (partial rand-transfer-id this))
         (take n)
         vec))

  (gen-get-account-transfers [this]
    (let [flags (cond-> (condp < (dg/double)
                          ; Very rarely, neither credits nor debits
                          0.95 #{}
                          ; Sometimes both
                          0.8 #{:credits :debits}
                          ; Mostly one
                          0.4 #{:credits}
                          #{:debits})
                  ; Mostly we want rcron; that way periodic reads will cover
                  ; more of the space
                  (< (dg/double) 9/10)
                  (conj :reversed))
          ; A pair of timestamps we can use for min and max.
          [t1 t2] (sort [(rand-timestamp this)
                         (rand-timestamp this)])]
      (cond-> {:flags       flags
               :account-id  (rand-account-id this)
               :limit       (dg/long 1 32)}
        (< (dg/double) 1/4)
        (assoc :min-timestamp t1)

        (< (dg/double) 1/4)
        (assoc :max-timestamp t2)

        ; These are relatively unlikely to match, so we generate them
        ; infrequently
        (< (dg/double) 1/16)
        (assoc :user-data (rand-user-data this))

        (< (dg/double) 1/16)
        (assoc :code (rand-code this)))))

  (saw-accounts [this accounts]
    (let [timestamps (keep :timestamp accounts)]
      (assoc this
             :timestamp-min (reduce min timestamp-min timestamps)
             :timestamp-max (reduce max timestamp-max timestamps)
             :unseen-accounts
             (binto bs/remove unseen-accounts (keep :id accounts)))))

  (saw-transfers [this transfers]
    (let [timestamps (keep :timestamp transfers)]
      (assoc this
             :timestamp-min (reduce min timestamp-min timestamps)
             :timestamp-max (reduce max timestamp-max timestamps)
             :unseen-transfers
             (binto bs/remove unseen-transfers (keep :id transfers))))))

(defn state
  "A fresh state."
  []
  (map->State
    {:next-id             1N
     :timestamp-min       Long/MAX_VALUE
     :timestamp-max       Long/MIN_VALUE
     :accounts            bm/empty
     :transfers           bm/empty
     :unseen-accounts     bs/empty
     :unseen-transfers    bs/empty
     :ledger->account-ids bm/empty}))

; A generator which maintains the state and ensures its wrapped generator has
; access to it via the context map.
(defrecord GenContext [gen state]
  gen/Generator
  (op [this test ctx]
      (when-let [[op gen'] (gen/op gen test (assoc ctx :state state))]
        [op (GenContext. gen' state)]))

  (update [this test ctx op]
    (let [ctx (assoc ctx :state state)
          gen' (gen/update gen test ctx op)
          {:keys [type f value]} op]
      (match
        [type f]

        ; We're creating new accounts
        [:invoke :create-accounts]
        (GenContext. gen' (add-new-accounts state value))

        ; We're creating new transfers
        [:invoke :create-transfers]
        (GenContext. gen' (add-new-transfers state value))

        ; We saw an account
        [:ok :lookup-accounts]
        (GenContext. gen' (saw-accounts state value))

        ; We saw a transfer
        [:ok :lookup-transfers]
        (GenContext. gen' (saw-transfers state value))

        ; This also tells us about transfers
        [:ok :get-account-transfers]
        (GenContext. gen' (saw-transfers state value))

        [_ _]
        this))))

(defn wrap-gen
  "Wraps a generator in one that maintains our state."
  [gen]
  (map->GenContext
    {:gen gen
     :state (state)}))

(defn rand-event-count
  "Generates a random number of events (e.g. for a single create-transfer op)"
  []
  (if (< (dg/double) 0.01)
    0
    (inc (zipf 4 100))))

(defn create-accounts-gen
  "A generator for create-accounts operations."
  [test ctx]
  {:f     :create-accounts
   :value (gen-new-accounts (:state ctx) (rand-event-count))})

(defn create-transfers-gen
  "A generator for create-transfers operations."
  [test ctx]
  {:f     :create-transfers
   :value (gen-new-transfers (:state ctx) (rand-event-count))})

(defn lookup-accounts-gen
  "A generator for lookup-accounts operations."
  [test ctx]
  {:f      :lookup-accounts
   :value  (gen-lookup-accounts (:state ctx) (rand-event-count))})

(defn lookup-transfers-gen
  "A generator for lookup-transfers operations."
  [test ctx]
  {:f       :lookup-transfers
   :value   (gen-lookup-transfers (:state ctx) (rand-event-count))})

(defn get-account-transfers-gen
  "A generator for get-account-transfers operations."
  [test ctx]
  {:f       :get-account-transfers
   :value   (gen-get-account-transfers (:state ctx))})

(defn rand-weighted-index
  "Takes a total weight and a vector of weights for a weighted discrete
  distribution and generates a random index into those weights, with
  probability proportionate to weight. Returns -1 when total-weight is 0."
  [^long total-weight weights]
  (if (= 0 total-weight)
    -1
    (let [r (dg/long 0 total-weight)]
      (loop [i   0
             sum 0]
        (let [sum' (+ sum (weights i))]
          (if (< r sum')
            i
            (recur (inc i) sum')))))))

(defrecord WeightedMix [^long total-weight  ; Total weight
                        weights             ; Vector of weights
                        gens                ; Vector of generators
                        ^long i]            ; Index of current weight/gen
  gen/Generator
  (op [this test ctx]
    (when-not (= 0 (count gens))
      (if-let [[op gen'] (gen/op (nth gens i) test ctx)]
        ; TODO: handle :pending
        [op (WeightedMix. total-weight
                          weights
                          (assoc gens i gen')
                          (long (rand-weighted-index total-weight weights)))]
        ; Out of ops from this gen; compact and retry.
        (let [total-weight' (- total-weight (weights i))
              weights'      (gen/dissoc-vec weights i)
              gens'         (gen/dissoc-vec gens i)
              i'            (rand-weighted-index total-weight' weights')]
          (gen/op (WeightedMix. total-weight' weights' gens' i') test ctx)))))

  (update [this test ctx event]
    ; Propagate to each gen
    (when-not (= 0 (count gens))
      (WeightedMix. total-weight
                    weights
                    (mapv #(gen/update % test ctx event) gens)
                    i))))

(defn weighted-mix
 "A generator which combines several generators in a random, weighted mixture.
 Takes a flat series of `weight gen` pairs: a generator with weight 6 is chosen
 three times as often as one with weight 2. Updates are propagated to all
 generators."
 [& weight-gens]
 (assert (even? (count weight-gens)))
 (when (seq weight-gens)
   (let [weight-gens  (partition 2 weight-gens)
         weights      (mapv first weight-gens)
         total-weight (reduce + weights)
         gens         (mapv second weight-gens)]
     (WeightedMix. total-weight weights gens
                   (rand-weighted-index total-weight weights)))))

(defn gen
  "Generator of events during the main phase"
  []
  (weighted-mix
    1  create-accounts-gen
    10 create-transfers-gen
    5  lookup-accounts-gen
    5  lookup-transfers-gen
    5  get-account-transfers-gen))

(def final-gen-chunk-size
  "Roughly how many things do we try to read per final read?"
  128)

(defn final-*-gen
  "Shared logic for both final read generators."
  [unseen-field f]
  (reify gen/Generator
    (op [this test ctx]
      (gen/op
        (->> (get (:state ctx) unseen-field)
             (partition-all 128)
             (map (fn [ids]
                    (gen/until-ok
                      (gen/repeat
                        {:f f, :value (vec ids)})))))
        test
        ctx))

    (update [this test ctx op]
      this)))

(defn final-accounts-gen
  "A generator that tries to observe every unseen account."
  []
  (final-*-gen :unseen-accounts :lookup-accounts))

(defn final-transfers-gen
  "A generator that tries to observe every unseen transfer."
  []
  (final-*-gen :unseen-transfers :lookup-transfers))

(defn final-gen
  "Final generator. Makes sure we try to observe every unseen account and
  transfer."
  []
  [(final-accounts-gen)
   (final-transfers-gen)])
