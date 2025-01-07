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
                          [int-map :as bim]
                          [map :as bm]
                          [list :as bl]
                          [set :as bs]]
            [clojure [datafy :refer [datafy]]
                     [pprint :refer [pprint]]]
            [clojure.core.match :refer [match]]
            [clojure.data.generators :as dg]
            [clojure.math.numeric-tower :refer [lcm]]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [loopr reducer]]
            [jepsen [generator :as gen]
                    [history :as h]
                    [util :as util]]
            [jepsen.tigerbeetle [core :refer [bireduce]]
                                [lifecycle-map :as lm]]
            [potemkin :refer [definterface+]])
  (:import (jepsen.tigerbeetle.lifecycle_map LifecycleMap)))

(defn rand-weighted-index
	"Takes a total weight and a vector of weights for a weighted discrete
 distribution and generates a random index into those weights, with
 probability proportionate to weight. Returns -1 when total-weight is 0."
  ([weights]
   (rand-weighted-index (reduce + weights) weights))
  ([^long total-weight weights]
   (if (= 0 total-weight)
     -1
     (let [r (dg/long 0 total-weight)]
       (loop [i   0
              sum 0]
         (let [sum' (+ sum (weights i))]
           (if (< r sum')
             i
             (recur (inc i) sum'))))))))

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

(defn long-weights
	"Takes an array of rational weights and scales them up such that all are
 integers. Approximate for floats."
	[weights]
	(let [denom (fn denominator+ [x]
								(cond (integer? x) 1
											(ratio? x) (denominator x)
											(float? x) (Math/round (/ x))))
				m (->> weights
							 (map denom)
							 (reduce lcm 1))]
		(mapv (fn scale [x]
						(let [s (* m x)]
							(cond (integer? s)  s
										(float? s)    (Math/round s)
										true          (throw (RuntimeException.
																					 (str "How did we even get "
																								(class s) "x" (pr-str s)))))))
					weights)))

(defn weighted-mix
  "A generator which combines several generators in a random, weighted mixture.
  Takes a flat series of `weight gen` pairs: a generator with weight 6 is
  chosen three times as often as one with weight 2. Updates are propagated to
  all generators."
	[& weight-gens]
	(assert (even? (count weight-gens)))
	(when (seq weight-gens)
		(let [weight-gens  (partition 2 weight-gens)
					weights      (long-weights (map first weight-gens))
					total-weight (reduce + weights)
					gens         (mapv second weight-gens)]
			(WeightedMix. total-weight weights gens
										(rand-weighted-index total-weight weights)))))

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

(def zipf-skew
  "When we choose zipf-distributed things, what skew do we generally pick?"
  1.001)

(defn zipf
  "Selects a Zipfian-distributed integer in [0, n) with a given skew
  factor. Adapted from the rejection sampling technique in
  https://jasoncrease.medium.com/rejection-sampling-the-zipf-distribution-6b359792cffa."
  ([^long n]
   (zipf 1.000001 n))
  ([^double skew ^long n]
   (if (= n 0)
     0
     (do (assert (not= 1.0 skew)
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
   (b/nth xs (zipf skew (b/size xs))))
  ([skew xs not-found]
   (if (= 0 (b/size xs))
     not-found
     (zipf-nth skew xs))))

(defn zipf-nth-many
  "Selects a random element from a vector of n Bifurcan collections. Uniformly
  distributed between collections, Zipfian within each collection."
  [xs not-found]
  (let [i (rand-weighted-index (mapv b/size xs))]
    (if (= i -1)
      not-found
      (zipf-nth (nth xs i) not-found))))

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

(def chain-zipf-skew
  "The skew factor for choosing chain lengths"
  4)

(defn chains
  "Takes a vector of events and partitions them into chains with zipfian
  distributed lengths, setting the :linked flag on events as appropriate. Note
  that chains amplify failure probablities, so we want to be careful about
  generating mostly chains of length 1."
  [events]
  (let [n (count events)]
    (if (<= n 1)
      events
      (loop [i            0             ; Index
             ; How many more events in this chain
             chain-length (zipf chain-zipf-skew n)
             events       (transient events)]
        (cond (= i n)
              ; Done.
              (persistent!
                ; Almost never generate a trailing linked event
                (if (< (dg/double) 1/1000)
                  events
                  (assoc! events (dec n)
                          (update (get events (dec n)) :flags disj :linked))))

              ; Chain done; re-roll length
              (= chain-length 0)
              (recur (inc i)
                     (zipf chain-zipf-skew n)
                     events)

              ; Link
              true
              (recur (inc i)
                     (dec chain-length)
                     (assoc! events i
                             (update (get events i) :flags conj :linked))))))))

(defn fallback-id
  "Sometimes you just need an ID to start with."
  []
  (bigint (inc (zipf 10))))

(defn account-id->ledger
  "Returns the ledger for a specific account ID."
  [id]
  (let [h (hash id)
        m (mod h 100)]
    ; A hardcoded Zipfian distribution over 3 ledgers. Chosen to align with
    ; rand-ledger-id.
    (condp < m
      46 1
      18 2
         3)))

(defn probe-lifecycle-map
  "Sometimes we want to find an zipfian-distributed ID that's both in a
  set of IDs and also has a particular lifecycle state. This tries to
  find an ID in `ids` and which, in `lifecycle-map`, is mostly seen or likely,
  as opposed to unlikely or imagined."
  [ids lifecycle-map]
  (let [n   (b/size ids)
        ; Get keys from the lifecycle map.
        submap (condp < (dg/double)
                          0.0  lm/seen
                          0.05 lm/likely
                               lm/unlikely)
        submap (submap lifecycle-map)
        subkeys (bm/keys submap)]
    (cond ; IDs pool is empty; use anything from the submap
          (= 0 n)
          (zipf-nth zipf-skew subkeys (fallback-id))

          ; Submap is empty; fall back to pool
          (= 0 (b/size submap))
          (zipf-nth zipf-skew ids (fallback-id))

          ; When one collection is small, just intersect them
          (or (<= (b/size ids) 128)
              (<= (b/size subkeys) 128))
          (zipf-nth zipf-skew
                    (bs/intersection ids subkeys)
                    (fallback-id))

          ; Alternate between probing ids and subkeys randomly, looking for
          ; presence in the other set.
          true
          (loop [tries (min n 10)]
            ; From IDs
            (if (= 0 tries)
              (do ;(info "probe for intersection of" (sort ids) "and"
                  ;      (sort subkeys) "failed\nIntersection: "
                  ;      (sort (bs/intersection ids subkeys)))
                  ; TODO: This intersection is probably too expensive for long
                  ; runs; might need to fall back. I'm still trying to tune
                  ; this to get something that succeeds reasonably often.
                  (zipf-nth zipf-skew (bs/intersection ids subkeys)
                            (fallback-id)))
              (let [id (zipf-nth ids)]
                (if (bs/contains? subkeys id)
                  id
                  (let [id (zipf-nth subkeys)]
                    (if (bs/contains? ids id)
                      id
                      (recur (dec tries)))))))))))

(defn add-pending-transfer
  "Adds a pending transfer, with probability p, to the pending transfer ID
  set. Does not add if present in completed-transfer-ids."
  [completed-transfer-ids pending-transfer-ids p id]
  (cond (< p (dg/double))
       pending-transfer-ids

        (bs/contains? completed-transfer-ids id)
        pending-transfer-ids

        true
        (bs/add pending-transfer-ids id)))

(defn remove-pending-transfer
  "Removes a pending transfer from the pending ID set, with probability p."
  [pending-transfer-ids p id]
  (if (< p (dg/double))
    pending-transfer-ids
    (bs/remove pending-transfer-ids id)))

(defn complete-pending-transfer
  "Called when we learn that a transfer has been completed. Takes the set of
  completed IDs and the set of pending IDs; returns a pair of [completed-ids
  pending-ids]. With probability p, marks the transfer as completed and removes
  it from pending."
  [completed-transfer-ids pending-transfer-ids p id]
  (if (< p (dg/double))
    [completed-transfer-ids pending-transfer-ids]
    [(bs/add completed-transfer-ids id) (bs/remove pending-transfer-ids id)]))

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

  (get-account [state id]
               "Look up our local cache of an account by ID.")

  (gen-new-accounts [state n]
                    "Generates a series of n new accounts.")

  (gen-new-transfer-1 [state id]
                      "Generates a single first-phase transfer with the given
                      ID--either single-phase or pending.")

  (gen-new-transfer-2 [state id]
                      "Generates a single second-phase transfer with the given
                      ID--closing or voiding a pending transfer.")

  (gen-new-transfer [state id]
                    "Generates a single transfer with the given ID.")

  (gen-new-transfers [state n]
                     "Generates a series of n transfers between accounts.")

  (add-new-accounts [state accounts]
                    [state accounts results p]
                    "Called when we invoke create-accounts, to track that these
                    accounts may now exist. The longer form handles when we get
                    a response back from the DB, and marks failed accounts as
                    unlikely with probability p. p=1 means we're completely
                    confident the operation did not happen.")

  (add-new-transfers [state transfers]
                     [state transfers results p]
                     "Called when we invoke create-transfers, to track that
                     those transfers may now exist. The longer form
                     handles when we get a response back from the DB, and marks
                     failed transfers as unlikely with probability p. p=1 means
                     we're completely confident the operation did not happen.")

  (read-accounts [state accounts]
                 [state ids accounts]
                 "Updates the state with the results of an account read. The
                 binary form is for a predicate read. The ternary form is for a
                 read of specific IDs.")

  (read-transfers [state transfers]
                  [state ids transfers]
                 "Updates the state with the results of a transfer read. The
                 binary form is for a predicate read. The ternary form is for a
                 read of specific IDs.")

  (log-invoke [state invoke]
              "Logs an invocation op")

  (gen-lookup-accounts [state n]
                       "Draws a vector of n account IDs that we might want to
                       look up.")

  (gen-lookup-transfers [state n]
                        "Draws a vector of n transfers IDs that we might want
                        to look up.")

  (gen-query-filter [state]
                    "Generates a query filter map for query-accounts or
                    query-transfers.")

  (gen-account-filter [state]
                      "Generates an account filter map for a
                      get-account-transfers operation."))

(defrecord State
	[
	 next-id              ; The next ID we'll hand out
	 ^long timestamp-min  ; The smallest timestamp observed
	 ^long timestamp-max  ; The largest timestamp observed
	 transfers            ; A LifecycleMap of id->transfer
   ledger->accounts     ; A Bifurcan map of a ledger to a LifeCycleMap of
                        ; accounts
   pending-transfer-ids ; A Bifurcan set of IDs of transfers we intend to
                        ; post or void later.
   completed-transfer-ids ; A Bifurcan set of IDs of transfers we believe have
                          ; been posted or voided.
   ; An integer map of processes to the last invocation that process performed.
   ; Used to connect (e.g.) create-transfer requests to their results.
   process->invoke
	 ]

	IState
	(rand-account-id [this]
    (rand-account-id this (rand-ledger this)))

	(rand-account-id [this ledger]
    (let [accounts (bm/get ledger->accounts ledger lm/empty)
          r (dg/double)]
      (bm/key
        ; Note: account IDs are very often resolved to seen quickly, which
        ; means the chances that we have *any* likely accounts is low. We try a
        ; likely account first, then fall back to seen. This means that during
        ; the window of uncertainty for an account, we have a high chance to
        ; try it, but we *don't* fall back to unlikely IDs as soon as that
        ; account is read.
        (or (when (< 0.4 r)  (zipf-nth zipf-skew (lm/likely accounts) nil))
            (when (< 0.05 r) (zipf-nth zipf-skew (lm/seen accounts)   nil))
            ; Sometimes an unlikely account
            (zipf-nth zipf-skew (lm/unlikely accounts) nil)
            ; If no options, make up a key
            (bm/->entry [(fallback-id) nil])))))

	(rand-transfer-id [this]
    (let [r (dg/double)]
      (bm/key
        (or (when (< 0.4 r)  (zipf-nth zipf-skew (lm/likely transfers) nil))
            (when (< 0.05 r) (zipf-nth zipf-skew (lm/seen transfers)   nil))
            ; Rarely, unlikely
            (zipf-nth zipf-skew (lm/unlikely transfers) nil)
            ; Make something up
            (bm/->entry [(bigint (inc (zipf 10))) nil])))))

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

  (get-account [this id]
    (-> (bm/get ledger->accounts (account-id->ledger id) lm/empty)
        lm/possible
        (bm/get id nil)))

	(gen-new-accounts [this n]
		(let [ids (range next-id (+ next-id n))]
			(->> ids
					 (mapv (fn [id]
                   (let [exceeds (dg/weighted
                                   {#{} 128
                                    #{:debits-must-not-exceed-credits} 32
                                    #{:credits-must-not-exceed-debits} 32
                                    #{:debits-must-not-exceed-credits
                                      :credits-must-not-exceed-debits} 1})
                         flags (cond-> exceeds
                                 )]
									 {:id        id
										:ledger    (account-id->ledger id)
										:code      (rand-code this)
										:user-data (rand-user-data this)
										:flags     flags})))
					 chains)))

	(gen-new-transfer-1 [this id]
		(let [debit-account-id  (rand-account-id this)
          ; NB: The account ID we generate might be fake!
          debit-account     (get-account this debit-account-id)
					ledger            (:ledger debit-account (rand-ledger this))
					; Mostly generate distinct debit/credit accounts
					credit-account-id (loop [tries 10]
															(let [id (rand-account-id this ledger)]
																(if (and (pos? tries) (= id debit-account-id))
																	(recur (dec tries))
																	id)))
          ; TODO: other flags
          pending? (< (dg/double) 1/2)
          closing? (< (dg/double) 1/16384)
					flags (cond-> #{}
                  ; Half of transfers are pending
									pending? (conj :pending)
                  ; Pending transfers have a small chance to close
                  (and pending? closing? (< (dg/double) 1/2))
                  (conj :closing-debit)
                  (and pending? closing? (< (dg/double) 1/2))
                  (conj :closing-credit)
                  ; Often we use balancing credits/debits
                  (< (dg/double) 1/3) (conj :balancing-credit)
                  (< (dg/double) 1/3) (conj :balancing-debit))]
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
			 :flags             flags}))

  (gen-new-transfer-2 [this id]
    (when-let [pending-id (zipf-nth zipf-skew pending-transfer-ids
                               ; If we can't do a pending ID, we've got a small
                               ; chance to try any transfer ID. There's a good
                               ; chance that during the test the pending set is
                               ; empty, so we don't want to do this *too*
                               ; often.
                               (when (< (dg/double) 0.05)
                                 (rand-transfer-id this)))]
      (let [pending (or (bm/get (lm/possible transfers) pending-id nil)
                        ; Make up a transfer that doesn't exist
                        (gen-new-transfer-1 this (rand-transfer-id this)))
            ledger (if (< (dg/double) 1/256)
                     (rand-ledger this)
                     (:ledger pending))
            post? (< (dg/double) 1/2)
            void? (not post?)
            flags #{(if post?
                      :post-pending-transfer
                      :void-pending-transfer)}]
        {:id                id
         :pending-id        (:id pending)
         :debit-account-id
         (cond ; Rarely, mismatch
               (< (dg/double) 1/1024)  (rand-account-id this ledger)
               (< (dg/double) 1/2)    (:debit-account-id pending)
               true                   0N)
         :credit-account-id
         (cond ; Rarely, mismatch
               (< (dg/double) 1/1024)  (rand-account-id this ledger)
               (< (dg/double) 1/2)    (:credit-account-id pending)
               true                   0N)
         :amount
         (cond ; Rarely: try for *more* than we reserved
               (< (dg/double) 1/1024)  (+ (:amount pending) (zipf 1000) 1)
               ; Sometimes try for less. These are zipfian to give us a
               ; higher chance of succeeding with balancing transfers.
               (and post? (< (dg/double) 1/4)) (zipf (:amount pending))
               ; Less often, the exact amount. This is likely to fail often
               ; because we often generate pending transfers with balancing
               ; debit/credit.
               (< (dg/double) 1/16)    (:amount pending)
               ; Mostly we complete with 0, which works well with balancing
               ; transfers.
               true                   0N)
         :ledger            ledger
         :code              (cond ; Rarely: wrong code
                                  (< (dg/double) 1/256) (rand-code this)
                                  (< (dg/double) 1/2)   (:code pending)
                                  true                  0)
         :user-data         (rand-user-data this)
         :flags             flags})))

	(gen-new-transfer [this id]
    ; Single-phase transfers are roughly half pending, so in a perfect world
    ; we'd do second-phase transfers roughly 1/3 of the time.
		(if (< (dg/double) 2/3)
			(gen-new-transfer-1 this id)
			(or (gen-new-transfer-2 this id)
          ; If that fails (e.g. because we don't think there's anything
          ; pending), we shouldn't spin our wheels on useless post/voids.
          (gen-new-transfer-1 this id))))

	(gen-new-transfers [this n]
		(let [ids (range next-id (+ next-id n))]
			(->> ids
					 (mapv (fn [id]
									 (gen-new-transfer this id)))
					 chains)))

	(gen-lookup-accounts [this n]
		(->> (repeatedly (partial rand-account-id this))
				 (take n)
				 vec))

	(gen-lookup-transfers [this n]
		(->> (repeatedly (partial rand-transfer-id this))
				 (take n)
				 vec))

	(gen-query-filter [this]
		(let [flags (cond-> #{}
									; Mostly we want rcron; that way periodic reads will cover
									; more of the space.
									(< (dg/double) 9/10) (conj :reversed))
					; A pair of timestamps for min and max
					[t1 t2] (sort [(rand-timestamp this)
												 (rand-timestamp this)])]
			(cond-> {:flags flags
							 :limit (dg/long 1 32)}
				(< (dg/double) 1/4)
				(assoc :timestamp-min t1)

				(< (dg/double) 1/4)
				(assoc :timestamp-max t2)

				(< (dg/double) 1/8)
				(assoc :ledger (rand-ledger this))

				; These are relatively unlikely to match, so we generate them
				; infrequently.
				(< (dg/double) 1/16)
				(assoc :code (rand-code this))

				(< (dg/double) 1/16)
				(assoc :user-data (rand-user-data this)))))

	(gen-account-filter [this]
		(let [flags (cond-> (condp < (dg/double)
													; Very rarely, neither credits nor debits
													0.99 #{}
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
				(assoc :timestamp-min t1)

				(< (dg/double) 1/4)
				(assoc :timestamp-max t2)

				; These are relatively unlikely to match, so we generate them
				; infrequently
				(< (dg/double) 1/16)
				(assoc :user-data (rand-user-data this))

				(< (dg/double) 1/16)
				(assoc :code (rand-code this)))))

	(add-new-accounts [this new-accounts]
    ; We're invoking create-accounts.
		(let [ids (mapv :id new-accounts)]
			(assoc this
						 :next-id (inc (reduce max next-id ids))
             :ledger->accounts
             (binto (fn ledgers [ledger->accounts account]
                      (bm/update ledger->accounts
                                 (:ledger account)
                                 (fn add [accounts]
                                   (lm/add-unlikely (or accounts lm/empty)
                                                    account))))
               ledger->accounts
               new-accounts))))

  (add-new-accounts [this new-accounts results p]
    ; Zip through results, marking as likely or unseen
    (assoc this
           :ledger->accounts
           (b/forked
             (bireduce (fn [ledger->accounts account result]
                         (let [id (:id account)]
                           (bm/update ledger->accounts
                                      (:ledger account)
                                      (fn add [accounts]
                                        (let [accounts (or accounts lm/empty)]
                                          (case result
                                            (:ok :exists)
                                            (lm/is-likely accounts id)
                                            (lm/is-unseen accounts p id)))))))
                       (b/linear ledger->accounts)
                       new-accounts
                       results))))

	(add-new-transfers [this new-transfers]
		(let [ids (mapv :id new-transfers)
          ; As soon as we submit a pending transfer, it's something we could
          ; try to finish. Not always--this tends to create lots of races with
          ; a high chance of failure.
          ptids (reduce (fn [ptids t]
                          (if (:pending (:flags t))
                            (add-pending-transfer completed-transfer-ids
                                                  ptids
                                                  0.2
                                                  (:id t))
                            ptids))
                        pending-transfer-ids
                        new-transfers)]
      (assoc this
             :next-id   (inc (reduce max next-id ids))
             :transfers (reduce lm/add-unlikely transfers new-transfers)
             :pending-transfer-ids ptids)))

  (add-new-transfers [this new-transfers results p]
    ; We've completed a create-transfers request.
    (let [[transfers
           completed-transfer-ids
           pending-transfer-ids]
          (bireduce (fn [[transfers ctids ptids] transfer result]
                      (let [id (:id transfer)]
                        (case result
                          (:ok :exists)
                          (let [; When we get an OK result, we know this
                                ; transfer is likely
                                transfers (lm/is-likely transfers id)
                                ; If this transfer posted or voided something,
                                ; we should remove *that* from the pending set.
                                pid (:pending-id transfer)
                                [ctids ptids] (if (nil? pid)
                                                [ctids ptids]
                                                (complete-pending-transfer
                                                  ctids ptids p pid))
                                ; If this transfer was itself pending, we have
                                ; a second chance to record it--it definitely
                                ; exists now.
                                ptids (if (:pending (:flags transfer))
                                        (add-pending-transfer ctids ptids 1 id)
                                        ptids)]
                            [transfers ctids ptids])

                          ; When we get nil or an error code, we treat that as
                          ; an unseen read of this specific transfer--it may
                          ; fall out of the likely set.
                          [(lm/is-unseen transfers p id)
                           ctids
                           ; And (mostly) remove it from the pending txn set.
                           (remove-pending-transfer ptids p id)])))
                    [transfers completed-transfer-ids pending-transfer-ids]
                    new-transfers
                    results)]
    (assoc this
           :transfers              transfers
           :completed-transfer-ids completed-transfer-ids
           :pending-transfer-ids   pending-transfer-ids)))

	(read-accounts [this results]
		(let [timestamps (keep :timestamp results)]
			(assoc this
						 :timestamp-min (reduce min timestamp-min timestamps)
						 :timestamp-max (reduce max timestamp-max timestamps)
						 :ledger->accounts
             (reduce (fn [ledger->accounts id]
                       (bm/update ledger->accounts
                                  (account-id->ledger id)
                                  lm/is-seen
                                  id))
                     ledger->accounts
                     (keep :id results)))))

  (read-accounts [this ids results]
    (let [this' (read-accounts this results)]
      ; Incorporate negative reads
      (assoc this'
             :ledger->accounts
             (bireduce (fn [ledger->accounts id result]
                         (if result
                           ; Positive read; already handled
                           ledger->accounts
                           ; Negative read
                           (bm/update ledger->accounts
                                      (account-id->ledger id)
                                      (fn [accounts]
                                        ; Each failed read has a 50% chance to
                                        ; knock this out of the likely pool.
                                        (lm/is-unseen (or accounts lm/empty)
                                                      0.5 id)))))
                       (:ledger->accounts this')
                       ids
                       results))))

	(read-transfers [this results]
		(let [timestamps (keep :timestamp transfers)]
			(assoc this
						 :timestamp-min (reduce min timestamp-min timestamps)
						 :timestamp-max (reduce max timestamp-max timestamps)
             :transfers
						 (reduce lm/is-seen transfers (keep :id results))
             ; If we read a pending txn, we have a chance to record it as
             ; pending (unless it's already completed). This helps us complete
             ; transfers that fall through the cracks.
             :pending-transfer-ids
             (reduce (fn [ptids transfer]
                       (if (:pending (:flags transfer))
                         (add-pending-transfer completed-transfer-ids
                                               ptids
                                               0.1
                                               (:id transfer))
                         ptids))
                     pending-transfer-ids
                     results)
             ; TODO: when we read a transfer that posts/voids another, we could
             ; record that too.
             )))

  (read-transfers [this ids results]
    ; First incorporate positive reads of transfers
    (let [this' (read-transfers this results)]
      ; Now, negative reads
      (let [[transfers pending-transfer-ids]
            (bireduce (fn [[transfers pending-transfer-ids :as pair] id result]
                        (if result
                          pair
                          ; Each failed read has a 50% chance to knock this out
                          ; of the likely pool
                          [(lm/is-unseen transfers 0.5 id)
                           ; And likewise, if we fail to read something, make
                           ; it less likely we'll try to complete it
                           (remove-pending-transfer pending-transfer-ids
                                                    0.5 id)]))
                      [(:transfers this')
                       (:pending-transfer-ids this')]
                      ids
                      results)]
        (assoc this'
               :transfers transfers
               :pending-transfer-ids pending-transfer-ids))))

  (log-invoke [this invoke]
    (update this :process->invoke bim/put (:process invoke) invoke)))

(defn state
	"A fresh state."
	[]
	(map->State
		{:next-id                1N
		 :timestamp-min          Long/MAX_VALUE
		 :timestamp-max          Long/MIN_VALUE
		 :transfers              (lm/lifecycle-map)
		 :pending-transfer-ids   bs/empty
     :completed-transfer-ids bs/empty
		 :ledger->accounts       bm/empty
     :process->invoke        (bim/int-map)}))

; A generator which maintains the state and ensures its wrapped generator has
; access to it via the context map.
(defrecord GenContext [gen state]
	gen/Generator
	(op [this test ctx]
		(when-let [[op gen'] (gen/op gen test (assoc ctx :state state))]
			[op (GenContext. gen' state)]))

	(update [this test ctx op]
		(let [{:keys [process type f value]} op
          ; Log invocation
          state (if (h/invoke? op)
                  (log-invoke state op)
                  state)
          ; Value of the invocation that produced us
          invoke-value (when (and (h/client-op? op)
                                  (not (h/invoke? op)))
                         (:value (bim/get (:process->invoke state) process)))
          ; Various state transformations
          state
          (match [type f]
                 ; We're creating new accounts
                 [:invoke :create-accounts]
                 (add-new-accounts state value)

                 ; We're creating new transfers
                 [:invoke :create-transfers]
                 (add-new-transfers state value)

                 ; Completing accounts tells us some may be unlikely/unseen;
                 ; we're very confident when we get an OK op with error codes.
                 [:ok :create-accounts]
                 (add-new-accounts state invoke-value value 0.95)

                 ; Ditto, completing transfers
                 [:ok :create-transfers]
                 (add-new-transfers state invoke-value value 0.95)

                 ; A timeout gives us less confidence, but since TigerBeetle
                 ; basically refuses to ever nack a request, we should assume
                 ; most of these actually fail.
                 [:info :create-accounts]
                 (add-new-accounts state invoke-value nil 0.8)

                 [:info :create-transfers]
                 (add-new-transfers state invoke-value nil 0.8)

                 ; An outright failure is a strong signal
                 [:fail :create-accounts]
                 (add-new-accounts state invoke-value nil 0.95)

                 [:fail :create-transfers]
                 (add-new-transfers state invoke-value nil 0.95)

                 ; We read accounts
                 [:ok :lookup-accounts]
                 (read-accounts state invoke-value value)

                 ; We read transfers
                 [:ok :lookup-transfers]
                 (read-transfers state invoke-value value)

                 ; Get-account-transfers tells us transfers exist
                 [:ok :get-account-transfers]
                 (read-transfers state value)

                 ; Queries tell us accounts/transfers exist
                 [:ok :query-accounts]
                 (read-accounts state value)

                 [:ok :query-transfers]
                 (read-transfers state value)

                 [_ _]
                 state)
          ctx  (assoc ctx :state state)
          gen' (gen/update gen test ctx op)]
      (GenContext. gen' state))))

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
		(inc (zipf 1.5 128))))

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

(defn query-accounts-gen
	"A generator for query-accounts operations."
	[test ctx]
	{:f     :query-accounts
	 :value (gen-query-filter (:state ctx))})

(defn query-transfers-gen
	"A generator for query-transfers operations."
	[test ctx]
	{:f     :query-transfers
	 :value (gen-query-filter (:state ctx))})

(defn get-account-transfers-gen
	"A generator for get-account-transfers operations."
	[test ctx]
	{:f       :get-account-transfers
	 :value   (gen-account-filter (:state ctx))})

(defn debug-gen-gen
  "A generator which emits :debug-gen operations whose invoke value tells us
  about the current state of the generator. Helpful for figuring out what the
  generator thought was going on during the test."
  [test {:keys [state] :as ctx}]
  {:f     :debug-gen
   :value {:ledger->accounts
           (update-vals (datafy (:ledger->accounts state))
                        lm/debug)}})

(defn r-gen
	"Generator purely of read operations during the main phase."
	[{:keys [fs] :as opts}]
	(weighted-mix
		1  (when (:lookup-accounts fs)       lookup-accounts-gen)
		1  (when (:lookup-transfers fs)      lookup-transfers-gen)
		1  (when (:get-account-transfers fs) get-account-transfers-gen)
		1  (when (:query-accounts fs)        query-accounts-gen)
		1  (when (:query-transfers fs)       query-transfers-gen)))

(defn rw-gen
	"Generator of read and write events during the main phase. Takes two options:

 :ta-ratio    The ratio of create-transfers to create-accounts
 :rw-ratio    The ratio of reads to writes overall"
	[{:keys [ta-ratio rw-ratio fs] :as opts}]
	(let [; Weights for create-accounts and create-transfers
				a 1
				t (* ta-ratio a)
				; Weight of all writes
				w (+ a t)
				; Weight of all reads
				r (* rw-ratio w)]
		(weighted-mix
			a   (when (:create-accounts fs)       create-accounts-gen)
			t   (when (:create-transfers fs)      create-transfers-gen)
			r   (r-gen opts)
      ;a   debug-gen-gen
      )))

(defn rw-threads
	"Given n nodes and c threads, how many threads should do reads *and* writes?"
	[n c]
	(assert (pos? n))
	(assert (pos? c))
	(if (< c n)
		c
		(-> (quot c n)
				(/ 2)
				Math/ceil
				long
				(* n))))

(defn split-rw-gen
	"An experimental main phase generator. We aim to have half of our processes
 performing reads only, and half performing reads and writes. This is only
 useful if TigerBeetle has a write path that gets stuck when reads wouldn't.
 Since right now reads go through the full consensus commit process, there's
 no point to doing this."
	[opts]
	(let [c (:concurrency opts)
				n (count (:nodes opts))]
		(gen/reserve (rw-threads n c) (rw-gen opts)
								 (r-gen opts))))

(def gen
	"The main phase generator."
	rw-gen)

(def final-gen-chunk-size
	"Roughly how many things do we try to read per final read?"
	128)

(defrecord FinalReadGen [f ; f for emitted ops
												 ; Bifurcan map of first id in chunk to a vector of IDs
												 chunks]
	gen/Generator
	(op [this test ctx]
		(when (pos? (b/size chunks))
			; Pick a random pending chunk
      (let [i    (rand-int (b/size chunks))
            pair (b/nth chunks i)]
        [(gen/fill-in-op
           {:f f, :value (bm/value pair), :chunk-id (bm/key pair)}
           ctx)
         this])))

  (update [this test ctx op]
    ; Every time we perform an OK read of a chunk-id, clear it from our pending
    ; chunks.
    (if (and (h/ok? op)
             (= f (:f op))
             (:chunk-id op))
      (update this :chunks bm/remove (:chunk-id op))
      this)))

(defn final-*-gen
  "Shared logic for both final read generators."
  [lifecycle-map-field f]
  (reify gen/Generator
    (op [this test ctx]
      (gen/op
        (let [lm (get (:state ctx) lifecycle-map-field)
              ; Ugh this is SUCH a hack, since accounts and transfers
              ; are now represented differently.
              unseen (if (instance? LifecycleMap lm)
                       (bm/keys (lm/unseen lm))
                       (mapcat (comp bm/keys lm/unseen bm/value) lm))]
          (->> unseen
               sort
               (partition-all 128)
               (reduce (fn [m chunk]
                         (bm/put m (first chunk) (vec chunk)))
                       bm/empty)
               (FinalReadGen. f)))
        test
        ctx))

    (update [this test ctx op]
      this)))

(defn final-accounts-gen
  "A generator that tries to observe every unseen account."
  []
  (final-*-gen :ledger->accounts :lookup-accounts))

(defn final-transfers-gen
  "A generator that tries to observe every unseen transfer."
  []
  (final-*-gen :transfers :lookup-transfers))

(defn final-gen
  "Final generator. Makes sure we try to observe every unseen account and
  transfer."
  []
  [;{:f :debug-gen}
   (final-accounts-gen)
   (final-transfers-gen)])
