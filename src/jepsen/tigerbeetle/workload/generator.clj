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
            [clojure.math.numeric-tower :refer [lcm]]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [generator :as gen]
                    [history :as h]
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
   (b/nth xs (zipf skew (b/size xs))))
  ([skew xs not-found]
   (if (= 0 (b/size xs))
     not-found
     (zipf-nth skew xs))))

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

(defn chains
  "Takes a vector of events and partitions them into chains with zipfian
  distributed lengths, setting the :linked flag on events as appropriate."
  [events]
  (let [n (count events)]
    (if (<= n 1)
      events
      (loop [i            0             ; Index
             chain-length (zipf 2.0 n)  ; How many more events in this chain
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
                     (zipf 2.0 n)
                     events)

              ; Link
              true
              (recur (inc i)
                     (dec chain-length)
                     (assoc! events i
                             (update (get events i) :flags conj :linked))))))))

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

	(gen-query-filter [state]
										"Generates a query filter map for query-accounts or
					query-transfers.")

	(gen-account-filter [state]
											"Generates an account filter map for a
					 get-account-transfers operation."))

(defrecord State
	[
	 next-id             ; The next ID we'll hand out
	 ^long timestamp-min ; The smallest timestamp observed
	 ^long timestamp-max ; The largest timestamp observed
	 accounts            ; A Bifurcan map of id->account
	 transfers           ; A Bifurcan map of id->transfer
	 ledger->account-ids ; A Bifurcan map of ledger to lists of account IDs.
	 pending-transfers   ; A Bifurcan map of id->transfer,
	 ; for transfers we intend to complete later.
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
			(->> ids
					 (mapv (fn [id]
									 {:id        id
										:ledger    (rand-ledger this)
										:code      (rand-code this)
										:user-data (rand-user-data this)
										:flags     #{}}))
					 chains)))

	(gen-new-transfer-1 [this id]
		(let [; TODO: occasionally generate incompatible ledgers
					ledger (rand-ledger this)
					debit-account-id (rand-account-id this ledger)
					; Mostly generate distinct debit/credit accounts
					credit-account-id (loop [tries 3]
															(let [id (rand-account-id this ledger)]
																(if (and (pos? tries) (= id debit-account-id))
																	(recur (dec tries))
																	id)))
					flags (cond-> #{}
									(< (dg/double) 1/2) (conj :pending))]
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
    ; TODO: sometimes try to conclude a non-pending transfer
    (let [pending (if (= 0 (b/size pending-transfers))
                      ; Make up a transfer that doesn't exist
                      (gen-new-transfer-1 this (rand-transfer-id this))
                      ; Complete a random pending transfer
                      (bm/value (zipf-nth pending-transfers)))
          ledger (if (< (dg/double) 1/256)
                   (rand-ledger this)
                   (:ledger pending))
          post? (< (dg/double) 1/2)
          void? (not post?)
          flags (cond-> #{(if post?
                            :post-pending-transfer
                            :void-pending-transfer)})]
      {:id                id
       :pending-id        (:id pending)
       :debit-account-id
       (cond ; Rarely, mismatch
             (< (dg/double) 1/256)  (rand-account-id this ledger)
             (< (dg/double) 1/2)    (:debit-account-id pending)
             true                   0)
       :credit-account-id
       (cond ; Rarely, mismatch
             (< (dg/double) 1/256)  (rand-account-id this ledger)
             (< (dg/double) 1/2)    (:credit-account-id pending)
             true                   0)
       :amount
       (cond ; Rarely: try for *more* than we reserved
             (< (dg/double) 1/256)  (+ (:amount pending) (zipf 1000) 1)
             ; Often: try for less
             (< (dg/double) 1/2)    (zipf (:amount pending))
             ; Or the exact amount
             (< (dg/double) 1/2)    (:amount pending)
             true                   0)
       :ledger            ledger
       :code              (cond ; Rarely: wrong code
                                (< (dg/double) 1/256) (rand-code this)
                                (< (dg/double) 1/2)   (:code pending)
                                true                  0)
       :user-data         (rand-user-data this)
       :flags             flags}))

	(gen-new-transfer [this id]
		; Single-phase transfers are roughly half pending, so we do second-phase
		; transfers roughly 1/3 of the time.
		(if (< (dg/double) 2/3)
			(gen-new-transfer-1 this id)
			(gen-new-transfer-2 this id)))

	(gen-new-transfers [this n]
		(let [ids (range next-id (+ next-id n))]
			(->> ids
					 (mapv (fn [id]
									 (gen-new-transfer this id)))
					 chains)))

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
						 :next-id           (inc (reduce max next-id ids))
						 :transfers         (into-by-id transfers new-transfers)
						 :unseen-transfers  (binto bs/add unseen-transfers ids)
						 :pending-transfers (into-by-id pending-transfers
																						(filter (comp :pending :flags)
																										new-transfers)))))

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
				(assoc :timestamp-min t1)

				(< (dg/double) 1/4)
				(assoc :timestamp-max t2)

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
		 :pending-transfers   bm/empty
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

				; Queries tell us about accounts/transfers
				[:ok :query-accounts]
				(GenContext. gen' (saw-accounts state value))

				[:ok :query-transfers]
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
 Takes a flat series of `weight gen` pairs: a generator with weight 6 is chosen
 three times as often as one with weight 2. Updates are propagated to all
 generators."
	[& weight-gens]
	(assert (even? (count weight-gens)))
	(when (seq weight-gens)
		(let [weight-gens  (partition 2 weight-gens)
					weights      (long-weights (map first weight-gens))
					total-weight (reduce + weights)
					gens         (mapv second weight-gens)]
			(WeightedMix. total-weight weights gens
										(rand-weighted-index total-weight weights)))))

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
			r   (r-gen opts))))

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
  [unseen-field f]
  (reify gen/Generator
    (op [this test ctx]
      (gen/op
        (->> (get (:state ctx) unseen-field)
             sort
             (partition-all 128)
             (reduce (fn [m chunk]
                       (bm/put m (first chunk) (vec chunk)))
                     bm/empty)
             (FinalReadGen. f))
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
