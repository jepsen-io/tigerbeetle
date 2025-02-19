(ns jepsen.tigerbeetle.workload.indefinite
  "A workload which verifies whether or not indefinite writes are aborted. We
  open a single smart client and have all processes submit request to it using
  the async API. If the nemesis takes down the cluster, or otherwise elevates
  latencies, we want to know if timed-out futures still complete. We measure
  this by performing a series of final reads at the end of the test, checking
  to see what fraction of info writes actually succeeded."
  (:require [bifurcan-clj [core :as b]
                          [map :as bm]
                          [list :as bl]
                          [set :as bs]]
            [clojure [datafy :refer [datafy]]
                     [pprint :refer [pprint]]]
            [clojure.core.match :refer [match]]
            [clojure.data.generators :as dg]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [letr loopr]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [history :as h]
                    [util :refer [timeout zipf zipf-default-skew
                                  nil-if-empty]]]
            [jepsen.tigerbeetle [core :refer :all]
                                [checker :as tigerbeetle.checker]
                                [client :as c]]
            [potemkin :refer [definterface+]]
            [slingshot.slingshot :refer [try+ throw+]]
            [tesser.core :as t]))

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    ; The whole point of this workload is to test what happens if we don't
    ; close on timeout and just leave the future as timed out.
    (Client. (c/open (assoc test :close-on-timeout? false)
                     node)))

  (setup! [this test])

  (invoke! [this test {:keys [f value] :as op}]
    (try+
      (merge op
             {:type :ok}
             (case f
               :create-accounts (c/create-accounts! conn value)
               :lookup-accounts (c/lookup-accounts conn value)))
    (catch [:type :timeout] e
      (assoc op :type :info, :value nil, :error :timeout))))

  (teardown! [this test])

  (close! [this test]
    (c/close! conn))

  ; Unlike the standard client, we're going to let callers re-use this client.
  ; Otherwise we'd close it immediately, and we're trying to measure what
  ; happens when async callers time out but *don't* nuke the whole client.
  client/Reusable
  (reusable? [this test]
    true))

(defn client
  "A new, blank client."
  []
  (Client. nil))

(defn main-gen
  "Emits a series of create-accounts ops"
  [test ctx]
  (let [id (inc (or (peek (:ids ctx)) 0N))
        account {:id        id
                 :ledger    1
                 :code      1
                 :user-data 0
                 :flags     #{}
                 :timestamp 0}]
    {:f :create-accounts, :value [account]}))

; Performs final reads of accounts until all ok
(defrecord FinalReadGen [; Bifurcan map of first id in chunk to a vector of IDs
                         chunks]
  gen/Generator
  (op [this test ctx]
    (when (pos? (b/size chunks))
      ; Pick a random pending chunk
      (let [i    (rand-int (b/size chunks))
            pair (b/nth chunks i)]
        [(gen/fill-in-op
           {:f        :lookup-accounts
            :value    (bm/value pair)
            :chunk-id (bm/key pair)}
           ctx)
         this])))

  (update [this test ctx op]
    ; Every time we perform an OK read of a chunk-id, clear it from our pending
    ; chunks.
    (info :update op (and (h/ok? op) (= :lookup-accounts (:f op)) (:chunk-id op)))
    (if (and (h/ok? op)
             (= :lookup-accounts (:f op))
             (:chunk-id op))
      (update this :chunks bm/remove (:chunk-id op))
      this)))

; At the end of the test, unfurls into a FinalReadGen
(defrecord FinalGenInit []
  gen/Generator
  (op [this test ctx]
    (let [gen (->> (:ids ctx)
                   sort
                   (partition-all 128)
                   (reduce (fn [m chunk]
                             (bm/put m (first chunk) (vec chunk)))
                           bm/empty)
                   (FinalReadGen.))]
      (gen/op gen test ctx)))

  (update [this test ctx op]
    this))

; Maintains shared context for normal and final generators
(defrecord ContextGen
  [; Inner generator
   gen
   ; Vector of all IDs we generated
   ids]

  gen/Generator
  (gen/op [this test ctx]
    (when-let [[op gen'] (gen/op gen test (assoc ctx :ids ids))]
      [op (ContextGen. gen' ids)]))

  (gen/update [this test ctx op]
    (if (and (identical? (:type op) :invoke)
             (identical? (:f op) :create-accounts))
      (let [ids' (into ids (map :id (:value op)))
            gen' (gen/update gen test (assoc ctx :ids ids') op)]
        (ContextGen. gen' ids'))
      (let [gen' (gen/update gen test ctx op)]
        (ContextGen. gen' ids)))))

(defn wrap-gen
  "Wraps a generator in one that tracks IDs."
  [gen]
  (ContextGen. gen []))

(defrecord Checker []
  checker/Checker
  (check [this test history opts]
    (h/ensure-pair-index history)
    (let [infos (->> (t/filter h/info?)
                     (t/filter (h/has-f? :create-accounts))
                     (t/map (fn [op']
                              (->> (h/invocation history op')
                                   :value
                                   (keep :id)
                                   bs/from)))
                     tigerbeetle.checker/bset-union)
          seen (->> (t/filter h/ok?)
                    (t/filter (h/has-f? :lookup-accounts))
                    (t/map (fn [op]
                             (->> (:value op)
                                  (keep :id)
                                  bs/from)))
                    tigerbeetle.checker/bset-union)
          {:keys [infos seen]} (->> (t/fuse {:infos infos
                                             :seen seen})
                                    (h/tesser history))
          recovered        (bs/intersection infos seen)
          incomplete       (bs/difference infos seen)
          infos-count      (b/size infos)
          seen-count       (b/size seen)
          recovered-count  (b/size recovered)
          incomplete-count (b/size incomplete)]
      {:valid?             true
       :infos-count        infos-count
       ;:infos              (datafy infos)
       ;:seen               (datafy seen)
       :seen-count         seen-count
       :incomplete-count   incomplete-count
       :recovered-count    recovered-count})))

(defn workload
  "Takes CLI opts and constructs a partial test map."
  [opts]
  {:client          (client)
   :generator       main-gen
   :final-generator (FinalGenInit.)
   :wrap-generator  wrap-gen
   :checker         (Checker.)})
