(ns jepsen.tigerbeetle.nemesis
  "Fault injection for TigerBeetle"
  (:require [clojure [set :as set]]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [real-pmap]]
            [jepsen [control :as c]
                    [nemesis :as n]
                    [generator :as gen]
                    [net :as net]
                    [util :as util]
                    [role :as role]]
            [jepsen.nemesis [combined :as nc]
                            [file :as nf]
                            [time :as nt]]
            [jepsen.tigerbeetle [db :as db]]
            [slingshot.slingshot :refer [try+ throw+]]))

(defn package-gen-helper
  "Helper for package-gen. Takes a collection of packages and draws a random
  nonempty subset of them."
  [packages]
  (when (seq packages)
    (let [pkgs (->> packages
                    ; And pick a random subset of those
                    util/random-nonempty-subset
                    vec)]
      ; If we drew nothing, try again.
      (if (seq pkgs)
        pkgs
        (do ; (info "no draw, retrying")
            (recur packages))))))

(defn package-gen
  "For long-running tests, it's nice to be able to have periods of no faults,
  periods with lots of faults, just one kind of fault, etc. This takes a time
  period in seconds, which is how long to emit nemesis operations for a
  particular subset of packages. Takes a collection of packages. Constructs a
  nemesis generator which emits faults for a shifting collection of packages
  over time."
  [period packages]
  ; We want a sequence of random subsets of packages
  (repeatedly
    (fn rand-pkgs []
      (let [; Pick packages
            pkgs (if (< (rand) 1/4)
                   ; Roughly 1/4 of the time, pick no pkgs
                    []
                    (package-gen-helper packages))
            ; Construct combined generators
            gen       (if (seq pkgs)
                        (apply gen/any (map :generator pkgs))
                        (gen/sleep period))
            final-gen (keep :final-generator pkgs)]
        ; Ops from the combined generator, followed by a final gen
        [(gen/log (str "Shifting to new mix of nemeses: "
                       (pr-str (map (comp n/fs :nemesis) pkgs))))
         (gen/time-limit period gen)
         final-gen]))))

(defn large-clock-skew-package
  "A custom scenario which introduces a large clock error across all nodes and
  sits back to wait."
  [opts]
  ; dt here is seconds between nodes. 21 is the minimum sufficient to break
  ; liveness with TB's defaults.
  (let [dt 3600
        needed? (:large-clock (:faults opts))]
    {:generator (when needed?
                  (gen/delay 5
                             [{:type :info, :f :check-clock-offsets}
                              (gen/once
                                (fn [test ctx]
                                  {:type :info
                                   :f    :bump-clock
                                   :value
                                   (zipmap (:nodes test)
                                           ; Default tolerance is 10s
                                           (iterate (partial + (* 1000 dt))
                                                    0))}))
                              (gen/repeat
                                {:type :info, :f :check-clock-offsets})]))
     :nemesis n/noop
     :final-generator
     (when needed?
       (gen/once
         (fn [test ctx]
           {:type :info
            :f :reset-clock
            :value (:nodes test)})))}))

; TigerBeetle-specific file corruptions
(defrecord FileCorruptionNemesis []
  n/Nemesis
  (setup! [this test] this)

  (invoke! [this test {:keys [f value] :as op}]
    (case f
      ; Reformat any nodes that look like they need it.
      :maybe-reformat
      (assoc op :value (c/on-nodes test (:nodes test) db/maybe-reformat!))))

  (teardown! [this test]
    this)

  n/Reflection
  (fs [this]
    [:maybe-reformat]))

(defrecord FileCorruptionGenerator [gen targets safe-nodes]
  gen/Generator
  (op [this test ctx]
    (if (nil? safe-nodes)
      ; The first time we execute, we select a majority of nodes to remain safe
      (let [nodes       (:nodes test)
            m           (util/majority (count nodes))
            safe-nodes  (set (take m (shuffle nodes)))]
        (gen/op (assoc this :safe-nodes safe-nodes) test ctx))

      ; We have a safe node.
      (let [target (rand-nth targets)
            nodes  (->> (nc/db-nodes test (:db test) target)
                        (remove safe-nodes)
                        vec)]
        (if (empty? nodes)
          ; Re-draw
          (gen/op this test ctx)
          ; Great, we have nodes. Ask the normal file corruption generator for
          ; an op and replace its nodes with ours.
          (when-let [[op gen'] (gen/op gen test ctx)]
            (if (identical? :pending op)
              [:pending this]
              [(update op :value assoc 0 nodes)
               (assoc this :gen gen')]))))))

  (update [this test ctx op]
    (assoc this :gen (gen/update gen test ctx op))))

(defn file-corruption-package
  "A generator of file corruption operations. For TigerBeetle, we want to
  ensure in general that a majority of nodes are never corrupted. Otherwise,
  this is basically like `nemesis.combined/file-corruption-package`."
  [{:keys [db faults file-corruption interval] :as opts}]
  ; Start with the stock file corruption package
  (let [pkg (nc/file-corruption-package opts)
        ; Then replace the generator
        targets (:targets file-corruption (nc/node-specs db))
        gen     (when-let [gen (:generator pkg)]
                  (gen/any (->> {:type :info, :f :maybe-reformat}
                                gen/repeat
                                (gen/stagger interval))
                           (FileCorruptionGenerator. gen targets nil)))]
    (assoc pkg
           :generator       gen
           :final-generator (when gen
                              [{:type :info, :f :maybe-reformat}
                               {:type :info, :f :start, :value :all}])
           :nemesis         (FileCorruptionNemesis.)
           :perf            nil)))

; Use `tigerbeetle inspect constants` to obtain these numbers.
(def superblock-size
  "Size of superblock, in bytes"
  (* 96 1024))

(def wal-headers-size
  "Size of WAL headers, in bytes"
  (* 256 1024))

(def wal-prepares-size
  "Size of WAL prepares, in bytes"
  (* 1024 1024 1024))

(def client-replies-size
  "Size of client replies, in bytes"
  (* 64 1024 1024))

(def grid-padding-size
  "Size of grid padding, in bytes"
  (* 160 1024))

(def grid-block-size
  "How big is a block in the grid?"
  (* 512 1024))

(def superblock-zone
  "The [lower, upper) region, in bytes, of the file devoted to storing the
  superblock."
  [0 superblock-size])

(def wal-headers-zone
  "The [lower, upper) region, in bytes, of the file devoted to WAL headers."
  (let [start (second superblock-zone)]
    [start (+ start wal-headers-size)]))

(def wal-prepares-zone
  "The [lower, upper) region, in bytes, of the file devoted to WAL prepares."
  (let [start (second wal-headers-zone)]
    [start (+ start wal-prepares-size)]))

(def wal-zone
  "The [lower, upper) region, in bytes, of the file devoted to the write-ahead
  log"
  [(first wal-headers-zone) (second wal-prepares-zone)])

(def client-replies-zone
  "The [lower, upper) region, in bytes, of the file devoted to storing client
  replies."
  (let [start (second wal-zone)]
    [start (+ start client-replies-size)]))

(def grid-zone
  "The [lower, nil] region, in bytes, of the file devoted to storing grid
  blocks. Here, `nil` signifies unbounded."
  [(+ (second client-replies-zone) grid-padding-size) nil])

(def file-targets
  "A set of ways we can target nodes for file corruption. :one, :minority,
  :majority, and :all target subsets of the test's nodes, picking a fixed
  subset for the duration of the test, and can affect the entire file. :helix
  targets all nodes, but picks disjoint chunks of the file."
  #{:one
    :minority
    :majority
    :all
    :helix})

(def file-zones
  "A map of keyword zones (e.g. :wal) to [lower, upper) ranges of the file
  where that zone is stored. Used to control where file corruption occurs."
  {:superblock     superblock-zone
   :wal-headers    wal-headers-zone
   :wal-prepares   wal-prepares-zone
   :wal            wal-zone
   :client-replies client-replies-zone
   :grid           grid-zone})

(def file-zone-chunk-sizes
  "We choose different chunk sizes for each zone of the file."
  {:superblock     (* 24 1024)       ; Each copy is 24K
   :wal-headers    (* 4 1024)        ; Each sector is 4K
   :wal-prepares   (* 1024 1024)     ; Each prepare is 1 MB
   :wal            (* 1024 1024)
   :client-replies (* 1024 1024)     ; Each reply is 1 MB
   :grid           (* 512 1024 10)}) ; Each block is 512 KB

(defn add-zone-fn
  "Takes a vector of zone names like [:superblock]. Returns a function which
  takes a nemesis op for file corruptions, picks a random zone, and fills in
  :zone, :start, :end, :chunk-size, and :probability options for each
  corruption. Zones can be nil, in which case we return identity."
  [zones]
  (if zones
    (fn add-zone [op]
      (let [zone (rand-nth zones)
            [start end] (get file-zones zone)
            chunk-size (get file-zone-chunk-sizes zone)
            value' (map (fn [corruption]
                          (cond-> (assoc corruption
                                         :zone  zone
                                         :start start
                                         :end   end
                                         :chunk-size chunk-size)
                            ; If we're doing a bitflip, add a probability
                            (= :bitflip-file-chunks (:f op))
                            (assoc :probability
                                   ; 2 flips per chunk ought to do it--we don't
                                   ; want to go too low, lest we do nothing at
                                   ; all.
                                   (double (/ 2 chunk-size)))))
                        (:value op))]
        (assoc op :value value')))
    identity))

(defn global-snapshot-package
  "This should ABSOLUTELY mess things up. We'll kill every node, roll back all
  their data files, then restart them."
  [{:keys [faults interval] :as opts}]
  (let [needed? (faults :global-snapshot)]
    {:nemesis nil ; Handled by copy-file-chunks-helix-package
     :generator
     (when needed?
       (gen/cycle
         [{:type :info, :f :kill, :value :all}
          (gen/once
            (fn [test ctx]
              {:type :info,
               :f :snapshot-file-chunks,
               :value (mapv (fn [node]
                              {:node node
                               :chunk-size (* 1024 1024 1024 1024)})
                           (:nodes test))}))
          {:type :info, :f :start, :value :all}
          (gen/sleep 10) ; Here's where the work happens
          {:type :info, :f :kill, :value :all}
          (gen/once
            (fn [test ctx]
              {:type :info,
               :f :restore-file-chunks,
               :value (mapv (fn [node]
                              {:node node
                               :chunk-size (* 1024 1024 1024 1024)})
                           (:nodes test))}))
          {:type :info, :f :start, :value :all}
          (gen/sleep 10) ; And more work here
          ]))
     :final-generator {:type :info, :f :start, :value :all}}))

(defn snapshot-file-chunks-package
  "A nemesis package which snapshots and restores the given zone of the data
  file, in its entirety, on selected nodes."
  [{:keys [faults interval file-zones] :as opts}]
  (let [needed? (faults :snapshot-file-chunks)
        add-zone (add-zone-fn file-zones)]
    {:nemesis nil ; Handled by copy-file-chunks-helix-package
     :generator (when needed?
                  (->> (gen/flip-flop
                         (gen/repeat {:type :info, :f :snapshot-file-chunks})
                         (gen/repeat {:type :info, :f :restore-file-chunks}))
                       (nf/nodes-gen
                         #_ (comp util/minority count :nodes)
                         1)
                       (gen/map add-zone)
                       (gen/stagger interval)))
     :perf #{{:name  "snapshot-file-chunks"
              :fs    #{:snapshot-file-chunks :restore-file-chunks}
              :start #{}
              :stop  #{}
              :color "#D2E9A0"}}}))

(defn corrupt-file-package
  "A nemesis package for file corruptions."
  [{:keys [faults interval corrupt-file] :as opts}]
  (let [{:keys [targets zones]} corrupt-file
        ; These are the specific faults we can perform
        faults (set/intersection faults
                                 #{:bitflip-file-chunks
                                   :copy-file-chunks
                                   :snapshot-file-chunks})
        needed? (seq faults)
        ; Generator of core faults, with no values attached
        f-gen (->> faults
                   (mapv
                     {:bitflip-file-chunks
                      (gen/repeat {:type :info, :f :bitflip-file-chunks})

                      :copy-file-chunks
                      (gen/repeat {:type :info, :f :bitflip-file-chunks})

                      :snapshot-file-chunks
                      (gen/flip-flop
                        (gen/repeat
                          {:type :info, :f :snapshot-file-chunks})
                        (gen/repeat
                          {:type :info, :f :restore-file-chunks}))})
                   gen/mix)
        ; Provide values which target specific nodes, indexes, and moduli
        gen (case targets
              :helix    (nf/helix-gen f-gen)
              :one      (nf/nodes-gen 1 f-gen)
              :minority (nf/nodes-gen (comp util/minority count :nodes)
                                      f-gen)
              :majority (nf/nodes-gen (comp util/majority count :nodes)
                                      f-gen)
              :all      (nf/nodes-gen (comp count :nodes)
                                      f-gen))
        ; To those faults, attach zones with :start, :stop, :chunk-size, etc
        gen (gen/map (add-zone-fn zones) gen)
        ; If we are performing anything other than a helical fault, we may
        ; corrupt the superblock entirely, requiring a reformat.
        reformat? (and (not (#{:helix} targets))
                       (some #{:superblock} zones))
        gen (if reformat?
              (gen/any gen
                       (gen/repeat {:type :info, :f :maybe-reformat}))
              gen)
        ; And slow down
        gen (gen/stagger interval gen)]
    {:nemesis         (nf/corrupt-file-nemesis
                        {:file db/data-file
                         ; By default, 16 MB
                         :chunk-size (* 1024 1024 16)})
     :generator       (when needed? gen)
     :final-generator (when (and needed? reformat?)
                        [{:type :info, :f :maybe-reformat}
                         {:type :info, :f :start, :value :all}])
     :perf #{{:name   "corrupt-file"
              :fs     #{:bitflip-file-chunks
                        :copy-file-chunks
                        :snapshot-file-chunks
                        :restore-file-chunks
                        :maybe-reformat}
              :start  #{}
              :stop   #{}
              :color  "#D2E9A0"}}}))

(defn bitflip-file-chunks-helix-package
  "A nemesis package which causes bitflips in selected zones of a data file,
  in a helical pattern across all nodes."
  [{:keys [faults interval file-zones] :as opts}]
  (let [needed? (faults :bitflip-file-chunks-helix)]
    {; Nemesis provided by copy-file-chunks-helix-package
     :generator (when needed?
                  (->> (gen/any
                         (->> {:type :info, :f :bitflip-file-chunks}
                              gen/repeat
                              nf/helix-gen
                              (gen/map (add-zone-fn file-zones)))
                         ; I think we can get away without this, so long as
                         ; we constrain ourselves to correct zone/block
                         ; alignment
                         #_(->> {:type :info, :f :maybe-reformat}
                              (gen/repeat)))
                       (gen/stagger interval)))
     :final-generator (when needed?
                        [#_{:type :info, :f :maybe-reformat}
                         {:type :info, :f :start, :value :all}])
     :perf #{{:name   "bitflip-file-chunks"
              :fs     #{:bitflip-file-chunks}
              :start  #{}
              :stop   #{}
              :color  "#D2E9A0"}}}))

(defn copy-file-chunks-helix-package
  "A nemesis package which introduces file corruptions in a helical pattern
  across the cluster. Divides files into chunks of 16 MB and corrupts every
  chunk on exactly one node."
  [{:keys [faults interval file-zones] :as opts}]
  (let [needed? (faults :copy-file-chunks-helix)
        ; Augment a corrupt-file-chunks operation with a :zone (for humans) and
        ; :start/:end (for the nemesis).
        add-zone (add-zone-fn file-zones)]
    {:nemesis   (nf/corrupt-file-nemesis
                  {:file db/data-file
                   ; By default, 16 MB
                   :chunk-size (* 1024 1024 16)})
     :generator (when needed?
                  (->> (gen/any
                         (->> {:type :info, :f :copy-file-chunks}
                              gen/repeat
                              nf/helix-gen
                              (gen/map add-zone))
                         ; If we break a superblock, we'll need to reformat the
                         ; node
                         (->> {:type :info, :f :maybe-reformat}
                              (gen/repeat)))
                       (gen/stagger interval)))
     :final-generator (when needed?
                        [{:type :info, :f :maybe-reformat}
                         {:type :info, :f :start, :value :all}])
     :perf #{{:name   "copy-file-chunks"
              :fs     #{:copy-file-chunks :maybe-reformat}
              :start  #{}
              :stop   #{}
              :color  "#D2E9A0"}}}))

(defn package
  "Takes CLI opts. Constructs a nemesis and generator for the test."
  [opts]
  (let [opts (update opts :faults set)
        packages
        (->> (concat
               ; Standard packages, except we do file corruption ourselves.
               (nc/nemesis-packages
                 (update opts :faults disj :file-corruption))
               [(large-clock-skew-package opts)
                (corrupt-file-package opts)
                ;(copy-file-chunks-helix-package opts)
                ;(bitflip-file-chunks-helix-package opts)
                ;(snapshot-file-chunks-package opts)
                (global-snapshot-package opts)
                (file-corruption-package opts)]))

        nsp (:stable-period opts)]
    ;(info :packages (map (comp n/fs :nemesis) packages))

    (cond-> (nc/compose-packages packages)
      nsp (assoc :generator (package-gen nsp packages)))))
