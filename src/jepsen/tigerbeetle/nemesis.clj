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
  ensure that one node is never corrupted. Otherwise, this is basically like
  `nemesis.combined/file-corruption-package`."
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

(defn corrupt-file-chunks-helix-package
  "A nemesis package which introduces file corruptions in a helical pattern
  across the cluster. Divides files into chunks of 16 MB and corrupts every
  chunk on exactly one node."
  [{:keys [faults interval] :as opts}]
  (let [needed? (faults :corrupt-file-chunks-helix)]
    {:nemesis   (nf/corrupt-file-nemesis
                  {:file db/data-file
                   :chunk-size (* 1024 1024 16)})
     :generator (when needed?
                  (->> (gen/any (nf/corrupt-file-chunks-helix-gen)
                                (gen/repeat {:type :info, :f :maybe-reformat}))
                       (gen/stagger interval)))
     :final-generator (when needed?
                        [{:type :info, :f :maybe-reformat}
                         {:type :info, :f :start, :value :all}])
     :perf #{{:name   "corrupt-file-chunks"
              :fs     #{:corrupt-file-chunks}
              :color "#D2E9A0"}}}))

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
                (corrupt-file-chunks-helix-package opts)
                (file-corruption-package opts)]))

        nsp (:stable-period opts)]
    ;(info :packages (map (comp n/fs :nemesis) packages))

    (cond-> (nc/compose-packages packages)
      nsp (assoc :generator (package-gen nsp packages)))))
