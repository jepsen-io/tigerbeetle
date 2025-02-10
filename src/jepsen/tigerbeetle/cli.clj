(ns jepsen.tigerbeetle.cli
  "Command-line entry point for TigerBeetle tests"
  (:require [clojure [string :as str]]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
                    [cli :as cli]
                    [control :as control]
                    [generator :as gen]
                    [nemesis :as jepsen.nemesis]
                    [os :as os]
                    [tests :as tests]
                    [util :as util]]
            [jepsen.control.scp :as scp]
            [jepsen.tigerbeetle [client :as client]
                                [checker :as tc]
                                [core :refer [all-fs]]
                                [db :as db]
                                [nemesis :as nemesis]]
            [jepsen.tigerbeetle.workload [transfer :as transfer]
                                          [idempotence :as idempotence]]
            [jepsen.os.debian :as debian]))

(def workloads
  "A map of workload names to functions that take CLI options and return
  workload maps"
  {:none           (constantly tests/noop-test)
   :transfer       transfer/workload
   :idempotence    idempotence/workload})

(def standard-workloads
  "All the workloads we run by default."
  [:transfer
   :idempotence])

(def db-node-targets
  "Different ways we can target single nodes for database faults."
  #{:one
    :minority
    :majority
    :primaries
    :all})

(def nemeses
  "Basic nemeses we have available."
  #{:kill
    :pause
    :partition
    :clock
    :large-clock
    :file-corruption
    :global-snapshot
    :snapshot-file-chunks
    :bitflip-file-chunks
    :copy-file-chunks})

(def file-corruption-nemeses
  "Which nemeses corrupt files (and therefore require file/zone targets)?"
  #{:snapshot-file-chunks
    :bitflip-file-chunks
    :copy-file-chunks})

(def standard-nemeses
  "Combinations of nemeses we run by default."
  [; Nothing
   []
   ; One fault at a time
   [:partition]
   [:kill]
   [:pause]
   [:bitflip-file-chunks :kill]
   [:copy-file-chunks :kill]
   [:snapshot-file-chunks :kill]
   [:clock]
   ; General chaos
   [:partition :pause :kill :clock :bitflip-file-chunks :copy-file-chunks
    :snapshot-file-chunks]])

(def special-nemeses
  "A map of special nemesis names to collections of faults."
  {:none []
   :all (peek standard-nemeses)})

(def standard-file-corruption-opts
  "A collection of maps defining combinations of file zones and node targets
  for tests. We choose these carefully; TigerBeetle can't survive all kinds of
  filesystem faults."
  [; Minority faults are survivable everywhere.
   {:nemesis-file-targets :minority
    :nemesis-file-zones (keys nemesis/file-zones)}
   ; Helical faults will break TB if we do them in the WAL. Otherwise they're
   ; survivable.
   {:nemesis-file-targets :helix
    :nemesis-file-zones [:superblock :client-replies :grid]}])

(defn parse-comma-kws
  "Takes a comma-separated string and returns a collection of keywords."
  [spec]
  (->> (str/split spec #",")
       (remove #{""})
       (map keyword)))

(defn parse-nemesis-spec
  "Takes a comma-separated nemesis string and returns a collection of keyword
  faults."
  [spec]
  (->> (parse-comma-kws spec)
       (mapcat #(get special-nemeses % [%]))
       set))

(def logging-overrides
  "New logging levels to quiet down noisy libraries"
  {; e.g.
   ; "org.apache.kafka.common.utils.AppInfoParser"                     :warn
   })

(defn test-name
  "Takes CLI options and constructs a test name as a string."
  [opts]
  (str (if-let [z (:zip opts)]
         (let [[_ filename] (re-find #"([^/]+)$" z)]
           filename)
         (:version opts))
       " " (name (:workload opts))
       " c=" (name (:client-nodes opts))
       (when (some file-corruption-nemeses (:nemesis opts))
         (str " t=" (name (:nemesis-file-targets opts))
              " z=" (->> (:nemesis-file-zones opts)
                         (map name)
                         (str/join ","))))
       (when-let [n (:nemesis opts)]
         (str " " (->> n (map name) sort (str/join ","))))))

(defn panic-checker
  "Looks for panic messages in the log file, *except* for panics we expect to
  cause, like missing superblocks."
  []
  (let [checker (checker/log-file-pattern #"panic: " "tigerbeetle.log")]
    (reify checker/Checker
      (check [this test history opts]
        (let [res (checker/check checker test history opts)
              interesting-matches
              (remove (fn [match]
                        (some #(re-find % (:line match))
                              db/expected-file-log-patterns))
                      (:matches res))]
          (if (seq interesting-matches)
            res
            (assoc res :valid? true)))))))

(defn tb-test
  "Takes CLI options and constructs a Jepsen test map"
  [opts]
  (let [; Fill in defaults for workload, nemesis-file-zones and
        ; nemesis-file-targets. We leave these blank in the arg parser so that
        ; we can do multiple choices in test-all.
        opts            (merge (first standard-file-corruption-opts)
                               {:workload :transfer}
                               opts)
        workload-name   (:workload opts)
        workload        ((workloads workload-name) opts)
        primary-tracker (client/primary-tracker)
        db              (db/db opts)
        os              debian/os
        nemesis       (nemesis/package
                        {:db            db
                         :nodes         (:nodes opts)
                         :faults        (:nemesis opts)
                         ;:partition     {:targets [:one ::majority]}
                         :pause         {:targets (:db-node-targets opts)}
                         :kill          {:targets (:db-node-targets opts)}
                         :file-corruption
                         {:targets (:db-node-targets opts)
                          :corruptions [; Truncations up to 10 GB
                                        {:type :truncate
                                         :file db/data-file
                                         :drop
                                         {:distribution :zipf
                                          :n (* 1024 1024 1024 10)}}]}
                         ; Not to be confused with file-corruption <sigh>; this
                         ; is the new file corrupter we added
                         :corrupt-file
                         {:zones   (:nemesis-file-zones opts)
                          :targets (:nemesis-file-targets opts)}
                         :stable-period (:nemesis-stable-period opts)
                         :interval      (:nemesis-interval opts)})
        ; Main workload
        generator (gen/phases
                    (->> (:generator workload)
                         (gen/stagger (/ (:rate opts)))
                         (gen/nemesis
                           (gen/phases (gen/sleep (:initial-quiet-period opts))
                                       (:generator nemesis)))
                         (gen/time-limit (:time-limit opts)))
                    ; Before we run the final nemesis generator (which recovers
                    ; the cluster) we need to give TB processes a chance to
                    ; finish crashing, if they're going to.
                    (gen/sleep 5)
                    ; We always run the nemesis final generator; it makes
                    ; it easier to do ad-hoc analysis of a running cluster
                    ; after the test.
                    (gen/nemesis (:final-generator nemesis)))
        ; With final generator, if present
        generator (if-let [fg (:final-generator workload)]
                    (gen/phases
                      generator
                      (gen/log "Waiting for recovery")
                      (gen/sleep 1)
                      (gen/time-limit (:final-time-limit opts)
                                      (gen/clients fg)))
                    generator)
        ; And wrap entire generator so we can track state.
        generator (if-let [w (:wrap-generator workload)]
                    (w generator)
                    generator)]
    (merge tests/noop-test
           opts
           {:name     (test-name opts)
            :os       os
            :db       db
            :primary-tracker primary-tracker
            :checker  (checker/compose
                        {:perf       (checker/perf)
                         :node-perf  (tc/node-perf-checker)
                         :clock      (checker/clock-plot)
                         :stats      (checker/stats)
                         :exceptions (checker/unhandled-exceptions)
                         :panic      (panic-checker)
                         :workload   (:checker workload)})
            :client    (:client workload)
            :nemesis   (:nemesis nemesis jepsen.nemesis/noop)
            :plot      {:nemeses (:perf nemesis)}
            :generator generator
            :logging   {:overrides logging-overrides}
            :nonserializable-keys [:primary-tracker]
            })))

(def cli-opts
  "Command-line option specification"
  [[nil "--client-nodes MODE" "Whether to connect a client to one node or all nodes. `all` increases the number of operations that succeed, since TigerBeetle generally lets all requests time out on non-leader nodes. However, it might mean missing consistency violations that occur on specific nodes. It also breaks how we do primary node inference, so nemeses that target primaries will target (typically) all nodes instead."
    :default :one
    :parse-fn keyword
    :validate [#{:one :all} "Must be one or all"]]

   [nil "--concurrency NUMBER" "How many workers should we run? Must be an integer, optionally followed by n (e.g. 3n) to multiply by the number of nodes."
    :default  "3n"
    :validate [(partial re-find #"^\d+n?$")
               "Must be an integer, optionally followed by n."]]

   [nil "--db-node-targets TARGETS" "A comma-separated list of ways to target DB nodes for faults, like 'one,majority'"
    :default [:one :primaries :majority :all]
    :parse-fn parse-comma-kws
    :validate [(partial every? db-node-targets) (cli/one-of db-node-targets)]]

   [nil "--download-data" "Whether to download data files from nodes."]

   [nil "--final-time-limit SECONDS" "How long should we run the final generator for, at most? In seconds."
    :default  300
    :parse-fn read-string
    :validate [#(and (number? %) (pos? %)) "must be a positive number"]]

   [nil "--import" "Simple flag which sets import-time-limit to 0.8x
                   time-limit."]

   [nil "--import-time-limit SECONDS" "Creates imported accounts/transfers until this many seconds into the test. Default 0 (no imported events)."
    :parse-fn read-string
    :validate [#(and (number? %) (not (neg? %))) "must be a non-negative number"]]

   [nil "--initial-quiet-period SECONDS" "How long to wait before beginning faults"
    :default 10
    :parse-fn read-string
    :validate [#(and (number? %) (pos? %)) "must be a positive number"]]

   [nil "--log-debug" "Turns on TigerBeetle debug logging."]

   [nil "--nemesis FAULTS" "A comma-separated list of nemesis faults to enable"
    :parse-fn parse-nemesis-spec
    :validate [(partial every? (into nemeses (keys special-nemeses)))
               (str (cli/one-of nemeses) " or the special nemeses, which " (cli/one-of special-nemeses))]]

   [nil "--nemesis-file-targets TARGETS" "Controls which nodes are targeted for disk faults. Can be one, minority, majority, all, or helix. Helix induces faults on every node, but arranges them so that no two chunks overlap. The others all target random subsets of a limited permutation of nodes throughout the test--minority, for example, picks a minority of nodes that can experience file corruption, and only causes faults there."
    :parse-fn keyword
    :validate [nemesis/file-targets (cli/one-of nemesis/file-targets)]]

   [nil "--nemesis-file-zones ZONES" "A comma-separated list of zones we want to interfere with in data files. Only works with corrupt-file-chunks-helix. wal-headers and wal-prepares will break TigerBeetle with helical faults; superblock, client-replies, and grid should be safe everywhere."
    :parse-fn parse-comma-kws
    :validate [(partial every? nemesis/file-zones)
               (cli/one-of nemesis/file-zones)]]

   [nil "--nemesis-interval SECS" "Roughly how long between nemesis operations."
    :default  10
    :parse-fn read-string
    :validate [pos? "Must be a positive number."]]

   [nil "--nemesis-stable-period SECS" "If given, rotates the mixture of nemesis faults over time with roughly this period."
    :default  nil
    :parse-fn parse-long
    :validate [pos? "Must be a positive number."]]

   ["-r" "--rate HZ" "Approximate request rate, in hz"
    :default 10000
    :parse-fn read-string
    :validate [pos? "Must be a positive number."]]

   [nil "--fs FUNS" "A comma-separated list of API functions (e.g. query-transfers,create-accounts) that we should run."
    :default all-fs
    :parse-fn (comp set parse-comma-kws)
    :validate [(partial every? all-fs) (cli/one-of all-fs)]]

   [nil "--rw-ratio RATIO" "Ratio of reads to writes performed by read-write processes, e.g. 2/1"
    :default  1
    :parse-fn read-string
    :validate [#(and (not (neg? %)) (rational? %)) "Must be a non-negative rational"]]

   [nil "--ta-ratio RATIO" "Ratio of create-transfers to create-accounts, e.g. 100/1"
    :default 100
    :parse-fn read-string
    :validate [#(and (not (neg? %)) (rational? %)) "Must be a non-negative rational."]]

   [nil "--timeout MILLIS" "Client timeout, in milliseconds"
    :default 5000
    :parse-fn parse-long
    :validate [pos? "Must be positive."]]

   [nil "--tcpdump" "Dumps traffic to a pcap file."]

   [nil "--zip PATH" "Installs a local zip file, rather than downloading an official release."]

   ["-v" "--version VERSION" "The TigerBeetle version to install."
    :default "0.16.26"]

   ["-w" "--workload NAME" "What workload should we run?"
    :parse-fn keyword
    :validate [workloads (cli/one-of workloads)]]
   ])

(defn opt-fn
  "Options map post-processor"
  [parsed]
  (let [opts (:options parsed)
        opts
        (cond-> opts
          (and (:import opts)
               (not (:import-time-limit opts)))
          (assoc :import-time-limit (* 0.8 (:time-limit opts))))]
    (assoc parsed :options opts)))

(defn all-tests
  "Turns CLI options into a sequence of tests."
  [opts]
  (let [nemeses   (if-let [n (:nemesis opts)]  [n] standard-nemeses)
        workloads (if-let [w (:workload opts)] [w] standard-workloads)
        file-corruption-opts
        (if (or ; They requested a specific zone or target
                (:nemesis-file-zones opts)
                (:nemesis-file-targets opts)
                ; Or the requested nemeses don't use them
                (not-any? file-corruption-nemeses nemeses))
          ; Use specified faults exactly
          [(select-keys opts [:nemesis-file-zones
                              :nemesis-file-targets])]
          ; Use defaults
          standard-file-corruption-opts)]
    (for [i     (range (:test-count opts))
          n     nemeses
          w     workloads
          fcos  file-corruption-opts]
      (tb-test (-> opts
                   (assoc :nemesis  n
                          :workload w)
                   (merge fcos))))))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn  tb-test
                                         :opt-fn opt-fn
                                         :opt-spec cli-opts})
                   (cli/test-all-cmd {:tests-fn all-tests
                                      :opt-fn opt-fn
                                      :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))
