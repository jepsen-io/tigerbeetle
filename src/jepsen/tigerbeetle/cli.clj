(ns jepsen.tigerbeetle.cli
  "Command-line entry point for TigerBeetle tests"
  (:gen-class)
  (:require [clojure [pprint :refer [pprint]]
                     [string :as str]]
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
            [jepsen.tigerbeetle [antithesis :as antithesis]
                                [client :as client]
                                [checker :as tc]
                                [core :refer [all-fs]]
                                [db :as db]
                                [nemesis :as nemesis]]
            [jepsen.tigerbeetle.workload [transfer :as transfer]
                                          [idempotence :as idempotence]
                                          [indefinite :as indefinite]]
            [jepsen.os.debian :as debian]))

(def workloads
  "A map of workload names to functions that take CLI options and return
  workload maps"
  {:none           (constantly tests/noop-test)
   :transfer       transfer/workload
   :idempotence    idempotence/workload
   :indefinite     indefinite/workload})

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
    :global-snapshot ; Guaranteed to break
    :reformat        ; You are not supposed to do this; just for stress testing
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
    ; WAL is a shorthand for the two WAL zones
    :nemesis-file-zones (remove #{:wal} (keys nemesis/file-zones))}
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
  (str (->> (:versions opts)
            ; Turn paths like foo/bar.zip into bar.zip
            (map (fn [v]
                   (let [[_ filename] (re-find #"([^/]+)$" v)]
                     filename)))
            (str/join ","))
       " " (name (:workload opts))
       " cn=" (name (:client-nodes opts))
       (when (:close-on-timeout? opts)
         " cot")
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
                      (:matches res))
              ; This winds up being noisy if we don't filter them out
              res (assoc res :matches interesting-matches)]
          (if (seq interesting-matches)
            res
            (assoc res :valid? true)))))))

(defn stats-checker
  "We don't care about having some operations that don't ever return OK."
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [r (checker/check (checker/stats) test history opts)]
        (assoc r :valid? true)))))

(defn tb-test
  "Takes CLI options and constructs a Jepsen test map"
  [opts]
  (let [; Fill in defaults for workload, nemesis-file-zones and
        ; nemesis-file-targets. We leave these blank in the arg parser so that
        ; we can do multiple choices in test-all.
        opts            (merge (first standard-file-corruption-opts)
                               {:workload :transfer}
                               opts)
        ; Close-on-timeout should always be a boolean--the default (nil) maps
        ; to true. Same deal, the arg parser needs to leave this nil so that we
        ; can distinguish presence vs absence at test-all time.
        opts (if (nil? (:close-on-timeout? opts))
               (assoc opts :close-on-timeout? true)
               opts)
        ; Client nodes should default to one.
        opts (if (nil? (:client-nodes opts))
               (assoc opts :client-nodes :one)
               opts)
        workload-name   (:workload opts)
        workload        ((workloads workload-name) opts)
        primary-tracker (client/primary-tracker)
        db              (if (:antithesis opts)
                          (:db tests/noop-test) ; No access to containers
                          (db/db opts))
        os              (if (:antithesis opts)
                          (:os tests/noop-test) ; No access to containers
                          debian/os)
        nemesis       (if (:antithesis opts)
                        (:nemesis tests/noop-test) ; No access to containers
                        (nemesis/package
                          {:db            db
                           :nodes         (:nodes opts)
                           :faults        (:nemesis opts)
                           ;:partition     {:targets [:one ::majority]}
                           :pause         {:targets (:db-node-targets opts)}
                           :kill          {:targets (:db-node-targets opts)}
                           :corrupt-file
                           {:zones   (:nemesis-file-zones opts)
                            :targets (:nemesis-file-targets opts)}
                           ; Unused; TB seems so robust we don't seem to need
                           ; it.
                           :stable-period (:nemesis-stable-period opts)
                           :interval      (:nemesis-interval opts)}))
        ; Main workload
        generator (->> (:generator workload)
                       (gen/stagger (/ (:rate opts)))
                       (gen/time-limit (:time-limit opts))
                       (gen/nemesis
                         (gen/phases
                           ; Note that we time-limit the nemesis and clients
                           ; separately, because TB clients might have very,
                           ; very long timeouts, and we need to move on to
                           ; ending the nemesis to let client ops recover.
                           (gen/time-limit
                             (:time-limit opts)
                             (gen/phases
                               (gen/sleep (:initial-quiet-period opts))
                               (:generator nemesis)))
                           ; Sleep before the final generator--if we run it too
                           ; soon after the regular nemesis opts, we might get
                           ; stuck in a race where the new TB process sees an
                           ; old, crashing TB process and also kills itself.
                           (gen/sleep 2)
                           ; We always run the nemesis final generator;
                           ; it makes it easier to do ad-hoc analysis of
                           ; a running cluster after the test.
                           (:final-generator nemesis))))
        ; With final generator, if present
        generator (if-let [fg (:final-generator workload)]
                    (gen/phases
                      generator
                      (gen/log "Beginning final reads")
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
                         :stats      (stats-checker)
                         :exceptions (checker/unhandled-exceptions)
                         :panic      (panic-checker)
                         :workload   (:checker workload)})
            :client    (:client workload)
            :nemesis   (:nemesis nemesis jepsen.nemesis/noop)
            :plot      {:nemeses (:perf nemesis)}
            :generator generator
            :logging   {:overrides logging-overrides}
            :nonserializable-keys [:primary-tracker]}
           (when (:antithesis opts)
             ; SSH isn't available in Antithesis
             {:ssh {:dummy? true}}))))

(def cli-opts
  "Command-line option specification"
  [["-a" "--antithesis" "Used to run the test in an Antithesis environment. In this mode, we open no SSH connections, perform no DB setup, and inject no faults."]

   [nil "--client-nodes MODE" "Whether to connect a client to one node or all nodes. The default, `one`, restricts each client to a single node. `all` increases the number of operations that succeed, since TigerBeetle generally lets all requests time out on non-leader nodes. However, it might mean missing consistency violations that occur on specific nodes. It also breaks how we do primary node inference, so nemeses that target primaries will target (typically) all nodes instead."
    :parse-fn keyword
    :validate [#{:one :all} "Must be one or all"]]

   [nil "--[no-]close-on-timeout" "Controls whether or not we close the client
                                  on encountering a timeout error."
    :assoc-fn (fn [m k v] (assoc m :close-on-timeout? v))]

   [nil "--concurrency NUMBER" "How many workers should we run? Must be an integer, optionally followed by n (e.g. 3n) to multiply by the number of nodes."
    :default  "3n"
    :validate [(partial re-find #"^\d+n?$")
               "Must be an integer, optionally followed by n."]]

   [nil "--db-node-targets TARGETS" "A comma-separated list of ways to target DB nodes for faults, like 'one,majority'"
    :default  [:one :primaries :majority :all]
    :parse-fn parse-comma-kws
    :validate [(partial every? db-node-targets) (cli/one-of db-node-targets)]]

   [nil "--debug" "Whether to download and install debug versions of versions. Ignored for zip files."]

   [nil "--download-data" "Whether to download data files from nodes."]

   [nil "--final-time-limit SECONDS" "How long should we run the final generator for, at most? In seconds."
    :default  300
    :parse-fn read-string
    :validate [#(and (number? %) (pos? %)) "must be a positive number"]]

   [nil "--import" "Simple flag which sets import-time-limit to 0.8x
                   time-limit."]

   [nil "--import-time-limit SECONDS" "Creates imported accounts/transfers until this many seconds into the test. Default 0 (no imported events)."
    :default  0
    :parse-fn read-string
    :validate [#(and (number? %) (not (neg? %))) "must be a non-negative number"]]

   [nil "--initial-quiet-period SECONDS" "How long to wait before beginning faults"
    :default  10
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
    :default  10000
    :parse-fn read-string
    :validate [pos? "Must be a positive number."]]

   [nil "--fs FUNS" "A comma-separated list of API functions (e.g. query-transfers,create-accounts) that we should run."
    :default  all-fs
    :parse-fn (comp set parse-comma-kws)
    :validate [(partial every? all-fs) (cli/one-of all-fs)]]

   [nil "--rw-ratio RATIO" "Ratio of reads to writes performed by read-write processes, e.g. 2/1"
    :default  1
    :parse-fn read-string
    :validate [#(and (not (neg? %)) (rational? %)) "Must be a non-negative rational"]]

   [nil "--ta-ratio RATIO" "Ratio of create-transfers to create-accounts, e.g. 100/1"
    :default  100
    :parse-fn read-string
    :validate [#(and (not (neg? %)) (rational? %)) "Must be a non-negative rational."]]

   [nil "--time-limit SECONDS"
    "Excluding setup and teardown, how long should a test run for, in seconds?"
    :default  600
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be positive"]]

   [nil "--timeout MILLIS" "Client timeout, in milliseconds"
    :default  5000
    :parse-fn parse-long
    :validate [pos? "Must be positive."]]

   [nil "--tcpdump" "Dumps traffic to a pcap file."]

   ["-v" "--versions VERSIONS" "The TigerBeetle version(s) to install,
                               comma-separated."
    :parse-fn #(str/split % #"\s*,\s*")
    :default ["0.16.20"
              "0.16.21"
              ; "0.16.23" Just to force a version jump
              "0.16.25"
              "0.16.26"
              "0.16.27"
              "0.16.28"]]

   ["-w" "--workload NAME" "What workload should we run?"
    :parse-fn keyword
    :validate [workloads (cli/one-of workloads)]]
   ])

(defn opt-fn
  "Options map post-processor"
  [parsed]
  (let [opts (:options parsed)

        ; The import option provides a default import time limit.
        opts (if (and (:import opts)
                      (not (:import-time-limit opts)))
               (assoc opts :import-time-limit (* 0.8 (:time-limit opts)))
               opts)

        ; If import-time-limit is set, we need to use infinite timeouts.
        opts (if (pos? (:import-time-limit opts))
               (assoc opts :timeout Long/MAX_VALUE)
               opts)]
    (assoc parsed :options opts)))

(defn all-tests
  "Turns CLI options into a sequence of tests."
  [opts]
  (let [nemeses   (if-let [n (:nemesis opts)]  [n] standard-nemeses)
        workloads (if-let [w (:workload opts)] [w] standard-workloads)
        close-on-timeouts (let [c (:close-on-timeout? opts)]
                            (if (nil? c)
                              [true false]
                              [c]))]
    ;(pprint (map :name
    (for [i     (range (:test-count opts))
          n     nemeses
          w     workloads
          c     close-on-timeouts
          ; Uhghghghhghghg so many layers of complexity here. I'm so sorry. If
          ; you want to start seeing test failures regularly (or if TB gets
          ; better about handling file corruption) you can start pruning these
          ; branches--I'm trying to get us to a place where tests only throw
          ; failures we *don't* know about.
          fcos (let [fcos (if (or ; They requested a specific zone or target
                                  (:nemesis-file-zones opts)
                                  (:nemesis-file-targets opts)
                                  ; Or these nemeses don't do file corruption
                                  (not-any? file-corruption-nemeses n))
                            ; Use the specified faults only.
                            [(select-keys opts [:nemesis-file-zones
                                                :nemesis-file-targets])]
                            ; Use the full set of defaults
                            standard-file-corruption-opts)
                     ; So when we're doing a snapshot file corruption fault, we
                     ; *can't* target the superblock: it causes the node to
                     ; crash, because the WAL has entries ahead of the
                     ; superblock, with a generic "reached unreachable code"
                     ; error.
                     fcos (if (some #{:snapshot-file-chunks} n)
                            (map (fn [fco]
                                   (update fco
                                           :nemesis-file-zones
                                           (partial remove #{:superblock})))
                                 fcos)
                            fcos)]
                 fcos)]
      (tb-test (-> opts
                   (assoc :nemesis  n
                          :workload w
                          :close-on-timeout? c)
                   (merge fcos))))));))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (antithesis/with-random
    (cli/run! (merge (cli/single-test-cmd {:test-fn  tb-test
                                           :opt-fn opt-fn
                                           :opt-spec cli-opts})
                     (cli/test-all-cmd {:tests-fn all-tests
                                        :opt-fn opt-fn
                                        :opt-spec cli-opts})
                     (cli/serve-cmd))
              args)))
