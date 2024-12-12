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
            [jepsen.tigerbeetle.workload [transfer :as transfer]]
            [jepsen.os.debian :as debian]))

(def workloads
  "A map of workload names to functions that take CLI options and return
  workload maps"
  {:none           (constantly tests/noop-test)
   :transfer       transfer/workload})

(def all-workloads
  "All the workloads we run by default."
  [:none])

(def nemeses
  "Basic nemeses we have available."
  #{:kill
    :pause
    :partition
    :clock})

(def all-nemeses
  "Combinations of nemeses we run by default."
  [; Nothing
   []
   ; One fault at a time
   [:partition]
   [:kill]
   [:pause]
   [:clock]
   ; General chaos
   [:partition :pause :kill :clock]])

(def special-nemeses
  "A map of special nemesis names to collections of faults."
  {:none []
   :all [:partition :pause :kill :clock]})

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
       (when-let [n (:nemesis opts)]
         (str " " (->> n (map name) sort (str/join ","))))))

(defn tb-test
  "Takes CLI options and constructs a Jepsen test map"
  [opts]
  (let [workload-name (:workload opts)
        workload      ((workloads workload-name) opts)
        primary-tracker (client/primary-tracker)
        db            (db/db opts)
        os            debian/os
        nemesis       (nemesis/package
                        {:db            db
                         :nodes         (:nodes opts)
                         :faults        (:nemesis opts)
                         :partition     {:targets [:one :primaries :majority]}
                         :pause         {:targets [:one, :primaries] #_[:one :primaries :majority :all]}
                         :kill          {:targets #_[:one :primaries :majority :all]
                                         [:one]}
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
                    ; We always run the nemesis final generator; it makes
                    ; it easier to do ad-hoc analysis of a running cluster
                    ; after the test
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
  [[nil "--client-nodes MODE" "Whether to connect a client to one node or all nodes."
    :default :one
    :parse-fn keyword
    :validate [#{:one :all} "Must be one or all"]]

   [nil "--download-data" "Whether to download data files from nodes."]

   [nil "--final-time-limit SECONDS" "How long should we run the final generator for, at most? In seconds."
    :default  200
    :parse-fn read-string
    :validate [#(and (number? %) (pos? %)) "must be a positive number"]]

   [nil "--initial-quiet-period SECONDS" "How long to wait before beginning faults"
    :default 10
    :parse-fn read-string
    :validate [#(and (number? %) (pos? %)) "must be a positive number"]]

   [nil "--log-debug" "Turns on TigerBeetle debug logging."]

   [nil "--nemesis FAULTS" "A comma-separated list of nemesis faults to enable"
    :parse-fn parse-nemesis-spec
    :validate [(partial every? (into nemeses (keys special-nemeses)))
               (str (cli/one-of nemeses) " or the special nemeses, which " (cli/one-of special-nemeses))]]

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
    :default "0.16.17"]

   ["-w" "--workload NAME" "What workload should we run?"
    :parse-fn keyword
    :default  :transfer
    :missing  (str "Must specify a workload: " (cli/one-of workloads))
    :validate [workloads (cli/one-of workloads)]]
   ])

(defn all-tests
  "Turns CLI options into a sequence of tests."
  [opts]
  (let [nemeses   (if-let [n (:nemesis opts)]  [n] all-nemeses)
        workloads (if-let [w (:workload opts)] [w] all-workloads)]
    (for [i     (range (:test-count opts))
          n     nemeses
          w     workloads]
      (tb-test (assoc opts
                      :nemesis  n
                      :workload w)))))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn  tb-test
                                         :opt-spec cli-opts})
                   (cli/test-all-cmd {:tests-fn all-tests
                                      :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))
