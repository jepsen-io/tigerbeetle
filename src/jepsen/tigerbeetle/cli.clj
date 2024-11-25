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
            [jepsen.tigerbeetle [db :as db]
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
  (str (:version opts)
       " " (name (:workload opts))))

(defn tb-test
  "Takes CLI options and constructs a Jepsen test map"
  [opts]
  (let [workload-name (:workload opts)
        workload      ((workloads workload-name) opts)
        db            (db/db opts)
        os            debian/os
        nemesis       (nemesis/package
                        {:db            db
                         :nodes         (:nodes opts)
                         :faults        (:nemesis opts)
                         :partition     {:targets [:one :majority]}
                         :pause         {:targets [:one :majority :all]}
                         :kill          {:targets [:one :majority :all]}
                         :stable-period (:nemesis-stable-period opts)
                         :interval      (:nemesis-interval opts)})
        ; Main workload
        generator (gen/phases
                    (->> (:generator workload)
                         (gen/stagger (/ (:rate opts)))
                         (gen/nemesis (:generator nemesis))
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
                      (gen/sleep 0)
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
            :plot     {:nemeses (:perf nemesis)}
            :checker  (checker/compose
                        {:perf       (checker/perf)
                         :clock      (checker/clock-plot)
                         :stats      (checker/stats)
                         :exceptions (checker/unhandled-exceptions)
                         :workload   (:checker workload)})
            :client    (:client workload)
            :nemesis   (:nemesis nemesis jepsen.nemesis/noop)
            :generator generator
            :logging   {:overrides logging-overrides}
            :remote    (scp/remote control/ssh)
            })))

(def cli-opts
  "Command-line option specification"
  [
   [nil "--final-time-limit SECONDS" "How long should we run the final generator for, at most? In seconds."
    :default  200
    :parse-fn read-string
    :validate [#(and (number? %) (pos? %)) "must be a positive number"]]

   [nil "--[no-]idempotence" "If true, asks producers to enable idempotence. If omitted, uses client defaults."]

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

   [nil "--tcpdump" "Dumps traffic to a pcap file."]

   [nil "--zip PATH" "Installs a local zip file, rather than downloading an official release."]

   ["-v" "--version VERSION" "The TigerBeetle version to install."
    :default "0.16.13"]

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
