(ns jepsen.tigerbeetle.db
  "Database automation"
  (:require [clojure [string :as str]]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [control :as c]
                    [db :as db]
                    [util :refer [meh]]]
            [jepsen.control [net :as cn]
                            [util :as cu]]
            [slingshot.slingshot :refer [try+ throw+]]))

(def dir "The top-level directory."
  "/opt/tigerbeetle")

(def data-file
  "Where we tell TigerBeetle to store data."
  (str dir "/data"))

(def bin
  "The full path to the TigerBeetle binary."
  (str dir "/tigerbeetle"))

(def log-file
  "Where we direct logs from stdout/stderr"
  (str dir "/tigerbeetle.log"))

(def pid-file
  "Where we write the process' pidfile"
  (str dir "/tigerbeetle.pid"))

(def cluster-id
  "The TigerBeetle cluster ID"
  0)

(def cache-grid-size
  "TigerBeetle's cache grid size"
  "1GiB")

(defn replica-index
  "The replica number for this node."
  [test node]
  (.indexOf (:nodes test) node))

(defn install!
  "Installs the TigerBeetle package."
  [test node]
  (c/su
    (let [url (str "https://github.com/tigerbeetle/tigerbeetle/releases/download/"
                   (:version test)
                   "/tigerbeetle-x86_64-linux.zip")]
      (cu/install-archive! url bin))))

(defn configure!
  "Writes the initial data file."
  [test node]
  (c/su
    (c/exec bin :format
            (str "--cluster=" cluster-id)
            (str "--replica=" (replica-index test node))
            (str "--replica-count=" (count (:nodes test)))
            data-file)))

(defn addresses
  "Computes the comma-separated address list for each node in the cluster."
  [test]
  (->> (:nodes test)
       (map (fn [node]
              (str (cn/ip node) ":3001")))
       (str/join ",")))

(defrecord DB []
  db/DB
  (setup! [this test node]
    (install! test node)
    (configure! test node)
    (db/start! this test node)
    )

  (teardown! [this test node]
    (c/su (db/kill! this test node)
          (c/exec :rm :-rf dir)))

  db/LogFiles
  (log-files [_ test node]
    {log-file "tigerbeetle.log"})

  db/Pause
  (pause! [_ test node]
    (c/su (cu/grepkill! :stop "tigerbeetle")))

  (resume! [_ test node]
    (c/su (cu/grepkill! :cont "tigerbeetle")))

  db/Kill
  (kill! [_ test node]
    (c/su (cu/grepkill! :kill "tigerbeetle")))

  (start! [_ test node]
    (c/su
      ; Adapted from https://docs.tigerbeetle.com/operating/linux
      (cu/start-daemon!
        {:logfile log-file
         :pidfile pid-file
         :chdir dir
         :env {"TIGERBEETLE_CACHE_GRID_SIZE" cache-grid-size
               "TIGERBEETLE_ADDRESSES" (addresses test)
               "TIGERBEETLE_REPLICA_COUNT" (count (:nodes test))
               "TIGERBEETLE_REPLICA_INDEX" (replica-index test node)
               "TIGERBEETLE_CLUSTER_ID" cluster-id
               "TIGERBEETLE_DATA_FILE" data-file}}
        ; For reasons I don't really understand, we specify some, but not all,
        ; of these both as env vars and as arguments
        bin
        :start
        (str "--cache-grid=" cache-grid-size)
        (str "--addresses=" (addresses test))
        data-file))))

(defn db
  "Constructs a new DB for the test, given CLI opts."
  [opts]
  (DB.))
