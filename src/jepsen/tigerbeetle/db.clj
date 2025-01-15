(ns jepsen.tigerbeetle.db
  "Database automation"
  (:require [clojure [string :as str]]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [control :as c :refer [|]]
                    [db :as db]
                    [util :refer [meh]]]
            [jepsen.control [net :as cn]
                            [util :as cu]]
            [jepsen.tigerbeetle [client :as client]
                                [core :refer [cluster-id port]]]
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
    (if-let [zip (:zip test)]
      (let [tmp (cu/tmp-dir!)
            tmp-zip (str tmp "/tb.zip")]
        (try
          (info "Installing TigerBeetle from local zip file" zip)
          (c/upload zip tmp-zip)
          (str "file://" tmp-zip)
          (cu/install-archive! (str "file://" tmp-zip) bin)
          (finally
            (c/exec :rm :-rf tmp))))
      ; Install from web
      (let [url (str "https://github.com/tigerbeetle/tigerbeetle/releases/download/"
                     (:version test)
                     "/tigerbeetle-x86_64-linux.zip")]
        (info "Installing TigerBeetle from remote URL" url)
        (cu/install-archive! url bin)))))

(defn format!
  "Writes a fresh data file."
  [test node]
  (c/su
    (c/exec bin :format
            (str "--cluster=" cluster-id)
            (str "--replica=" (replica-index test node))
            (str "--replica-count=" (count (:nodes test)))
            data-file)))

(defn data-corrupt?
  "Checks to see if the most recent log message is about file corruption."
  []
  (let [patterns [#"data file inode size was truncated or corrupted"
                  #"superblock not found"]]
    (c/su
      (try+
        (let [log (c/exec :tail :-n 2 log-file)]
          (boolean (some #(re-find % log) patterns)))
        (catch [:exit 1] _
          false)))))

(defn maybe-reformat!
  "If we corrupt enough of a disk file, the node will crash on startup
  complaining of a corrupt data file. This function checks for that log message
  and, if found, formats a fresh data file."
  [test node]
  (when (data-corrupt?)
    (c/su (c/exec :rm :-f data-file))
    (format! test node)
    :formatted))

(defn configure!
  "Configures the node for operation."
  [test node]
  (format! test node))


(defn addresses
  "Computes the comma-separated address list for each node in the cluster."
  [test]
  (->> (:nodes test)
       (map (fn [node]
              (str (cn/ip node) ":" port)))
       (str/join ",")))

(defrecord DB [tcpdump]
  db/DB
  (setup! [this test node]
    (when (:tcpdump test) (db/setup! tcpdump test node))
    (install! test node)
    (configure! test node)
    (db/start! this test node)
    )

  (teardown! [this test node]
    (c/su (db/kill! this test node)
          (c/exec :rm :-rf dir))
    (when (:tcpdump test) (db/teardown! tcpdump test node)))

  db/LogFiles
  (log-files [_ test node]
    (merge (when (:tcpdump test) (db/log-files tcpdump test node))
           (when (:download-data test) {data-file "data"})
           {log-file "tigerbeetle.log"}))

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
        (when (:log-debug test) ["--log-debug" "--experimental"])
        (str "--cache-grid=" cache-grid-size)
        (str "--addresses=" (addresses test))
        data-file)))

  db/Primary
  (setup-primary! [_ _ _])

  (primaries [this test]
    (client/primary-tracker-primaries (:primary-tracker test))))

(defn db
  "Constructs a new DB for the test, given CLI opts."
  [opts]
  (map->DB {:tcpdump (db/tcpdump {:ports [port]})}))

