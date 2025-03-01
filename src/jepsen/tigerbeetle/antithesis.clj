(ns jepsen.tigerbeetle.antithesis
  "Support for running tests in Antithesis."
  (:require [cheshire.core :as json]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [info]])
  (:import (java.io Writer)))

(let [d (delay (System/getenv "ANTITHESIS_OUTPUT_DIR"))]
  (defn dir
    "The Antithesis SDK directory, if present, or nil."
    []
    @d)

  (defn log-file
    "The Antithesis log file we write JSON to, or nil."
    []
    (when-let [d @d]
      (str d "/sdk.jsonl"))))

(defn log
  "Logs a message (e.g. {:antithesis_setup ...} as JSON to the SDK log file."
  [msg]
  (info "Logging antithesis start message to" (log-file))
  (when-let [f (log-file)]
    (with-open [w (io/writer f :append true)]
      (json/generate-stream msg w)
      (.append ^Writer w \n)
      (.flush ^Writer w))))

(defonce started?
  (atom false))

(defn started!
  "Logs that we've started up. Only emits once per JVM run."
  []
  (locking started?
    (when-not @started?
      (log {:antithesis_setup {:status "complete"
                               :details nil}})
      (reset! started? true))))
