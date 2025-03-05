(ns jepsen.tigerbeetle.antithesis
  "Support for running tests in Antithesis."
  (:require [cheshire.core :as json]
            [clojure [pprint :refer [pprint]]]
            [clojure.data.generators :as data.generators]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [info]])
  (:import (com.antithesis.sdk Assert)
           (com.fasterxml.jackson.databind ObjectMapper)
           (com.fasterxml.jackson.databind.node ArrayNode
                                                ObjectNode)
           (java.io Writer)
           (jepsen.tigerbeetle AntithesisRandom)))

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

(defn antithesis?
  "Are we running in an Antithesis environment?"
  []
  (boolean (dir)))

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

(defmacro with-random
  "Replaces data.generators' random with an Antithesis-controlled source of
  entropy, when running in Antithesis."
  [& body]
  `(binding [data.generators/*rnd* (if (antithesis?)
                                     (AntithesisRandom.)
                                     data.generators/*rnd*)]
     ~@body))

(def ^ObjectMapper om (ObjectMapper.))

(defprotocol ToJsonNode
  (->json-node [x] "Coerces x to a Jackson JsonNode."))

(extend-protocol ToJsonNode
  clojure.lang.IPersistentMap
  (->json-node [x]
    (reduce (fn [^ObjectNode n, [k v]]
              ; my kingdom for a JSON with non-string keys
              (.put n
                    (if (instance? clojure.lang.Named k)
                      (name k)
                      (str k))
                    (->json-node v))
              n)
            (.createObjectNode om)
            x))

  clojure.lang.IPersistentSet
  (->json-node [x]
    (reduce (fn [^ArrayNode a, e]
              (.add a (->json-node e)))
            (.createArrayNode om)
            x))

  clojure.lang.Sequential
  (->json-node [x]
    (reduce (fn [^ArrayNode a, e]
              (.add a (->json-node e)))
            (.createArrayNode om)
            x))

  clojure.lang.Keyword
  (->json-node [x]
    (name x))

  clojure.lang.BigInt
  (->json-node [x]
    ; Weirdly Jackson takes bigdecimals but not bigintegers
    (bigdec x))

  java.lang.Number
  (->json-node [x] x)

  java.lang.String
  (->json-node [x] x)

  java.lang.Boolean
  (->json-node [x] x)

  nil
  (->json-node [x] nil))

(defmacro assert-always
  "Asserts that expr is true every time, and that it's called at least once.
  Takes a map of data which is serialized to JSON."
  [expr message data]
  `(Assert/always (boolean ~expr)
                  ~message
                  (->json-node ~data)))
