(ns jepsen.tigerbeetle.core
  "Common utilities"
  (:require [clojure [set :as set]]))

(def cluster-id
  "The TigerBeetle cluster ID"
  0)

(def port
  "What port do we connect to?"
  3001)

(def write-fs
  "A set of :fs that change state."
  #{:create-accounts
    :create-transfers})

(def read-fs
  "A set of fs that read without changing state."
  #{:lookup-accounts
    :lookup-transfers
    :query-accounts
    :query-transfers
    :get-account-transfers})

(def read-account-fs
  "A set of :fs that return a vector of accounts."
  #{:lookup-accounts
    :query-accounts})

(def read-transfer-fs
  "A set of :fs that return a vector of transfers."
  #{:lookup-transfers
    :get-account-transfers
    :query-transfers})

(def read-by-id-fs
  "A set of :fs that read something by a vector of IDs."
  #{:lookup-accounts, :lookup-transfers})

(def read-by-predicate-fs
  "A set of :fs that read by a predicate."
  (set/difference read-fs read-by-id-fs))

(def all-fs
  "All :fs we perform."
  (set/union write-fs read-fs))
