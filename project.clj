(defproject jepsen.tigerbeetle "0.1.0-SNAPSHOT"
  :description "Jepsen tests for the TigerBeetle financial database"
  :url "https://github.com/jepsen-io/tigerbeetle"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.11.4"]
                 [org.clojure/core.match "1.1.0"]
                 [jepsen "0.3.7"]
                 [com.tigerbeetle/tigerbeetle-java "0.16.11"]]
  :repl-options {:init-ns jepsen.tigerbeetle.repl}
  :main jepsen.tigerbeetle.cli
  :jvm-opts ["-server"
             ;"-XX:-OmitStackTraceInFastThrow"
             "-Djava.awt.headless=true"
             "-Xmx24g"
             ])
