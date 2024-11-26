(defproject jepsen.tigerbeetle "0.1.0-SNAPSHOT"
  :description "Jepsen tests for the TigerBeetle financial database"
  :url "https://github.com/jepsen-io/tigerbeetle"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[camel-snake-kebab "0.4.3"]
                 [com.aphyr/bifurcan-clj "0.1.3-SNAPSHOT"]
                 [com.tigerbeetle/tigerbeetle-java "0.16.13"]
                 [jepsen "0.3.8-SNAPSHOT"]
                 [org.clojure/clojure "1.12.0"]
                 [org.clojure/core.match "1.1.0"]
                 [org.clojure/core.logic "1.1.0"]
                 [org.clojure/data.generators "1.1.0"]
                 [tesser.math "1.0.6"]]
  :java-source-paths ["src"]
  :javac-options ["--release" "11"]
  :repl-options {:init-ns jepsen.tigerbeetle.repl}
  :main jepsen.tigerbeetle.cli
  :jvm-opts ["-server"
             "-XX:-OmitStackTraceInFastThrow"
             "-Djava.awt.headless=true"
             "-Xmx24g"
             ]
  :test-selectors {:default (fn [m] true)
                   :all     (fn [m] true)
                   :focus   :focus}
  :profiles {:dev {:dependencies
                   [[com.gfredericks/test.chuck "0.2.14"]
                    [org.clojure/test.check "1.1.1"]]}})

