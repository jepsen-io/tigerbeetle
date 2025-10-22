(defproject jepsen.tigerbeetle "0.1.1-SNAPSHOT"
  :description "Jepsen tests for the TigerBeetle financial database"
  :url "https://github.com/jepsen-io/tigerbeetle"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[camel-snake-kebab "0.4.3"]
                 [com.aphyr/bifurcan-clj "0.1.3"]
                 [com.tigerbeetle/tigerbeetle-java "0.16.26"]
                 [io.jepsen/antithesis "0.1.0-SNAPSHOT"
                  :exclusions [com.fasterxml.jackson.core/jackson-databind]]
                 [jepsen "0.3.10-SNAPSHOT"]
                 [org.clojure/clojure "1.12.3"]
                 [org.clojure/core.match "1.1.0"]
                 [org.clojure/core.logic "1.1.0"]
                 [tesser.math "1.0.8-SNAPSHOT"
                  :exclusions [org.slf4j/slf4j-api]]]
  :java-source-paths ["src"]
  :javac-options ["--release" "11"]
  :repl-options {:init-ns jepsen.tigerbeetle.repl}
  :main jepsen.tigerbeetle.cli
  :jvm-opts ["-server"
            ; "-XX:-OmitStackTraceInFastThrow"
             "-Djava.awt.headless=true"
             "-Xmx24g"
             ]
  :test-selectors {:default (fn [m] true)
                   :all     (fn [m] true)
                   :focus   :focus}
  :profiles {:dev {:dependencies
                   [[com.gfredericks/test.chuck "0.2.15"]
                    [org.clojure/test.check "1.1.1"]]}})

