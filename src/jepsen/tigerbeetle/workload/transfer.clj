(ns jepsen.tigerbeetle.workload.transfer
  "Our general-purpose TigerBeetle workload. Performs random transfers between
  accounts. During the final read phase, we switch exclusively to :f
  :final-lookup-accounts and :f :final-lookup-transfers, whose semantics are
  otherwise identical to lookup-accounts/lookup-transfers."
  (:require [clojure.core.match :refer [match]]
            [jepsen [checker :as checker]
                    [generator :as gen]
                    [util :refer [timeout]]]
            [jepsen.tigerbeetle [client :as c]
                                [checker :as tigerbeetle.checker]]
            [jepsen.tigerbeetle.workload [client :refer [client]]
                                         [generator :refer [final-gen
                                                            gen
                                                            wrap-gen]]]
            [potemkin :refer [definterface+]]
            [slingshot.slingshot :refer [try+ throw+]]))

(defn workload
  "Takes CLI opts and constructs a partial test map."
  [opts]
  {:client          (client)
   :generator       (gen opts)
   :final-generator (final-gen)
   :wrap-generator  wrap-gen
   :checker         (tigerbeetle.checker/checker)})
