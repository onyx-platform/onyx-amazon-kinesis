(ns onyx.plugin.test-utils
  (:require [clojure.core.async :refer [<! go-loop]]
            [com.stuartsierra.component :as component]
            [aero.core]
            [schema.core :as s]
            [taoensso.timbre :as log]))

(defn read-config []
  (aero.core/read-config (clojure.java.io/resource "config.edn") 
                         {:profile (if (System/getenv "CIRCLECI")
                                     :ci
                                     :test)}))
