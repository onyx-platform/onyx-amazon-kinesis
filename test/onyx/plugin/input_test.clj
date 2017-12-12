(ns onyx.plugin.input-test
  (:require [clojure.test :refer [deftest is]]
            [com.stuartsierra.component :as component]
            [onyx.test-helper :refer [with-test-env]]
            [onyx.job :refer [add-task]]
            [onyx.tasks.kinesis :refer [consumer]]
            [onyx.tasks.core-async :as core-async]
            [onyx.plugin.core-async :refer [get-core-async-channels]]
            [onyx.plugin.test-utils :as test-utils]
            [onyx.plugin.kinesis]
            [onyx.kinesis.utils :refer [create-stream delete-stream client-builder]]
            [onyx.api]))

(def n-partitions 2)

(defn build-job [stream-name region batch-size batch-timeout]
  (let [batch-settings {:onyx/batch-size batch-size :onyx/batch-timeout batch-timeout}
        base-job (merge {:workflow [[:read-messages :identity]
                                    [:identity :out]]
                         :catalog [(merge {:onyx/name :identity
                                           :onyx/fn :clojure.core/identity
                                           :onyx/type :function}
                                          batch-settings)]
                         :lifecycles []
                         :windows []
                         :triggers []
                         :flow-conditions []
                         :task-scheduler :onyx.task-scheduler/balanced})]
    (-> base-job
        (add-task (consumer :read-messages
                            (merge {:kinesis/stream-name stream-name
                                    ;:kinesis/shard-initialize-type :trim-horizon
                                    :kinesis/shard-initialize-type :latest
                                    :kinesis/deserializer-fn :onyx.tasks.kinesis/deserialize-message-edn
                                    :kinesis/region region
                                    :onyx/min-peers n-partitions 
                                    :onyx/max-peers n-partitions}
                                   batch-settings)))
        (add-task (core-async/output :out batch-settings)))))

(def stream-name "ulul")

(deftest kinesis-input-test
  (let [client (onyx.plugin.kinesis/new-client {:kinesis/region "us-west-2"})
        _ (try
           (delete-stream client stream-name)
           ;; stream already existed (bad), give it some time to delete.
           ;; FIXME: poll until deleted
           (Thread/sleep 60000)
           (catch Exception _))
        _ (create-stream client stream-name n-partitions)
        ;; FIXME: poll until created
        _ (Thread/sleep 60000)]
    (try 
     (let [{:keys [test-config env-config peer-config]} (onyx.plugin.test-utils/read-config)
           tenancy-id (str (java.util.UUID/randomUUID)) 
           env-config (assoc env-config :onyx/tenancy-id tenancy-id)
           peer-config (assoc peer-config :onyx/tenancy-id tenancy-id)
           zk-address (get-in peer-config [:zookeeper/address])
           region "us-west-2"
           segments (mapv (fn [n] {:partition-key (rand-int 200)
                                   :data {:n n}})
                          (range 100))
           job (build-job stream-name region 1 1000)
           {:keys [out read-messages]} (get-core-async-channels job)]
       (with-test-env [test-env [(+ n-partitions 2) env-config peer-config]]
         ;; Give it some time to initialize to latest

         (onyx.test-helper/validate-enough-peers! test-env job)
         (let [job-id (:job-id (onyx.api/submit-job peer-config job))]
           (Thread/sleep 4000)
           (.putRecords client 
                        (onyx.plugin.kinesis/build-put-request stream-name 
                                                               segments 
                                                               onyx.tasks.kinesis/serialize-message-edn))
           (println "Taking segments")
           ;(onyx.test-helper/feedback-exception! peer-config job-id)
           (let [results (onyx.plugin.core-async/take-segments! out 20000)] 
             (is (= (sort-by :n (mapv :data segments)) (sort-by :n (mapv :data results)))))
           (println "Done taking segments")
           (onyx.api/kill-job peer-config job-id))))
     (finally
      ;; FIXME: poll until deleted
      (delete-stream client stream-name)))))
