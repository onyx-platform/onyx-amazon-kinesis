(ns onyx.plugin.input-fault-tolerance-test
  (:require [clojure.test :refer [deftest is]]
            [com.stuartsierra.component :as component]
            [onyx.test-helper :refer [with-test-env]]
            [onyx.job :refer [add-task]]
            [onyx.tasks.kinesis :refer [consumer]]
            [onyx.tasks.core-async :as core-async]
            [onyx.plugin.core-async :refer [get-core-async-channels]]
            [onyx.plugin.test-utils :as test-utils]
            [onyx.plugin.kinesis]
            [onyx.api]))

(def n-partitions 2)

(def test-state (atom nil))

(defn update-atom! [event window trigger {:keys [lower-bound upper-bound event-type] :as state-event} extent-state]
  (reset! test-state extent-state))

(def read-crash
  {:lifecycle/after-batch
   (fn [event lifecycle]
     (when (and (not (empty? (:onyx.core/batch event)))
                (zero? (rand-int 70)))
       (println "Cycle job.")
       (throw (ex-info "Restartable" {:restartable? true})))
     {})
   :lifecycle/handle-exception (constantly :restart)})

(defn build-job [stream-name region batch-size batch-timeout]
  (let [batch-settings {:onyx/batch-size batch-size :onyx/batch-timeout batch-timeout}
        base-job (merge {:workflow [[:read-messages :identity]
                                    [:identity :out]]
                         :catalog [(merge {:onyx/name :identity
                                           :onyx/fn :clojure.core/identity
                                           :onyx/type :function}
                                          batch-settings)]
                         :lifecycles [{:lifecycle/task :read-messages
                                       :lifecycle/calls ::read-crash}]
                         :windows [{:window/id :collect-segments
                                    :window/task :identity
                                    :window/type :global
                                    :window/aggregation :onyx.windowing.aggregation/conj}]
                         :triggers [{:trigger/window-id :collect-segments
                                     :trigger/refinement :onyx.refinements/accumulating
                                     :trigger/fire-all-extents? true
                                     :trigger/id :collect-trigger
                                     :trigger/on :onyx.triggers/segment
                                     :trigger/threshold [1 :elements]
                                     :trigger/sync ::update-atom!}]
                         :flow-conditions []
                         :task-scheduler :onyx.task-scheduler/balanced})]
    (-> base-job
        (add-task (consumer :read-messages
                            (merge {:kinesis/stream-name stream-name
                                    :kinesis/shard-initialize-type :latest
                                    :kinesis/deserializer-fn :onyx.tasks.kinesis/deserialize-message-edn
                                    :kinesis/region region
                                    :onyx/min-peers n-partitions 
                                    :onyx/max-peers n-partitions}
                                   batch-settings)))
        (add-task (core-async/output :out batch-settings)))))

(deftest kinesis-input-test
  (let [{:keys [test-config env-config peer-config]} (onyx.plugin.test-utils/read-config)
        tenancy-id (str (java.util.UUID/randomUUID)) 
        env-config (assoc env-config :onyx/tenancy-id tenancy-id)
        peer-config (assoc peer-config :onyx/tenancy-id tenancy-id)
        zk-address (get-in peer-config [:zookeeper/address])
        stream-name "ulul"
        region "us-west-2"
        segments (mapv (fn [n] {:partition-key (rand-int 200)
                                :data {:n n}})
                       (range 100))
        client (onyx.plugin.kinesis/new-client {:kinesis/region "us-west-2"})
        job (build-job stream-name region 1 1000)
        {:keys [out read-messages]} (get-core-async-channels job)]
    (reset! test-state nil)
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
        (let [results (onyx.plugin.core-async/take-segments! out 120000)] 
          (is (= (set (mapv :data segments)) (set (mapv :data results)))))
        (is (= (mapv :data segments) (sort-by :n (mapv :data @test-state))))
        (println "Done taking segments")
        (onyx.api/kill-job peer-config job-id)))))
