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
            [onyx.api])
  (:import [java.util.concurrent.atomic AtomicLong]
           [java.util.concurrent.locks LockSupport]
           [com.amazonaws.services.kinesis 
            AmazonKinesisClient AmazonKinesisAsyncClient
            AmazonKinesisClientBuilder AmazonKinesisAsyncClientBuilder]
           [com.amazonaws.auth AWSStaticCredentialsProvider BasicAWSCredentials]
           [com.amazonaws.client.builder AwsClientBuilder$EndpointConfiguration]
           [com.amazonaws.services.kinesis.model GetShardIteratorRequest GetRecordsRequest DescribeStreamResult
            Record PutRecordsRequest PutRecordsRequestEntry]
           [java.nio ByteBuffer]))

(def n-partitions 2)

(def test-state (atom nil))

(defn update-atom! [event window trigger {:keys [lower-bound upper-bound event-type] :as state-event} extent-state]
  (reset! test-state extent-state))

(def run (atom nil))

(defn remov [segment]
  (if (= @run (:run (:data segment)))
    segment
    []))

(defn new-client ^AmazonKinesisClient
  [{:keys [kinesis/access-key kinesis/secret-key kinesis/region kinesis/endpoint-url]}]
  (if-let [builder (cond-> (AmazonKinesisClientBuilder/standard)

                           access-key ^AmazonKinesisClientBuilder 
                           (.withCredentials (AWSStaticCredentialsProvider. (BasicAWSCredentials. access-key secret-key)))

                           region ^AmazonKinesisClientBuilder
                           (.withRegion ^String region)

                           endpoint-url ^AmazonKinesisClientBuilder
                           (.withEndpointConfiguration (AwsClientBuilder$EndpointConfiguration. endpoint-url region)))]
    (.build builder)))

(def read-crash
  {:lifecycle/handle-exception (constantly :restart)})

(defn build-job [stream-name region batch-size batch-timeout]
  (let [batch-settings {:onyx/batch-size batch-size :onyx/batch-timeout batch-timeout}
        base-job (merge {:workflow [[:read-messages :identity]
                                    [:identity :out]]
                         :catalog [(merge {:onyx/name :identity
                                           :onyx/fn ::remov
                                           :onyx/type :function}
                                          batch-settings)]
                         :lifecycles [{:lifecycle/task :read-messages
                                       :lifecycle/calls ::read-crash}]
                         :windows [{:window/id :collect-segments
                                    :window/task :identity
                                    :window/type :global
                                    :window/aggregation :onyx.windowing.aggregation/conj}]
                         :triggers [{:trigger/window-id :collect-segments
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
                                    :kinesis/shard-initialize-type :at-timestamp
                                    :timestamp (java.util.Date.)
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
        peer-config (assoc peer-config :onyx/tenancy-id tenancy-id :onyx.peer/coordinator-barrier-period-ms 1)
        zk-address (get-in peer-config [:zookeeper/address])
        stream-name "ulul"
        region "us-west-2"
        _ (reset! run (java.util.UUID/randomUUID))
        segments (mapv (fn [n] {:partition-key (rand-int 200)
                                :data {:run @run
                                       :n n}})
                       (range 100))
        client (onyx.plugin.kinesis/new-client {:kinesis/region "us-west-2"})
        job (build-job stream-name region 1 1)
        {:keys [out read-messages]} (get-core-async-channels job)]
    (reset! test-state nil)
    (with-test-env [test-env [(+ n-partitions 5) env-config peer-config]]
      ;; Give it some time to initialize to latest
      (future 
       (Thread/sleep 4000)
       (println (onyx.api/shutdown-peer (rand-nth @(:peers test-env))))
       (Thread/sleep 2000)
       (println (onyx.api/shutdown-peer (rand-nth @(:peers test-env))))
       (Thread/sleep 2000)
       (println (onyx.api/shutdown-peer (rand-nth @(:peers test-env)))))

      (onyx.test-helper/validate-enough-peers! test-env job)
      (let [job-id (:job-id (onyx.api/submit-job peer-config job))]
        (loop [segs segments]
          (when-not (empty? segs)
            (Thread/sleep 1000)
            (println "PUTTING RECORDS")
            (.putRecords client 
                         (onyx.plugin.kinesis/build-put-request stream-name 
                                                                (take 10 segs)
                                                                onyx.tasks.kinesis/serialize-message-edn))

            (recur (drop 10 segs))))
        
        (println "Taking segments")
        ;(onyx.test-helper/feedback-exception! peer-config job-id)
        (let [results (onyx.plugin.core-async/take-segments! out 100000)] 
          (is (= (set (mapv :data segments)) (set (mapv :data results)))))
        (is (= (mapv :data segments) (sort-by :n (mapv :data @test-state))))
        (println "Done taking segments")
        (onyx.api/kill-job peer-config job-id)))))
