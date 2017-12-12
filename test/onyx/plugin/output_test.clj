(ns onyx.plugin.output-test
  (:require [clojure.core.async :refer [<!! go pipe close! >!!]]
            [clojure.test :refer [deftest is testing]]
            [com.stuartsierra.component :as component]
            [onyx.test-helper :refer [with-test-env]]
            [onyx.job :refer [add-task]]
            [onyx.tasks.kinesis :refer [producer deserialize-message-edn]]
            [onyx.tasks.core-async :as core-async]
            [onyx.plugin.core-async :refer [get-core-async-channels]]
            [onyx.plugin.test-utils :as test-utils]
            [onyx.kinesis.utils :refer [create-stream delete-stream client-builder]]
            [onyx.plugin.kinesis]
            [onyx.api]
            [taoensso.timbre :as log])
  (:import [com.amazonaws.services.kinesis AmazonKinesisClient AmazonKinesisClientBuilder]
           [com.amazonaws.auth AWSStaticCredentialsProvider BasicAWSCredentials]
           [com.amazonaws.client.builder AwsClientBuilder$EndpointConfiguration]
           [com.amazonaws.services.kinesis.model GetShardIteratorRequest GetRecordsRequest 
            Record PutRecordsRequest PutRecordsRequestEntry]
           [java.nio ByteBuffer]))

(defn build-job [stream-name batch-size batch-timeout]
  (let [batch-settings {:onyx/batch-size batch-size
                        :onyx/batch-timeout batch-timeout}
        base-job (merge {:workflow   [[:in :identity]
                                      [:identity :write-messages]]
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
        (add-task (core-async/input :in batch-settings))
        (add-task (producer :write-messages
                                (merge {:kinesis/stream-name stream-name
                                        :kinesis/region "us-west-2"
                                        :kinesis/serializer-fn :onyx.tasks.kinesis/serialize-message-edn}
                                       batch-settings))))))


; AT_SEQUENCE_NUMBER - Start reading from the position denoted by a specific sequence number, provided in the value StartingSequenceNumber.
; AFTER_SEQUENCE_NUMBER - Start reading right after the position denoted by a specific sequence number, provided in the value StartingSequenceNumber.
; AT_TIMESTAMP - Start reading from the position denoted by a specific timestamp, provided in the value Timestamp.
; TRIM_HORIZON - Start reading at the last untrimmed record in the shard in the system, which is the oldest data record in the shard.
; LATEST - Start reading just after the most recent record in the shard, so that you always read the most recent data in the shard.
(defn get-records [client shard-iterator]
  (let [records-request (-> (GetRecordsRequest.)
                            (.withShardIterator shard-iterator)
                            (.withLimit (int 100)))
        records-result (.getRecords client records-request)]
    (mapv (fn [rec]
            (deserialize-message-edn (.array (.getData rec))))
          (.getRecords records-result))))

(def stream-name "ulul")

(def n-partitions 2)

(deftest kinesis-output-test
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
           job (build-job stream-name 10 1000)
           {:keys [in]} (get-core-async-channels job)
           test-data [{:partition-key 1 :data {:n 0}}
                      {:partition-key 2 :data {:n 1}}
                      {:partition-key "tarein" :data {:n 2}}
                      {:partition-key 3 :data {:n 3}}]
           ;; get shard offset prior to writing any messages
           iterator-req1 (-> (GetShardIteratorRequest.)
                             (.withStreamName stream-name)
                             (.withShardId "0")
                             (.withShardIteratorType "LATEST"))
           iterator-result1 (.getShardIterator client iterator-req1)
           shard-iterator1 (.getShardIterator iterator-result1)
           iterator-req2 (-> (GetShardIteratorRequest.)
                             (.withStreamName stream-name)
                             (.withShardId "1")
                             (.withShardIteratorType "LATEST"))
           iterator-result2 (.getShardIterator client iterator-req2)
           shard-iterator2 (.getShardIterator iterator-result2)]
       (with-test-env [test-env [4 env-config peer-config]]
         (onyx.test-helper/validate-enough-peers! test-env job)
         (run! #(>!! in %) test-data)
         (close! in)
         (->> (onyx.api/submit-job peer-config job)
              :job-id
              (onyx.test-helper/feedback-exception! peer-config))
         (testing "routing to default topic"
           (let [msgs (into (get-records client shard-iterator1)
                            (get-records client shard-iterator2))]
             (is (= [{:n 0} {:n 1} {:n 2} {:n 3}]
                    msgs))))))  
     (finally
      ;; FIXME: poll until deleted
      (delete-stream client stream-name)))))
