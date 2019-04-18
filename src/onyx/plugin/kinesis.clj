(ns onyx.plugin.kinesis
  (:require [onyx.plugin.partition-assignment :refer [partitions-for-slot]]
            [taoensso.timbre :as log :refer [fatal debug info warn]]
            [primitive-math :as pm]
            [onyx.static.default-vals :refer [arg-or-default]]
            [onyx.plugin.protocols :as p]
            [onyx.tasks.kinesis]
            [onyx.static.util :refer [kw->fn]]
            [schema.core :as s]
            [onyx.api])
  (:import [java.util.concurrent.atomic AtomicLong]
           [com.amazonaws.services.kinesis 
            AmazonKinesisClient AmazonKinesisAsyncClient
            AmazonKinesisClientBuilder AmazonKinesisAsyncClientBuilder]
           [com.amazonaws.auth AWSStaticCredentialsProvider BasicAWSCredentials]
           [com.amazonaws.client.builder AwsClientBuilder$EndpointConfiguration]
           [com.amazonaws.services.kinesis.model GetShardIteratorRequest GetRecordsRequest DescribeStreamResult
            Record PutRecordsRequest PutRecordsRequestEntry ProvisionedThroughputExceededException]
           [java.nio ByteBuffer]))

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

(defn new-async-client ^AmazonKinesisAsyncClient
  [{:keys [kinesis/access-key kinesis/secret-key kinesis/region kinesis/endpoint-url]}]
  (if-let [builder (cond-> (AmazonKinesisAsyncClientBuilder/standard)

                           access-key ^AmazonKinesisAsyncClientBuilder 
                           (.withCredentials (AWSStaticCredentialsProvider. (BasicAWSCredentials. access-key secret-key)))

                           region ^AmazonKinesisAsyncClientBuilder
                           (.withRegion ^String region)

                           endpoint-url ^AmazonKinesisAsyncClientBuilder
                           (.withEndpointConfiguration (AwsClientBuilder$EndpointConfiguration. endpoint-url region)))]
    (.build builder)))

(defn shard-initialize-type ^String [task-map]
  (case (:kinesis/shard-initialize-type task-map)

    :trim-horizon
    "TRIM_HORIZON"

    :latest
    "LATEST"

    ;; Currently unsupported initialize types
    ;; Requires extra task-map / initialization support

    ; :at-sequence-number 
    ; "AT_SEQUENCE_NUMBER"

    ; :after-sequence-number
    ; "AFTER_SEQUENCE_NUMBER"

    ; :at-timestamp
    ; "AT_TIMESTAMP"

    (throw (ex-info "kinesis/shard-initialize-type setting is invalid."
                    {:type (:kinesis/shard-initialize-type task-map)}))))

(def defaults
  {})

(defn check-shard-properties! 
  [{:keys [onyx/min-peers onyx/max-peers onyx/n-peers kinesis/shard] :as task-map} ^AmazonKinesisClient client ^String stream-name]
  (let [desc (.getStreamDescription ^DescribeStreamResult (.describeStream client stream-name))
        _ (when (not= (.getStreamStatus desc) "ACTIVE")
            (throw (ex-info "Stream is not currently active."
                            {:status (.getStreamStatus desc)
                             :recoverable? true})))
        _ (when (.isHasMoreShards desc)
            (throw (ex-info "More shards available, and unable to determine whether the number of peers matches the number of shards. This case is not currently handled."
                            {:recoverable? false})))
        n-shards (count (.getShards desc))
        fixed-shard? (and shard 
                          (or (= 1 n-peers)
                              (= 1 max-peers)))
        fixed-npeers? (or (= min-peers max-peers) (= 1 max-peers)
                          (and n-peers (and (not min-peers) (not max-peers))))
        n-peers (or max-peers n-peers)
        n-peers-less-eq-n-shards (<= n-peers n-shards)] 
    (when-not (or fixed-shard? fixed-npeers? n-peers-less-eq-n-shards)
      (let [e (ex-info ":onyx/min-peers must equal :onyx/max-peers, or :onyx/n-peers must be set, and :onyx/min-peers and :onyx/max-peers must not be set. Number of peers should also be less than or equal to the number of shards"
                       {:n-partitions n-shards 
                        :n-peers n-peers
                        :min-peers min-peers
                        :max-peers max-peers
                        :recoverable? false
                        :task-map task-map})] 
        (log/error e)
        (throw e)))))

(defn new-record-request [shard-iterator limit]
  (-> (GetRecordsRequest.)
      (.withShardIterator shard-iterator)
      (.withLimit (int limit))))

(defn rec->segment [^Record rec deserializer-fn]
  {:timestamp (.getApproximateArrivalTimestamp rec)
   :partition-key (.getPartitionKey rec)
   :sequence-number (.getSequenceNumber rec)
   :data (deserializer-fn (.array (.getData rec)))})

(defn- paced-get-records
  [log-prefix
   backoff-ms
   client 
   request
   timeout-at-ms]
  (let [now (System/currentTimeMillis)]
    (when (pm/< now ^long timeout-at-ms) 
      (if (some? backoff-ms)
        (let [result (try (.getRecords ^AmazonKinesisClient client ^GetRecordsRequest request)
                          (catch ProvisionedThroughputExceededException ex
                            ; We may end up sleeping until a time after timeout-at-ms, 
                            ; but we take precendence over that in order to enforce the backoff period.
                            (warn log-prefix (str "Backing off reading records for " backoff-ms "ms") ex)
                            (Thread/sleep backoff-ms)
                            ::backoff))]
          (if (= ::backoff result)
            (recur log-prefix backoff-ms client request timeout-at-ms)
            result))
        (.getRecords ^AmazonKinesisClient client ^GetRecordsRequest request)))))

(deftype KinesisReadMessages 
  [log-prefix task-map shard-id stream-name batch-size batch-timeout deserializer-fn ^AmazonKinesisClient client
   ^:unsynchronized-mutable offset ^:unsynchronized-mutable items ^:unsynchronized-mutable shard-iterator reader-backoff-ms]
  p/Plugin
  (start [this event]
    (info log-prefix "Starting kinesis/read-messages task")
    (check-shard-properties! task-map client stream-name)
    (s/validate onyx.tasks.kinesis/KinesisInputTaskMap task-map)
    this)

  (stop [this event] 
    this)

  p/Checkpointed
  (checkpoint [this]
    offset)

  (recover! [this replica-version checkpoint]
    (let [initial (if checkpoint 
                    (-> (GetShardIteratorRequest.)
                        (.withStreamName stream-name)
                        (.withShardId shard-id)
                        (.withStartingSequenceNumber checkpoint)
                        (.withShardIteratorType "AFTER_SEQUENCE_NUMBER"))
                    (-> (GetShardIteratorRequest.)
                        (.withStreamName stream-name)
                        (.withShardId shard-id)
                        (.withShardIteratorType (shard-initialize-type task-map))))
          shard-iter (.getShardIterator (.getShardIterator client initial))]
      (set! shard-iterator shard-iter)
      (set! offset checkpoint))
    this)

  (checkpointed! [this epoch])

  p/BarrierSynchronization
  (synced? [this epoch]
    true)

  (completed? [this]
    false)

  p/Input
  (poll! [this _ timeout-ms]
    (if (empty? items)
      (let [end-time-ms (pm/+ (System/currentTimeMillis) ^long timeout-ms)
            request (new-record-request shard-iterator batch-size)
            record-result (paced-get-records log-prefix reader-backoff-ms client request end-time-ms)]
        (when (some? record-result)
          (let [items* (.getRecords record-result)]
            (set! items (rest items*))
            (set! shard-iterator (.getNextShardIterator record-result))
            (when-let [rec ^Record (first items*)]
              (set! offset (.getSequenceNumber rec))
              (rec->segment rec deserializer-fn)))))
      (let [items* (rest items)
            rec ^Record (first items)]
        (set! offset (.getSequenceNumber rec))
        (set! items items*)
        (rec->segment rec deserializer-fn)))))

(defn read-messages [{:keys [onyx.core/task-map onyx.core/log-prefix onyx.core/monitoring onyx.core/slot-id] :as event}]
  (let [{:keys [kinesis/stream-name kinesis/deserializer-fn]} task-map
        batch-timeout (arg-or-default :onyx/batch-timeout task-map)
        batch-size (:onyx/batch-size task-map)
        reader-backoff-ms (:kinesis/reader-backoff-ms task-map)
        deserializer-fn (kw->fn (:kinesis/deserializer-fn task-map))
        shard-id (str (if-let [shard (:kinesis/shard task-map)]
                        shard
                        slot-id))
        client (new-client task-map)]
    (->KinesisReadMessages log-prefix task-map shard-id stream-name batch-size 
                           batch-timeout deserializer-fn client nil nil nil reader-backoff-ms)))

(defn segment->put-records-entry [{:keys [partition-key data]} serializer-fn]
  (-> (PutRecordsRequestEntry.)
      (.withPartitionKey (str partition-key))
      (.withData (ByteBuffer/wrap (serializer-fn data)))))

(defn build-put-request [stream-name segments serializer-fn]
  (let [records (->> segments
                     (map (fn [seg] (segment->put-records-entry seg serializer-fn)))
                     (into-array PutRecordsRequestEntry))] 
    (-> (PutRecordsRequest.)
        (.withStreamName stream-name)
        (.withRecords ^"[Lcom.amazonaws.services.kinesis.model.PutRecordsRequestEntry;" records))))

(defrecord KinesisWriteMessages [task-map config stream-name ^AmazonKinesisClient client serializer-fn]
  p/Plugin
  (start [this event] 
    this)

  (stop [this event] 
    (.shutdown client)
    this)

  p/BarrierSynchronization
  (synced? [this epoch]
    true)

  (completed? [this]
    true)

  p/Checkpointed
  (recover! [this _ _] 
    this)
  (checkpoint [this])
  (checkpointed! [this epoch])

  p/Output
  (prepare-batch [this event replica _]
    true)
  (write-batch [this {:keys [onyx.core/write-batch]} replica _]
    (when-not (empty? write-batch)
      (let [put-results (.putRecords client 
                                     (build-put-request stream-name 
                                                        write-batch 
                                                        serializer-fn))]

        (when-not (zero? (.getFailedRecordCount put-results))
          (throw (ex-info "Put request failed. Rewinding job."
                          {:restartable? true})))))
    true))

(def write-defaults {})

(defn write-messages [{:keys [onyx.core/task-map onyx.core/log-prefix] :as event}]
  (let [_ (s/validate onyx.tasks.kinesis/KinesisOutputTaskMap task-map)
        _ (info log-prefix "Starting kinesis/write-messages task")
        stream-name (:kinesis/stream-name task-map)
        config {}
        serializer-fn (kw->fn (:kinesis/serializer-fn task-map))
        client (new-client task-map)]
    (when (> (:onyx/batch-size task-map) 500)
      (throw (ex-info "Batch size greater than maximum kinesis write size of 500" task-map)))
    (->KinesisWriteMessages task-map config stream-name client serializer-fn)))

(defn read-handle-exception [event lifecycle lf-kw exception]
  (if (false? (:recoverable? (ex-data exception)))
    :kill
    :restart))

(def read-messages-calls
  {:lifecycle/handle-exception read-handle-exception})

(defn write-handle-exception [event lifecycle lf-kw exception]
  (if (false? (:recoverable? (ex-data exception)))
    :kill
    :restart))

(def write-messages-calls
  {:lifecycle/handle-exception write-handle-exception})
