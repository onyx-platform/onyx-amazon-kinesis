(ns onyx.plugin.kinesis
  (:require [onyx.plugin.partition-assignment :refer [partitions-for-slot]]
            [taoensso.timbre :as log :refer [fatal info]]
            [onyx.static.default-vals :refer [arg-or-default]]
            [onyx.plugin.protocols :as p]
            [onyx.static.util :refer [kw->fn]]
            ;[onyx.tasks.kinesis]
            [schema.core :as s]
            [onyx.api])
  (:import [java.util.concurrent.atomic AtomicLong]
           [com.amazonaws.services.kinesis AmazonKinesisClient AmazonKinesisClientBuilder]
           [com.amazonaws.auth AWSStaticCredentialsProvider BasicAWSCredentials]
           [com.amazonaws.client.builder AwsClientBuilder$EndpointConfiguration]
           [com.amazonaws.services.kinesis.model GetShardIteratorRequest GetRecordsRequest 
            Record PutRecordsRequest PutRecordsRequestEntry]
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


; AT_SEQUENCE_NUMBER - Start reading from the position denoted by a specific sequence number, provided in the value StartingSequenceNumber.
; AFTER_SEQUENCE_NUMBER - Start reading right after the position denoted by a specific sequence number, provided in the value StartingSequenceNumber.
; AT_TIMESTAMP - Start reading from the position denoted by a specific timestamp, provided in the value Timestamp.
; TRIM_HORIZON - Start reading at the last untrimmed record in the shard in the system, which is the oldest data record in the shard.
; LATEST - Start reading just after the most recent record in the shard, so that you always read the most recent data in the shard.
(defn shard-initialize-type [task-map]
  (case (:kinesis/shard-initialize-type task-map)
    :at-sequence-number 
    "AT_SEQUENCE_NUMBER"

    :after-sequence-number
    "AFTER_SEQUENCE_NUMBER"

    :at-timestamp
    "AT_TIMESTAMP"

    :trim-horizon
    "TRIM_HORIZON"

    :latest
    "LATEST"
    (throw (Exception. "kinesis/shard-initial setting is invalid."))))

(def defaults
  {})

; (defn check-num-peers-equals-partitions 
;   [{:keys [onyx/min-peers onyx/max-peers onyx/n-peers kinesis/partition] :as task-map} n-partitions]
;   (let [fixed-partition? (and partition (or (= 1 n-peers)
;                                             (= 1 max-peers)))
;         fixed-npeers? (or (= min-peers max-peers) (= 1 max-peers)
;                           (and n-peers (and (not min-peers) (not max-peers))))
;         n-peers (or max-peers n-peers)
;         n-peers-less-eq-n-partitions (<= n-peers n-partitions)] 
;     (when-not (or fixed-partition? fixed-npeers? n-peers-less-eq-n-partitions)
;       (let [e (ex-info ":onyx/min-peers must equal :onyx/max-peers, or :onyx/n-peers must be set, and :onyx/min-peers and :onyx/max-peers must not be set. Number of peers should also be less than or equal to the number of partitions."
;                        {:n-partitions n-partitions 
;                         :n-peers n-peers
;                         :min-peers min-peers
;                         :max-peers max-peers
;                         :recoverable? false
;                         :task-map task-map})] 
;         (log/error e)
;         (throw e)))))

(defn new-record-request [shard-iterator limit]
  (-> (GetRecordsRequest.)
      (.withShardIterator shard-iterator)
      (.withLimit (int limit))))

(defn rec->segment [^Record rec deserializer-fn]
  {:timestamp (.getApproximateArrivalTimestamp rec)
   :partition-key (.getPartitionKey rec)
   :sequence-number (.getSequenceNumber rec)
   :data (deserializer-fn (.array (.getData rec)))})

(deftype KinesisReadMessages 
  [log-prefix task-map slot-id stream-name batch-size batch-timeout deserializer-fn client
   ^:unsynchronized-mutable offset ^:unsynchronized-mutable items ^:unsynchronized-mutable shard-iterator]
  p/Plugin
  (start [this event]
    (let [{:keys []} task-map
          _ (s/validate onyx.tasks.kinesis/KinesisInputTaskMap task-map)
          _ (info log-prefix "Starting kinesis/read-messages task")]
      this))

  (stop [this event] 
    this)

  p/Checkpointed
  (checkpoint [this]
    offset)

  (recover! [this replica-version checkpoint]
    (let [initial (if checkpoint 
                    (-> (GetShardIteratorRequest.)
                        (.withStreamName stream-name)
                        (.withShardId (str slot-id))
                        (.withStartingSequenceNumber checkpoint)
                        (.withShardIteratorType "AT_SEQUENCE_NUMBER"))
                    (-> (GetShardIteratorRequest.)
                        (.withStreamName stream-name)
                        (.withShardId (str slot-id))
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
  (poll! [this _]
    (if (empty? items)
      (let [record-result (.getRecords client (new-record-request shard-iterator batch-size))
            items* (.getRecords record-result)]
        (set! items (rest items*))
        (set! shard-iterator (.getNextShardIterator record-result))
        (some-> items*
                (first)
                (rec->segment deserializer-fn)))
      (let [items* (rest items)
            rec ^Record (first items)]
        (set! offset (.getSequenceNumber rec))
        (set! items items*)
        (rec->segment rec deserializer-fn)))))

(defn read-messages [{:keys [onyx.core/task-map onyx.core/log-prefix onyx.core/monitoring onyx.core/slot-id] :as event}]
  (let [{:keys [kinesis/stream-name kinesis/deserializer-fn]} task-map
        batch-timeout (arg-or-default :onyx/batch-timeout task-map)
        batch-size (:onyx/batch-size task-map)
        deserializer-fn (kw->fn (:kinesis/deserializer-fn task-map))
        client (new-client task-map)]
    (->KinesisReadMessages log-prefix task-map slot-id stream-name batch-size 
                           batch-timeout deserializer-fn client nil nil nil)))


(defn segment->put-records-entry [{:keys [partition-key data]} serializer-fn]
  (-> (PutRecordsRequestEntry.)
      (.withPartitionKey (str partition-key))
      (.withData (ByteBuffer/wrap (serializer-fn data)))))

(defn build-put-request [stream-name segments serializer-fn]
  (-> (PutRecordsRequest.)
      (.withStreamName stream-name)
      (.withRecords (doall (map (fn [seg] (segment->put-records-entry seg serializer-fn)) segments)))))

(defrecord KinesisWriteMessages [task-map config stream-name client serializer-fn]
  p/Plugin
  (start [this event] 
    this)

  (stop [this event] 
    this)

  p/BarrierSynchronization
  (synced? [this epoch]
    ; (when @exception (throw @exception))
    ; (empty? (vswap! write-futures clear-write-futures!))
    true
    
    )

  (completed? [this]
    ; (when @exception (throw @exception))
    ; (empty? (vswap! write-futures clear-write-futures!))
    true
    )

  p/Checkpointed
  (recover! [this _ _] 
    this)
  (checkpoint [this])
  (checkpointed! [this epoch])

  p/Output
  (prepare-batch [this event replica _]
    true)
  (write-batch [this {:keys [onyx.core/results]} replica _]
    ;(when @exception (throw @exception))
    (let [segments (mapcat :leaves (:tree results))]
      ;; check PutRecordsResult 0x38a71aa0 {FailedRecordCount
      (when-not (empty? segments)
        (.putRecords client 
                     (build-put-request stream-name 
                                        segments 
                                        serializer-fn))))
    true))

(def write-defaults {})

(defn write-messages [{:keys [onyx.core/task-map onyx.core/log-prefix] :as event}]
  (let [;_ (s/validate onyx.tasks.kinesis/kinesisOutputTaskMap task-map)
        _ (info log-prefix "Starting kinesis/write-messages task")
        stream-name (:kinesis/stream-name task-map)
        config {}
        serializer-fn (kw->fn (:kinesis/serializer-fn task-map))
        client (new-client task-map)]
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
