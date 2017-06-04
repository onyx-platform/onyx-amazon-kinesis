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

(def defaults
  {})

(defn seek-offset! []
  )

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

; (defn assign-partitions-to-slot! [consumer* task-map topic n-partitions slot]
;   (if-let [part (:partition task-map)]
;     (let [p (Integer/parseInt part)]
;       (cp/assign-partitions! consumer* [{:topic topic :partition p}])
;       [p])
;     (let [n-slots (or (:onyx/n-peers task-map) (:onyx/max-peers task-map))
;           [lower upper] (partitions-for-slot n-partitions n-slots slot)
;           parts-range (range lower (inc upper))
;           parts (map (fn [p] {:topic topic :partition p}) parts-range)]
;       (cp/assign-partitions! consumer* parts)
;       parts-range)))

; (deftype kinesisReadMessages 
;   [log-prefix task-map topic ^:unsynchronized-mutable kpartitions batch-timeout
;    deserializer-fn segment-fn read-offset ^:unsynchronized-mutable consumer 
;    ^:unsynchronized-mutable iter ^:unsynchronized-mutable partition->offset ^:unsynchronized-mutable drained]
;   p/Plugin
;   (start [this event]
;     (let [{:keys [kinesis/group-id kinesis/consumer-opts]} task-map
;           brokers (find-brokers task-map)
;           _ (s/validate onyx.tasks.kinesis/kinesisInputTaskMap task-map)
;           consumer-config (merge {:bootstrap.servers brokers
;                                   :group.id (or group-id "onyx")
;                                   :enable.auto.commit false
;                                   :receive.buffer.bytes (or (:kinesis/receive-buffer-bytes task-map)
;                                                             (:kinesis/receive-buffer-bytes defaults))
;                                   :auto.offset.reset (:kinesis/offset-reset task-map)}
;                                  consumer-opts)
;           _ (info log-prefix "Starting kinesis/read-messages task with consumer opts:" consumer-config)
;           key-deserializer (byte-array-deserializer)
;           value-deserializer (byte-array-deserializer)
;           consumer* (consumer/make-consumer consumer-config key-deserializer value-deserializer)
;           partitions (mapv :partition (metadata/partitions-for consumer* topic))
;           n-partitions (count partitions)]
;       (check-num-peers-equals-partitions task-map n-partitions)
;       (let [kpartitions* (assign-partitions-to-slot! consumer* task-map topic n-partitions (:onyx.core/slot-id event))]
;         (set! consumer consumer*)
;         (set! kpartitions kpartitions*)
;         this)))

;   (stop [this event] 
;     (when consumer 
;       (.close ^FranzConsumer consumer)
;       (set! consumer nil))
;     this)

;   p/Checkpointed
;   (checkpoint [this]
;     partition->offset)

;   (recover! [this replica-version checkpoint]
;     (set! drained false)
;     (set! iter nil)
;     (set! partition->offset checkpoint)
;     (seek-offset! log-prefix consumer kpartitions task-map topic checkpoint)
;     this)

;   (checkpointed! [this epoch])

;   p/BarrierSynchronization
;   (synced? [this epoch]
;     true)

;   (completed? [this]
;     drained)

;   p/Input
;   (poll! [this _]
;     (if (and iter (.hasNext ^java.util.Iterator iter))
;       (let [rec ^ConsumerRecord (.next ^java.util.Iterator iter)
;             deserialized (some-> rec segment-fn)]
;         (cond (= :done deserialized)
;               (do (set! drained true)
;                   nil)
;               deserialized
;               (let [new-offset (.offset rec)
;                     part (.partition rec)]
;                 (.set ^AtomicLong read-offset new-offset)
;                 (set! partition->offset (assoc partition->offset part new-offset))
;                 deserialized)))
;       (do (set! iter 
;                 (.iterator ^ConsumerRecords 
;                            (.poll ^Consumer (.consumer ^FranzConsumer consumer) 
;                                   batch-timeout)))
;           nil))))

; (defn read-messages [{:keys [onyx.core/task-map onyx.core/log-prefix onyx.core/monitoring] :as event}]
;   (let [{:keys [kinesis/topic kinesis/deserializer-fn]} task-map
;         batch-timeout (arg-or-default :onyx/batch-timeout task-map)
;         wrap-message? (or (:kinesis/wrap-with-metadata? task-map) (:kinesis/wrap-with-metadata? defaults))
;         deserializer-fn (kw->fn (:kinesis/deserializer-fn task-map))
;         key-deserializer-fn (if-let [kw (:kinesis/key-deserializer-fn task-map)] (kw->fn kw) identity)
;         segment-fn (if wrap-message?
;                      (fn [^ConsumerRecord cr]
;                        {:topic (.topic cr)
;                         :partition (.partition cr)
;                         :key (when-let [k (.key cr)] (key-deserializer-fn k))
;                         :message (deserializer-fn (.value cr))
;                         :serialized-key-size (.serializedKeySize cr)
;                         :serialized-value-size (.serializedValueSize cr)
;                         :timestamp (.timestamp cr)
;                         :offset (.offset cr)})
;                      (fn [^ConsumerRecord cr]
;                        (deserializer-fn (.value cr))))
;         read-offset (:read-offset monitoring)]
;     (->kinesisReadMessages log-prefix task-map topic nil batch-timeout
;                          deserializer-fn segment-fn read-offset nil nil nil false)))


(defn- message->producer-record
  [key-serializer-fn serializer-fn topic kpartition m]
  (let [message (:message m)
        k (some-> m :key serializer-fn)
        message-topic (get m :topic topic)
        message-partition (some-> m (get :partition kpartition) int)]
    (cond (not (contains? m :message))
          (throw (ex-info "Payload is missing required. Need message key :message"
                          {:recoverable? false
                           :payload m}))

          (nil? message-topic)
          (throw (ex-info
                  (str "Unable to write message payload to kinesis! "
                       "Both :kinesis/topic, and :topic in message payload "
                       "are missing!")
                  {:recoverable? false
                   :payload m}))

          :else
          {:my :thing}
          #_(ProducerRecord. message-topic message-partition k (serializer-fn message)))))

(defn clear-write-futures! [fs]
  (doall (remove (fn [f] 
                   (assert (not (.isCancelled ^java.util.concurrent.Future f)))
                   (realized? f)) 
                 fs)))


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
