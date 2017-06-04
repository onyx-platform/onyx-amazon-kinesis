(ns onyx.kinesis.information-model)

(def model
  {:catalog-entry
   {:onyx.plugin.kinesis/read-messages
    {:summary "An input task to read messages from a kinesis topic."
     :model {:kinesis/topic
             {:doc "The topic name to read from."
              :type :string}

             :kinesis/group-id
             {:doc "The consumer identity to store in ZooKeeper."
              :type :string}

             :kinesis/partition
             {:doc "Partition to read from if auto-assignment is not used."
              :type :string
              :optional? true}

             :kinesis/zookeeper
             {:doc "The ZooKeeper connection string."
              :type :string}

             :kinesis/offset-reset
             {:doc "Offset bound to seek to when not found - `:earliest` or `:latest`."
              :choices [:earliest :latest]
              :type :keyword}

             :kinesis/force-reset?
             {:doc "Force to read from the beginning or end of the log, as specified by `:kinesis/offset-reset`. If false, reads from the last acknowledged messsage if it exists."
              :deprecation-version "0.9.10.0"
              :deprecation-doc ":kinesis/force-reset? deprecated as this functionality has been subsumed by onyx resume-point."
              :type :boolean}

             :kinesis/chan-capacity
             {:doc "The buffer size of the kinesis reading channel."
              :type :long
              :default 1000
              :deprecation-version "0.9.10.0"
              :deprecation-doc ":kinesis/chan-capacity deprecated as onyx-kinesis no longer uses a separate producer thread."
              :optional? true}

             :kinesis/receive-buffer-bytes
             {:doc "The size in the receive buffer in the kinesis consumer."
              :type :long
              :default 65536
              :optional? true}

             :kinesis/consumer-opts
             {:doc "A map of arbitrary configuration to merge into the underlying kinesis consumer base configuration. Map should contain keywords as keys, and the valid values described in the [kinesis Docs](http://kinesis.apache.org/documentation.html#newconsumerconfigs). Please note that key values such as `fetch.min.bytes` must be in keyword form, i.e. `:fetch.min.bytes`."
              :type :map
              :optional? true}

             :kinesis/fetch-size
             {:doc "The size in bytes to request from ZooKeeper per fetch request."
              :type :long
              :default 307200
              :deprecation-version "0.9.10.0"
              :deprecation-doc ":kinesis/fetch-size deprecated. Use :kinesis/receive-buffer-bytes instead."
              :optional? true}

             :kinesis/empty-read-back-off
             {:doc "The amount of time to back off between reads when nothing was fetched from a consumer."
              :type :long
              :default 500
              :deprecation-version "0.9.10.0"
              :deprecation-doc ":kinesis/empty-read-back-off deprecated in lieu of better use of :onyx/batch-timeout"
              :optional? true}

             :kinesis/commit-interval
             {:doc "The interval in milliseconds to commit the latest acknowledged offset to ZooKeeper."
              :type :long
              :default 2000
              :optional? true}

             :kinesis/deserializer-fn
             {:doc "A keyword that represents a fully qualified namespaced function to deserialize a record's value. Takes one argument, which must be a byte array."
              :type :keyword}

             :kinesis/key-deserializer-fn
             {:doc "A keyword that represents a fully qualified namespaced function to deserialize a record's key. Takes one argument, which must be a byte array. Only used when `:kinesis/wrap-with-metadata?` is true."
              :type :keyword
              :optional? true}

             :kinesis/wrap-with-metadata?
             {:doc "Wraps message into map with keys `:offset`, `:partitions`, `:topic` and `:message` itself."
              :type :boolean
              :default false
              :optional? true}

             :kinesis/start-offsets
             {:doc "Allows a task to be supplied with the starting offsets for all partitions. Maps partition to offset, e.g. `{0 50, 1, 90}` will start at offset 50 for partition 0, and offset 90 for partition 1."
              :type :map
              :optional? true}}}

    :onyx.plugin.kinesis/write-messages
    {:summary "Write messages to kinesis."
     :model {:kinesis/topic
             {:doc "The topic name to write to. Must either be supplied or otherwise all messages must contain a `:topic` key"
              :optional? true
              :type :string}

             :kinesis/partition
             {:doc "Partition to write to, if you do not wish messages to be auto allocated to partitions. Must either be supplied in the task map, or all messages should contain a `:partition` key."
              :type :string
              :optional? true}

             :kinesis/zookeeper
             {:doc "The ZooKeeper connection string."
              :type :string}

             :kinesis/request-size
             {:doc "The maximum size of request messages.  Maps to the `max.request.size` value of the internal kinesis producer."
              :type :long
              :optional? true}

             :kinesis/serializer-fn
             {:doc "A keyword that represents a fully qualified namespaced function to serialize a record's value. Takes one argument - the segment."
              :type :keyword}

             :kinesis/key-serializer-fn
             {:doc "A keyword that represents a fully qualified namespaced function to serialize a record's key. Takes one argument - the segment."
              :type :keyword
              :optional? true}

             :kinesis/producer-opts
             {:doc "A map of arbitrary configuration to merge into the underlying kinesis producer base configuration. Map should contain keywords as keys, and the valid values described in the [kinesis Docs](http://kinesis.apache.org/documentation.html#producerconfigs). Please note that key values such as `buffer.memory` must be in keyword form, i.e. `:buffer.memory`."
              :type :map
              :optional? true}

             :kinesis/no-seal?
             {:doc "Do not write :done to the topic when task receives the sentinel signal (end of batch job)."
              :type :boolean
              :default false
              :optional? true}}}}

   :lifecycle-entry
   {:onyx.plugin.kinesis/read-messages
    {:model
     [{:task.lifecycle/name :read-messages
       :lifecycle/calls :onyx.plugin.kinesis/read-messages-calls}]}

    :onyx.plugin.kinesis/write-messages
    {:model
     [{:task.lifecycle/name :write-messages
       :lifecycle/calls :onyx.plugin.kinesis/write-messages-calls}]}}

   :display-order
   {:onyx.plugin.kinesis/read-messages
    [:kinesis/topic
     :kinesis/partition
     :kinesis/group-id
     :kinesis/zookeeper
     :kinesis/offset-reset
     :kinesis/force-reset?
     :kinesis/deserializer-fn
     :kinesis/key-deserializer-fn
     :kinesis/receive-buffer-bytes
     :kinesis/commit-interval
     :kinesis/wrap-with-metadata?
     :kinesis/start-offsets
     :kinesis/consumer-opts
     :kinesis/empty-read-back-off
     :kinesis/fetch-size
     :kinesis/chan-capacity]

    :onyx.plugin.kinesis/write-messages
    [:kinesis/topic
     :kinesis/zookeeper
     :kinesis/partition
     :kinesis/serializer-fn
     :kinesis/key-serializer-fn
     :kinesis/request-size
     :kinesis/no-seal?
     :kinesis/producer-opts]}})
