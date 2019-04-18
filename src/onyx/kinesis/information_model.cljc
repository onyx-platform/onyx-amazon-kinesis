(ns onyx.kinesis.information-model)

(def model
  {:catalog-entry
   {:onyx.plugin.kinesis/read-messages
    {:summary "An input task to read messages from a kinesis topic."
     :model {:kinesis/stream-name
             {:doc "The stream to read from."
              :type :string}

             :kinesis/deserializer-fn
             {:doc "A keyword that represents a fully qualified namespaced function to deserialize a record's value. Takes one argument, which must be a byte array."
              :type :keyword}

             :kinesis/shard
             {:doc "Shard to read from if auto-assignment is not used."
              :type :string
              :optional? true}

             :kinesis/region
             {:doc "Kinesis AWS region."
              :type :string
              :optional? true}

             :kinesis/endpoint-url
             {:doc "The Kinesis endpoint-url to connect to."
              :type :string
              :optional? true}

             :kinesis/access-key
             {:doc "AWS access key to authorize when not using default provider chain. Avoid using kinesis/access-key if possible, as the key will be stored in ZooKeeper."
              :type :string
              :optional? true}


             :kinesis/secret-key
             {:doc "Optional: AWS access key to authorize when not using default provider chain. Avoid using kinesis/access-key if possible, as the key will be stored in ZooKeeper."
              :type :string
              :optional? true}

             :kinesis/reader-backoff-ms
             {:doc "Optional: Time to backoff a shard reader upon a ProvisionedThroughputExceededException"
              :type :integer
              :optional? true}
             }}

    :onyx.plugin.kinesis/write-messages
    {:summary "Write messages to kinesis."
     :model {:kinesis/stream-name
             {:doc "The stream to read from."
              :type :string}

             :kinesis/serializer-fn
             {:doc "A keyword that represents a fully qualified namespaced function to serialize a record's value. Takes one argument - the segment."
              :type :keyword}

             :kinesis/region
             {:doc "Kinesis AWS region."
              :type :string
              :optional? true}

             :kinesis/endpoint-url
             {:doc "The Kinesis endpoint-url to connect to."
              :type :string
              :optional? true}

             :kinesis/access-key
             {:doc "AWS access key to authorize when not using default provider chain. Avoid using kinesis/access-key if possible, as the key will be stored in ZooKeeper."
              :type :string
              :optional? true}

             :kinesis/secret-key
             {:doc "Optional: AWS access key to authorize when not using default provider chain. Avoid using kinesis/access-key if possible, as the key will be stored in ZooKeeper."
              :type :string
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
    [:kinesis/stream-name :kinesis/deserializer-fn :kinesis/shard :kinesis/region :kinesis/endpoint-url :kinesis/access-key :kinesis/secret-key]

    :onyx.plugin.kinesis/write-messages
    [:kinesis/stream-name :kinesis/serializer-fn :kinesis/region :kinesis/endpoint-url :kinesis/access-key :kinesis/secret-key]}})
