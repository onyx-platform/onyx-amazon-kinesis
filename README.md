## onyx-kinesis

Onyx plugin providing consumer and producer facilities for kinesis.

Please note that this plugin is currently alpha quality, and does not support
splitting or combining shards. Pull requests welcome!

## Development

The tests currently run slowly, as they have to create and delete shards on each run.

#### Installation

In your project file:

```clojure
[org.onyxplatform/onyx-amazon-kinesis "0.12.7.0"]
```

In your peer boot-up namespace:

```clojure
(:require [onyx.plugin.kinesis])
```

#### Functions

##### read-messages

Reads segments from a kinesis topic. Peers will automatically be assigned to each
of the topics partitions, balancing the number of partitions over the number of
peers, unless `:kinesis/partition` is supplied in which case only one partition
will be read from.

Data is emitted in the following form:

``` clj
{:timestamp (.getApproximateArrivalTimestamp record)
 :partition-key (.getPartitionKey record)
 :sequence-number (.getSequenceNumber record)
 :data (deserializer-fn (.array (.getData record)))}
```

Catalog entry:

```clojure
{:onyx/name :read-messages
 :onyx/plugin :onyx.plugin.kinesis/read-messages
 :onyx/type :input
 :onyx/medium :kinesis
 :kinesis/stream-name "mystreamname"
 :kinesis/shard-initialize-type :trim-horizon
 :kinesis/deserializer-fn :my.ns/deserializer-fn
 :onyx/batch-timeout 50
 :onyx/n-peers << NUMBER OF SHARDS TO READ PARTITIONS, UP TO N-SHARDS MAX >>
 :onyx/batch-size 100
 :onyx/doc "Reads messages from a kinesis topic"}
```

Lifecycle entry:

```clojure
{:lifecycle/task :read-messages
 :lifecycle/calls :onyx.plugin.kinesis/read-messages-calls}
```

###### Attributes

|key                                  | type      | default | description
|-------------------------------------|-----------|---------|------------
|`:kinesis/stream-name`               | `string`  |         | The stream to read from
|`:kinesis/shard`                     | `integer or string` |         | Optional: shard to read or write to from if auto-assignment is not used
|`:kinesis/shard-initialize-type`     | `keyword` |         | Offset bound to seek to when not found - `:latest` or `:trim-horizon`
|`:kinesis/deserializer-fn`           | `keyword` |         | A keyword that represents a fully qualified namespaced function to deserialize a record's value. Takes one argument - a byte array
|`:kinesis/region`                    | `string`  |         | Optional: Kinesis AWS region
|`:kinesis/endpoint-url`              | `string`  |         | Optional: The Kinesis endpoint-url to connect to.
|`:kinesis/access-key`                | `string`  |         | Optional: AWS access key to authorize when not using default provider chain. Avoid using kinesis/access-key if possible, as the key will be stored in ZooKeeper.
|`:kinesis/secret-key`                | `string`  |         | Optional: AWS access key to authorize when not using default provider chain. Avoid using kinesis/access-key if possible, as the key will be stored in ZooKeeper.

##### write-messages

Writes segments to a kinesis topic using the kinesis "new" producer.

Catalog entry:

```clojure
{:onyx/name :write-messages
 :onyx/plugin :onyx.plugin.kinesis/write-messages
 :onyx/type :output
 :onyx/medium :kinesis
 :kinesis/stream-name "stream-name"
 :kinesis/serializer-fn :my.ns/serializer-fn
 :onyx/batch-size batch-size
 :onyx/doc "Writes messages to a kinesis topic"}
```

Lifecycle entry:

```clojure
{:lifecycle/task :write-messages
 :lifecycle/calls :onyx.plugin.kinesis/write-messages-calls}
```

Segments supplied to a `:onyx.plugin.kinesis/write-messages` task should be in in
the following form:

``` clj
{:partition-key partition-key
 :data data}
```

###### Attributes

|key                                  | type      | default | description
|-------------------------------------|-----------|---------|------------
|`:kinesis/stream-name`               | `string`  |         | The stream to read from
|`:kinesis/serializer-fn`      | `keyword` |         | A keyword that represents a fully qualified namespaced function to serialize a record's value. Takes one argument - the segment
|`:kinesis/region`                    | `string`  |         | Optional: kinesis AWS region
|`:kinesis/endpoint-url`              | `string`  |         | Optional: The kinesis endpoint-url to connect to.
|`:kinesis/access-key`                | `string`  |         | Optional: AWS access key to authorize when not using default provider chain. Avoid using kinesis/access-key if possible, as the key will be stored in ZooKeeper.
|`:kinesis/secret-key`                | `string`  |         | Optional: AWS access key to authorize when not using default provider chain. Avoid using kinesis/access-key if possible, as the key will be stored in ZooKeeper.

#### Contributing

Pull requests into the master branch are welcomed.

#### License

Copyright © 2017 Distributed Masonry

Distributed under the Eclipse Public License, the same as Clojure.
