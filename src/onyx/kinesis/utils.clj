(ns onyx.kinesis.utils
  (:require [onyx.plugin.kinesis]
            [taoensso.timbre :as log]
            [aero.core :refer [read-config]]
            [clojure.core.async :as async])
  (:import [com.amazonaws.services.kinesis.model.CreateStreamRequest]
           [com.amazonaws.services.kinesis AmazonKinesisClientBuilder AmazonKinesis]))

(defn client-builder []
  (AmazonKinesisClientBuilder/defaultClient))

(defn create-stream [^AmazonKinesis client stream-name shard-count]
  (.createStream client
                 (-> (com.amazonaws.services.kinesis.model.CreateStreamRequest.)
                     (.withShardCount (int shard-count))
                     (.withStreamName stream-name))))

(defn delete-stream [^AmazonKinesis client ^String stream-name]
  (.deleteStream client stream-name))

(create-stream (client-builder) "mystream" 1)

(delete-stream (client-builder) "mystream")
