(ns onyx.tasks.kinesis
  (:require [cheshire.core :as json]
            [schema.core :as s]
            [onyx.job :refer [add-task]]
            [onyx.schema :as os]))

;;;; Reader task
(defn deserialize-message-json [bytes]
  (try
    (json/parse-string (String. bytes "UTF-8"))
    (catch Exception e
      {:error e})))

(defn deserialize-message-edn [bytes]
  (try
    (read-string (String. bytes "UTF-8"))
    (catch Exception e
      {:error e})))

(def KinesisInputTaskMap
  {:kinesis/stream-name s/Str
   ;:kinesis/offset-reset (s/enum :earliest :latest)
   :kinesis/deserializer-fn os/NamespacedKeyword
   (s/optional-key :kinesis/shard) (s/cond-pre s/Int s/Str)
   (s/optional-key :kinesis/wrap-with-metadata?) s/Bool
   (os/restricted-ns :kinesis) s/Any})

(s/defn ^:always-validate consumer
  ([task-name :- s/Keyword opts]
   {:task {:task-map (merge {:onyx/name task-name
                             :onyx/plugin :onyx.plugin.kinesis/read-messages
                             :onyx/type :input
                             :onyx/medium :kinesis
                             :kinesis/receive-buffer-bytes 65536
                             :kinesis/wrap-with-metadata? false
                             :onyx/doc "Reads messages from a kinesis topic"}
                            opts)
           :lifecycles [{:lifecycle/task task-name
                         :lifecycle/calls :onyx.plugin.kinesis/read-messages-calls}]}
    :schema {:task-map KinesisInputTaskMap}})
  ([task-name :- s/Keyword
    topic :- s/Str
    group-id :- s/Str
    zookeeper :- s/Str
    offset-reset :- (s/enum :smallest :largest)
    deserializer-fn :- os/NamespacedKeyword
    task-opts :- {s/Any s/Any}]
   (consumer task-name (merge {:kinesis/topic topic
                               :kinesis/group-id group-id
                               :kinesis/zookeeper zookeeper
                               :kinesis/offset-reset offset-reset
                               :kinesis/deserializer-fn deserializer-fn}
                              task-opts))))

;;;; Writer task
(defn serialize-message-json [segment]
  (.getBytes (json/generate-string segment)))

(defn serialize-message-edn [segment]
  (.getBytes (pr-str segment)))

(def KinesisOutputTaskMap
  {(s/optional-key :kinesis/stream-name) s/Str
   :kinesis/serializer-fn os/NamespacedKeyword
   :kinesis/region s/Str
   (s/optional-key :kinesis/access-key) s/Str
   (s/optional-key :kinesis/secret-key) s/Str
   (s/optional-key :kinesis/endpoint-url) s/Str
   (s/optional-key :kinesis/shard) (s/cond-pre s/Int s/Str)
   (os/restricted-ns :kinesis) s/Any})

(s/defn ^:always-validate producer
  ([task-name :- s/Keyword opts]
   {:task {:task-map (merge {:onyx/name task-name
                             :onyx/plugin :onyx.plugin.kinesis/write-messages
                             :onyx/type :output
                             :onyx/medium :kinesis
                             :onyx/doc "Writes messages to a kinesis topic"}
                            opts)
           :lifecycles [{:lifecycle/task task-name
                         :lifecycle/calls :onyx.plugin.kinesis/write-messages-calls}]}
    :schema {:task-map KinesisOutputTaskMap}})
  ([task-name :- s/Keyword
    stream-name :- s/Str
    serializer-fn :- os/NamespacedKeyword
    task-opts :- {s/Any s/Any}]
   (producer task-name (merge {:kinesis/stream-name stream-name
                               :kinesis/serializer-fn serializer-fn}
                              task-opts))))
