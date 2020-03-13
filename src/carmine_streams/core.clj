(ns carmine-streams.core
  (:require [taoensso.carmine :as car]
            [clojure.tools.logging :as log]
            [clojure.string :as string]))

(defn stream-name [s]
  (str "stream/" s))

(defn group-name [s]
  (str "group/" s))

(defn consumer-name
  ([s] (str "consumer/" s))
  ([s i] (str "consumer/" s "/" i)))

(defn kvs->map [kvs]
  (reduce (fn [m [k v]]
            (assoc m (keyword k) v))
          {}
          (partition-all 2 kvs)))

(defn all-stream-keys
  ([conn-opts] (all-stream-keys conn-opts (stream-name "*")))
  ([conn-opts key-pattern]
   (as-> (car/wcar conn-opts (car/keys key-pattern)) ks
     (zipmap ks (car/wcar conn-opts :as-pipeline (mapv car/type ks)))
     (filter #(= "stream" (val %)) ks)
     (keys ks)
     (set ks))))

(defn group-names [conn-opts stream]
  (->> (car/wcar conn-opts (car/xinfo :groups stream))
       (map kvs->map)
       (map :name)
       set))

(defn rebalance-consumers! [conn-opts])
;; end todo

(defn create-consumer-group!
  "An idempotent function that creates a consumer group for the stream"
  ([conn-opts stream group]
   ;; default to reading new messages from the stream
   (create-consumer-group! conn-opts stream group "$"))
  ([conn-opts stream group from-id]
   (try (= "OK" (car/wcar conn-opts (car/xgroup :create stream group from-id :mkstream)))
        (catch Throwable t
          (if (= :busygroup (:prefix (ex-data t)))
            true ;; consumer group already exists
            (throw t))))))

(defn start-consumer!
  "Start consuming messages from the consumer group.
   On startup reads pending messages first before processing new ones.
   Blocks indefinitely waiting for new messages.
   Calls f on each message (which presumably has side effects)."
  [conn-opts stream group f consumer-name]
  (log/info consumer-name "Starting")
  (loop [last-id "0-0"]
    (let [[_ok? [[_stream-name messages]]]
          (try (car/wcar conn-opts
                         (car/client-setname consumer-name)
                         (car/xreadgroup :group group consumer-name
                                         :block 0 ;; block indefinitely
                                         :count 1 ;; one message at a time
                                         :streams stream
                                         (or last-id ">")))
               (catch Throwable t
                 (log/error t consumer-name "Error reading from stream")
                 nil))
          [[id kvs]] messages]

      (cond
        (and last-id (empty? messages)) ;; finished consuming backlog
        (recur nil)

        kvs
        (do (try (f (kvs->map kvs))
                 (car/wcar conn-opts (car/xack stream group id))
                 (catch Throwable t
                   (log/error t consumer-name "Error in callback processing" id kvs)))
            (recur (when last-id id)))

        :else
        (log/info consumer-name "Shutting down")))))

(defn stop-consumers!
  "Stop all the consumers for the consumer group by sending an UNBLOCK message"
  [conn-opts stream group consumer-name-pattern]
  (let [all-clients (car/wcar conn-opts (car/client-list))
        consumer-clients (->> (string/split-lines all-clients)
                              (filter #(re-find (re-pattern consumer-name-pattern) %))
                              (map #(subs (re-find #"id=\d*\b" %) 3)))]
    (car/wcar conn-opts (mapv car/client-unblock consumer-clients))))

(defn group-stats
  "Useful stats about the consumer group"
  [conn-opts stream group]
  (let [[groups-info consumer-info]
        (car/wcar conn-opts
                  (car/xinfo :groups stream)
                  (car/xinfo :consumers stream group))
        group-info (->> groups-info
                        (map kvs->map)
                        (filter #(= group (:name %)))
                        first)]
    (assoc group-info
           :consumers (map kvs->map consumer-info)
           :unconsumed (->> (car/xrange stream (:last-delivered-id group-info) "+")
                            (car/wcar conn-opts)
                            count
                            dec
                            (max 0)))))
