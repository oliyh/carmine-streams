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
   Blocks for `block` ms waiting for new messages. If none are delivered in this time
   will check for and process pending messages, which will be present either as a result
   of an earlier failed callback or having been rebalanced from a dead sibling.
   Calls f on each message (which presumably has side effects)."
  [conn-opts stream group f consumer-name & [{:keys [block]
                                              :or {block 5000}
                                              :as opts}]]
  (let [logging-context {:stream stream
                         :group group
                         :consumer consumer-name}]
    (log/info logging-context "Starting")
    (loop [last-id "0-0"]
      (let [[_setname-ok? response]
            (try (car/wcar conn-opts
                           (car/client-setname consumer-name)
                           (car/xreadgroup :group group consumer-name
                                           :block block
                                           :count 1 ;; one message at a time
                                           :streams stream
                                           (or last-id ">")))
                 (catch Throwable t
                   (log/error t logging-context "Exception reading from stream, exiting")
                   t))]

        (cond
          (and (instance? Exception response)
               (= :unblocked (:prefix (ex-data response))))
          (log/info logging-context "Shutdown signal received")

          (instance? Exception response)
          (do (log/error response logging-context "Exception reading from stream, exiting")
              response)

          :else
          (let [[[_stream-name messages]] response
                [[id kvs]] messages]
            (cond
              (and last-id (empty? messages))
              (do (log/info logging-context "Finished processing pending messages")
                  (recur nil))

              kvs
              (do (try (f (kvs->map kvs))
                       (car/wcar conn-opts (car/xack stream group id))
                       (catch Throwable t
                         (log/error t logging-context "Error in callback processing" id kvs)))
                  (recur (when last-id id)))

              :else ;; unblocked naturally, this is a quiet time to check for pending messages
              (if-let [id (->> (car/xpending stream group "-" "+" 1 consumer-name)
                               (car/wcar conn-opts)
                               ffirst)]
                (do (log/info logging-context "Processing pending messages")
                    (recur "0-0"))
                (recur nil)))))))))

(defn stop-consumers!
  "Stop all the consumers for the consumer group by sending an UNBLOCK message"
  ([conn-opts] (stop-consumers! conn-opts (consumer-name nil)))
  ;; todo stop just consumers of a stream/group combination
  ([conn-opts consumer-name-pattern]
   (let [all-clients (car/wcar conn-opts (car/client-list))
         consumer-clients (->> (string/split-lines all-clients)
                               (filter #(re-find (re-pattern consumer-name-pattern) %))
                               (map #(subs (re-find #"id=\d*\b" %) 3)))]
     (car/wcar conn-opts (mapv #(car/client-unblock % :error) consumer-clients )))))

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
