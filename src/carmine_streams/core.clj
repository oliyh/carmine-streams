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

(defn next-id
  "Given a redis message id returns the next smallest possible id"
  [id]
  (let [[timestamp sequence-number] (string/split id #"-")]
    (str timestamp "-" (inc (if sequence-number
                              (bigint sequence-number)
                              0)))))

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
  "Consumer behaviour is as follows:

 - Calls the callback for every message received, with the message
   coerced into a keywordized map, and acks the message.
   If the callback throws an exception the message will not be acked
 - Processes all pending messages on startup before processing new ones
 - Processes new messages until either:
   - The consumer is stopped (see `stop-consumers!`)
   - There are no messages delivered during the time it was blocked waiting
     for a new message, upon which it will check for pending messages and
     begin processing the backlog if any are found, returning to wait for
     new messages when the backlog is cleared

 Options to the consumer consist of:

 - `:block` ms to block waiting for a new message before checking the backlog"
  [conn-opts stream group consumer-name f & [{:keys [block]
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

(defn message-exceeds? [thresholds [_ _ idle deliveries]]
  (or (and (:idle thresholds)
           (<= (:idle thresholds) idle))
      (and (:deliveries thresholds)
           (<= (:deliveries thresholds) deliveries))))

(defn gc-consumer-group! [conn-opts stream group & [{:keys [rebalance
                                                            dlq]
                                                     :or {rebalance {:siblings :active
                                                                     :distribution :random
                                                                     :idle (* 60 1000)}
                                                          dlq {:stream (stream-name "dlq")
                                                               :deliveries 10}}
                                                     :as opts}]]
  (let [logging-context {:stream stream
                         :group group}
        all-consumers (:consumers (group-stats conn-opts stream group))
        active-consumers (if (= :active (:siblings rebalance))
                           (remove #(and (pos? (:pending %))
                                         (< (:idle rebalance) (:idle %)))
                                   all-consumers)
                           all-consumers)]
    (if (empty? active-consumers)
      (log/warn logging-context "No active consumers found" all-consumers)

      (doseq [consumer-name (map :name all-consumers)
              :let [logging-context (assoc logging-context :consumer consumer-name)]]
        (loop [last-id "-"]
          (let [pending-messages (car/wcar
                                  conn-opts
                                  (car/xpending stream group last-id "+" 100 consumer-name))]
            (when (seq pending-messages)
              (car/wcar conn-opts
                        (doseq [[message-id _consumer idle deliveries :as message] pending-messages]
                          (cond
                            (and dlq (message-exceeds? dlq message))
                            (do (log/info logging-context "Sending message" message-id "to" (:stream dlq) message)
                                (car/xack stream group message-id)
                                (car/xadd (:stream dlq) "*" "stream" stream "group" group "consumer" consumer-name "id" message-id "idle" idle "deliveries" deliveries))

                            (and rebalance (message-exceeds? rebalance message))
                            (let [next-consumer (as-> active-consumers %
                                                  (condp = (:distribution rebalance)
                                                    :activity (sort-by :idle %)
                                                    :inactivity (sort-by :idle > %)
                                                    (shuffle %))
                                                  (map :name %)
                                                  (set %)
                                                  (disj % consumer-name)
                                                  (first %))]
                              (log/info logging-context "Claiming message" message-id "for" next-consumer message)
                              (car/xclaim stream group next-consumer idle message-id))

                            :else
                            :noop)))
              (recur (next-id (first (last pending-messages)))))))))))
