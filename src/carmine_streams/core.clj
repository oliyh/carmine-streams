(ns carmine-streams.core
  (:require [taoensso.carmine :as car]
            [clojure.tools.logging :as log]
            [clojure.string :as string]
            [clojure.walk :as walk]))

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

(defn xadd-map [& args]
  (apply car/xadd (concat (butlast args) (reduce into [] (last args)))))

(defn next-id
  "Given a redis message id returns the next smallest possible id"
  [id]
  (let [[timestamp sequence-number] (string/split id #"-")]
    (str timestamp "-" (inc (if sequence-number
                              (bigint sequence-number)
                              0)))))

(defn prev-id
  "Given a redis message id returns the previous largest possible id"
  [id]
  (let [[timestamp sequence-number] (string/split id #"-")]
    (if (contains? #{nil "0"} sequence-number)
      ;; largest possible sequence number
      (str (dec (bigint timestamp)) "-18446744073709551615")
      (str timestamp "-" (dec (bigint sequence-number))))))

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

(defn unblocked? [v]
  (and (instance? Throwable v)
       (= :unblocked (:prefix (ex-data v)))))

(defn default-control-fn
  "The default control flow for consumers.
   Must return either `:recur` (to read the next message) or `:exit` to exit the loop.
   May have any side effects you need.
   Exits when unblocked via `unblock-consumers!` or any other error reading from redis.
   Recurs in all other scenarios."
  [phase context value & [id kvs]]
  (cond
    (and (instance? Throwable value)
         (= :callback phase))
    (do (log/error value context "Error in callback processing" id kvs)
        :recur)

    (unblocked? value)
    (do (log/info context "Shutdown signal received")
        :exit)

    (instance? Throwable value)
    (do (log/error value context "Exception during" phase ", exiting")
        :exit)

    :else
    :recur))

(defn- process-highest-priority!
  [{:keys [conn-opts streams group delivery-counts f
           last-ids stream->message logging-context]}
   control-fn]
  (let [;; look through streams in priority order, returning the first
        ;; one that has a message we should process
        received-stream
        (->> streams
             ;; if a stream was looking at pending messages, but there
             ;; are no more pending messages, we want to check for new
             ;; messages on that stream before we process any messages
             ;; on lower priority streams. we can identify the streams
             ;; that just finished looking at pending messages,
             ;; because their last-id is not nil and they haven't
             ;; received a message. this `take-while` ensures that all
             ;; the lower priority streams than the one that just
             ;; finished looking at pending messages are ignored.
             (take-while
              #(not
                (and (last-ids %)
                     (nil? (stream->message %)))))
             ;; find the first stream that has a message
             (filter stream->message)
             first)]
    (if received-stream
      (let [[id kvs] (stream->message received-stream)
            delivery-counts-key (str received-stream "/" id)
            ;; if there's a message, process and ack it
            v
            (try
              (car/wcar conn-opts
                        (car/hincrby delivery-counts
                                     delivery-counts-key
                                     1))
              (let [v (f (kvs->map kvs))]
                (car/wcar conn-opts
                          (car/multi)
                          (car/xack received-stream group id)
                          (car/hdel delivery-counts delivery-counts-key)
                          (car/exec))
                v)
              (catch Exception e
                e))

            control-instruction (control-fn :callback
                                            logging-context
                                            v id kvs)]
        {:received-message {:id id
                            :stream received-stream}
         :control-instruction control-instruction
         :processing-result v})
      {:control-instruction :recur})))

(defn- rescue-abandoned-work!
  "Should be called from within a wcar."
  [{:keys [delivery-counts group logging-context consumer-name]
    {:keys [min-idle-time max-deliveries message-rescue-count]
     :or {min-idle-time (* 60 1000)
          max-deliveries 10
          message-rescue-count 100}
     {dlq-stream :stream
      dlq-include-message? :include-message?
      :or {dlq-stream (stream-name "dlq")
           dlq-include-message? true}}
     :dlq}
    :claim-opts}
   stream]
  (let [[all-idle-pending my-pending]
        (car/with-replies
          (car/xpending stream
                        group
                        :idle min-idle-time
                        "-" "+"
                        message-rescue-count)
          (car/xpending stream
                        group
                        "-" "+"
                        message-rescue-count
                        consumer-name))]
    (->>
     (concat
      (remove (fn [[_ owner _ _]]
                (= owner consumer-name))
              all-idle-pending)
      my-pending)
     (#(do (when (seq %)
             (log/info logging-context "Found" (count %) "pending message(s)"))
           %))
     (mapv
      (fn [[id owner idle _ :as pending-message]]
        (let [delivery-counts-key (str stream "/" id)
              delivery-count
              (or (car/as-int
                   (car/with-replies
                     (car/hget delivery-counts
                               delivery-counts-key)))
                  0)

              poison?
              (>= delivery-count max-deliveries)

              ;; claim the message:

              ;; if this message already belongs to us and isn't
              ;; poison, we don't need to claim it again. if it is
              ;; poison, then we claim it to make sure it isn't added
              ;; to the DLQ twice.
              claimed-message
              (when (or (not= owner consumer-name)
                        poison?)
                (first
                 ;; returns a list of 0 or 1 successfully claimed
                 ;; messages

                 ;; minimum idle time on xclaim is the idle time
                 ;; returned by xpending: if idle time is large, then
                 ;; this xclaim will only succeed for one consumer,
                 ;; even if multiple consumers attempt to claim the
                 ;; same message. if idle time is small, then this
                 ;; message must have been returned by the xpending
                 ;; call for this consumer's own messages, so there
                 ;; can only be one consumer trying to claim it.
                 (car/with-replies
                   (if (and poison?
                            dlq-include-message?)
                     (car/xclaim stream group consumer-name idle id)
                     (car/xclaim stream group consumer-name idle id :justid)))))]
          (when (and claimed-message poison?)
            (log/info logging-context "Sending message" id "to" dlq-stream pending-message)
            (car/multi)
            (xadd-map
             dlq-stream "*"
             (-> {:stream stream
                  :group group
                  :consumer consumer-name
                  :id id}
                 (cond-> dlq-include-message?
                   (assoc :message (second claimed-message)))))
            (car/xack stream group id)
            (car/hdel delivery-counts delivery-counts-key)
            (car/exec))
          (let [should-retry? (and claimed-message (not poison?))]
            (when should-retry?
              (log/info logging-context consumer-name "claimed message" pending-message))
            should-retry?))))
     ;; only messages that should be re-tried
     (filter identity))))

(defn- get-pending-work!
  [{:keys [conn-opts streams group consumer-name logging-context stream->message last-ids]
    :as context}
   rescue-abandoned?]
  (let [;; if a stream is processing pending messages (has a non-nil
        ;; last-id) or just received a new message, then we don't want
        ;; to check for abandoned work on that stream or any lower
        ;; priority streams
        streams-to-check (take-while
                          #(and (nil? (last-ids %))
                                (nil? (stream->message %)))
                          streams)
        stream->pending (delay
                         (when (seq streams-to-check)
                           (->> (car/with-replies
                                  (apply
                                   car/xreadgroup :group group
                                   consumer-name
                                   :count 1
                                   :streams
                                   (concat streams-to-check
                                           (map (constantly "0-0")
                                                streams-to-check))))
                                (map (fn [[stream messages]]
                                       [stream (first messages)]))
                                (into {}))))]
    {:stream-with-pending-messages
     (car/wcar
      conn-opts
      (car/return
       (->> streams-to-check
            (filter
             (fn [stream]
               (or (when rescue-abandoned?
                     (let [rescued-messages (rescue-abandoned-work! context stream)]
                       (when (seq rescued-messages)
                         (log/info (assoc logging-context :stream stream)
                                   "Rescued abandoned messages")
                         true)))
                   (when (get @stream->pending stream)
                     (log/info (assoc logging-context :stream stream)
                               "Found my own pending messages")
                     true))))
            first)))
     :checked-streams streams-to-check}))

(defn- update-last-ids
  [{:keys [stream->message logging-context last-ids]} received-message]
  (walk/walk
   (fn [[stream last-id]]
     [stream
      (case [(if (some? last-id)
               :processing-pending
               :processing-new)
             (cond
               (nil? (stream->message stream)) :no-message
               (= stream (:stream received-message)) :consumed-message
               :else :got-message)]
        [:processing-new :no-message]
        nil ;; keep waiting for new messages

        [:processing-new :consumed-message]
        nil ;; there are new messages, try waiting for another new one

        [:processing-new :got-message]
        ;; we didn't consume the message, but will want to later.
        ;; use an id that will return the same message again on the
        ;; next xreadgroup.
        (prev-id (first (stream->message stream)))

        [:processing-pending :no-message]
        (do
          (log/info (assoc logging-context :stream stream)
                    "Finished processing pending messages")
          nil)

        [:processing-pending :consumed-message]
        (:id received-message) ;; move on to the next pending message

        [:processing-pending :got-message]
        ;; didn't process it, so don't move on to the next
        last-id)])
   identity
   last-ids))

(defn start-multi-consumer!
  [conn-opts streams group consumer-name delivery-counts f
   & [{:keys [block control-fn]
       :or   {block      5000
              control-fn default-control-fn}
       {:keys [min-idle-time]
        :or   {min-idle-time (* 60 1000)}
        :as   claim-opts}
       :claim-opts}]]
  (let [logging-context {:streams  streams
                         :group    group
                         :consumer consumer-name}
        context         {:conn-opts       conn-opts
                         :streams         streams
                         :group           group
                         :consumer-name   consumer-name
                         :delivery-counts delivery-counts
                         :f               f
                         :claim-opts      claim-opts
                         :logging-context logging-context}]
    (log/info logging-context "Starting")
    (loop [last-ids           (->> streams
                                   (map (fn [stream] [stream "0-0"]))
                                   (into {}))
           last-pending-check (System/currentTimeMillis)]
      (if (.isInterrupted (Thread/currentThread))
        (log/info logging-context "Thread interrupted")
        (let [[_setname-ok? response]
              (try
                ;; one item from each stream
                (car/wcar conn-opts
                          (car/client-setname consumer-name)
                          (apply car/xreadgroup
                                 :group group
                                 consumer-name
                                 :block block
                                 :count 1
                                 :streams
                                 (concat streams
                                         (map #(or (last-ids %) ">")
                                              streams))))
                (catch Exception e
                  e))]
          (if (instance? Exception response)
            (case (control-fn :read logging-context response)
              :exit response
              :recur (recur last-ids last-pending-check))
            (let [;; response format is:
                  ;; [["stream3" [[id3 message3]]]
                  ;;  ["stream1" [[id1 message1]]]]

                  ;; {"stream3" [id3 message3]
                  ;;  "stream1" [id1 message1]}
                  stream->message
                  (->> response
                       (map (fn [[stream messages]]
                              [stream
                               ;; one message per stream because of
                               ;; `:count 1`
                               (first messages)]))
                       (into {}))

                  context (assoc context
                                 :last-ids last-ids
                                 :stream->message stream->message)

                  {:keys [control-instruction received-message
                          processing-result]}
                  (process-highest-priority! context control-fn)]
              (case control-instruction
                :exit processing-result
                :recur
                (let [rescue-abandoned?
                      (> (- (System/currentTimeMillis) last-pending-check)
                         min-idle-time)

                      {:keys [stream-with-pending-messages
                              checked-streams]}
                      (get-pending-work! context
                                         rescue-abandoned?)]
                  (recur
                   (-> (update-last-ids context received-message)
                       (cond-> stream-with-pending-messages
                         (assoc stream-with-pending-messages "0-0")))
                   (if (and rescue-abandoned? (seq checked-streams))
                     (System/currentTimeMillis)
                     last-pending-check)))))))))))

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

(defn create-consumer-group!
  "An idempotent function that creates a consumer group for the
  stream(s) and deregisters idle consumers.

  Idle-time threshold for when a consumer is considered dead can be
  configured like so:
  `(create-consumer-group!
    conn-opts stream group from-id
    {:deregister-idle milliseconds-idle-threshold})`"
  ([conn-opts streams group]
   ;; default to reading new messages from the stream
   (create-consumer-group! conn-opts streams group "$"))
  ([conn-opts streams group from-id
    & [{:keys [deregister-idle]
        :or {deregister-idle (* 10 60 1000)}}]]
   (car/wcar
    conn-opts
    (car/return
     (every?
      identity
      ;; `every?` short-circuits on a false value, be we want to
      ;; attempt to create every group, so use `mapv`
      (mapv
       (fn [stream]
         (let [exists? (try (= "OK"
                               (car/with-replies
                                 (car/xgroup :create stream group from-id :mkstream)))
                            (catch Throwable t
                              (if (= :busygroup (:prefix (ex-data t)))
                                true ;; consumer group already exists
                                (throw t))))

               {:keys [consumers]} (group-stats conn-opts stream group)]
           (doseq [consumer consumers
                   :when (>= (:idle consumer) deregister-idle)]
             (car/xgroup :delconsumer stream group (:name consumer))
             (log/info "Deregistering consumer" (:name consumer) "which has been idle for" (:idle consumer) "ms"))
           exists?))
       (if (coll? streams) streams [streams])))))))

(defn unblock-consumers!
  "Unblock all the consumers for the consumer group by sending an UNBLOCK message.
   The default control-fn will terminate the consumer loop"
  ([conn-opts] (unblock-consumers! conn-opts (consumer-name nil)))
  ([conn-opts consumer-name-pattern]
   (let [all-clients (car/wcar conn-opts (car/client-list))
         consumer-clients (->> (string/split-lines all-clients)
                               (filter #(re-find (re-pattern consumer-name-pattern) %))
                               (map #(subs (re-find #"id=\d*\b" %) 3)))]
     (car/wcar conn-opts (mapv #(car/client-unblock % :error) consumer-clients))))
  ([conn-opts stream group]
   (let [consumer-names (->> (group-stats conn-opts stream group)
                             :consumers
                             (map :name))]
     (doseq [consumer-name consumer-names]
       (unblock-consumers! conn-opts consumer-name)))))

(defn clear-pending!
  ([conn-opts stream group]
   (doseq [consumer-name (map :name (:consumers (group-stats conn-opts stream group)))]
     (clear-pending! conn-opts stream group consumer-name)))
  ([conn-opts stream group consumer-name]
   (loop [last-id "-"]
     (let [pending-messages (car/wcar conn-opts (car/xpending stream group last-id "+" 100 consumer-name))]
       (when (seq pending-messages)
         (car/wcar conn-opts
                   (doseq [[message-id] pending-messages]
                     (car/xack stream group message-id)))
         (recur (next-id (first (last pending-messages)))))))))
