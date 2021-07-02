(ns carmine-streams.core-test
  (:require [clojure.test :refer [deftest testing is are use-fixtures]]
            [carmine-streams.core :as cs]
            [taoensso.carmine :as car]))

(def conn-opts {})

(defn- clear-redis! [f]
  (car/wcar conn-opts (car/flushall))
  (try (f)
       (finally (cs/unblock-consumers! conn-opts))))

(use-fixtures :each clear-redis!)

(deftest kvs->map-test
  (is (= {}
         (cs/kvs->map nil)
         (cs/kvs->map [])))

  (is (= {:a 1 :b 2 :c 3}
         (cs/kvs->map ["a" 1 "c" 3 "b" 2]))))

(deftest xadd-map-test
  (let [stream (cs/stream-name "maps")
        ids (car/wcar conn-opts
                      (cs/xadd-map stream "0-1" {:a 1})
                      (cs/xadd-map stream "0-2" {:a 2 :b 3})
                      (cs/xadd-map stream "*" {:a 3})
                      (cs/xadd-map stream :MAXLEN "~" 1000 "*" {:a 4}))]

    (is (= 4 (count ids)))
    (is (= ["0-1" "0-2"] (take 2 ids)))

    (let [[[_stream messages]] (car/wcar conn-opts (car/xread :count 4 :streams stream "0-0"))]
      (is (= [{:a "1"}
              {:a "2" :b "3"}
              {:a "3"}
              {:a "4"}]
             (map (fn [[_id kvs]] (cs/kvs->map kvs))
                  messages))))))

(deftest next-id-test
  (are [from expected] (= expected (cs/next-id from))
    "0-0" "0-1" ;; smallest id redis supports
    "123-234" "123-235"
    "123" "123-1"
    ;; biggest id redis supports
    "18446744073709551615-18446744073709551614" "18446744073709551615-18446744073709551615"))

(deftest all-stream-names-test
  (testing "empty at first"
    (is (= #{} (cs/all-stream-keys conn-opts))))

  (testing "one standard entry"
    (car/wcar conn-opts (car/xadd (cs/stream-name "stream-1") "*" "foo" "bar"))
    (is (= #{"stream/stream-1"} (cs/all-stream-keys conn-opts))))

  (testing "two standard entries"
    (car/wcar conn-opts (car/xadd (cs/stream-name "stream-2") "*" "foo" "bar"))
    (is (= #{"stream/stream-1" "stream/stream-2"} (cs/all-stream-keys conn-opts))))

  (testing "non-standard entry"
    (car/wcar conn-opts (car/xadd "explicit-stream-key" "*" "foo" "bar"))
    (is (= #{"stream/stream-1" "stream/stream-2"} (cs/all-stream-keys conn-opts)))
    (is (= #{"stream/stream-1" "stream/stream-2" "explicit-stream-key"}
           (cs/all-stream-keys conn-opts "*stream*")))
    (is (= #{"explicit-stream-key"}
           (cs/all-stream-keys conn-opts "explicit-*")))))

(deftest group-names-test
  (let [stream (cs/stream-name "my-stream")]
    (testing "exception bubbles if stream doesn't exist"
      (is (thrown? Exception (cs/group-names conn-opts "non-existent-stream"))))

    (testing "shows all group names"
      (cs/create-consumer-group! conn-opts stream (cs/group-name "foo"))
      (is (= #{"group/foo"} (cs/group-names conn-opts stream)))

      (cs/create-consumer-group! conn-opts stream (cs/group-name "bar"))
      (is (= #{"group/foo" "group/bar"} (cs/group-names conn-opts stream))))))

(deftest create-idempotency-test
  (dotimes [_ 3]
    (is (cs/create-consumer-group! conn-opts "foo" "bar"))))

(deftest create-start-and-shutdown-test
  (let [stream (cs/stream-name "my-stream")
        group (cs/group-name "my-group")
        consumer-prefix "my-consumer"
        consumed-messages (atom #{})
        callback (fn [v]
                   (let [data (update v :temperature read-string)]
                     (if (neg? (:temperature data))
                       (throw (Exception. "Too cold!"))
                       (swap! consumed-messages conj data))))]

    (testing "can create stream and consumer group"
      (is (cs/create-consumer-group! conn-opts stream group))

      (is (= {:name group
              :consumers []
              :pending 0
              :last-delivered-id "0-0"
              :unconsumed 0}
             (cs/group-stats conn-opts stream group))))

    (testing "can create consumers"
      (let [consumers (mapv #(future (cs/start-consumer! conn-opts
                                                         stream
                                                         group
                                                         (cs/consumer-name consumer-prefix %)
                                                         callback))
                            (range 3))]
        (Thread/sleep 100) ;; wait for futures to start

        (testing "stats show the consumers"
          (let [group-stats (cs/group-stats conn-opts stream group)]
            (is (= {:name group
                    :pending 0
                    :last-delivered-id "0-0"
                    :unconsumed 0}
                   (dissoc group-stats :consumers)))

            (is (= [{:name "consumer/my-consumer/0" :pending 0}
                    {:name "consumer/my-consumer/1" :pending 0}
                    {:name "consumer/my-consumer/2" :pending 0}]
                   (map #(dissoc % :idle) (:consumers group-stats))))))

        (testing "can write to stream and messages are consumed"
          (car/wcar conn-opts (car/xadd stream "0-1" :temperature 19.7))
          (Thread/sleep 100)

          (is (= #{{:temperature 19.7}}
                 @consumed-messages)))

        (testing "exceptions in callback leaves message pending"
          (car/wcar conn-opts (car/xadd stream "0-2" :temperature -14.1))
          (Thread/sleep 100)

          (let [group-stats (cs/group-stats conn-opts stream group)]
            (is (= {:name group
                    :pending 1
                    :last-delivered-id "0-2"
                    :unconsumed 0}
                   (dissoc group-stats :consumers)))))

        (testing "can unblock consumers"
          (cs/unblock-consumers! conn-opts (cs/consumer-name consumer-prefix))
          (is (every? #(cs/unblocked? (deref % 100 ::timed-out)) consumers)))))))

(deftest unblock-consumers-test
  (let [stream (cs/stream-name "my-stream")
        group (cs/group-name "my-group")]
    (cs/create-consumer-group! conn-opts stream group)

    (testing "can unblock explicit consumer"
      (let [consumer (future (cs/start-consumer! conn-opts stream group (cs/consumer-name "consumer" 0) identity))
            another-consumer (future (cs/start-consumer! conn-opts stream group (cs/consumer-name "consumer" 1) identity))]
        (Thread/sleep 100)
        (cs/unblock-consumers! conn-opts (cs/consumer-name "consumer" 0))
        (is (cs/unblocked? (deref consumer 100 ::timed-out)))
        (is (= ::timed-out (deref another-consumer 100 ::timed-out)))

        (testing "can unblock consumers for a stream/group"
          (cs/unblock-consumers! conn-opts stream group)
          (is (cs/unblocked? (deref another-consumer 100 ::timed-out)))))))

  (testing "can unblock all consumers"
    (let [consumers (reduce (fn [acc k]
                              (let [stream (cs/stream-name k)
                                    group (cs/group-name k)]
                                (cs/create-consumer-group! conn-opts stream group)
                                (conj acc (future (cs/start-consumer! conn-opts stream group (cs/consumer-name k 0) identity)))))
                            []
                            ["foo" "bar" "baz"])]
      (Thread/sleep 100)

      (is (pos? (count consumers)))
      (cs/unblock-consumers! conn-opts)
      (is (every? #(cs/unblocked? (deref % 100 ::timed-out)) consumers)))))

(deftest stop-consumers-test
  (testing "an example of how to stop a consumer in another thread"
    (let [stream (cs/stream-name "stop-consumers")
          group (cs/group-name "stop-consumers")
          consumer-name (cs/consumer-name "stop-consumers" 0)
          waiting? (promise)
          continue? (promise)
          finished? (promise)
          callback (fn [_v]
                     (deliver waiting? true)
                     @continue?)]
      (cs/create-consumer-group! conn-opts stream group)

      (let [consumer (Thread. (fn []
                                (cs/start-consumer! conn-opts stream group consumer-name callback)
                                (deliver finished? true)))]
        (.start consumer)
        (car/wcar conn-opts (cs/xadd-map stream "*" {:foo "bar"}))

        (is (true? (deref waiting? 1000 ::timed-out)))
        (testing "sending unblock when not blocking has no effect"
          ;; consumer is now processing the message, NOT blocking on xreadgroup
          ;; unblocking should have no effect
          (cs/unblock-consumers! conn-opts consumer-name)
          (deliver continue? true)
          (testing "consumer is still running (blocking on xreadgroup)"
            (is (= ::timed-out (deref finished? 100 ::timed-out)))))

        (testing "interrupting thread and then unblocking will stop consumer"
          (.interrupt consumer) ;; set the interrupt flag
          (cs/unblock-consumers! conn-opts consumer-name) ;; unblock to allow it to check the interrupt flag
          (is (true? (deref finished? 100 ::timed-out))))))))

(deftest pending-processing-test
  (let [stream (cs/stream-name "my-stream")
        group (cs/group-name "my-group")
        consumer-prefix "my-consumer"
        succeed? (atom false)
        processed-messages (atom #{})
        failed? (promise)
        succeeded? (promise)
        callback (fn [v]
                   (when-not @succeed?
                     (deliver failed? true)
                     (throw (Exception. "Failing on purpose")))

                   (swap! processed-messages conj v)
                   (deliver succeeded? true))]

    (is (cs/create-consumer-group! conn-opts stream group))

    (let [consumer (future (cs/start-consumer! conn-opts
                                               stream
                                               group
                                               (cs/consumer-name consumer-prefix 1)
                                               callback
                                               {:block 100}))]

      (testing "wait for first message to fail"
        (car/wcar conn-opts (cs/xadd-map stream "0-1" {:foo "bar"}))
        (is (true? (deref failed? 500 ::timed-out)))

        (testing "message is now pending"
          (is (= {:name group
                  :pending 1
                  :last-delivered-id "0-1"
                  :unconsumed 0}
                 (dissoc (cs/group-stats conn-opts stream group) :consumers))))

        (testing "will check backlog and process"
          (reset! succeed? true)
          (is (true? (deref succeeded? 500 ::timed-out)))
          (is (= #{{:foo "bar"}} @processed-messages))
          (is (= {:name group
                  :pending 0
                  :last-delivered-id "0-1"
                  :unconsumed 0}
                 (dissoc (cs/group-stats conn-opts stream group) :consumers))))))))

(deftest gc-consumer-group-test

  (testing "bad messages get moved to the dlq"
    (let [stream (cs/stream-name "bad-messages")
          group (cs/group-name "bad-messages")
          consumer (cs/consumer-name "bad-messages" 0)
          failed? (promise)
          callback (fn [v]
                     (deliver failed? true)
                     (throw (Exception. "Bad message")))]

      (is (cs/create-consumer-group! conn-opts stream group))

      (future (cs/start-consumer! conn-opts stream group consumer callback))

      (car/wcar conn-opts (car/xadd stream "0-1" :foo "bar"))
      (is (true? (deref failed? 500 ::timed-out)))

      (testing "message is now pending"
        (is (= {:name group
                :pending 1
                :last-delivered-id "0-1"
                :unconsumed 0}
               (dissoc (cs/group-stats conn-opts stream group) :consumers))))

      (testing "a gc moves it to the dlq"
        (let [dlq (cs/stream-name "dlq")]
          (is (= [{:action :dlq, :id "0-1", :consumer consumer}]
                 (cs/gc-consumer-group! conn-opts stream group {:dlq {:deliveries 1
                                                                      :stream dlq}})))

          (is (= {:name group
                  :pending 0
                  :last-delivered-id "0-1"
                  :unconsumed 0}
                 (dissoc (cs/group-stats conn-opts stream group) :consumers)))

          (let [[message] (car/wcar conn-opts (car/xread :count 1 :streams dlq "0-0"))
                [_stream-name [[_message-id kvs]]] message]
            (is (= {:stream stream
                    :group group
                    :consumer consumer
                    :id "0-1"}
                   (dissoc (cs/kvs->map kvs) :idle :deliveries))))))))

  (testing "dead consumer messages are rebalanced to other consumers"
    (let [stream (cs/stream-name "dead-consumers")
          group (cs/group-name "dead-consumers")
          alive-consumer (cs/consumer-name "dead-consumers" "alive")
          dead-consumer (cs/consumer-name "dead-consumers" "dead")
          processed-messages (atom #{})
          failed-messages (atom #{})
          failed? (promise)
          succeeded? (promise)]

      (is (cs/create-consumer-group! conn-opts stream group))

      (future (cs/start-consumer! conn-opts stream group dead-consumer
                                  (fn [v]
                                    (when (= 10 (count (swap! failed-messages conj v)))
                                      (deliver failed? true))
                                    (throw (Exception. "I'm going to die")))))

      (dotimes [i 10]
        (car/wcar conn-opts (car/xadd stream "*" :counter i)))

      (is (true? (deref failed? 500 ::timed-out)))
      (cs/unblock-consumers! conn-opts dead-consumer)

      (future (cs/start-consumer! conn-opts stream group alive-consumer
                                  (fn [v]
                                    (when (= 10 (count (swap! processed-messages conj v)))
                                      (deliver succeeded? true)))
                                  {:block 100}))

      (Thread/sleep 100)

      (testing "all messages are now pending for dead consumer"
        (let [consumers-pending (->> (cs/group-stats conn-opts stream group)
                                     :consumers
                                     (reduce (fn [acc {:keys [name pending]}]
                                               (assoc acc name pending))
                                             {}))]

          (is (= {dead-consumer 10
                  alive-consumer 0}
                 consumers-pending))))

      (testing "a gc is a no-op when the criteria aren't met"
        (is (every?
             #(= :noop (:action %))
             (cs/gc-consumer-group! conn-opts stream group {:rebalance {:idle 99999999
                                                                        :siblings :active
                                                                        :distribution :random}})))

        (let [consumers-pending (->> (cs/group-stats conn-opts stream group)
                                     :consumers
                                     (reduce (fn [acc {:keys [name pending]}]
                                               (assoc acc name pending))
                                             {}))]

          (is (= {dead-consumer 10
                  alive-consumer 0}
                 consumers-pending))))

      (testing "a gc moves it to another consumer"
        (is (every?
             (fn [{:keys [action consumer claimant]}]
               (and (= :rebalance action)
                    (= dead-consumer consumer)
                    (= alive-consumer claimant)))
             (cs/gc-consumer-group! conn-opts stream group {:rebalance {:idle 0
                                                                        :siblings :active
                                                                        :distribution :random}})))

        (is (true? (deref succeeded? 500 ::timed-out)))

        (let [consumers-pending (->> (cs/group-stats conn-opts stream group)
                                     :consumers
                                     (reduce (fn [acc {:keys [name pending]}]
                                               (assoc acc name pending))
                                             {}))]

          (is (= {dead-consumer 0
                  alive-consumer 0}
                 consumers-pending)))

        (is (= 10 (count @processed-messages)))))))

(deftest default-control-fn-test
  (are [expected phase value] (= expected (cs/default-control-fn phase {} value))
    :recur :callback {}
    :recur :callback false
    :recur :callback nil
    :recur :callback (Exception. "Couldn't handle message")

    :exit :read (Exception. "Blew up reading from Redis")
    :exit :read (ex-info "Unblocking" {:prefix :unblocked})))

(deftest control-fn-test
  (testing "can control from callback"
    (let [stream (cs/stream-name "callback")
          group (cs/group-name "callback")
          consumer-name (cs/consumer-name group 1)
          callback (constantly :return-foo)
          control (fn [phase context value id kvs]
                    (is (= :callback phase))
                    (is (= {:stream stream
                            :group group
                            :consumer consumer-name}
                           context))
                    (is (= :return-foo value))
                    (is (contains? #{"0-1" "0-2"} id))
                    (is (= ["foo" "bar"] kvs))
                    (if (= "0-1" id)
                      :recur
                      :exit))]

      (cs/create-consumer-group! conn-opts stream group)

      (let [consumer (future (cs/start-consumer! conn-opts
                                                 stream
                                                 group
                                                 consumer-name
                                                 callback
                                                 {:control-fn control}))]
        (Thread/sleep 100) ;; wait for futures to start

        (testing "can recur"
          (car/wcar conn-opts (cs/xadd-map stream "0-1" {:foo "bar"}))
          (is (= ::still-running (deref consumer 500 ::still-running))))

        (testing "can exit"
          (car/wcar conn-opts (cs/xadd-map stream "0-2" {:foo "bar"}))
          (is (= :return-foo (deref consumer 500 ::still-running)))))))

  (testing "can control from exception"
    (let [stream (cs/stream-name "no-group")
          group (cs/group-name "no-group")
          consumer-name (cs/consumer-name group 1)
          callback (constantly :return-foo)
          exception-count (atom 0)
          control (fn [phase context value]
                    (is (= :read phase))
                    (is (= {:stream stream
                            :group group
                            :consumer consumer-name}
                           context))
                    (is (= :nogroup
                           (:prefix (ex-data value))))
                    (if (= 1 (swap! exception-count inc))
                      :recur
                      :exit))]

      (let [consumer (future (cs/start-consumer! conn-opts
                                                 stream
                                                 group
                                                 consumer-name
                                                 callback
                                                 {:control-fn control}))]

        (is (= :nogroup (:prefix (ex-data (deref consumer 1000 ::still-running)))))
        (is (= 2 @exception-count)))))

  (testing "exits if control-fn returns unknown value"
    (let [stream (cs/stream-name "bad-control")
          group (cs/group-name "bad-control")
          consumer-name (cs/consumer-name group 1)
          callback (constantly :return-foo)
          control (constantly :bad-control)]

      (let [consumer (future (cs/start-consumer! conn-opts
                                                 stream
                                                 group
                                                 consumer-name
                                                 callback
                                                 {:control-fn control}))]

        (is (thrown? Exception
                     (deref consumer 1000 ::still-running)))))))

(deftest nil-values-test
  (let [stream (cs/stream-name "my-stream")
        group (cs/group-name "my-group")
        consumer-name (cs/consumer-name "my-consumer")
        finished? (promise)
        values (atom #{})
        callback (fn [v]
                   (when (= 4 (count (swap! values conj v)))
                     (deliver finished? true)))]

    (car/wcar conn-opts (cs/xadd-map stream :MAXLEN 3 "0-1" {:n "1"}))
    (car/wcar conn-opts (cs/xadd-map stream :MAXLEN 3 "0-2" {:n "2"}))
    (car/wcar conn-opts (cs/xadd-map stream :MAXLEN 3 "0-3" {:n "3"}))

    (cs/create-consumer-group! conn-opts stream group "0")

    ;; read, but don't ack, the first message from the stream
    (car/wcar conn-opts (car/xreadgroup :group group consumer-name :count 1 :streams stream ">"))
    ;; add another message to the stream, the first one is trimmed from the stream when the 4th one is added
    (car/wcar conn-opts (cs/xadd-map stream :MAXLEN 3 "0-4" {:n "4"}))

    ;; the consumer's personal pending items (read but not acked messages) will now contain a message which
    ;; has since been deleted from the stream, it should be able to deal with that
    (let [consumer (future (cs/start-consumer! conn-opts
                                               stream
                                               group
                                               consumer-name
                                               callback))]

      (is (true? (deref finished? 1000 ::timeout)))
      (is (= #{{} {:n "2"} {:n "3"} {:n "4"}}
             @values))

      (future-cancel consumer)
      (cs/unblock-consumers! conn-opts consumer-name))))
