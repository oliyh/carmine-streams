(ns carmine-streams.core-test
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [carmine-streams.core :as cs]
            [taoensso.carmine :as car]
            [clojure.string :as string]))

(def conn-opts {})

(defn- clear-redis! [f]
  (car/wcar conn-opts (car/flushall))
  (try (f)
       (finally (cs/stop-consumers! conn-opts))))

(use-fixtures :each clear-redis!)

(deftest kvs->map-test
  (is (= {}
         (cs/kvs->map nil)
         (cs/kvs->map [])))

  (is (= {:a 1 :b 2 :c 3}
         (cs/kvs->map ["a" 1 "c" 3 "b" 2]))))

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
        consumer-name-prefix "my-consumer"
        consumed-messages (atom #{})]

    (testing "can create stream and consumer group"
      (is (cs/create-consumer-group! conn-opts stream group))

      (is (= {:name group
              :consumers []
              :pending 0
              :last-delivered-id "0-0"
              :unconsumed 0}
             (cs/group-stats conn-opts stream group))))

    (testing "can create consumers"
      (let [consumers (mapv #(future (cs/start-consumer! conn-opts stream group
                                                         (fn [v]
                                                           (let [data (update v :temperature read-string)]
                                                             (if (neg? (:temperature data))
                                                               (throw (Exception. "Too cold!"))
                                                               (swap! consumed-messages conj data))))
                                                         (cs/consumer-name consumer-name-prefix %)))
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

        (testing "can stop consumers"
          (cs/stop-consumers! conn-opts (cs/consumer-name consumer-name-prefix))
          (is (every? #(nil? (deref % 100 ::timed-out)) consumers)))))))

(deftest stop-consumers-test
  (testing "can stop explicit consumer")

  (testing "can stop consumers for a stream/group")

  (testing "can stop all consumers"))

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

    (let [consumer (future (cs/start-consumer! conn-opts stream group callback
                                               (cs/consumer-name consumer-prefix 1)
                                               {:block 100}))]

      (testing "wait for first message to fail"
        (car/wcar conn-opts (car/xadd stream "0-1" :foo "bar"))
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
