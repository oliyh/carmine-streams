# carmine-streams

Utility functions for working with [Redis streams](https://redis.io/topics/streams-intro) in Clojure using [carmine](https://github.com/ptaoussanis/carmine).

Redis does a brilliant job of being fast with loads of features and Carmine does a brilliant job of exposing all the low-level Redis commands
in Clojure. Working with Redis' streams API requires quite a lot of interaction to produce desirable high-level behaviour, and that is what this
library provides.

**carmine-streams** allows you to create streams and consumer groups, consume streams reliably, deal with failed consumers and unprocessable messages
and gain visibility on the state of it all with a few simple functions. A single consumer can also process messages from multiple streams in priority order.

[![Clojars Project](https://img.shields.io/clojars/v/carmine-streams.svg)](https://clojars.org/carmine-streams)

## Upgrade notice

:fire: Version `0.2.0` was recently released with breaking API changes. Please read the [Upgrade](UPGRADE.md) guide for more information.

## Usage

- [Consumer groups and consumers](#consumer-groups-and-consumers)
- [Visibility](#visibility)
- [Recovering from failures](#recovering-from-failures)
- [Utilities](#utilities)

### Consumer groups and consumers

#### Naming things

Consistent naming conventions for streams, groups and consumers:

```clj
(require '[carmine-streams.core :as cs])
(def conn-opts {})

(def stream (cs/stream-name "sensor-readings"))        ;; -> stream/sensor-readings
(def group (cs/group-name "persist-readings"))         ;; -> group/persist-readings
(def consumer (cs/consumer-name "persist-readings" 0)) ;; -> consumer/persist-readings/0
```

#### Writing to streams

A convenience function `xadd-map` for writing Clojure maps to streams:

```clj
(car/wcar conn-opts (cs/xadd-map (cs/stream-name "maps") "*" {:foo "bar"}))
```

and parsing them back with `kvs->map`:

```clj
(let [[[_stream messages]] (car/wcar conn-opts (car/xread :count 1 :streams (cs/stream-name "maps") "0-0"))]
  (map (fn [[_id kvs]] (cs/kvs->map kvs))
       messages))

;; [{:foo "bar"}]
```

#### Consumer group creation

Idempotent consumer group creation:

```clj
(cs/create-consumer-group! conn-opts stream group)
```

Or create a consumer group on multiple streams at once:

```clj
(cs/create-consumer-group! conn-opts [stream1 stream2 stream3] group)
```

This function also de-registers idle consumers on the group. The amount of time before a consumer is considered idle can be configured:


```clj
(cs/create-consumer-group! conn-opts stream group "$" {:deregister-idle (* 5 60 1000)})
```

#### Consumer creation

Start an infinite loop that consumes from the group:

```clj
(def opts {:block 5000
           :control-fn cs/default-control-fn
           :claim-opts {:min-idle-time (* 60 1000)
                        :max-deliveries 10
                        :message-rescue-count 100
                        :dlq {:stream (cs/stream-name "dlq")
                              :include-message? true}}})

(future
 (cs/start-multi-consumer! conn-opts
                           stream
                           group
                           consumer
                           #(println "Yum yum, tasty message" %)
                           opts))
```

Consumer behaviour when there is only one stream is as follows:

 - Calls the callback for every message received, with the message
   coerced into a keywordized map, and acks the message.
   If the callback throws an exception the message will not be acked
 - Processes all pending messages on startup before processing new ones
 - Processes new messages until either:
   - The consumer is unblocked (see `unblock-consumers!`)
   - There are no messages delivered during the time it was blocked
     waiting for a new message. If this happens, it will check for
     pending messages and begin processing the backlog if any are
     found, returning to wait for new messages when the backlog is
     cleared.


When checking for pending messages, if it has been sufficiently long
since the last check, it will check for idle messages on the backlog
of other consumers and claim them, or putting messages on the dlq if
they have been retried too many times. This ensures that even if a
consumer dies, its messages will still be processed.

#### Consumers with multiple streams

A consumer can also be passed multiple streams:

```clj
(def opts {:block 5000
           :control-fn cs/default-control-fn})

(future
 (cs/start-multi-consumer! conn-opts
                           [stream1 stream2 stream3]
                           group
                           consumer
                           #(println "Yum yum, tasty message" %)
                           opts))
```

When passed multiple streams, the consumer will behave similarly to
when it is passed a single stream, except it will process messages
from the first stream, then the second stream, then the third, etc. If
a new message arrives on a higher priority stream while it is
receiving messages on a lower priority stream, it will process the
higher priority message as soon as it has finished processing its
current message.

Options to the consumer consist of:
- `:block` ms to block waiting for a new message when there are no
  pending messages on any of the streams
- `:control-fn` a function for controlling the flow of operation, see `default-control-fn`
- `:claim-opts` an options map for configuring how messages are
  claimed from other consumers. See [Recovering from failures](#recovering-from-failures) for available options.

Each stream being processed by a multi-stream consumer will be processed as shown in this flowchart:
![consumer flowchart](/doc/consumer-state-machine.svg)

#### Control flow

The default control flow is as follows:
- Exit on errors reading from Redis (including unblocking)
- Recur on successful message callback
- Recur on failed message callback

You can provide your own `:control-fn` callback to change or add additional behaviour
to the consumer. The `control-fn` may do whatever it pleases
but must return either `:exit` or `:recur`. See `default-control-fn` for an example.

#### Stopping consumers

You should first interrupt the threads that your consumers are running on.
The interrupt will be checked before each read operation and the consumer will exit gracefully.

In addition you should send an unblock message. This will allow the consumer to stop any blocking
read of redis it might currently be performing in order to exit.

Sending an unblock message to blocked consumers can be done like this:

```clj
;; unblock all consumers matching consumer/*
(cs/unblock-consumers! conn-opts)

;; unblock only consumers matching consumer/persist-readings/*
(cs/unblock-consumers! conn-opts (cs/consumer-name "persist-readings"))

;; unblock all consumers of group
(cs/unblock-consumers! conn-opts stream group)
```

### Visibility

#### All stream keys

```clj
;; all stream keys matching stream/*
(cs/all-stream-keys conn-opts) ;; -> #{"stream/sensor-readings"}

;; all stream keys matching persist-*
(cs/all-stream-keys conn-opts "persist-*")
```

#### All group names for a stream

```clj
(cs/group-names conn-opts stream) ;; -> #{"group/persist-readings"}
```

#### Stats for a consumer group

```clj
(cs/group-stats conn-opts stream group)

{:name "group/my-group",
 :consumers ({:name "consumer/my-consumer/0", :pending 1, :idle 102}
             {:name "consumer/my-consumer/1", :pending 0, :idle 208}
             {:name "consumer/my-consumer/2", :pending 0, :idle 311}),
 :pending 1,
 :last-delivered-id "0-2",
 :unconsumed 0}
```

### Recovering from failures

Live consumers are responsible for finding pending messages from dead consumers and claiming them so that they can be processed. This functionality is included in the `start-multi-consumer!` function, which periodically checks for such messages in addition to sending undeliverable messages to a Dead Letter Queue (DLQ).

When a message is not acknowledged by the consumer (i.e. your consumer died halfway through,
or the callback threw an exception) it remains pending and its idle time is how long it has been
since it was first read.

These two possibilities are handled differently:

  - `:min-idle-time` the minimum time (ms) a message has to be idle
    before it can be claimed. Also the minimum amount of time between
    checking for abandoned messages
  - `:max-deliveries` the maximum number of times a message should be
    delivered (attempted to be processed) before it is put in the dlq
  - `:message-rescue-count` the number of message to attempt to claim
    in one go
  - `:dlq` dead letter queue options map. Options are:
    - `:stream` the stream to which poison messages are added
    - `:include-message?` set this to false if you don't want to
      include original message content in the dlq message


- If your consumer died and remains dead
  - The delivery count will remain at 1 and the idle time will increase
  - When the idle time has increased enough that it's obvious the
    consumer can't still be processing it we want another consumer
    that is alive to claim it.
  - The `:min-idle-time` option in the `:claim-opts` map inside the
    `start-multi-consumer!` options is the time necessary for a
    consumer/message to be considered dead before its messages may be
    claimed by another consumer. This option is also used as the
    minimum amount of time between checking for abandoned messages.
- If the message was bad and the worker throws an exception trying to process it
  - It will remain in the backlog which the worker will attempt to
    process during quiet times
  - The appropriate entry in the delivery counts hash-map[^1] will
    increase on each attempt
  - When it reaches a particular value we will decide it cannot be processed and send it to a DLQ for later inspection
  - The `:max-deliveries` key of `:claim-opts` is the number of deliveries required before the message is considered unprocessable or 'poison'.
  - The `:dlq` option of `:claim-opts` specifies
    - The name of the `:stream` to write the message metadata to
    - Whether to `:include-message?` data inside the DLQ message.
- The `:claim-opts` map also specifies the `:message-rescue-count`:
  the number of messages to inspect from other consumers during a
  periodic check.

[^1]: When a consumer reads from multiple streams, redis's inbuilt
    message delivery counts are no longer useful, so a separate redis
    hash is used to store delivery counts for a consumer group. This
    is stored under a key generated using
    `(cs/group-name->delivery-counts-key group)`.

#### Clearing pending messages
If you need to clear pending messages from all consumers, or a particular one, you can use one of these:

```clj
(cs/clear-pending! conn-opts stream group) ;; clears pending messages for all consumers

(cs/clear-pending! conn-opts stream group "consumer-1") ;; clears pending messages for 'consumer-1'
```

You may want to pair this with trimming the stream (caveat: this can result in data loss):
```clj
(car/wcar conn-opts (car/xtrim stream MAXLEN 0))
```

### Utilities

#### Message ids

Get the next smallest message id (useful for iterating through ranges as per `xrange` or `xpending`:

```clj
(cs/next-id "0-1") ;; -> 0-2
```

Get the largest id that is smaller than this one:

```clj
(cs/prev-id "0-2") ;; -> 0-1
```

## Development

Start a normal REPL. You will need redis-server v7.0.0+ running on the default port to run the tests.

[![CircleCI](https://circleci.com/gh/oliyh/carmine-streams.svg?style=svg)](https://circleci.com/gh/oliyh/carmine-streams)

## License

Copyright © 2020 oliyh

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
