# carmine-streams

Utility functions for working with Redis streams in [carmine](https://github.com/ptaoussanis/carmine).

## Usage

### Creating consumer groups and consumers

Consistent naming conventions for streams, groups and consumers:

```
(require '[carmine-streams.core :as cs])
(def conn-opts {})

(def stream (cs/stream-name "sensor-readings"))        ;; -> stream/sensor-readings
(def group (cs/group-name "persist-readings"))         ;; -> group/persist-readings
(def consumer (cs/consumer-name "persist-readings" 0)) ;; -> consumer/persist-readings/0
```

Create a consumer group:

```clj
(cs/create-consumer-group! conn-opts stream group)
```

Create consumers:

```clj
(def opts {:block 5000})

(future
 (cs/start-consumer! conn-opts
                     stream
                     group
                     consumer
                     #(println "Yum yum, tasty message" %)
                     opts))
```

Consumer behaviour is as follows:

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

 - `:block` ms to block waiting for a new message before checking the backlog

Stop consumers:

```clj
;; stop all consumers matching consumer/*
(cs/stop-consumers! conn-opts)

;; stop only consumers matching consumer/persist-readings/*
(cs/stop-consumers! conn-opts (cs/consumer-name "persist-readings"))

;; stop all consumers of group
(cs/stop-consumers! conn-opts stream group)
```

### Visibility

Find all stream keys:

```clj
;; all stream keys matching stream/*
(cs/all-stream-keys conn-opts) ;; -> #{"stream/sensor-readings"}

;; all stream keys matching persist-*
(cs/all-stream-keys conn-opts "persist-*")
```

Find all group names for a stream:

```clj
(cs/group-names conn-opts stream) ;; -> #{"group/persist-readings"}
```

View stats for a consumer group:

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
