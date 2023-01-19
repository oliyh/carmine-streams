# Upgrade

## From 0.1.x to 0.2.0

### Rationale

A useful feature offered by other messaging protocols is the ability to prioritise messages such that high
priority messages are consumed before low priority ones. On a Redis stream fronted by consumer groups this is not
possible, but tantalisingly Redis' `XREADGROUP` command offers the ability to read from multiple streams at once.

Using this in the same way as consuming from a single stream does not result in this priority behaviour, however -
messages are taken from all streams if available on each read, meaning you would process some lower priority messages
even when there were higher priority messages waiting.

The implementation had to change significantly to accommodate this and thus the responsibilities of the consumer are
now different - whereas before they simply consumed and processed messages, periodically checking their backlog,
they are now also responsible for claiming messages from other consumers that have stopped.
The function to start a consumer has now been renamed `start-multi-consumer` to reflect its abilities.

This also means that the `gc-consumer-group!` function is no longer required for claiming messages.
The remaining behaviour of that function, which was to clean up old consumers, has now been moved into `create-consumer-group!`.

### API changes

Replace calls to `start-consumer!` with `start-multi-consumer!`:

```clj
(future
 (cs/start-consumer! conn-opts
                     stream
                     group
                     consumer
                     #(println "Yum yum, tasty message" %)
                     opts))
```

Becomes:

```clj
(future
 (cs/start-multi-consumer! conn-opts
                           [stream]
                           group
                           consumer
                           #(println "Yum yum, tasty message" %)
                           opts))
```

You should also consider adding `:claim-opts` to your `opts` map to be explicit about when
consumers claim messages from other consumers. If you previously made calls to `gc-consumer-group!` like this:

```clj
(cs/gc-consumer-group! conn-opts stream group {:rebalance {:idle 60000
                                                           :siblings :active
                                                           :distribution :random}
                                               :dlq {:deliveries 5
                                                     :stream "dlq"})
```

You should now start your multi-consumer with these `:claim-opts`:

```clj
(def opts {:block 5000
           :control-fn cs/default-control-fn
           :claim-opts {:min-idle-time 60000
                        :max-deliveries 5
                        :message-rescue-count 100
                        :dlq {:stream (cs/stream-name "dlq")
                              :include-message? true}}})
```

Finally, if you called `gc-consumer-group!` with options to deregister old consumers:

```clj
(cs/gc-consumer-group! conn-opts stream group {:deregister {:idle 120000})
```

You should now pass the following options to `create-consumer-group!`:

```clj
(cs/create-consumer-group! conn-opts stream group "$" {:deregister-idle 120000})
```

Please see the documentation for those functions for all available options.
