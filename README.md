# carmine-streams

Utility functions for working with Redis streams in [carmine](https://github.com/ptaoussanis/carmine).

## Usage

Create a consumer group:

```clj
(require '[carmine-streams.core :as cs])

(def conn-opts {})

(cs/create-consumer-group! conn-opts "my-stream" "my-group")
```

Create consumers:

```clj
(future
 (cs/start-consumer! conn-opts "my-stream" "my-group"
                     #(println "Yum yum, tasty message" %)
                     "my-consumer-0"))
```

Stop consumers:

```clj
(cs/stop-consumers! conn-opts "my-stream" "my-consumer-*")
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
