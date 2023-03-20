(defproject carmine-streams "0.2.0-SNAPSHOT"
  :description "Carmine helpers for redis streams"
  :url "http://github.com/oliyh/carmine-streams"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[com.taoensso/carmine "3.2.0"]
                 [org.clojure/tools.logging "1.2.4"]]
  :profiles {:provided {:dependencies [[org.clojure/clojure "1.11.1"]]}}
  :repl-options {:init-ns carmine-streams.core})
