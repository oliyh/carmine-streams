(defproject carmine-streams "0.1.4-SNAPSHOT"
  :description "Carmine helpers for redis streams"
  :url "http://github.com/oliyh/carmine-streams"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [com.taoensso/carmine "2.20.0-RC1"]
                 [org.clojure/tools.logging "1.1.0"]]
  :repl-options {:init-ns carmine-streams.core})
