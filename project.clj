(defproject tp_clojure "0.1.0-SNAPSHOT"
  :description "HTTP Server"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/data.csv "0.1.2"]
                 [clojure-csv/clojure-csv "2.0.1"]
                 [http-kit "2.2.0"]
                 [clj-time "0.14.0"]
                 [net.mikera/vectorz-clj "0.48.0"]
                 [compojure "1.6.1"]             ; Routing library
                 [http-kit "2.3.0"]              ; HTTP Library for client/server
                 [ring/ring-defaults "0.3.2"]    ; Ring defaults - for query params
                 [org.clojure/data.json "0.2.6"]]; Clojure data.JSON library 
  :main tp-clojure.core)
