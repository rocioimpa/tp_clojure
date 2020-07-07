(ns tp-clojure.core
  (:require [org.httpkit.server :refer [run-server]]
            [clj-time.core :as t]))

(defn app [req]
  {:status  200
   :headers {"Content-Type" "text/html"}
   :body    (str (t/time-now))})

(defn -main 
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!")
  (run-server app {:port 8080})
  (println "Server started at port 8080"))
