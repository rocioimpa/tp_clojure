(ns tp-clojure.core
  (:gen-class)
  (:require [clojure-csv.core :as csvcore]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io]))

;(defn app [req]
; {:status  200
; :headers {"Content-Type" "text/html"}
;:body    (str (t/time-now))})

(defn process-csv [file]
  (with-open [in-file (clojure.java.io/reader file)]
    (doall
     (clojure.data.csv/read-csv in-file))))


(defn take-csv
  [file]
  (with-open  [rdr  (io/reader file)]
    (csvcore/parse-csv rdr)))

(defn -main
  "I don't do a whole lot ... yet."
  [&]
  (println "Hello, World!")
  (println (process-csv "./data/survey_results_public.csv"))
;(run-server app {:port 8080})
  (println "Server started at port 8080"))