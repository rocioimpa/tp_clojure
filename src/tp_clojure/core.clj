(ns tp-clojure.core
  (:require [clojure.java.io :as io]
            [clojure.data.csv :as csv]))

(def test-csv "dataset.csv")

(defn test-read []
  (with-open [reader (io/reader (io/resource test-csv))]
    (doall
     (csv/read-csv reader)))
  )

(defn -main [& args]
  (println (test-read)))






