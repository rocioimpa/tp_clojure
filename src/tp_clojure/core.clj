(ns tp-clojure.core
  (:require [clojure.java.io :as io]
            [clojure.data.csv :as csv]
            [mikera.vectorz.core :as v]))





;function that take all csv in memory
(defn read-all-dataset [arg]
  (with-open [reader (io/reader (io/resource arg))]
    (doall
     (csv/read-csv reader)))
  )

;function that read data from csv (already readed) and transform in file in dictionary
(defn csv-data->maps [csv-data]
  (map zipmap
       (repeat (map keyword (first csv-data)))
       (rest csv-data))
  )

(def filename "dataset.csv")
(def csvdata (read-all-dataset filename))
(def dataset (csv-data->maps csvdata))

(defn -main [& args]
  (run! println dataset)
  )






