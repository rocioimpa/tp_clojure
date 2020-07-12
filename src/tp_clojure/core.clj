(ns tp-clojure.core
  (:require [clojure.java.io :as io]
            [clojure.data.csv :as csv]
            [mikera.vectorz.core :as v]))



;function that open csv to read one column into vector
(defn read-column [filename column-index]
  (with-open [reader (io/reader (io/resource filename))]
    (let [data (csv/read-csv reader)]
      ;mapv is not lazy, to avoid csv not open exception
      (mapv #(nth % column-index) data))))

;function that take all csv data in memory
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
(def names (read-column filename 2))
(def durations (read-column filename 9))
(def csvdata (read-all-dataset filename))
(def dataset (csv-data->maps csvdata))

;function to parse int
(defn parse-int [s]
  (Integer. (re-find  #"\d+" s )))

;function that calculate the average
(defn average [numbers]
  (/ (reduce + numbers) (count numbers)))

(def movies-duration-average
  (average (map #( parse-int (:duration %)) (filter #(= (:type %) "Movie") dataset))))


(defn -main [& args]
  (println names)
  (println durations)
  ;(run! println dataset)
  (println (format "La duracion promedio de las peliculas es de: %.3f minutos" (double movies-duration-average)))
  )






