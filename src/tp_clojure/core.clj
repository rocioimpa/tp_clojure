(ns tp-clojure.core
  (:import (java.io RandomAccessFile))
  (:import (java.io BufferedReader))
  (:import (java.io FileInputStream))
  (:import (java.io BufferedInputStream))
  (:import (java.io InputStreamReader))
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

;function to parse int
(defn parse-int [s]
  (Integer. (re-find  #"\d+" s )))

;function average
(defn average [total_sum total_count] (/ total_sum total_count))

;function sum
(defn sum-all [numbers] (reduce + numbers))

;function count
(defn count-all [numbers] (count numbers))


(defn get-durations [dataset]
  (map #( parse-int (:duration %)) (filter #(= (:type %) "Movie") dataset)))


(def filename "dataset.csv")
(def names (read-column filename 2))
(def csvdata (read-all-dataset filename))
(def dataset (csv-data->maps csvdata))

;function that print movies duration average
(defn print-movies-duration-avarage []
  (print "Columna de nombres =")(println names)
  ;(print "El dataset como diccionario =")(run! println dataset)
  (def durations (get-durations dataset) )
  (def total-count (count-all durations))
  (def total-sum (sum-all durations))
  (println (format "La duracion promedio de las peliculas es de: %.3f minutos"
                   (double
                    (average total-sum total-count)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


;function sum
(defn sum-all-future [numbers]
  (future (reduce + numbers)))

;function count
(defn count-all-future [numbers]
  (future (count numbers)))


(defn use-futures []
  (def vec-one (vector 5 10 6))
  (def vec-two (vector 4 3 7))
  (def future-count (count-all-future vec-one))
  (def future-sum (sum-all-future vec-one))
  (println (format "El promedio usando futures es: %.3f "
                   (double
                    (average  (deref future-sum)  (deref future-count))))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;Partitions a file into n line-aligned chunks.
;Returns a list of start and end byte offset pairs. ((0 124) (124 200))
(defn chunk-file  [filename n]
  (with-open [file (RandomAccessFile. filename "r")]
    (let [offsets (for [offset (range 0 (.length file) (/ (.length file) n))]
                    (do (when-not (zero? offset)
                          (.seek file offset)
                          (while (not= (.read file) (int \newline))))
                      (.getFilePointer file)))
          offsets (concat offsets [(.length file)])]
      (doall (partition 2 (interleave offsets (rest offsets)))))))

(def csv-path
  (apply str [(System/getProperty "user.dir") "/resources/dataset.csv"]))


; Returns a lazy sequence of lines from file between start-byte and end-byte.
(defn read-lines-range [file start-byte end-byte]

  (let [reader (-> (doto (FileInputStream. file)
                         (.skip start-byte))
                   (BufferedInputStream. 131072)
                   (InputStreamReader. "US-ASCII")
                   (BufferedReader.))]
    (letfn [(gobble-lines [remaining]
                          (lazy-seq
                            (if-let [line (and (pos? remaining) (.readLine reader))]
                              (cons line (gobble-lines (- remaining (.length line))))
                              (.close reader))))]
      (gobble-lines (- end-byte start-byte)))))

(def number-of-file-partitions 2)
(def first-file-part 0)
(def second-file-part 1)
(defn read-file-chunks []
  (println "")
  (println "Dividimos el archivo en dos partes con la misma cantidad de lineas:")
  (def file-bytes-division (chunk-file csv-path number-of-file-partitions))
  (print "La primer parte ")
  (println  (let [[start end](nth file-bytes-division first-file-part)]
              (println (format "empieza en %s y termina en los %s bytes....." start end))
              (read-lines-range  (io/file csv-path) start end)))
  (print "La segunda parte ")
  (println  (let [[start end](nth file-bytes-division second-file-part)]
              (println (format "empieza en %s y termina en los %s bytes....." start end))
              (read-lines-range  (io/file csv-path) start end)))
  )


(defn -main [& args]
  (print-movies-duration-avarage)
  (use-futures)
  (read-file-chunks)
  (shutdown-agents))






