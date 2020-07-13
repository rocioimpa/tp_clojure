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


(def filename "dataset.csv")
(def names (read-column filename 2))
(def durations (read-column filename 9))
(def csvdata (read-all-dataset filename))
(def dataset (csv-data->maps csvdata))

;function to parse int
(defn parse-int [s]
  (Integer. (re-find  #"\d+" s )))

;function average
(defn average [numbers]
  (/ (reduce + numbers) (count numbers)))

(def movies-duration-average
  (average (map #( parse-int (:duration %)) (filter #(= (:type %) "Movie") dataset))))

;function that print movies duration average
(defn print-movies-duration-avarage []
  (print "Columna de duraciones =")(println durations)
  ;(print "El dataset como diccionario =")(run! println dataset)
  (println (format "La duracion promedio de las peliculas es de: %.3f minutos" (double movies-duration-average)))
  )


;Partitions a file into n line-aligned chunks.
;Returns a list of start and end byte offset pairs.
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
  (read-file-chunks)
  )






