 (ns tp-clojure.core
  (:require [clojure-csv.core :as csv])
  (:require [clojure.java.io :as io]))

  

  (def movie-durations ())
  ;(def average-duration (promise))

  (defn split [sep s]
    (clojure.string/split s sep))

  (defn lines [sep contents]
    (->> contents
      (split #"\n")
        (map (partial split sep))))
  
  (defn maps [sep contents]
    (let [lines (lines sep contents)
          cols (first lines)
          rows (rest lines)]
        ;(print lines) ;uncomment this to print the lines
      (map (partial zipmap cols) rows)))

(defn read-csv [filepath]
  (let [contents (slurp filepath)]
    (maps #"," contents)))

(def filepath "resources/dataset.csv")    
(def stored-data (read-csv filepath))  
(def names (get stored-data "title"))

(defn -main [& args]
  (println stored-data)
;  (println (deref average-duration))
)
