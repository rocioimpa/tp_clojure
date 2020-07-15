 (ns tp-clojure.core
  (:require [clojure-csv.core :as csv])
  (:require [clojure.java.io :as io]))

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
        (print lines)
      (map (partial zipmap cols) rows)))

(defn read-csv [filepath]
  (let [contents (slurp filepath)]
    (maps #"," contents)))

(defn -main [& args]
  (read-csv "resources/netflix_titles.csv")
)
