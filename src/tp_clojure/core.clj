 (ns tp-clojure.core
  (:require [clojure-csv.core :as csv])
  (:require [clojure.java.io :as io]))

  (defn calculate-average-runtime [total-sum, amount]
    (double (/ total-sum amount)))

  (defn filterGenre [map]
  (filter (fn [entry] (= (get entry ":genre") "Adventure")) map))

  (defn parse-int [s]
    (Integer. (re-find  #"\d+" s )))

  (defn sum-runtime [mapa]
    (reduce + (map (fn [entry] (+ (parse-int (get entry ":runtime")))) mapa)))

(defn calculate-max-votes [mapa]
 (reduce max (map (fn [entry] (+ (parse-int (get entry ":votes")))) mapa)))

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

(defn sum-key [data key]
  (reduce + (map (fn [entry] (+ (parse-int (get entry key)))) data))
)

(defn calculate-average-key [data key]
  (double (/ (sum-key data key) (count data)))
)

(defn count-values [data key]
  (map (fn [x] (list (first x) (count (second x)))) (group-by (fn [movie] (get movie key)) data))
)



(def filepath "resources/movies_1.csv")
(def stored-data (read-csv filepath))
(def names (get stored-data "title"))
(def resultado (filterGenre stored-data))
(def movies-amount (count stored-data))
(def total-sum (sum-runtime stored-data))
(def average-runtime (calculate-average-key stored-data ":runtime"))
(def average-budget (calculate-average-key stored-data ":budget"))
(def cv-genre (count-values stored-data ":genre"))
(def cv-company (count-values stored-data ":company"))
(def maxVotes (calculate-max-votes stored-data))

(defn -main [& args]
  ;(println stored-data)
  (println "promedio de runtime:" average-runtime)
  (println "promedio de budget:" average-budget)
  (println "count-values genre:" (count-values stored-data ":genre") )
  (println "count-values company:" (count-values stored-data ":company") )

  ;(println maxVotes)
  (shutdown-agents)
)
