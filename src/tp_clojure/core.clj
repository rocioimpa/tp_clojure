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
    (reduce + (map (fn [entry] (+ (get entry ":runtime"))) mapa)))

(defn calculate-max-votes [mapa]
 (reduce max (map (fn [entry] (+ (get entry ":votes"))) mapa)))

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

(defn parser-tipos [data]
  (map (fn [fila]
          (-> fila (update ":budget" #(.intValue (Double. %)))
                   (update ":gross" #(.intValue (Double. %)))
                   (update ":runtime" #(Integer. %))
                   (update ":score" #(Double. %))
                   (update ":votes" #(Integer. %))
                   (update ":year" #(Integer. %))
          )
        ) data)
  )

(defn read-csv [filepath]
  (let [contents (slurp filepath)]
    (parser-tipos (maps #"," contents)) ))

(defn sum-key [data key]
  (reduce + (map (fn [entry] (+  (get entry key))) data))
)

(defn columns [data]
  (keys (first data))
)

(defn calculate-average-key [data key]
  (.intValue (double (/ (sum-key data key) (count data))))
)

(defn count-values [data key]
  (map (fn [x] (list (first x) (count (second x)))) (group-by (fn [movie] (get movie key)) data))
)

(defn print-col-types [data]
  (doseq [keyval (first data)] (println (key keyval) "type: "(type (val keyval)) ))
)

(defn obtener-info-columna-cadena [data key]
  (println key "Valores mas frecuentes:" (take 3 (sort-by #(- (second %)) (count-values data key))))
)

(defn obtener-info-columna-numero [data key]
  (println key "Valor promedio:" (calculate-average-key data key))
)

;Establezco dependencias para el dispatch del multimethod.
(derive java.lang.Double ::numero)
(derive java.lang.Integer ::numero)
(derive java.lang.String  ::cadena)

(defmulti obtener-info-columna (fn [data key] (type (get (first data) key)) ))
(defmethod obtener-info-columna ::cadena [data key] (obtener-info-columna-cadena data key) )
(defmethod obtener-info-columna ::numero [data key] (obtener-info-columna-numero data key ))

(defn obtener-info [data]
  (doseq [keyval (first data)]
    (obtener-info-columna data (key keyval))
    )
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
  ;(print-col-types stored-data)
  (obtener-info stored-data)
  ;(println "promedio de runtime:" average-runtime)
  ;(println "count-values genre:" cv-genre )

  ;(println maxVotes)
  (shutdown-agents)
)
