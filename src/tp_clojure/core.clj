 (ns tp-clojure.core
  (:require [clojure-csv.core :as csv])
  (:require [clojure.java.io :as io])
  (:require [org.httpkit.server :as server]
    [compojure.core :refer :all]
    [compojure.route :as route]
    [ring.middleware.defaults :refer :all]
    [clojure.pprint :as pp]
    [clojure.string :as str]
    [clojure.data.json :as json])
  (:gen-class))

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
  (try (let [contents (slurp filepath)]
         (parser-tipos (maps #"," contents)))
       (catch Exception e (println (str "Caught Exception: " (.getMessage e))))))

(defn sum-key [data key]
  (reduce + (map (fn [entry] (+  (get entry key))) data))
)

;(defn columns [data]
;  (keys (first data))
;)

(defn calculate-average-key [data key]
  (format "%.2f" (double (/ (sum-key data key) (count data))))
)

(defn count-values [data key]
  (map (fn [x] (list (first x) (count (second x)))) (group-by (fn [movie] (get movie key)) data))
)

;(defn print-col-types [data]
;  (doseq [keyval (first data)] (println (key keyval) "type: "(type (val keyval)) ))
;)

(defn obtener-info-columna-cadena [data key]
  (list key "Valores mas frecuentes:" (take 3 (sort-by #(- (second %)) (count-values data key))))
)

(defn obtener-info-columna-numero [data key]
  (list key "Valor promedio:" (calculate-average-key data key))
)

;Establezco dependencias para el dispatch del multimethod.
(derive java.lang.Double ::numero)
(derive java.lang.Integer ::numero)
(derive java.lang.String  ::cadena)

(defmulti obtener-info-columna (fn [data key] (type (get (first data) key)) ))
(defmethod obtener-info-columna ::cadena [data key] (obtener-info-columna-cadena data key) )
(defmethod obtener-info-columna ::numero [data key] (obtener-info-columna-numero data key ))

(defn obtener-info [data]
  (for [clave (keys (first data))]
    (obtener-info-columna data clave)
    )
)

(defn imprimir-listas [listas]
  ;Imprime prolijamente una lista de listas
  (doseq [lista listas]
    (apply println lista))
  )

(defn imprimir-info-por-grupo [data-agrupado]
  ;Secuencial, sin hilos
  (doseq [grupo data-agrupado]
    (println "GRUPO:" (first grupo))
    (imprimir-listas (obtener-info (second grupo)))
    (println " ")
    )
)

(defn imprimir-info-por-grupo-descoordinado [data-agrupado]
  ;Con hilos descoordinados creados con futures
  (doseq [grupo data-agrupado]
    (println "GRUPO:" (first grupo) )
    (future (imprimir-listas (obtener-info (second grupo))))
    (println " ")
    )
)

(defn actualizar-resultado [resultado-total nombre-grupo resultado-parcial]
  (concat resultado-total (list (list "\nGrupo:" nombre-grupo)) resultado-parcial )
  )

(defn imprimir-info-por-grupo-usando-atomo [data-agrupado]
  ;Con hilos creados con future, sincronizando el resultado mediante un atom.
  (def resultado (atom (list)))
  (doseq [grupo data-agrupado]
    (future (swap! resultado #(actualizar-resultado % (first grupo) (obtener-info (second grupo)))))
    )
  ;Problema: no hay forma de saber cuando todos los hilos terminaron. Por eso luego usamos refs.
  (imprimir-listas @resultado)
)

(defn imprimir-info-por-grupo-usando-refs [data-agrupado]
  ;Con hilos creados con future, comunicandose mediante refs
  (def resultado (ref (list)))
  (def hilos-terminados (ref 0))
  (def cant-grupos (count data-agrupado))
  (doseq [grupo data-agrupado]
    (future
      (dosync
        (alter resultado #(actualizar-resultado % (first grupo) (obtener-info (second grupo))))
        (alter hilos-terminados inc)
        ;Si todos los hilos terminaron, imprimo el resultado.
        (if (= @hilos-terminados cant-grupos) (imprimir-listas @resultado))
      )
    )
  )
)

(defn imprimir-info-por-grupo-usando-agents [data-agrupado]
  (def resultado (ref (list)))
  (def hilos-terminados (agent 0))
  (def cant-grupos (count data-agrupado))
   (doseq [grupo data-agrupado]
     (future
       (dosync
        (alter resultado #(actualizar-resultado % (first grupo) (obtener-info (second grupo))))
        (send-off hilos-terminados + 1))
       (println (imprimir-listas @resultado))
        ;Si todos los hilos terminaron, imprimo el resultado.
        (if (= @hilos-terminados cant-grupos) (imprimir-listas @resultado)))))

(def filepath "resources/movies_1.csv")
(def stored-data (read-csv filepath))
(def names (get stored-data "title"))
(def movies-amount (count stored-data))
(def total-sum (sum-runtime stored-data))
(def average-runtime (calculate-average-key stored-data ":runtime"))
(def average-budget (calculate-average-key stored-data ":budget"))
(def maxVotes (calculate-max-votes stored-data))

(defn default-page [req]
  {:status  200
   :headers {"Content-Type" "text/html"}
   :body    "<h1>Trabajo Práctico Clojure</h1>
            <h2>FIUBA - Teoría del Lenguaje (75.31)</h2>
             <ul>
             <li>Aieta, Noelia</li>
             <li>Ferreiro, Jazmin</li>
             <li>Impaglione, Rocío</li>
             <li>Ortiz, Luciano</li>
             </ul>
   "}) 

(defn show-results [req]
  {:status  200
   :headers {"Content-Type" "text/json"}
   :body (->>
         (str (json/write-str (obtener-info stored-data))))})

(defroutes app-routes
  (GET "/" [] default-page)
  (GET "/results" [] show-results)
  (route/not-found "ERROR 404, no se encontró la página!"))   

(defn -main [& args]
  ;(println stored-data)
  ;(print-col-types stored-data)
  ;(imprimir-info-por-grupo-usando-agents (group-by (fn [entry] (Math/round (get entry ":score"))) stored-data))
  ;(imprimir-listas (obtener-info stored-data))
  ;(imprimir-info-por-grupo-usando-refs (group-by (fn [entry] (Math/round (get entry ":score"))) stored-data))
  ;(println "promedio de runtime:" average-runtime)
  ;(println "count-values genre:" cv-genre )

  (let [port (Integer/parseInt (or (System/getenv "PORT") "3000"))]
    (server/run-server (wrap-defaults #'app-routes site-defaults) {:port port})
    (println (str "Webserver corriendo en http:/127.0.0.1:" port "/")))

  ;(println maxVotes)
  (shutdown-agents)
  )
