(ns servel.core
  (:require [org.httpkit.client :as http]
            [clojure.data.json :as json]
            [clojure.pprint :refer [pprint]]
            [clojure.java.io :refer [writer]]
            [chileno.dv :as dv])
  (:gen-class :main true))

(def keys-table
  {"NOMBRE" :name
   "LOCVOTACION" :vote_venue
   "CIRNOMBRE" :cirnombre
   "COLEGIO" :colegio
   "NMESA" :table
   "NCOMUNA" :county
   "HABILITADO" :enabled
   "DIRLOCVOTACION"  :vote_venue_address
   "NPROVINCIA" :province
   "NOREGION" :region
   "VOCAL" :vocal
   "RUT" :rut
   "DV" :dv
   "errCode" :err-code
   "errDesc" :err-desc})

(defn values
  [prop value]
  (case prop
    :enabled (= value 0)
    :vocal (not= value 0)
    :err-code (= value 1)
    value))

(defn options
  ([] {:timeout 20000
       :keep-alive 3000})
  ([run]
     (assoc (options)
       :form-params {"run" run
                     "dv" (dv/make run)})))

(defn wait-range
  ([limit]
     (wait-range 0 limit))
  ([start limit]
     (wait-range start limit 5))
  ([start limit secs]
     (map #(do
             (println "producing... " %1)
             (Thread/sleep (* 1000 (rand-int secs)))
             %1)
          (range start limit))))

(defn info [rut]
  @(http/post "http://www.servel.cl/ConsultaDatosElectorales/CdeConsultaDatosElectorales" (options rut)
              (fn [{body :body}]
                (json/read-str body
                               :key-fn keys-table
                               :value-fn values))))
(defn store
  [record wrtr]
  (pprint record)
  (pprint record wrtr))

(defn -main [& args]
  (println "Servel Webscrapper: Start")
  (with-open [wrtr (writer "results.edn")]
    (let [thirty-millions 30000000]
      (dorun
       (map #(store %1 wrtr)
            (remove :err-code
                    (pmap info (wait-range thirty-millions)))))))
  (shutdown-agents)
  (println "Stop"))
