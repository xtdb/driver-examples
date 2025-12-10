(ns my-project.main
  (:require [pod.babashka.postgresql :as pg]))

(defn -main [& _args]
  (def db {:dbtype   "postgresql"
           :host     (or (System/getenv "XTDB_HOST") "xtdb")
           :dbname   "xtdb"
           :port     5432})

  (let [conn (pg/get-connection db)]
    (prn (pg/execute! conn ["SELECT xt.version()"]))
    (pg/close-connection conn)))
