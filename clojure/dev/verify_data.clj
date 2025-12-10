(ns verify-data
  (:require [clojure.tools.logging :as log]
            [xtdb.api :as xt]))

(def xtdb-host (or (System/getenv "XTDB_HOST") "xtdb"))

(defn get-client []
  (xt/client {:host xtdb-host
              :port 5432
              :user "xtdb"}))

(defn verify []
  (let [client (get-client)
        result (first (xt/q client ["SELECT count(*) as c FROM rental"]))]
    (if (= (:c result) 16044)
      (do
        (log/info "Data verification passed")
        (println result)
        (System/exit 0))
      (do
        (log/error "Data verification failed")
        (System/exit 1)))))

(defn -main [& args]
  (verify))
