(ns verify-data
  (:require [next.jdbc :as jdbc]
            [xtdb.next.jdbc :as xt-jdbc]
            [clojure.tools.logging :as log]))

(def db-spec {:dbtype "xtdb"
              :dbname "xtdb"
              :host "xtdb" ;; Uses "xtdb" for GitHub Actions and DevContainer
              :user "your-username"
              :password "your-password"})

(defn verify []
  (with-open [conn (jdbc/get-connection db-spec)]
    (let [result (jdbc/execute-one! conn ["SELECT count(*) as c FROM rental"] {:builder-fn xt-jdbc/builder-fn})]
      (if (= (:c result) 16044)
        (do
          (log/info "✅ Data verification passed")
          (println result)
          (System/exit 0))
        (do
          (log/error "❌ Data verification failed")
          (System/exit 1))))))

(defn -main [& args]
  (verify))
