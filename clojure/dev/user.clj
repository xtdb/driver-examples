(ns user
  (:require [next.jdbc :as jdbc]
            [xtdb.next.jdbc :as xt-jdbc]))

(with-open [conn (jdbc/get-connection "jdbc:xtdb://localhost:5432/xtdb")]
  (jdbc/execute! conn ["INSERT INTO users RECORDS {_id: 'jms', first_name: 'James'}"])
  (jdbc/execute! conn ["INSERT INTO users RECORDS ?"
                       (xt-jdbc/->pg-obj {:xt/id "joe", :first-name "Joe"})])

  (prn (jdbc/execute! conn ["SELECT * FROM users"]))
  ;; => [{:_id "joe", :first_name "Joe"}
  ;;     {:_id "jms", :first_name "James"}]

  ;; optional: use the XT col-reader to transform nested values too
  (prn (jdbc/execute! conn ["SELECT * FROM users"]
                      {:builder-fn xt-jdbc/builder-fn}))

  ;; => [{:xt/id "joe", :first-name "Joe"}
  ;;     {:xt/id "jms", :first-name "James"}]
  )
