(ns user
  (:require [next.jdbc :as jdbc]
            [xtdb.api :as xt]))

(defn -main [& _args]
  (with-open [conn (jdbc/get-connection "jdbc:xtdb://xtdb:5432/xtdb")]
    (jdbc/execute! conn
                   ["INSERT INTO users RECORDS {_id: 'jms', first_name: 'James'}"])

    (jdbc/execute! conn
                   ["INSERT INTO users RECORDS ?"
                    {:xt/id "joe", :first-name "Joe", :a-map {:keys #{"nested" :edn 1 1.23}}}])

    (jdbc/execute! conn
                   ["PATCH INTO users RECORDS ?" {:xt/id "joe", :likes "chocolate"}])

    (prn (jdbc/execute! conn ["SELECT * FROM users"]))

    ))

(comment
  (-main)
  ;; to explore the Sakila dataset using datafy/nav tooling
  (tap> (jdbc/execute! conn ["select * from inventory"]
                       {:schema-opts {:pk "_id"}}))

  )
