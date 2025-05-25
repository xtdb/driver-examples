(ns user
  (:require [next.jdbc :as jdbc]
            [xtdb.next.jdbc :as xt-jdbc]))

(defn -main [& _args]
  (with-open [conn (jdbc/get-connection "jdbc:xtdb://xtdb:5432/xtdb?options=-c fallback_output_format=transit")]
    (jdbc/execute! conn
                   ["INSERT INTO users RECORDS {_id: 'jms', first_name: 'James'}"]
                   {:builder-fn xt-jdbc/builder-fn})

    (jdbc/execute! conn
                   ["INSERT INTO users RECORDS ?"
                    (xt-jdbc/->pg-obj {:xt/id "joe", :first-name "Joe", :a-map {:keys #{"nested" :edn 1 1.23}}})]
                   {:builder-fn xt-jdbc/builder-fn})

    (jdbc/execute! conn
                     ["PATCH INTO users RECORDS ?"
                      (xt-jdbc/->pg-obj {:xt/id "joe", :likes "chocolate"})]
                     {:builder-fn xt-jdbc/builder-fn})

    (prn (jdbc/execute! conn ["SELECT * FROM users"]
                        {:builder-fn xt-jdbc/builder-fn}))

    ))

(comment
  ;; to explore the Sakila dataset using datafy/nav tooling
  (tap> (jdbc/execute! conn ["select * from inventory"]
                       {:schema-opts {:pk "_id"}}))

  )
