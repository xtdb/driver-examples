(ns user
  (:require [next.jdbc :as jdbc]
            [xtdb.api :as xt]
            [xtdb.next.jdbc :as xt-jdbc]))

(defn get-conn []
  (jdbc/get-connection "jdbc:xtdb://xtdb:5432/xtdb?options=-c%20TimeZone=UTC"))

(defn -main [& _args]
  (with-open [conn (get-conn)]

    ;; using the REPL? you probably want this:
    ;; (def conn (get-conn))

    (jdbc/execute! conn ["INSERT INTO users RECORDS {_id: 'jms', first_name: 'James'}"])

    (def m {:xt/id "joe"
            :first-name "Joe" ;; string
            :my-keyword :your-own.ns-etc/key-word ;; keyword values are roundtripped, and printed as strings via non-Clojure SQL clients
            :my.ns.col/col1-key "foo" ;; keyword keys (columns) are also roundtripped, with the limitation that they get 'normalized' to a snake_case SQL-compatible representation, using '$' in place of the '.' separators and a final '$' which is implicitly the '/' ns separator
            :my-map {:your-own.ns-etc/key-word "v"} ;; nested keywords keys are roundtripped similarly and are also printed in the normalized form via non-Clojure SQL clients (unlike keyword values)
            :my-set #{["nested" :edn] {:etc true}} ;; sets, vectors and booleans all work great too
            :my-bigint 1
            :my-float 1.23
            :my-decimal 1.0M
            :my-date (java.time.LocalDate/of 2025 6 12) ;; also #xt/date "2025-06-12"
            :my-timestamptz #xt/zdt "2025-06-12T00:00Z[UTC]"
            :my-time (java.time.LocalTime/of 22 15 04 1237) ;; also #xt/time "22:15:04.1237"
            :my-duration (java.time.Duration/parse "PT1H3M5.533S") ;; also #xt/duration "PT1H3M5.533S"
            :my-interval #xt/interval "P163DT12H"
            :my-period #xt/tstz-range [#xt/zdt "2025-01-01Z"  #xt/zdt "2025-06-12Z"] ;; upper value can also be nil (which is used extensively for _valid_time and _system_time periods)
            :my-uri #xt/uri "https://xtdb.com"
            :my-uuid #uuid "97a392d5-5e3f-406f-9651-a828ee79b156"})

    (jdbc/execute! conn ["INSERT INTO users RECORDS ?" m])

    ;; everything above can roundtrip when using the provided builder-fn
    ;; such that this expression should eval to true:
    (= m (first (jdbc/execute! conn ["SELECT * FROM users WHERE _id = 'joe'"]
                               {:builder-fn xt-jdbc/builder-fn})))

    ;; should you see `org.postgresql.util.PSQLException: ERROR: Relevant table schema has changed since preparing query, please prepare again`
    ;; or recently inserted columns you'd now expect to see via `SELECT *` are missing
    ;; ...then simply re-def the conn and try again:
    ;; (def conn (get-conn))

    (jdbc/execute! conn
                   ["PATCH INTO users RECORDS ?" {:xt/id "joe", :likes "chocolate"}])

    (prn (jdbc/execute! conn ["SELECT * FROM users"] {:builder-fn xt-jdbc/builder-fn}))

    ;; access temporal columns
    (jdbc/execute! conn ["SELECT *, _valid_time, _system_time FROM users"] {:builder-fn xt-jdbc/builder-fn})

    ;; the "database as a value" semantics are achieved with _system_time columns and SNAPSHOT_TIME
    (jdbc/execute! conn ["SETTING SNAPSHOT_TIME TO '2020-01-01Z' SELECT * FROM users"] {:builder-fn xt-jdbc/builder-fn})

    ;; users can time-travel with DEFAULT VALID_TIME
    (jdbc/execute! conn ["SETTING DEFAULT VALID_TIME AS OF '2027-01-01Z' SELECT * FROM users"] {:builder-fn xt-jdbc/builder-fn})

    ;; or per-table filters
    (jdbc/execute! conn ["SELECT * FROM users FOR VALID_TIME AS OF '2027-01-01Z'"] {:builder-fn xt-jdbc/builder-fn})

    ;; transactions are reified
    (jdbc/execute! conn ["SELECT * FROM xt.txs"] {:builder-fn xt-jdbc/builder-fn})

    ;; use XTQL within SQL
    (jdbc/execute! conn [(format "SELECT * FROM (XTQL $$ %s $$) xtql_res WHERE _id = 'joe'"
                                 '(-> (from :users [first-name xt/id])
                                      (with {:full-name (concat first-name " Smith")})))]
                   {:builder-fn xt-jdbc/builder-fn})
  ))

(comment
  (-main)

  ;; to explore the Sakila dataset using datafy/nav tooling
  (tap> (jdbc/execute! (get-conn) ["select * from inventory"]
                       {:schema-opts {:pk "_id"}
                        :builder-fn xt-jdbc/builder-fn}))

  )
