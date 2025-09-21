(ns user
  (:require [xtdb.api :as xt]))

(defn get-client []
  (xt/client {:host "xtdb"
              :port 5432
              :user "xtdb"}))

(defn -main [& _args]
  (let [client (get-client)]

    ;; using the REPL? you probably want this:
    ;; (def client (get-client))

    (xt/execute-tx client ["INSERT INTO users RECORDS {_id: 'jms', first_name: 'James'}"])

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
            #_#_:my-period #xt/tstz-range [#xt/zdt "2025-01-01Z"  #xt/zdt "2025-06-12Z"] ;; upper value can also be nil (which is used extensively for _valid_time and _system_time periods) ;; blocked by https://github.com/xtdb/xtdb/issues/4379
            :my-uri #xt/uri "https://xtdb.com"
            :my-uuid #uuid "97a392d5-5e3f-406f-9651-a828ee79b156"})

    ;; Insert with parameterized SQL
    (xt/execute-tx client [["INSERT INTO users RECORDS ?" m]])

    ;; Insert with parameterized SQL batches
    (xt/execute-tx client [[:sql "INSERT INTO users RECORDS ?" [m]]])

    ;; everything above can be roundtripped through XTDB
    ;; implemented using Apache Arrow types internally (but also available via FlightSQL!)
    ;; such that this expression should eval to true:
    (= m (first (xt/q client ["SELECT * FROM users WHERE _id = 'joe'"])))

    ;; should you see `org.postgresql.util.PSQLException: ERROR: Relevant table schema has changed since preparing query, please prepare again`
    ;; or recently inserted columns you'd now expect to see via `SELECT *` are missing
    ;; ...then simply re-def the client and try again:
    ;; (def client (get-client))

    (xt/execute-tx client
                  [["PATCH INTO users RECORDS ?" {:xt/id "joe", :likes "chocolate"}]])

    (prn (xt/q client "SELECT * FROM users"))

    ;; access temporal columns
    (prn (xt/q client "SELECT *, _valid_time, _system_time FROM users"))

    ;; the "database as a value" semantics are achieved with _system_time columns and SNAPSHOT_TIME
    (prn (xt/q client "SETTING SNAPSHOT_TIME TO '2020-01-01Z' SELECT * FROM users"))

    ;; users can time-travel with DEFAULT VALID_TIME
    (prn (xt/q client "SETTING DEFAULT VALID_TIME AS OF '2027-01-01Z' SELECT * FROM users"))

    ;; or per-table filters
    (prn (xt/q client "SELECT * FROM users FOR VALID_TIME AS OF '2027-01-01Z'"))

    ;; transactions are reified
    (prn (xt/q client "SELECT * FROM xt.txs"))

    ;; use XTQL within SQL
    (prn (xt/q client (format "SELECT * FROM (XTQL $$ %s $$) xtql_res WHERE _id = 'joe'"
                              '(-> (from :users [first-name xt/id])
                                   (with {:full-name (concat first-name " Smith")})))))))

(comment
  (-main)

  (require '[next.jdbc :as jdbc])

  ;; To explore the Sakila dataset using datafy/nav tooling with next.jdbc
  (defn get-jdbc-conn []
    (jdbc/get-connection "jdbc:xtdb://xtdb:5432/xtdb?options=-c%20TimeZone=UTC"))

  (tap> (jdbc/execute! (get-jdbc-conn) ["select * from inventory"]
                       {:schema-opts {:pk "_id"}}))

  )
