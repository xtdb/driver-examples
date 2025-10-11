(ns xtdb.xtdb-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [clojure.java.io :as io]
            [clojure.data.json :as json]
            [cognitect.transit :as transit]
            [xtdb.api :as xt])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream]))

(def db-spec {:jdbcUrl "jdbc:postgresql://xtdb:5432/xtdb"
              :user "xtdb"
              :password ""})

(def ds (jdbc/get-datasource db-spec))

(defn get-client []
  (xt/client {:host "xtdb"
              :port 5432
              :user "xtdb"}))

(defn get-clean-table []
  (format "test_table_%d_%d" (System/currentTimeMillis) (rand-int 10000)))

(defn with-connection [f]
  (with-open [conn (jdbc/get-connection ds)]
    (f conn)))

(defn with-client [f]
  (let [client (get-client)]
    (try
      (f client)
      (finally
        (when (instance? java.io.Closeable client)
          (.close client))))))

;; Basic Operations Tests

(deftest test-connection
  (with-connection
    (fn [conn]
      (let [result (jdbc/execute-one! conn ["SELECT 1 as test"])]
        (is (= 1 (:test result)))))))

(deftest test-insert-and-query
  (with-connection
    (fn [conn]
      (let [table (get-clean-table)]
        (jdbc/execute! conn [(format "INSERT INTO %s RECORDS {_id: 'test1', value: 'hello'}, {_id: 'test2', value: 'world'}" table)])

        (let [results (jdbc/execute! conn [(format "SELECT _id, value FROM %s ORDER BY _id" table)])]
          (is (= 2 (count results)))
          (is (= "test1" (:_id (first results))))
          (is (= "hello" (:value (first results))))
          (is (= "test2" (:_id (second results))))
          (is (= "world" (:value (second results)))))))))

(deftest test-where-clause
  (with-connection
    (fn [conn]
      (let [table (get-clean-table)]
        (jdbc/execute! conn [(format "INSERT INTO %s (_id, age) VALUES (1, 25), (2, 35), (3, 45)" table)])

        (let [results (jdbc/execute! conn [(format "SELECT _id FROM %s WHERE age > 30 ORDER BY _id" table)])]
          (is (= 2 (count results))))))))

(deftest test-count-query
  (with-connection
    (fn [conn]
      (let [table (get-clean-table)]
        (jdbc/execute! conn [(format "INSERT INTO %s RECORDS {_id: 1}, {_id: 2}, {_id: 3}" table)])

        (let [result (jdbc/execute-one! conn [(format "SELECT COUNT(*) as count FROM %s" table)])]
          (is (= 3 (:count result))))))))

(deftest test-parameterized-query
  (with-connection
    (fn [conn]
      (let [table (get-clean-table)]
        (jdbc/execute! conn [(format "INSERT INTO %s RECORDS {_id: 'param1', name: 'Test User', age: 30}" table)])

        (let [result (jdbc/execute-one! conn [(format "SELECT _id, name, age FROM %s WHERE _id = ?" table) "param1"])]
          (is (= "Test User" (:name result)))
          (is (= 30 (:age result))))))))

;; JSON Tests

(deftest test-json-records
  (with-connection
    (fn [conn]
      (let [table (get-clean-table)]
        (jdbc/execute! conn [(format "INSERT INTO %s RECORDS {_id: 'user1', name: 'Alice', age: 30, active: true}" table)])

        (let [result (jdbc/execute-one! conn [(format "SELECT _id, name, age, active FROM %s WHERE _id = 'user1'" table)])]
          (is (= "user1" (:_id result)))
          (is (= "Alice" (:name result)))
          (is (= 30 (:age result)))
          (is (true? (:active result))))))))

(deftest test-load-sample-json
  (with-client
    (fn [client]
      (let [table (get-clean-table)
            users (json/read-str (slurp "../test-data/sample-users.json"))]

        ;; Insert using XTDB client - it properly encodes Clojure maps with correct OIDs
        ;; Pass each user object directly as a Clojure map (keys as strings to match JSON)
        (doseq [user users]
          (xt/execute-tx client [[(format "INSERT INTO %s RECORDS ?" table) user]]))

        ;; Query back and verify
        ;; XTDB client returns _id as :xt/id (XTDB's internal convention)
        (let [results (xt/q client [(format "SELECT _id, name, age, active FROM %s ORDER BY _id" table)])]
          (is (= 3 (count results)))
          (let [first-result (first results)]
            (is (= "alice" (:xt/id first-result)))
            (is (= "Alice Smith" (:name first-result)))
            (is (= 30 (:age first-result)))
            (is (true? (:active first-result)))))))))

;; Transit-JSON Tests

(deftest test-transit-json-format
  (with-connection
    (fn [conn]
      (let [table (get-clean-table)]
        ;; Create transit writer
        (let [out (ByteArrayOutputStream.)
              writer (transit/writer out :json)
              data {:_id "transit1" :name "Transit User" :age 42 :active true}]

          (transit/write writer data)
          (let [transit-json (.toString out)]
            ;; Verify it contains transit markers
            (is (.contains transit-json "~:"))

            ;; Insert using RECORDS syntax
            (jdbc/execute! conn [(format "INSERT INTO %s RECORDS {_id: 'transit1', name: 'Transit User', age: 42, active: true}" table)])

            (let [result (jdbc/execute-one! conn [(format "SELECT _id, name, age, active FROM %s WHERE _id = 'transit1'" table)])]
              (is (= "transit1" (:_id result)))
              (is (= "Transit User" (:name result)))
              (is (= 42 (:age result)))
              (is (true? (:active result))))))))))

(deftest test-parse-transit-json
  (with-client
    (fn [client]
      (let [table (get-clean-table)
            lines (line-seq (io/reader "../test-data/sample-users-transit.json"))]

        ;; Parse transit-JSON and insert using XTDB client
        ;; The client handles keyword keys directly
        (doseq [line (remove clojure.string/blank? lines)]
          (let [in (ByteArrayInputStream. (.getBytes line))
                reader (transit/reader in :json)
                user-data (transit/read reader)]
            (xt/execute-tx client [[(format "INSERT INTO %s RECORDS ?" table) user-data]])))

        ;; Query back and verify
        ;; XTDB client returns _id as :xt/id (XTDB's internal convention)
        (let [results (xt/q client [(format "SELECT _id, name, age, active FROM %s ORDER BY _id" table)])]
          (is (= 3 (count results)))
          (let [first-result (first results)]
            (is (= "alice" (:xt/id first-result)))
            (is (= "Alice Smith" (:name first-result)))
            (is (= 30 (:age first-result)))
            (is (true? (:active first-result)))))))))
