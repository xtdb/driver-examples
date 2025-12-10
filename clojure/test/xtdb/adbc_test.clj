(ns xtdb.adbc-test
  "XTDB ADBC Tests

   Tests for connecting to XTDB via Arrow Flight SQL protocol using ADBC.
   Uses Java interop with Apache Arrow ADBC libraries.
   Demonstrates DML operations (INSERT, UPDATE, DELETE, ERASE) and temporal queries."
  (:require [clojure.test :refer [deftest is testing use-fixtures]])
  (:import [org.apache.arrow.adbc.core AdbcConnection AdbcDatabase AdbcStatement]
           [org.apache.arrow.adbc.driver.flightsql FlightSqlDriver]
           [org.apache.arrow.memory RootAllocator]
           [org.apache.arrow.vector VectorSchemaRoot]
           [org.apache.arrow.vector.ipc ArrowReader]))

(def ^:private flight-sql-uri
  (str "grpc+tcp://" (or (System/getenv "XTDB_HOST") "xtdb") ":9833"))

(def ^:private ^:dynamic *allocator* nil)
(def ^:private ^:dynamic *database* nil)
(def ^:private ^:dynamic *connection* nil)

(defn- get-clean-table []
  (format "test_adbc_%d_%d" (System/currentTimeMillis) (rand-int 10000)))

(defn- setup-connection []
  (let [allocator (RootAllocator.)
        database (.open (FlightSqlDriver. allocator) {"uri" flight-sql-uri})
        connection (.connect database)]
    {:allocator allocator
     :database database
     :connection connection}))

(defn- teardown-connection [{:keys [connection database allocator]}]
  (when connection (.close connection))
  (when database (.close database))
  (when allocator (.close allocator)))

(defn with-adbc-connection [f]
  (let [ctx (setup-connection)]
    (binding [*allocator* (:allocator ctx)
              *database* (:database ctx)
              *connection* (:connection ctx)]
      (try
        (f)
        (finally
          (teardown-connection ctx))))))

(use-fixtures :each with-adbc-connection)

(defn- convert-value
  "Convert Arrow values to Clojure-friendly types."
  [v]
  (cond
    (instance? org.apache.arrow.vector.util.Text v) (str v)
    :else v))

(defn- read-results
  "Read all results from an ArrowReader into a vector of maps."
  [^ArrowReader reader]
  (loop [results []]
    (if (.loadNextBatch reader)
      (let [root (.getVectorSchemaRoot reader)
            row-count (.getRowCount root)
            field-vectors (.getFieldVectors root)
            batch-results (vec (for [i (range row-count)]
                                 (into {}
                                       (for [fv field-vectors]
                                         [(keyword (.getName fv))
                                          (convert-value (.getObject fv i))]))))]
        (recur (into results batch-results)))
      results)))

(defn- with-statement
  "Execute operations within a statement context."
  [f]
  (with-open [stmt (.createStatement *connection*)]
    (f stmt)))

(defn- execute-update!
  "Execute a DML statement."
  [^AdbcStatement stmt sql]
  (.setSqlQuery stmt sql)
  (.executeUpdate stmt))

(defn- execute-query
  "Execute a query and return results."
  [^AdbcStatement stmt sql]
  (.setSqlQuery stmt sql)
  (with-open [reader (.getReader (.executeQuery stmt))]
    (read-results reader)))

(defn- cleanup! [table & ids]
  (with-open [allocator (RootAllocator.)
              database (.open (FlightSqlDriver. allocator) {"uri" flight-sql-uri})
              connection (.connect database)
              stmt (.createStatement connection)]
    (doseq [id ids]
      (try
        (.setSqlQuery stmt (format "ERASE FROM %s WHERE _id = %s" table id))
        (.executeUpdate stmt)
        (catch Exception _)))))

;; === Connection Tests ===

(deftest test-connection
  (is (some? *connection*)))

(deftest test-simple-query
  (with-statement
    (fn [stmt]
      (let [results (execute-query stmt "SELECT 1 AS x, 'hello' AS greeting")]
        (is (= 1 (count results)))
        (is (= 1 (:x (first results))))
        (is (= "hello" (:greeting (first results))))))))

(deftest test-query-with-expressions
  (with-statement
    (fn [stmt]
      (let [results (execute-query stmt "SELECT 2 + 2 AS sum, UPPER('hello') AS upper_greeting")]
        (is (= 1 (count results)))
        (is (= 4 (:sum (first results))))
        (is (= "HELLO" (:upper_greeting (first results))))))))

(deftest test-system-tables
  (with-statement
    (fn [stmt]
      (let [results (execute-query stmt
                     "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' LIMIT 10")]
        (is (vector? results))))))

;; === DML Tests ===

(deftest test-insert-and-query
  (let [table (get-clean-table)]
    (try
      (with-statement
        (fn [stmt]
          (execute-update! stmt
           (format "INSERT INTO %s RECORDS {_id: 1, name: 'Widget', price: 19.99, category: 'gadgets'}, {_id: 2, name: 'Gizmo', price: 29.99, category: 'gadgets'}, {_id: 3, name: 'Thingamajig', price: 9.99, category: 'misc'}"
                   table))
          (let [results (execute-query stmt (format "SELECT * FROM %s ORDER BY _id" table))]
            (is (= 3 (count results))))))
      (finally
        (cleanup! table 1 2 3)))))

(deftest test-update
  (let [table (get-clean-table)]
    (try
      (with-statement
        (fn [stmt]
          (execute-update! stmt
           (format "INSERT INTO %s RECORDS {_id: 1, name: 'Widget', price: 19.99}" table))
          (execute-update! stmt
           (format "UPDATE %s SET price = 24.99 WHERE _id = 1" table))
          (let [results (execute-query stmt (format "SELECT price FROM %s WHERE _id = 1" table))]
            (is (= 1 (count results)))
            (is (== 24.99 (:price (first results)))))))
      (finally
        (cleanup! table 1)))))

(deftest test-delete
  (let [table (get-clean-table)]
    (try
      (with-statement
        (fn [stmt]
          (execute-update! stmt
           (format "INSERT INTO %s RECORDS {_id: 1, name: 'ToDelete'}, {_id: 2, name: 'ToKeep'}" table))
          (execute-update! stmt
           (format "DELETE FROM %s WHERE _id = 1" table))
          (let [results (execute-query stmt (format "SELECT * FROM %s" table))]
            (is (= 1 (count results))))))
      (finally
        (cleanup! table 1 2)))))

(deftest test-historical-query
  (let [table (get-clean-table)]
    (try
      (with-statement
        (fn [stmt]
          (execute-update! stmt
           (format "INSERT INTO %s RECORDS {_id: 1, name: 'Widget', price: 19.99}" table))
          (execute-update! stmt
           (format "UPDATE %s SET price = 24.99 WHERE _id = 1" table))
          (let [results (execute-query stmt
                         (format "SELECT *, _valid_from, _valid_to FROM %s FOR ALL VALID_TIME ORDER BY _id, _valid_from"
                                 table))]
            (is (= 2 (count results)))
            (is (== 19.99 (:price (first results))))
            (is (== 24.99 (:price (second results)))))))
      (finally
        (cleanup! table 1)))))

(deftest test-erase
  (let [table (get-clean-table)]
    (try
      (with-statement
        (fn [stmt]
          (execute-update! stmt
           (format "INSERT INTO %s RECORDS {_id: 1, name: 'ToErase'}, {_id: 2, name: 'ToKeep'}" table))
          (execute-update! stmt
           (format "UPDATE %s SET name = 'UpdatedErase' WHERE _id = 1" table))
          (execute-update! stmt
           (format "ERASE FROM %s WHERE _id = 1" table))
          (let [results (execute-query stmt
                         (format "SELECT * FROM %s FOR ALL VALID_TIME ORDER BY _id" table))]
            (is (= 1 (count results))))))
      (finally
        (cleanup! table 2)))))
