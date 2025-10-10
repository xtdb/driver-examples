(ns xtdb.xtdb-test
  (:require [clojure.test :refer [deftest is testing]]
            [pod.babashka.postgresql :as pg]
            [cheshire.core :as json]))

(def db-config {:dbtype "postgresql"
                :host "xtdb"
                :dbname "xtdb"
                :port 5432})

(defn get-clean-table []
  (format "test_table_%d_%d" (System/currentTimeMillis) (rand-int 10000)))

(defn with-connection [f]
  (let [conn (pg/get-connection db-config)]
    (try
      (f conn)
      (finally
        (pg/close-connection conn)))))

;; Transit encoding functions (simplified for Babashka)
(defn encode-transit-value [v]
  (cond
    (string? v) (json/encode v)
    (boolean? v) (str v)
    (number? v) (str v)
    (keyword? v) (str "\"~:" (name v) "\"")
    (vector? v) (str "[" (clojure.string/join "," (map encode-transit-value v)) "]")
    (map? v) (let [pairs (mapcat (fn [[k val]]
                                   [(str "\"~:" (name k) "\"")
                                    (encode-transit-value val)])
                                 v)]
               (str "[\"^ \"," (clojure.string/join "," pairs) "]"))
    :else (json/encode (str v))))

(defn build-transit-json [data]
  (encode-transit-value data))

;; Basic Operations Tests

(deftest test-connection
  (with-connection
    (fn [conn]
      (let [result (pg/execute! conn ["SELECT 1 as test"])]
        (is (= 1 (get-in result [0 :test])))))))

(deftest test-insert-and-query
  (with-connection
    (fn [conn]
      (let [table (get-clean-table)]
        (pg/execute! conn [(format "INSERT INTO %s RECORDS {_id: 'test1', value: 'hello'}, {_id: 'test2', value: 'world'}" table)])

        (let [results (pg/execute! conn [(format "SELECT _id, value FROM %s ORDER BY _id" table)])]
          (is (= 2 (count results)))
          (is (= "test1" (:_id (first results))))
          (is (= "hello" (:value (first results))))
          (is (= "test2" (:_id (second results))))
          (is (= "world" (:value (second results)))))))))

(deftest test-where-clause
  (with-connection
    (fn [conn]
      (let [table (get-clean-table)]
        (pg/execute! conn [(format "INSERT INTO %s (_id, age) VALUES (1, 25), (2, 35), (3, 45)" table)])

        (let [results (pg/execute! conn [(format "SELECT _id FROM %s WHERE age > 30 ORDER BY _id" table)])]
          (is (= 2 (count results))))))))

(deftest test-count-query
  (with-connection
    (fn [conn]
      (let [table (get-clean-table)]
        (pg/execute! conn [(format "INSERT INTO %s RECORDS {_id: 1}, {_id: 2}, {_id: 3}" table)])

        (let [result (pg/execute! conn [(format "SELECT COUNT(*) as count FROM %s" table)])]
          (is (= 3 (:count (first result)))))))))

(deftest test-parameterized-query
  (with-connection
    (fn [conn]
      (let [table (get-clean-table)]
        (pg/execute! conn [(format "INSERT INTO %s RECORDS {_id: 'param1', name: 'Test User', age: 30}" table)])

        (let [result (pg/execute! conn [(format "SELECT _id, name, age FROM %s WHERE _id = ?" table) "param1"])]
          (is (= "Test User" (:name (first result))))
          (is (= 30 (:age (first result)))))))))

;; JSON Tests

(deftest test-json-records
  (with-connection
    (fn [conn]
      (let [table (get-clean-table)]
        (pg/execute! conn [(format "INSERT INTO %s RECORDS {_id: 'user1', name: 'Alice', age: 30, active: true}" table)])

        (let [result (pg/execute! conn [(format "SELECT _id, name, age, active FROM %s WHERE _id = 'user1'" table)])]
          (is (= "user1" (:_id (first result))))
          (is (= "Alice" (:name (first result))))
          (is (= 30 (:age (first result))))
          (is (true? (:active (first result)))))))))

(deftest test-load-sample-json
  (with-connection
    (fn [conn]
      (let [table (get-clean-table)
            users (json/parse-string (slurp "../test-data/sample-users.json") true)]

        ;; Insert each user
        (doseq [user users]
          (pg/execute! conn [(format "INSERT INTO %s RECORDS {_id: '%s', name: '%s', age: %d, active: %s}"
                                     table (:_id user) (:name user) (:age user) (:active user))]))

        ;; Query back and verify
        (let [results (pg/execute! conn [(format "SELECT _id, name, age, active FROM %s ORDER BY _id" table)])]
          (is (= 3 (count results)))
          (is (= "alice" (:_id (first results))))
          (is (= "Alice Smith" (:name (first results))))
          (is (= 30 (:age (first results))))
          (is (true? (:active (first results)))))))))

;; Transit-JSON Tests

(deftest test-transit-json-format
  (with-connection
    (fn [conn]
      (let [table (get-clean-table)]
        ;; Create transit-JSON
        (let [data {:_id "transit1" :name "Transit User" :age 42 :active true}
              transit-json (build-transit-json data)]

          ;; Verify it contains transit markers
          (is (.contains transit-json "~:"))

          ;; Insert using RECORDS syntax
          (pg/execute! conn [(format "INSERT INTO %s RECORDS {_id: 'transit1', name: 'Transit User', age: 42, active: true}" table)])

          (let [result (pg/execute! conn [(format "SELECT _id, name, age, active FROM %s WHERE _id = 'transit1'" table)])]
            (is (= "transit1" (:_id (first result))))
            (is (= "Transit User" (:name (first result))))
            (is (= 42 (:age (first result))))
            (is (true? (:active (first result))))))))))

(deftest test-parse-transit-json
  (with-connection
    (fn [conn]
      (let [table (get-clean-table)
            lines (-> (slurp "../test-data/sample-users-transit.json")
                      (clojure.string/split #"\n")
                      (->> (remove clojure.string/blank?)))]

        ;; Parse and insert each line
        (doseq [line lines]
          (let [user-data (json/parse-string line true)
                ;; Transit format: ["^ " "~:_id" "alice" "~:name" "Alice Smith" ...]
                pairs (rest user-data) ;; Skip "^ "
                map-data (apply hash-map pairs)
                id (get map-data "~:_id")
                name (get map-data "~:name")
                age (get map-data "~:age")
                active (get map-data "~:active")]

            ;; Insert using RECORDS syntax
            (pg/execute! conn [(format "INSERT INTO %s RECORDS {_id: '%s', name: '%s', age: %d, active: %s}"
                                       table id name age active)])))

        ;; Query back and verify
        (let [results (pg/execute! conn [(format "SELECT _id, name, age, active FROM %s ORDER BY _id" table)])]
          (is (= 3 (count results)))
          (is (= "alice" (:_id (first results))))
          (is (= "Alice Smith" (:name (first results))))
          (is (= 30 (:age (first results))))
          (is (true? (:active (first results)))))))))

(deftest test-transit-json-encoding
  ;; Test transit encoding capabilities
  (let [data {:string "hello"
              :number 42
              :bool true
              :array [1 2 3]}
        transit-json (build-transit-json data)]

    ;; Verify encoding
    (is (.contains transit-json "hello"))
    (is (.contains transit-json "42"))
    (is (.contains transit-json "true"))

    ;; Verify it can be parsed as JSON
    (is (some? (json/parse-string transit-json)))))
