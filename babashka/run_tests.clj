#!/usr/bin/env bb

(require '[clojure.test :as t])

;; Load test namespace
(load-file "test/xtdb/xtdb_test.clj")

;; Run tests
(let [results (t/run-tests 'xtdb.xtdb-test)]
  (System/exit (if (t/successful? results) 0 1)))
