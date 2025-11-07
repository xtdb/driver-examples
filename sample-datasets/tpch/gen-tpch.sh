#!/usr/bin/env bash
set -euo pipefail

# Check if clojure is available
if ! command -v clojure >/dev/null 2>&1; then
    echo "Error: clojure command not found. Please install Clojure CLI tools."
    exit 1
fi

# Standalone TPC-H generator for XTDB
# Usage: ./gen-tpch.sh <host> <port> <database> <scale-factor> [tables]
# Example: ./gen-tpch.sh localhost 5432 xtdb 0.01
# Example: ./gen-tpch.sh localhost 5432 xtdb 0.01 "nation region"
# Example: ./gen-tpch.sh localhost 5432 mydb 0.1 "customer orders lineitem"

if [ $# -lt 4 ] || [ $# -gt 5 ]; then
    echo "Usage: $0 <host> <port> <database> <scale-factor> [tables]"
    echo "Example: $0 localhost 5432 xtdb 0.01"
    echo "Example: $0 localhost 5432 xtdb 0.01 \"nation region\""
    echo "Example: $0 localhost 5432 mydb 0.1 \"customer orders lineitem\""
    exit 1
fi

HOST=$1
PORT=$2
DATABASE=$3
SCALE=$4
TABLES=${5:-}

echo "TPC-H Dataset Generator"
echo "======================="
echo "Host: $HOST"
echo "Port: $PORT"
echo "Database: $DATABASE"
echo "Scale Factor: $SCALE"
if [ -n "$TABLES" ]; then
    echo "Tables: $TABLES"
fi
echo ""
echo "Starting..."
echo ""

# Build the tables vector for Clojure
if [ -n "$TABLES" ]; then
    # Convert space-separated string to Clojure vector of strings
    TABLES_VEC="["
    for table in $TABLES; do
        TABLES_VEC="$TABLES_VEC\"$table\" "
    done
    TABLES_VEC="${TABLES_VEC% }]"
    TABLES_ARG="$TABLES_VEC"
else
    TABLES_ARG=""
fi

if [ -n "$TABLES_ARG" ]; then
  clojure -Sdeps '{:paths ["."]
 :deps {org.clojure/clojure {:mvn/version "1.12.0"}
        com.xtdb/xtdb-api {:mvn/version "2.x-SNAPSHOT"}
        com.xtdb/xtdb-datasets {:mvn/version "2.x-SNAPSHOT"
                                :exclusions [com.xtdb/xtdb]}
        com.github.seancorfield/next.jdbc {:mvn/version "1.3.939"}
        org.clojure/tools.logging {:mvn/version "1.2.4"}
        ch.qos.logback/logback-classic {:mvn/version "1.4.5"}}
 :mvn/repos {"central" {:url "https://repo1.maven.org/maven2/"}
             "clojars" {:url "https://clojars.org/repo"}
             "sonatype-snapshots" {:url "https://central.sonatype.com/repository/maven-snapshots/"}}
 :aliases {:gen {:jvm-opts ["--add-opens=java.base/java.nio=ALL-UNNAMED" "-Dlogback.configurationFile=/dev/null"]}}}' \
-M:gen -e "(require '[next.jdbc :as jdbc] '[xtdb.datasets.tpch :as tpch])
    (let [url (str \"jdbc:postgresql://$HOST:$PORT/$DATABASE\")]
      (with-open [conn (jdbc/get-connection url)]
        (println \"Connected! Generating TPC-H data...\")
        (tpch/submit-dml-jdbc! conn (double $SCALE) $TABLES_ARG)
        (println \"\")
        (println \"✓ Success! TPC-H dataset generated.\")))"
else
  clojure -Sdeps '{:paths ["."]
 :deps {org.clojure/clojure {:mvn/version "1.12.0"}
        com.xtdb/xtdb-api {:mvn/version "2.x-SNAPSHOT"}
        com.xtdb/xtdb-datasets {:mvn/version "2.x-SNAPSHOT"
                                :exclusions [com.xtdb/xtdb]}
        com.github.seancorfield/next.jdbc {:mvn/version "1.3.939"}
        org.clojure/tools.logging {:mvn/version "1.2.4"}
        ch.qos.logback/logback-classic {:mvn/version "1.4.5"}}
 :mvn/repos {"central" {:url "https://repo1.maven.org/maven2/"}
             "clojars" {:url "https://clojars.org/repo"}
             "sonatype-snapshots" {:url "https://central.sonatype.com/repository/maven-snapshots/"}}
 :aliases {:gen {:jvm-opts ["--add-opens=java.base/java.nio=ALL-UNNAMED" "-Dlogback.configurationFile=/dev/null"]}}}' \
-M:gen -e "(require '[next.jdbc :as jdbc] '[xtdb.datasets.tpch :as tpch])
    (let [url (str \"jdbc:postgresql://$HOST:$PORT/$DATABASE\")]
      (with-open [conn (jdbc/get-connection url)]
        (println \"Connected! Generating TPC-H data...\")
        (tpch/submit-dml-jdbc! conn (double $SCALE))
        (println \"\")
        (println \"✓ Success! TPC-H dataset generated.\")))"
fi
