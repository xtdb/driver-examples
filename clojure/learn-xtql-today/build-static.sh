#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

rm -rf .clerk/cache

clj -J--add-opens=java.base/java.nio=ALL-UNNAMED \
    -J-Dio.netty.tryReflectionSetAccessible=true \
    -J--enable-native-access=ALL-UNNAMED \
    -M -e "(require '[nextjournal.clerk :as clerk]) (clerk/build! {:paths [\"src/learn-xtql-today-with-clojure.clj\"]}) (System/exit 0)"

echo "Static build complete. Output in public/build/"
