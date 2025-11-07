#!/usr/bin/env bash
set -euo pipefail

# Setup TPC-H databases with Redpanda+Garage
# Each TPC-H table gets its own database with dedicated log and storage

HOST=${1:-xtdb}
PORT=${2:-5432}
SCALE=${3:-0.005}

echo "TPC-H Database Setup"
echo "===================="
echo "Host: $HOST"
echo "Port: $PORT"
echo "Scale Factor: $SCALE"
echo ""

# TPC-H tables
TABLES=("nation" "region" "part" "supplier" "partsupp" "customer" "orders" "lineitem")

echo "Step 1: Attaching databases..."
echo "==============================="

for table in "${TABLES[@]}"; do
    echo "Attaching database: ${table}_db"

    psql -h "$HOST" -p "$PORT" -U postgres -d xtdb << EOF
ATTACH DATABASE ${table}_db WITH \$\$
  log: !Kafka
    cluster: kafkaCluster
    topic: "${table}_log"
    autoCreateTopic: true
  storage: !Remote
    objectStore: !S3
      bucket: "xtdb-storage"
      prefix: "tpch/${table}"
      endpoint: "http://garage:3902"
      pathStyleAccessEnabled: true
      region: "garage"
      credentials: !Basic
        accessKey: "GK31c2f218bd3e1932929759c1"
        secretKey: "b8e1ec4d832d1038fb34242fc0f8e4f1ee8e0ce00fc1be1f12e28550b060c2d5"
\$\$;
EOF

    if [ $? -eq 0 ]; then
        echo "✓ ${table}_db attached successfully"
    else
        echo "✗ Failed to attach ${table}_db"
        exit 1
    fi
    echo ""
done

echo ""
echo "Step 2: Populating databases with TPC-H data..."
echo "==============================================="

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GEN_TPCH_SCRIPT="$SCRIPT_DIR/./gen-tpch.sh"

if [ ! -f "$GEN_TPCH_SCRIPT" ]; then
    echo "Error: gen-tpch.sh not found at $GEN_TPCH_SCRIPT"
    exit 1
fi

for table in "${TABLES[@]}"; do
    echo ""
    echo "Populating ${table}_db with table: $table"
    echo "-------------------------------------------"

    "$GEN_TPCH_SCRIPT" "$HOST" "$PORT" "${table}_db" "$SCALE" "$table"

    if [ $? -eq 0 ]; then
        echo "✓ ${table}_db populated successfully"
    else
        echo "✗ Failed to populate ${table}_db"
        exit 1
    fi
done

echo ""
echo "========================================"
echo "✓ All TPC-H databases setup complete!"
echo "========================================"
echo ""
echo "Attached databases:"
for table in "${TABLES[@]}"; do
    echo "  - ${table}_db (topic: ${table}_log, storage: tpch/${table})"
done
echo ""
echo "Scale factor: $SCALE"
