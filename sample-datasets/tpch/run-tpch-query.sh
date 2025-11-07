#!/usr/bin/env bash
set -euo pipefail

# Run TPC-H Query 5 across multiple attached databases
# This query shows the revenue volume done through local suppliers

HOST=${1:-localhost}
PORT=${2:-5432}
REGION=${3:-ASIA}
YEAR=${4:-1994}

echo "TPC-H Query 5: Local Supplier Volume"
echo "====================================="
echo "Host: $HOST"
echo "Port: $PORT"
echo "Region: $REGION"
echo "Year: $YEAR"
echo ""
echo "This query lists revenue by nation for a given region and year,"
echo "showing revenue from customers in that region who ordered from"
echo "suppliers also from that region."
echo ""
echo "Tables queried across multiple databases:"
echo "  - customer_db.customer"
echo "  - orders_db.orders"
echo "  - lineitem_db.lineitem"
echo "  - supplier_db.supplier"
echo "  - nation_db.nation"
echo "  - region_db.region"
echo ""
echo "Running query..."
echo ""

psql -h "$HOST" -p "$PORT" -U postgres -d xtdb << EOF
-- TPC-H Query 5: Local Supplier Volume Query
-- This query joins 6 tables across 6 different databases

SELECT
    n.n_name,
    SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
FROM
    customer_db.customer c
    JOIN orders_db.orders o ON c.c_custkey = o.o_custkey
    JOIN lineitem_db.lineitem l ON l.l_orderkey = o.o_orderkey
    JOIN supplier_db.supplier s ON l.l_suppkey = s.s_suppkey
        AND c.c_nationkey = s.s_nationkey
    JOIN nation_db.nation n ON s.s_nationkey = n.n_nationkey
    JOIN region_db.region r ON n.n_regionkey = r.r_regionkey
WHERE
    r.r_name = '$REGION'
    AND o.o_orderdate >= DATE '$YEAR-01-01'
    AND o.o_orderdate < DATE '$YEAR-01-01' + INTERVAL '1' year
GROUP BY
    n.n_name
ORDER BY
    revenue DESC;
EOF

echo ""
echo "Query complete!"
