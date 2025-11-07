#!/usr/bin/env bash
set -euo pipefail

# Billing Report showing temporal evolution across multiple databases
# Demonstrates how billing data changes over time using XTDB's temporal features

HOST=${1:-localhost}
PORT=${2:-5432}

echo "=========================================="
echo "Temporal Billing Report"
echo "=========================================="
echo "Host: $HOST"
echo "Port: $PORT"
echo ""
echo "This report shows how customer billing data evolves over time"
echo "by querying the same data at different points in time."
echo ""

# Calculate dates for the past week
TODAY=$(date +%Y-%m-%d)
DAY_1=$(date -d "$TODAY -6 days" +%Y-%m-%d)
DAY_2=$(date -d "$TODAY -5 days" +%Y-%m-%d)
DAY_3=$(date -d "$TODAY -4 days" +%Y-%m-%d)
DAY_4=$(date -d "$TODAY -3 days" +%Y-%m-%d)
DAY_5=$(date -d "$TODAY -2 days" +%Y-%m-%d)
DAY_6=$(date -d "$TODAY -1 days" +%Y-%m-%d)
DAY_7=$TODAY

# Billing query that joins customer, orders, and line items
BILLING_QUERY="
SELECT
    c.c_custkey,
    c.c_name,
    c.c_acctbal AS customer_balance,
    COUNT(DISTINCT o.o_orderkey) AS order_count,
    COUNT(l.l_orderkey) AS line_item_count,
    COALESCE(SUM(l.l_extendedprice * (1 - l.l_discount)), 0) AS total_revenue,
    COALESCE(AVG(l.l_extendedprice), 0)::DECIMAL(10,2) AS avg_line_price,
    COALESCE(AVG(l.l_discount), 0)::DECIMAL(4,2) AS avg_discount
FROM
    customer_db.customer c
    LEFT JOIN orders_db.orders o ON c.c_custkey = o.o_custkey
    LEFT JOIN lineitem_db.lineitem l ON o.o_orderkey = l.l_orderkey
WHERE
    c._id IN (SELECT _id FROM customer_db.customer LIMIT 10)
GROUP BY
    c.c_custkey, c.c_name, c.c_acctbal
LIMIT 10;
"

echo "Billing Query:"
echo "-------------"
echo "Tracking first 10 customers from backfill data across 7 days"
echo "Joining: customer_db → orders_db → lineitem_db"
echo ""
echo "Metrics:"
echo "  • Customer account balance"
echo "  • Order count"
echo "  • Line item count"
echo "  • Total revenue"
echo "  • Average line price"
echo "  • Average discount"
echo ""
echo "=========================================="
echo ""

# Function to run query at a specific time
run_temporal_query() {
    local timestamp=$1
    local day_label=$2

    echo ""
    echo "┌─────────────────────────────────────────────────────────────────────────────┐"
    echo "│ $day_label: $timestamp                                           │"
    echo "└─────────────────────────────────────────────────────────────────────────────┘"
    echo ""

    psql -h "$HOST" -p "$PORT" -U postgres -d xtdb << EOF
SETTING DEFAULT VALID_TIME AS OF TIMESTAMP '$timestamp'
$BILLING_QUERY
EOF

    echo ""
}

# Run the billing query at different points in time

run_temporal_query "$DAY_1 23:59:59" "Day 1 - End of First Day"

run_temporal_query "$DAY_3 23:59:59" "Day 3 - After Initial Orders"

run_temporal_query "$DAY_5 23:59:59" "Day 5 - After Price Corrections"

run_temporal_query "$DAY_7 23:59:59" "Day 7 - Current State"

echo ""
echo "=========================================="
echo "Analysis Summary"
echo "=========================================="
echo ""
echo "What to observe:"
echo ""
echo "1. CUSTOMER BALANCES"
echo "   • Initial balances set on Day 1"
echo "   • May increase over time as corrections are applied"
echo ""
echo "2. ORDER COUNT"
echo "   • Day 1: ~1 order per customer (some customers created)"
echo "   • Day 3: ~2-3 orders per customer (progressive ordering)"
echo "   • Day 5: 3-4 orders per customer"
echo "   • Day 7: 4-5 orders per customer"
echo ""
echo "3. TOTAL REVENUE"
echo "   • Day 3: Revenue starts accumulating"
echo "   • Day 5: Revenue DECREASES for some customers due to price corrections"
echo "   • Day 7: Final revenue after all corrections"
echo ""
echo "4. AVERAGE PRICES & DISCOUNTS"
echo "   • Day 5+: Prices drop by ~5% due to billing corrections"
echo "   • Day 5+: Discounts increase by ~3% as adjustments"
echo ""
echo "This demonstrates:"
echo "  ✓ Temporal queries across multiple databases"
echo "  ✓ Late-arriving billing corrections (Day 5 corrections affect Day 3 data)"
echo "  ✓ Order lifecycle progression"
echo "  ✓ Price and discount adjustments"
echo "  ✓ SETTING DEFAULT VALID_TIME for point-in-time queries"
echo ""

# Additional detailed query showing specific changes
echo "=========================================="
echo "Detailed Price Evolution for Sample Customer"
echo "=========================================="
echo ""

psql -h "$HOST" -p "$PORT" -U postgres -d xtdb << 'EOF'
-- Show all versions of line items that have temporal history
SELECT
    l.l_orderkey,
    l.l_linenumber,
    l.l_extendedprice,
    l.l_discount,
    l._valid_from,
    l._valid_to,
    CASE
        WHEN l._valid_to = TIMESTAMP '9999-12-31 23:59:59.999999' THEN 'CURRENT'
        ELSE 'SUPERSEDED'
    END AS status
FROM
    lineitem_db.lineitem FOR ALL VALID_TIME l
WHERE
    l._valid_from < TIMESTAMP '2025-11-07'
ORDER BY
    l._valid_from
LIMIT 15;
EOF

echo ""

# Show order status changes
echo "=========================================="
echo "Order Status Evolution for Sample Orders"
echo "=========================================="
echo ""

psql -h "$HOST" -p "$PORT" -U postgres -d xtdb << 'EOF'
-- Show how order status and prices changed over time (backfilled data)
SELECT
    o.o_orderkey,
    o.o_orderstatus,
    o.o_totalprice,
    o._valid_from,
    o._valid_to,
    CASE
        WHEN o._valid_to = TIMESTAMP '9999-12-31 23:59:59.999999' THEN 'CURRENT'
        ELSE 'SUPERSEDED'
    END AS status
FROM
    orders_db.orders FOR ALL VALID_TIME o
WHERE
    o._valid_from < TIMESTAMP '2025-11-07'
ORDER BY
    o._valid_from
LIMIT 20;
EOF

echo ""
echo "=========================================="
echo "Report Complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "  • Compare revenue totals across different days"
echo "  • Notice how Day 5 corrections affect earlier transactions"
echo "  • Observe order status progressions (O → P → F)"
echo "  • Track price adjustments in line item history"
echo ""
echo "Try your own temporal queries:"
echo ""
echo "  psql -h $HOST -p $PORT -U postgres -d xtdb"
echo ""
echo "  SETTING DEFAULT VALID_TIME AS OF TIMESTAMP '$DAY_4 12:00:00'"
echo "  SELECT * FROM customer_db.customer LIMIT 5;"
echo ""
