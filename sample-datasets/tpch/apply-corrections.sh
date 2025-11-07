#!/usr/bin/env bash
set -euo pipefail

# Apply late-arriving corrections/adjustments to TPC-H data
# Simulates billing corrections, price adjustments, and data corrections

HOST=${1:-localhost}
PORT=${2:-5432}

echo "TPC-H Late-Arriving Corrections"
echo "================================"
echo "Host: $HOST"
echo "Port: $PORT"
echo ""
echo "Simulating billing corrections and data adjustments..."
echo "This demonstrates XTDB's temporal capabilities with late-arriving data."
echo ""

# Function to run updates on a specific database
run_updates() {
    local db_name=$1
    local table_name=$2
    local update_sql=$3

    echo "Applying corrections to ${db_name}.${table_name}..."

    psql -h "$HOST" -p "$PORT" -U postgres -d "$db_name" -c "$update_sql"

    if [ $? -eq 0 ]; then
        echo "✓ Corrections applied to ${db_name}"
    else
        echo "✗ Failed to apply corrections to ${db_name}"
        return 1
    fi
    echo ""
}

# 1. Customer corrections - update account balances (simulate billing adjustments)
echo "1. Customer Database: Adjusting account balances"
echo "-------------------------------------------------"
run_updates "customer_db" "customer" "
UPDATE customer
SET c_acctbal = c_acctbal * 1.05
WHERE _id IN (SELECT _id FROM customer LIMIT 100);
"

# 2. Orders corrections - update order priorities and dates
echo "2. Orders Database: Correcting order priorities and dates"
echo "----------------------------------------------------------"
run_updates "orders_db" "orders" "
UPDATE orders
SET o_orderpriority = '1-URGENT'
WHERE _id IN (SELECT _id FROM orders WHERE o_orderstatus = 'O' LIMIT 100);
"

# 3. LineItem corrections - adjust prices and discounts (billing corrections)
echo "3. LineItem Database: Applying price and discount corrections"
echo "--------------------------------------------------------------"
run_updates "lineitem_db" "lineitem" "
UPDATE lineitem
SET
    l_extendedprice = l_extendedprice * 0.98,
    l_discount = LEAST(l_discount + 0.02, 0.10)
WHERE _id IN (SELECT _id FROM lineitem LIMIT 100);
"

# 4. Supplier corrections - update account balances
echo "4. Supplier Database: Adjusting supplier account balances"
echo "----------------------------------------------------------"
run_updates "supplier_db" "supplier" "
UPDATE supplier
SET s_acctbal = s_acctbal * 1.03
WHERE _id IN (SELECT _id FROM supplier LIMIT 100);
"

# 5. PartSupp corrections - adjust supply costs
echo "5. PartSupp Database: Correcting supply costs"
echo "----------------------------------------------"
run_updates "partsupp_db" "partsupp" "
UPDATE partsupp
SET ps_supplycost = ps_supplycost * 0.97
WHERE _id IN (SELECT _id FROM partsupp LIMIT 100);
"

# 6. Part corrections - update retail prices
echo "6. Part Database: Adjusting retail prices"
echo "------------------------------------------"
run_updates "part_db" "part" "
UPDATE part
SET p_retailprice = p_retailprice * 1.02
WHERE _id IN (SELECT _id FROM part LIMIT 100);
"

# 7. Nation corrections - update comments (simulate metadata corrections)
echo "7. Nation Database: Updating nation metadata"
echo "---------------------------------------------"
run_updates "nation_db" "nation" "
UPDATE nation
SET n_comment = n_comment || ' [Verified 2024]'
WHERE _id IN (SELECT _id FROM nation LIMIT 5);
"

# 8. Region corrections - update comments
echo "8. Region Database: Updating region metadata"
echo "---------------------------------------------"
run_updates "region_db" "region" "
UPDATE region
SET r_comment = r_comment || ' [Updated]'
WHERE _id IN (SELECT _id FROM region LIMIT 5);
"

echo ""
echo "========================================"
echo "✓ All corrections applied successfully!"
echo "========================================"
echo ""
echo "Summary of corrections:"
echo "  - customer_db: 100 rows - Account balance adjustments (+5%)"
echo "  - orders_db: ~100 rows - Order priority corrections"
echo "  - lineitem_db: ~600 rows - Price corrections (-2%) and discount increases"
echo "  - supplier_db: 100 rows - Account balance adjustments (+3%)"
echo "  - partsupp_db: ~100 rows - Supply cost corrections (-3%)"
echo "  - part_db: 100 rows - Retail price adjustments (+2%)"
echo "  - nation_db: 5 rows - Metadata verification updates"
echo "  - region_db: 5 rows - Metadata updates"
echo ""
echo "These corrections demonstrate:"
echo "  ✓ Late-arriving billing adjustments"
echo "  ✓ Cross-database temporal updates"
echo "  ✓ Price and discount corrections"
echo "  ✓ Metadata verification updates"
echo ""
echo "You can now query historical data using XTDB's temporal features:"
echo "  - FOR VALID_TIME AS OF <timestamp>"
echo "  - FOR ALL VALID_TIME"
echo "  - FOR VALID_TIME FROM <start> TO <end>"
