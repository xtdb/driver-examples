#!/usr/bin/env bash
set -euo pipefail

# Backfill temporal TPC-H data evolving daily over the past week
# Demonstrates XTDB's bitemporal capabilities for billing scenarios

HOST=${1:-localhost}
PORT=${2:-5432}

echo "TPC-H Temporal Data Backfill"
echo "============================"
echo "Host: $HOST"
echo "Port: $PORT"
echo ""
echo "Backfilling data with daily evolution over the past week..."
echo "Using XTDB's _valid_from column for bitemporal tracking."
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

echo "Backfilling data from $DAY_1 to $DAY_7"
echo ""

# Function to insert temporal data
insert_temporal() {
    local db_name=$1
    local sql=$2

    psql -h "$HOST" -p "$PORT" -U postgres -d "$db_name" -c "$sql" > /dev/null

    if [ $? -ne 0 ]; then
        echo "✗ Failed to insert into ${db_name}"
        return 1
    fi
}

# 1. Customer evolution - progressive account activity
echo "1. Customer Database: Backfilling customer account evolution"
echo "-------------------------------------------------------------"

for i in {1..7}; do
    day_var="DAY_$i"
    valid_date="${!day_var}"
    base_key=$((10000 + i * 100))

    echo "  Day $i ($valid_date): Inserting 20 customers with initial balances"

    # Generate UUIDs for each customer
    customer_values=""
    for n in {1..20}; do
        uuid=$(uuidgen | tr '[:upper:]' '[:lower:]')
        custkey=$((base_key + n))
        acctbal=$(echo "5000.00 + ($n * 100) + ($i * 50)" | bc)
        nationkey=$((n % 25))
        segment_idx=$((n % 5))
        case $segment_idx in
            0) segment="AUTOMOBILE" ;;
            1) segment="BUILDING" ;;
            2) segment="FURNITURE" ;;
            3) segment="MACHINERY" ;;
            *) segment="HOUSEHOLD" ;;
        esac

        if [ -n "$customer_values" ]; then
            customer_values="${customer_values},"
        fi
        customer_values="${customer_values}('${uuid}', ${custkey}, 'Customer#${custkey}', 'Address Line ${n}, Building $((i * 10 + n))', ${nationkey}, '15-$((100 + n))-$((1000 + n))', ${acctbal}, '${segment}', 'Customer created on day ${i}', TIMESTAMP '${valid_date} 00:00:00')"
    done

    insert_temporal "customer_db" "
    INSERT INTO customer (_id, c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment, _valid_from)
    VALUES ${customer_values};
    "
done

echo "✓ Customer backfill complete (140 customers across 7 days)"
echo ""

# 2. Orders evolution - daily order placement and status changes
echo "2. Orders Database: Backfilling daily orders and status evolution"
echo "------------------------------------------------------------------"

for i in {1..7}; do
    day_var="DAY_$i"
    valid_date="${!day_var}"
    base_orderkey=$((100000 + i * 1000))
    base_custkey=$((10000 + i * 100))

    echo "  Day $i ($valid_date): 30 new orders"

    # Generate UUIDs for each order
    order_values=""
    for n in {1..30}; do
        uuid=$(uuidgen | tr '[:upper:]' '[:lower:]')
        orderkey=$((base_orderkey + n))
        custkey=$((base_custkey + (n % 20) + 1))
        totalprice=$(echo "10000.00 + ($n * 500) + ($i * 1000)" | bc)
        status_idx=$((n % 3))
        case $status_idx in
            0) status="F" ;;
            1) status="O" ;;
            *) status="P" ;;
        esac
        priority_idx=$((n % 5))
        case $priority_idx in
            0) priority="1-URGENT" ;;
            1) priority="2-HIGH" ;;
            2) priority="3-MEDIUM" ;;
            3) priority="4-NOT SPECIFIED" ;;
            *) priority="5-LOW" ;;
        esac
        clerk_num=$(( (n % 100) + 1 ))
        shippriority=$((n % 2))

        if [ -n "$order_values" ]; then
            order_values="${order_values},"
        fi
        order_values="${order_values}('${uuid}', ${orderkey}, ${custkey}, '${status}', ${totalprice}, DATE '${valid_date}', '${priority}', 'Clerk#${clerk_num}', ${shippriority}, 'Order placed on day ${i} of backfill', TIMESTAMP '${valid_date} 08:00:00')"
    done

    insert_temporal "orders_db" "
    INSERT INTO orders (_id, o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment, _valid_from)
    VALUES ${order_values};
    "

    # Some orders get status updates on later days
    if [ $i -lt 7 ]; then
        next_i=$((i + 1))
        next_day_var="DAY_$next_i"
        next_date="${!next_day_var}"

        echo "  Day $next_i ($next_date): Updating 10 orders from day $i"

        insert_temporal "orders_db" "
        INSERT INTO orders (_id, o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment, _valid_from)
        SELECT
            _id,
            o_orderkey,
            o_custkey,
            CASE o_orderstatus
                WHEN 'O' THEN 'P'
                WHEN 'P' THEN 'F'
                ELSE o_orderstatus
            END,
            o_totalprice * 1.05,
            o_orderdate,
            o_orderpriority,
            o_clerk,
            o_shippriority,
            o_comment || ' - Updated on day $next_i',
            TIMESTAMP '$next_date 14:00:00'
        FROM orders
        WHERE _id IN (
            SELECT _id FROM orders
            WHERE o_orderdate = DATE '$valid_date'
            LIMIT 10
        );
        "
    fi
done

echo "✓ Orders backfill complete (210 orders + ~60 status updates)"
echo ""

# 3. LineItem evolution - daily line items with price changes
echo "3. LineItem Database: Backfilling line items with price evolution"
echo "------------------------------------------------------------------"

for i in {1..7}; do
    day_var="DAY_$i"
    valid_date="${!day_var}"
    base_orderkey=$((100000 + i * 1000))

    echo "  Day $i ($valid_date): 90 line items (3 per order)"

    # Generate UUIDs for each line item
    lineitem_values=""
    for n in {1..90}; do
        uuid=$(uuidgen | tr '[:upper:]' '[:lower:]')
        orderkey=$((base_orderkey + ((n - 1) / 3)))
        partkey=$((1000 + (n % 200)))
        suppkey=$((100 + (n % 100)))
        linenumber=$(( ((n - 1) % 3) + 1 ))
        quantity=$(( (n % 50) + 1 ))
        extendedprice=$(echo "(50.00 + ($n * 10)) * (($n % 50) + 1)" | bc)
        discount=$(echo "scale=2; ($n % 10) / 100.0" | bc)
        tax=$(echo "scale=2; ($n % 8) / 100.0" | bc)

        if [ $((n % 4)) -eq 0 ]; then returnflag="R"; else returnflag="N"; fi
        if [ $((n % 3)) -eq 0 ]; then linestatus="O"; else linestatus="F"; fi

        instruct_idx=$((n % 4))
        case $instruct_idx in
            0) shipinstruct="DELIVER IN PERSON" ;;
            1) shipinstruct="COLLECT COD" ;;
            2) shipinstruct="NONE" ;;
            *) shipinstruct="TAKE BACK RETURN" ;;
        esac

        mode_idx=$((n % 7))
        case $mode_idx in
            0) shipmode="TRUCK" ;;
            1) shipmode="MAIL" ;;
            2) shipmode="REG AIR" ;;
            3) shipmode="AIR" ;;
            4) shipmode="RAIL" ;;
            5) shipmode="SHIP" ;;
            *) shipmode="FOB" ;;
        esac

        if [ -n "$lineitem_values" ]; then
            lineitem_values="${lineitem_values},"
        fi
        lineitem_values="${lineitem_values}('${uuid}', ${orderkey}, ${partkey}, ${suppkey}, ${linenumber}, ${quantity}, ${extendedprice}, ${discount}, ${tax}, '${returnflag}', '${linestatus}', DATE '${valid_date}' + INTERVAL '7' DAY, DATE '${valid_date}' + INTERVAL '5' DAY, DATE '${valid_date}' + INTERVAL '10' DAY, '${shipinstruct}', '${shipmode}', 'Line item created day ${i}', TIMESTAMP '${valid_date} 09:00:00')"
    done

    insert_temporal "lineitem_db" "
    INSERT INTO lineitem (_id, l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment, _valid_from)
    VALUES ${lineitem_values};
    "

    # Apply pricing corrections to earlier line items
    if [ $i -ge 3 ]; then
        correction_day=$((i - 2))
        correction_orderkey=$((100000 + correction_day * 1000))

        echo "  Day $i ($valid_date): Price corrections for day $correction_day items"

        insert_temporal "lineitem_db" "
        INSERT INTO lineitem (_id, l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment, _valid_from)
        SELECT
            _id,
            l_orderkey,
            l_partkey,
            l_suppkey,
            l_linenumber,
            l_quantity,
            l_extendedprice * 0.95,
            LEAST(l_discount + 0.03, 0.10),
            l_tax,
            l_returnflag,
            l_linestatus,
            l_shipdate,
            l_commitdate,
            l_receiptdate,
            l_shipinstruct,
            l_shipmode,
            l_comment || ' - Price corrected on day $i',
            TIMESTAMP '$valid_date 16:00:00'
        FROM lineitem
        WHERE _id IN (
            SELECT _id FROM lineitem
            LIMIT 20
        );
        "
    fi
done

echo "✓ LineItem backfill complete (630 line items + ~100 price corrections)"
echo ""

# 4. Supplier account evolution
echo "4. Supplier Database: Backfilling supplier account changes"
echo "-----------------------------------------------------------"

for i in {1..7}; do
    day_var="DAY_$i"
    valid_date="${!day_var}"

    if [ $i -eq 1 ]; then
        echo "  Day $i ($valid_date): Creating 50 suppliers"

        # Generate UUIDs for each supplier
        supplier_values=""
        for n in {1..50}; do
            uuid=$(uuidgen | tr '[:upper:]' '[:lower:]')
            suppkey=$((20000 + n))
            acctbal=$(echo "1000.00 + ($n * 50)" | bc)
            nationkey=$((n % 25))

            if [ -n "$supplier_values" ]; then
                supplier_values="${supplier_values},"
            fi
            supplier_values="${supplier_values}('${uuid}', ${suppkey}, 'Supplier#${suppkey}', 'Supplier Address ${n}, Suite $((n * 10))', ${nationkey}, '25-$((200 + n))-$((2000 + n))', ${acctbal}, 'Supplier established day 1', TIMESTAMP '${valid_date} 00:00:00')"
        done

        insert_temporal "supplier_db" "
        INSERT INTO supplier (_id, s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment, _valid_from)
        VALUES ${supplier_values};
        "
    else
        # Update supplier balances each day
        echo "  Day $i ($valid_date): Updating 20 supplier account balances"

        insert_temporal "supplier_db" "
        INSERT INTO supplier (_id, s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment, _valid_from)
        SELECT
            _id,
            s_suppkey,
            s_name,
            s_address,
            s_nationkey,
            s_phone,
            s_acctbal + (100.00 * $i),
            s_comment || ' - Balance updated day $i',
            TIMESTAMP '$valid_date 12:00:00'
        FROM supplier
        WHERE _id IN (
            SELECT _id FROM supplier
            LIMIT 20
        );
        "
    fi
done

echo "✓ Supplier backfill complete (50 suppliers + ~120 balance updates)"
echo ""

# 5. Part price evolution
echo "5. Part Database: Backfilling part price changes"
echo "-------------------------------------------------"

for i in {1..7}; do
    day_var="DAY_$i"
    valid_date="${!day_var}"

    if [ $i -eq 1 ]; then
        echo "  Day $i ($valid_date): Creating 100 parts"

        # Generate UUIDs for each part
        part_values=""
        for n in {1..100}; do
            uuid=$(uuidgen | tr '[:upper:]' '[:lower:]')
            partkey=$((30000 + n))
            material_idx=$((n % 5))
            case $material_idx in
                0) material="steel" ;;
                1) material="aluminum" ;;
                2) material="brass" ;;
                3) material="copper" ;;
                *) material="nickel" ;;
            esac
            mfgr_num=$(( (n % 5) + 1 ))
            type_idx=$((n % 5))
            case $type_idx in
                0) type_prefix="STANDARD POLISHED" ;;
                1) type_prefix="SMALL BRUSHED" ;;
                2) type_prefix="MEDIUM BURNISHED" ;;
                3) type_prefix="LARGE PLATED" ;;
                *) type_prefix="ECONOMY ANODIZED" ;;
            esac
            type_material_idx=$((n % 3))
            case $type_material_idx in
                0) type_material="BRASS" ;;
                1) type_material="STEEL" ;;
                *) type_material="COPPER" ;;
            esac
            size=$(( (n % 50) + 1 ))
            container_idx=$((n % 7))
            case $container_idx in
                0) container="SM CASE" ;;
                1) container="SM BOX" ;;
                2) container="SM BAG" ;;
                3) container="SM JAR" ;;
                4) container="SM PACK" ;;
                5) container="SM PKG" ;;
                *) container="SM DRUM" ;;
            esac
            retailprice=$(echo "900.00 + ($n * 10)" | bc)

            if [ -n "$part_values" ]; then
                part_values="${part_values},"
            fi
            part_values="${part_values}('${uuid}', ${partkey}, 'Part ${partkey} ${material}', 'Manufacturer#${mfgr_num}', 'Brand#${mfgr_num}${mfgr_num}', '${type_prefix} ${type_material}', ${size}, '${container}', ${retailprice}, 'Part introduced day 1', TIMESTAMP '${valid_date} 00:00:00')"
        done

        insert_temporal "part_db" "
        INSERT INTO part (_id, p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment, _valid_from)
        VALUES ${part_values};
        "
    else
        # Daily price adjustments
        echo "  Day $i ($valid_date): Adjusting 30 part prices"

        insert_temporal "part_db" "
        INSERT INTO part (_id, p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment, _valid_from)
        SELECT
            _id,
            p_partkey,
            p_name,
            p_mfgr,
            p_brand,
            p_type,
            p_size,
            p_container,
            p_retailprice * (1.00 + (0.01 * $i)),
            p_comment || ' - Price adjusted day $i',
            TIMESTAMP '$valid_date 10:00:00'
        FROM part
        WHERE _id IN (
            SELECT _id FROM part
            LIMIT 30
        );
        "
    fi
done

echo "✓ Part backfill complete (100 parts + ~180 price adjustments)"
echo ""

echo ""
echo "========================================"
echo "✓ Temporal backfill complete!"
echo "========================================"
echo ""
echo "Summary of temporal data:"
echo "  - customer_db: 140 customers created progressively over 7 days"
echo "  - orders_db: 210 orders + ~60 status evolution records"
echo "  - lineitem_db: 630 line items + ~100 price correction records"
echo "  - supplier_db: 50 suppliers + ~120 account balance updates"
echo "  - part_db: 100 parts + ~180 price adjustment records"
echo ""
echo "Total temporal records: ~1,500 across 7 days"
echo ""
echo "Temporal query examples:"
echo ""
echo "  # View customer balances as of 3 days ago:"
echo "  SELECT c_custkey, c_name, c_acctbal"
echo "  FROM customer_db.customer"
echo "  FOR VALID_TIME AS OF TIMESTAMP '$DAY_4 12:00:00';"
echo ""
echo "  # View all price changes for a line item:"
echo "  SELECT l_orderkey, l_linenumber, l_extendedprice, _valid_from, _valid_to"
echo "  FROM lineitem_db.lineitem"
echo "  FOR ALL VALID_TIME"
echo "  WHERE l_orderkey = 101000"
echo "  ORDER BY _valid_from;"
echo ""
echo "  # View orders in a specific time window:"
echo "  SELECT o_orderkey, o_orderstatus, o_totalprice, _valid_from"
echo "  FROM orders_db.orders"
echo "  FOR VALID_TIME FROM TIMESTAMP '$DAY_2 00:00:00' TO TIMESTAMP '$DAY_5 23:59:59';"
