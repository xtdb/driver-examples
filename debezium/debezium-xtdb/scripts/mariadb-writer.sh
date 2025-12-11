#!/bin/bash
# Continuously write to MySQL/MariaDB to generate CDC events

MYSQL_HOST="${MYSQL_HOST:-127.0.0.1}"
MYSQL_PORT="${MYSQL_PORT:-3306}"
MYSQL_USER="${MYSQL_USER:-cdc_user}"
MYSQL_PASS="${MYSQL_PASSWORD:-cdc_password}"
MYSQL_DB="${MYSQL_DATABASE:-accounts}"
INTERVAL="${INTERVAL:-2}"

# Use mariadb client (installed via system package manager)
if ! command -v mariadb &>/dev/null; then
    echo "ERROR: mariadb client not found - run 'mise run mysql-install' first"
    exit 1
fi

counter=100
column_counter=1
last_column_time=$(date +%s)
COLUMN_INTERVAL="${COLUMN_INTERVAL:-30}"

echo "Writing to MariaDB every ${INTERVAL}s (Ctrl+C to stop)"
echo "Adding new column every ${COLUMN_INTERVAL}s to demonstrate schema evolution"
echo "Host: $MYSQL_HOST:$MYSQL_PORT"
echo ""

mysql_cmd() {
    mariadb -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u "$MYSQL_USER" -p"$MYSQL_PASS" "$MYSQL_DB" -e "$1" 2>&1
}

add_column_if_due() {
    now=$(date +%s)
    elapsed=$((now - last_column_time))
    if [ $elapsed -ge $COLUMN_INTERVAL ]; then
        col_name="custom_field_${column_counter}"
        echo ""
        echo "[$(date +%H:%M:%S)] *** SCHEMA CHANGE: Adding column '$col_name' ***"
        mysql_cmd "ALTER TABLE users ADD COLUMN $col_name VARCHAR(100)"
        echo "[$(date +%H:%M:%S)] *** XTDB will automatically adapt to the new schema ***"
        echo ""
        column_counter=$((column_counter + 1))
        last_column_time=$now
    fi
}

while true; do
    add_column_if_due

    username="user_${counter}"
    email="${username}@example.com"

    # Cycle through INSERT, UPDATE, DELETE operations
    op=$((counter % 3))

    case $op in
        0)
            # INSERT new user (include value for latest custom field if any exist)
            echo "[$(date +%H:%M:%S)] INSERT: $username"
            if [ $column_counter -gt 1 ]; then
                latest_col="custom_field_$((column_counter - 1))"
                mysql_cmd "INSERT INTO users (id, email, username, created_at, $latest_col) VALUES ($counter, '$email', '$username', NOW(), 'value_${counter}')"
            else
                mysql_cmd "INSERT INTO users (id, email, username, created_at) VALUES ($counter, '$email', '$username', NOW())"
            fi
            ;;
        1)
            # UPDATE existing user (previous one)
            prev=$((counter - 1))
            echo "[$(date +%H:%M:%S)] UPDATE: user_${prev} -> verified"
            mysql_cmd "UPDATE users SET verified_at = NOW(), phone_number = '+1-555-${prev}' WHERE id = $prev"
            ;;
        2)
            # DELETE old user (2 back)
            old=$((counter - 2))
            echo "[$(date +%H:%M:%S)] DELETE: user_${old}"
            mysql_cmd "DELETE FROM users WHERE id = $old"
            ;;
    esac

    counter=$((counter + 1))
    sleep "$INTERVAL"
done
