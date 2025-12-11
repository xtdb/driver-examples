#!/bin/bash
# Poll XTDB to show CDC updates flowing in

XTDB_HOST="${XTDB_HOST:-xtdb}"
XTDB_PORT="${XTDB_PORT:-5432}"
INTERVAL="${INTERVAL:-2}"

prev_count=0

echo "Polling XTDB every ${INTERVAL}s (Ctrl+C to stop)"
echo "Host: $XTDB_HOST:$XTDB_PORT"
echo ""

while true; do
    # Get current count
    count=$(psql -h "$XTDB_HOST" -p "$XTDB_PORT" -U xtdb -d xtdb -t -A -c "SELECT COUNT(*) FROM users" 2>/dev/null)

    if [ -n "$count" ]; then
        # Get 3 most recent users
        recent=$(psql -h "$XTDB_HOST" -p "$XTDB_PORT" -U xtdb -d xtdb -t -A -c \
            "SELECT username FROM users FOR ALL VALID_TIME ORDER BY _valid_from DESC LIMIT 3" 2>/dev/null | tr '\n' ', ' | sed 's/,$//')

        # Highlight if count changed
        if [ "$count" != "$prev_count" ]; then
            change=$((count - prev_count))
            if [ "$prev_count" -eq 0 ]; then
                printf "[%s] Users: %s | Recent: %s\n" "$(date +%H:%M:%S)" "$count" "$recent"
            elif [ "$change" -gt 0 ]; then
                printf "[%s] Users: %s (+%s) | Recent: %s\n" "$(date +%H:%M:%S)" "$count" "$change" "$recent"
            else
                printf "[%s] Users: %s (%s) | Recent: %s\n" "$(date +%H:%M:%S)" "$count" "$change" "$recent"
            fi
            prev_count=$count
        else
            printf "[%s] Users: %s | Recent: %s\n" "$(date +%H:%M:%S)" "$count" "$recent"
        fi
    else
        printf "[%s] ERROR: Could not connect to XTDB\n" "$(date +%H:%M:%S)"
    fi

    sleep "$INTERVAL"
done
