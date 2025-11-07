#!/usr/bin/env bash
set -euo pipefail

# Reset Redpanda and Garage storage volumes
# Shuts down these services, wipes their volumes, and restarts them

echo "=========================================="
echo "Reset Redpanda & Garage Storage"
echo "=========================================="
echo ""
echo "This will:"
echo "  1. Stop Redpanda and Garage services"
echo "  2. Remove their data volumes"
echo "  3. Restart the services with clean storage"
echo ""
echo "Other services (XTDB, app) will continue running."
echo ""

# Stop Redpanda and Garage
echo "Stopping Redpanda and Garage..."
docker-compose stop redpanda garage

# Remove their containers
echo "Removing containers..."
docker-compose rm -f redpanda garage

# Remove their volumes
echo "Removing data volumes..."
docker volume rm devcontainer_redpanda-data 2>/dev/null || echo "  (redpanda-data volume not found)"
docker volume rm devcontainer_garage-data 2>/dev/null || echo "  (garage-data volume not found)"
docker volume rm devcontainer_garage-meta 2>/dev/null || echo "  (garage-meta volume not found)"

echo ""
echo "✓ Storage wiped"
echo ""

# Restart services
echo "Starting Redpanda and Garage..."
docker-compose up -d redpanda garage

echo ""
echo "Waiting for services to be healthy..."
echo ""

# Wait for Redpanda to be healthy
echo -n "Waiting for Redpanda"
for i in {1..30}; do
    if docker-compose ps redpanda | grep -q "healthy"; then
        echo " ✓"
        break
    fi
    echo -n "."
    sleep 1
done

# Wait for Garage to be healthy
echo -n "Waiting for Garage"
for i in {1..30}; do
    if docker-compose ps garage | grep -q "healthy"; then
        echo " ✓"
        break
    fi
    echo -n "."
    sleep 1
done

echo ""
echo "Running Garage initialization..."
docker-compose up -d init-garage
docker-compose wait init-garage

echo ""
echo "=========================================="
echo "✓ Reset Complete!"
echo "=========================================="
echo ""
echo "Redpanda and Garage are now running with clean storage."
echo ""
echo "Next steps:"
echo "  1. Restart XTDB to reconnect: docker-compose restart xtdb"
echo "  2. Re-run setup-tpch-dbs.sh to recreate databases"
echo "  3. Re-run backfill scripts to populate data"
echo ""
