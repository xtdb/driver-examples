#!/bin/sh

# Set proper permissions for XTDB volumes (UID 20000)
# This script ensures the XTDB container can write to its volumes

echo "Setting permissions for XTDB volumes..."

# Create and set permissions for XTDB data directory
mkdir -p /var/lib/xtdb
chown -R 20000:20000 /var/lib/xtdb
echo "Permissions set for /var/lib/xtdb"

# Create and set permissions for logs directory
mkdir -p /workspaces/logs
chown -R 20000:20000 /workspaces/logs
echo "Permissions set for /workspaces/logs"

echo "Volume initialization complete"