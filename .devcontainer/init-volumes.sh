#!/bin/sh

# Set proper permissions for XTDB volumes (UID 20000)
# This script ensures the XTDB container can write to its volumes

echo "Setting permissions for XTDB volumes..."

# Create and set permissions for XTDB data and logs directories
mkdir -p /var/lib/xtdb/data
mkdir -p /var/lib/xtdb/logs
chown -R 20000:20000 /var/lib/xtdb
echo "Permissions set for /var/lib/xtdb with data and logs subdirectories"

# Create symlink to log file in workspace directory
mkdir -p /workspaces/logs
if [ ! -L /workspaces/logs/xtdb.log ]; then
    ln -s /var/lib/xtdb/logs/xtdb.log /workspaces/logs/xtdb.log
    echo "Created symlink /workspaces/logs/xtdb.log -> /var/lib/xtdb/logs/xtdb.log"
fi

echo "Volume initialization complete"