#!/bin/bash

# Set proper permissions for XTDB volumes (UID 20000)
# This script ensures the XTDB container can write to its volumes

echo "Setting permissions for XTDB volumes..."

# Create directories if they don't exist and set permissions
if [ -d "/var/lib/xtdb" ]; then
    chown -R 20000:20000 /var/lib/xtdb
    echo "Permissions set for /var/lib/xtdb"
fi

if [ -d "/workspaces/logs" ]; then
    chown -R 20000:20000 /workspaces/logs
    echo "Permissions set for /workspaces/logs"
fi

echo "Volume initialization complete"