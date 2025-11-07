#!/bin/sh
set -e

# Static credentials for sandbox environment
ACCESS_KEY="GK31c2f218bd3e1932929759c1"
SECRET_KEY="b8e1ec4d832d1038fb34242fc0f8e4f1ee8e0ce00fc1be1f12e28550b060c2d5"

echo "Waiting for Garage to be ready..."
sleep 3

# Try to connect to the running garage instance
until garage -h garage:3900 status 2>/dev/null; do
  echo "Garage not ready yet, waiting..."
  sleep 2
done

echo "Garage is ready, checking cluster setup..."

# Get node ID from the remote garage instance
NODE_ID=$(garage -h garage:3900 status 2>/dev/null | grep -E "^[a-f0-9]{64}" | head -1 | awk '{print $1}')

if [ -z "$NODE_ID" ]; then
  echo "ERROR: Could not get node ID"
  exit 1
fi

echo "Found node ID: $NODE_ID"

# Check if layout is configured
if ! garage -h garage:3900 layout show | grep -q "$NODE_ID"; then
  echo "Setting up Garage layout..."
  garage -h garage:3900 layout assign -z dc1 -c 1 "$NODE_ID"
  garage -h garage:3900 layout apply --version 1
  echo "Layout configured, waiting for it to stabilize..."
  sleep 3
else
  echo "Layout already configured"
fi

# Import key
echo "Importing access key..."
garage -h garage:3900 key import "$ACCESS_KEY" "$SECRET_KEY" xtdb || echo "Key may already exist"

# Create xtdb-storage bucket
echo "Creating bucket 'xtdb-storage'..."
garage -h garage:3900 bucket create xtdb-storage || echo "Bucket may already exist"

# Allow key to access bucket
echo "Granting permissions..."
garage -h garage:3900 bucket allow --read --write xtdb-storage --key "$ACCESS_KEY" || true

echo ""
echo "Garage setup complete!"
echo "Bucket created: xtdb-storage"
echo "Using static sandbox credentials"
