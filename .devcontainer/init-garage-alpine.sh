#!/bin/sh
set -e

# Static credentials for sandbox environment
ACCESS_KEY="GK31c2f218bd3e1932929759c1"
SECRET_KEY="b8e1ec4d832d1038fb34242fc0f8e4f1ee8e0ce00fc1be1f12e28550b060c2d5"
RPC_SECRET="1799bccfd7411eddcf9ebd316bc1f5287ad12a68094e1c6ac6abde7e6feae1ec"

echo "Installing dependencies..."
apk add --no-cache curl

echo "Downloading garage CLI..."
cd /tmp
curl -sL https://garagehq.deuxfleurs.fr/_releases/v2.1.0/x86_64-unknown-linux-musl/garage > /tmp/garage
chmod +x /tmp/garage

echo "Waiting for Garage to be ready..."
sleep 5

# Try to connect to the running garage instance using the config file
until /tmp/garage -c /config/garage.toml status 2>/dev/null; do
  echo "Garage not ready yet, waiting..."
  sleep 2
done

echo "Garage is ready, checking cluster setup..."

# Get node ID from the local garage instance
NODE_ID=$(/tmp/garage -c /config/garage.toml status 2>/dev/null | grep -E "^[a-f0-9]{16}" | head -1 | awk '{print $1}')

if [ -z "$NODE_ID" ]; then
  echo "ERROR: Could not get node ID"
  exit 1
fi

echo "Found node ID: $NODE_ID"

# Check if layout is configured
if ! /tmp/garage -c /config/garage.toml layout show | grep -q "$NODE_ID"; then
  echo "Setting up Garage layout..."
  /tmp/garage -c /config/garage.toml layout assign -z dc1 -c 1G "$NODE_ID"
  /tmp/garage -c /config/garage.toml layout apply --version 1
  echo "Layout configured, waiting for it to stabilize..."
  sleep 5
else
  echo "Layout already configured"
fi

# Import key (use --yes to force reimport of specific credentials)
echo "Importing access key..."
/tmp/garage -c /config/garage.toml key import "$ACCESS_KEY" "$SECRET_KEY" -n xtdb --yes 2>/dev/null || {
  echo "Import failed, trying to create key..."
  # If import fails, the key might not exist yet, so create it
  /tmp/garage -c /config/garage.toml key create xtdb 2>/dev/null || echo "Key creation also failed, key may already exist"
}

# Create xtdb-storage bucket
echo "Creating bucket 'xtdb-storage'..."
/tmp/garage -c /config/garage.toml bucket create xtdb-storage || echo "Bucket may already exist"

# Allow key to access bucket
echo "Granting permissions..."
/tmp/garage -c /config/garage.toml bucket allow --read --write --key "$ACCESS_KEY" xtdb-storage || true

echo ""
echo "Garage setup complete!"
echo "Bucket created: xtdb-storage"
echo "Using static sandbox credentials"
