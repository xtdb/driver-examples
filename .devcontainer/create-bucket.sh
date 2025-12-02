#!/bin/sh
set -e

if [ -z "$1" ]; then
  echo "Usage: $0 <bucket-name>"
  exit 1
fi

BUCKET_NAME="$1"
ACCESS_KEY="GK31c2f218bd3e1932929759c1"

docker exec garage /garage bucket create "$BUCKET_NAME" || echo "Bucket may already exist"
docker exec garage /garage bucket allow --read --write --key "$ACCESS_KEY" "$BUCKET_NAME"

echo "Bucket '$BUCKET_NAME' created and configured"
