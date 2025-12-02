#!/bin/bash
# Pull latest xtdb image
docker-compose pull xtdb

# Start infrastructure services in detached mode (will stay running after ctrl-c)
docker-compose up -d init-volumes garage init-garage redpanda metabase

# Start xtdb in foreground (ctrl-c will stop only this)
docker-compose up xtdb
