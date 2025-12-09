#!/bin/bash
# Useful script for local development setup

# Pull latest xtdb image
docker-compose pull xtdb

# Start infrastructure services in detached mode (will stay running after ctrl-c)
docker-compose up -d init-volumes garage garage-webui init-garage redpanda metabase app

# Start xtdb in foreground (ctrl-c will stop only this)
docker-compose up xtdb
