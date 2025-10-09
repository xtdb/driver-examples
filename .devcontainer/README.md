# XTDB Dev Container

A [Dev Container](https://containers.dev) that you can use to try [XTDB](https://xtdb.com).

[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/xtdb/xtdb)

Once the Codespace loads, you can start running SQL queries against XTDB!

Use the VSCode 'SQL Tools' extension (connecting to the pre-installed XTDB connection).

## Useful commands for running locally

Note that your local repo root directory will be mounted to the `/workspaces/driver-examples` directory within the `app` container (matching GitHub Codespaces behavior), so that you can easily test and commit code changes.

```bash
# start in the foreground, with xtdb logs
docker-compose up -d --build app && docker-compose up --build xtdb

# or, start in the background
docker-compose up -d --build xtdb app

# view logs
docker-compose logs --follow xtdb

# enter the app container's shell
docker exec -it --user codespace app /bin/bash

# once inside, cd into a language directory and run examples
cd /workspaces/driver-examples/python
mise run

# shutdown
docker-compose down

# copy the database into a zip on the host
sudo zip -r xtdb-data.zip /var/lib/docker/volumes/devcontainer_xtdb-data/_data

# copy the log file
sudo cp /var/lib/docker/volumes/devcontainer_xtdb-logs/_data/xtdb.log .

# pull the latest xtdb/xtdb:edge image
docker compose pull xtdb

# cleanup existing volumes and containers
docker-compose down --volumes --remove-orphans
```

## Claude Code - useful for iterating on this project

A helpful command for running inside the container as a poor man's sandbox:

```bash
./shell.sh # open a shell within the app container
# within that shell, run a fresh Claude login with max permissions (use at your own risk!!)
npm install -g @anthropic-ai/claude-code && claude --dangerously-skip-permissions --resume
```
