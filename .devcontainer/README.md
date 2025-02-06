# XTDB Dev Container

A [Dev Container](https://containers.dev) that you can use to try [XTDB](https://xtdb.com).

[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/xtdb/xtdb)

Once the Codespace loads, you can start running SQL queries against XTDB!

Use the VSCode 'SQL Tools' extension (connecting to the pre-installed XTDB connection).

## Useful commands for running locally

Note that your local repo root directory will be mounted to the `/workspaces` directory within the `app` container by default, so that you easily test and commit code changes.

```bash
# start in the foreground, with xtdb logs
docker-compose up -d --build app && docker-compose up --build xtdb

# or, start in the background
docker-compose up -d --build xtdb app

# view logs
docker-compose logs --follow xtdb

# enter the app container's shell
docker exec -it --user codespace app /bin/bash

# shutdown
docker-compose down
```
