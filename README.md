# XTDB Driver Examples

This [Dev Container](https://containers.dev/) environment is a [monorepo](https://github.com/xtdb/driver-examples) that is primarily intended for use with GitHub Codespaces. This environment showcases how to use [XTDB](https://xtdb.com/) from a variety of languages and runtimes in a convenient sandbox.

To very simply get started, without any local installation, create your own GitHub Codespace in the cloud and use the browser-based VS Code tooling:

[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/xtdb/driver-examples)

Once the Codespace loads (note: this can take several minutes), you can start running SQL queries against XTDB!

If you would like to run the Dev Container setup fully locally (offline, and without a GitHub account) you can use VS Code or docker-compose directly - see the instructions within `.devcontainer/README.md` for guidance.

## Getting Started with mise

Most language runtimes are pre-installed during container build via [mise](https://mise.jdx.dev). Each language example has its own directory with a `.mise.toml` file that defines available tasks.

### Running Examples

Navigate into any language directory and run `mise run` to pull all relevant dependencies and run the example.

To run all examples use `mise run run:all`.

## Running your first queries

The VSCode 'SQL Tools' extension is pre-configured to connect to the running XTDB Docker container. You can use this extension to explore the database and run queries.

Alternatively open the terminal and run: `psql -h xtdb xtdb`

You should now be able to execute a query like `SELECT 1 + 1`

## Plenty to explore

Within the `driver-examples` directory you will find a range of subdirectories for various languages, each containing a minimal example. All examples are connected to the same XTDB instance, running as an *ephemeral* Docker container within the sandbox environment (no data is persisted).

Feel free to hack on the examples - your Codespace is your own to explore!

## Help

Debug logs for the XTDB container can be found under `logs/xtdb.log`.

To view logs in real-time:
```bash
tail -f logs/xtdb.log
```

For any assistance or questions, please [open an issue](https://github.com/xtdb/driver-examples) or post on [the forums](https://discuss.xtdb.com/). PRs are welcome too!

### Running locally (and testing changes)

With Docker Compose installed, clone [this repo](https://github.com/xtdb/driver-examples) then run the following to start and access the container shell:

```bash
cd driver-examples/.devcontainer
docker compose build
docker compose up -d
./shell.sh
```

Once inside the container, use mise to install languages and run examples as described above.

Note that you will likely still need an internet connection after the containers have started to download dependencies.

When finished, you can shut down the containers using: `docker compose down`
