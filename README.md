# XTDB Driver Examples

This [Dev Container](https://containers.dev/) environment is a [monorepo](https://github.com/xtdb/driver-examples) that is primarily intended for use with GitHub Codespaces. This environment showcases how to use [XTDB](https://xtdb.com/) from a variety of languages and runtimes in a convenient sandbox.

To very simply get started, without any local installation, create your own GitHub Codespace in the cloud and use the browser-based VS Code tooling:

[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/xtdb/driver-examples)

Once the Codespace loads (note: this can take several minutes), you can start running SQL queries against XTDB!

## Running your first queries

The VSCode 'SQL Tools' extension is pre-configured to connects to the running XTDB Docker container. You can use this extension to explore the database and run queries.

Alternatively open the terminal and run: `psql -h xtdb xtdb`

You should now be able to execute a query like `SELECT 1 + 1`

## Plenty to explore

Within the `driver-examples` directory you will find a range of subdirectories for various languages, where each contains a minimal example as well as a `run.sh` script that you can use to execute the example (the script will also first install the dependencies).

All examples are connected to the same XTDB instance, running as an *ephemeral* Docker container within the sandbox environment (no data is persisted).

Feel free to hack on the examples - your Codespace is your own to explore!

## Help

Debug logs for the XTDB container can be found under `logs/xtdb.log`.

For any assistance or questions, please [open an issue](https://github.com/xtdb/driver-examples) or post on [the forums](https://discuss.xtdb.com/). PRs are welcome too!

### Running locally (and testing changes)

With Docker Compose installed, clone [this repo](https://github.com/xtdb/driver-examples) then run:

`docker compose build`
`docker compose up -d`
`docker exec -it --user codespace app /bin/bash`

Note that you will likely still need an internet connection after the containers have started to download dependencies.

When finished, you can shut down the containers using: `docker compose down`

### Use `asdf` to manage additional installations

Run `sudo su`

The `asdf` command should now be available to install new things.

e.g. Erlang/Elixir
```bash
asdf plugin-add erlang
asdf plugin-add elixir

# optional build deps
# apt-get install build-essential autoconf m4 libncurses5-dev libwxgtk3.0-dev libgl1-mesa-dev libglu1-mesa-dev libpng-dev libssh-dev unixodbc-dev xsltproc fop

asdf install erlang 25.2.3
asdf install elixir 1.14.3-otp-25

asdf global erlang 25.2.3
asdf global elixir 1.14.3-otp-25

elixir -v
```
