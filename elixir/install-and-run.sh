#!/bin/sh

if [ "$(id -u)" -ne 0 ]; then
        echo 'This script must be run by root - run `sudo su` first' >&2
        exit 1
fi

apt-get update
apt-get install -y --no-install-recommends build-essential autoconf m4 libncurses5-dev libgl1-mesa-dev libglu1-mesa-dev libpng-dev libssh-dev unixodbc-dev xsltproc fop # libwxgtk3.0-dev
asdf plugin-add erlang
asdf plugin-add elixir
asdf install erlang 27.2
asdf install elixir 1.18.0
asdf global erlang 27.2
asdf global elixir 1.18.0

elixir -v
mix deps.get
mix run -e "XTDBExample.connect_and_query()"
