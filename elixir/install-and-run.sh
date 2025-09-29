#!/bin/bash

set -e  # Exit on error

# Get the original user who ran sudo (if any)
ORIG_USER=${SUDO_USER:-$(whoami)}
ORIG_HOME=$(getent passwd "$ORIG_USER" | cut -d: -f6)

# If already root and no SUDO_USER, ask to run as non-root
if [ "$(id -u)" -eq 0 ] && [ -z "$SUDO_USER" ]; then
        echo 'This script should be run with sudo, not as root directly' >&2
        echo 'Usage: sudo ./install-and-run.sh' >&2
        exit 1
fi

if [ "$(id -u)" -ne 0 ]; then
        echo 'This script must be run with sudo' >&2
        exit 1
fi

echo "Installing system dependencies..."
apt-get update
apt-get install -y --no-install-recommends build-essential autoconf m4 libncurses5-dev libgl1-mesa-dev libglu1-mesa-dev libpng-dev libssh-dev unixodbc-dev xsltproc fop wget curl git

echo "Setting up Elixir for user: $ORIG_USER"

# Check if asdf is already installed
ASDF_INSTALLED=false
if [ -f "$ORIG_HOME/.asdf/asdf.sh" ]; then
    echo "asdf already installed at $ORIG_HOME/.asdf"
    ASDF_INSTALLED=true
elif [ -f "/opt/asdf-vm/asdf.sh" ]; then
    echo "asdf already installed at /opt/asdf-vm"
    ASDF_INSTALLED=true
fi

# Install asdf if not present
if [ "$ASDF_INSTALLED" = false ]; then
    echo "Installing asdf..."
    su "$ORIG_USER" << 'ASDF_INSTALL'
set -e
cd "$HOME"
git clone https://github.com/asdf-vm/asdf.git ~/.asdf --branch v0.14.1
echo '. "$HOME/.asdf/asdf.sh"' >> ~/.bashrc
echo '. "$HOME/.asdf/completions/asdf.bash"' >> ~/.bashrc
ASDF_INSTALL
    echo "asdf installed successfully"
fi

# Create a temporary script to run as the user
TEMP_SCRIPT=$(mktemp)
cat > "$TEMP_SCRIPT" << 'SCRIPT_EOF'
#!/bin/bash
set -e

# Source asdf
if [ -f "$HOME/.asdf/asdf.sh" ]; then
    . "$HOME/.asdf/asdf.sh"
elif [ -f "/opt/asdf-vm/asdf.sh" ]; then
    . "/opt/asdf-vm/asdf.sh"
else
    echo "ERROR: asdf not found!" >&2
    exit 1
fi

echo "Adding asdf plugins..."
asdf plugin-add erlang https://github.com/asdf-vm/asdf-erlang.git 2>/dev/null || echo "erlang plugin already added"
asdf plugin-add elixir https://github.com/asdf-vm/asdf-elixir.git 2>/dev/null || echo "elixir plugin already added"

echo "Installing Erlang 27.2 (this may take a while)..."
asdf install erlang 27.2 || echo "Erlang 27.2 already installed or installation failed"

echo "Installing Elixir 1.18.0..."
asdf install elixir 1.18.0 || echo "Elixir 1.18.0 already installed or installation failed"

echo "Setting global versions..."
asdf global erlang 27.2
asdf global elixir 1.18.0

# Refresh asdf after setting global versions
asdf reshim erlang 27.2
asdf reshim elixir 1.18.0

# Verify installation
echo "Verifying installation..."
asdf current
elixir --version 2>&1

# Get dependencies
echo "Installing Elixir dependencies..."
cd /workspaces/elixir || exit 1
mix local.hex --force
mix local.rebar --force
mix deps.get

echo ""
echo "=== Running XTDB Elixir Examples ==="
echo ""
mix run -e "XTDBExample.connect_and_query()"

echo ""
echo "=== Running COPY FROM STDIN Test ==="
echo ""
mix run -e "XTDBExample.test_copy_from_stdin()"

echo ""
echo "=== Demonstrating Transit-JSON Format ==="
echo ""
mix run -e "XTDBExample.demonstrate_format()"

echo ""
echo "✓ All examples completed successfully!"
SCRIPT_EOF

# Make the script executable and change ownership
chmod +x "$TEMP_SCRIPT"
chown "$ORIG_USER:$ORIG_USER" "$TEMP_SCRIPT"

# Run the script as the original user
su "$ORIG_USER" -c "bash $TEMP_SCRIPT"

# Clean up
rm -f "$TEMP_SCRIPT"

echo ""
echo "✓ Installation and tests complete!"
