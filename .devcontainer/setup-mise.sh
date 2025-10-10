#!/bin/bash
# Shared mise installation and configuration script
# Used by both .devcontainer/Dockerfile.dev and .github/workflows/test.yml

set -e

USER_NAME="${1:-codespace}"
WORKSPACE_PATH="${2:-/workspaces/driver-examples}"

echo "Installing mise for user: $USER_NAME"
echo "Workspace path: $WORKSPACE_PATH"

# Install mise
curl https://mise.run | sh

# Activate mise in bash
echo 'eval "$(~/.local/bin/mise activate bash)"' >> ~/.bashrc

# Activate mise in fish (if available)
if command -v fish &> /dev/null; then
    mkdir -p ~/.config/fish
    echo '~/.local/bin/mise activate fish | source' >> ~/.config/fish/config.fish
fi

# Configure mise to trust workspace directory
mkdir -p ~/.config/mise
cat > ~/.config/mise/config.toml <<EOF
[settings]
trusted_config_paths = ['$WORKSPACE_PATH']
experimental = true
EOF

echo "✓ Mise installed and configured successfully"
