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

# Activate mise in bash (add at the TOP of .bashrc so it runs for non-interactive shells too)
# This ensures mise is available in all contexts (interactive terminals, CI, VS Code, etc.)
if ! grep -q "mise activate bash" ~/.bashrc 2>/dev/null; then
    # Create a temporary file with mise activation at the top
    {
        echo '# Activate mise BEFORE the interactive check so it'\''s available in all shells'
        echo 'eval "$(~/.local/bin/mise activate bash)"'
        echo 'eval "$(~/.local/bin/mise activate bash --shims)"'
        echo ''
        cat ~/.bashrc 2>/dev/null || true
    } > ~/.bashrc.tmp
    mv ~/.bashrc.tmp ~/.bashrc
fi

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
