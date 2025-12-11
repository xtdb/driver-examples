#!/bin/bash
# Shared system dependencies installation script
# Used by both .devcontainer/Dockerfile.dev and .github/workflows/test.yml

set -e

# Essential packages (required by both CI and dev container)
ESSENTIAL_PACKAGES=(
    curl
    ca-certificates
    rlwrap
    sudo
    git
    gnupg
    python
    python-pip
)

# Dev container only packages (interactive tools)
DEV_ONLY_PACKAGES=(
    fish
    vi
)

# Language runtime packages (for running all examples)
LANGUAGE_PACKAGES=(
    ruby
    ruby-bundler
    elixir
    erlang-asn1
    erlang-public_key
    erlang-ssl
    erlang-inets
    erlang-parsetools
)

# PHP package (installed separately to handle PEAR prompts)
PHP_PACKAGE=(php)

# PostgreSQL client (for testing)
POSTGRES_PACKAGES=(
    postgresql-libs
)

# Determine which packages to install based on mode
MODE="${1:-all}"

case "$MODE" in
    essential)
        PACKAGES=("${ESSENTIAL_PACKAGES[@]}")
        ;;
    dev)
        PACKAGES=("${ESSENTIAL_PACKAGES[@]}" "${DEV_ONLY_PACKAGES[@]}")
        ;;
    languages)
        PACKAGES=("${LANGUAGE_PACKAGES[@]}")
        ;;
    postgres)
        PACKAGES=("${POSTGRES_PACKAGES[@]}")
        ;;
    all)
        PACKAGES=(
            "${ESSENTIAL_PACKAGES[@]}"
            "${DEV_ONLY_PACKAGES[@]}"
            "${LANGUAGE_PACKAGES[@]}"
            "${POSTGRES_PACKAGES[@]}"
        )
        ;;
    ci)
        # CI needs essential + languages + postgres, but not dev tools
        PACKAGES=(
            "${ESSENTIAL_PACKAGES[@]}"
            "${LANGUAGE_PACKAGES[@]}"
            "${POSTGRES_PACKAGES[@]}"
        )
        ;;
    *)
        echo "Usage: $0 [essential|dev|languages|postgres|all|ci]"
        echo "  essential  - Install only essential packages"
        echo "  dev        - Install essential + dev tools"
        echo "  languages  - Install only language runtime packages"
        echo "  postgres   - Install only PostgreSQL client"
        echo "  all        - Install all packages (default for dev container)"
        echo "  ci         - Install packages for CI (essential + languages + postgres)"
        exit 1
        ;;
esac

echo "Installing ${MODE} packages..."
echo "Packages: ${PACKAGES[*]}"

# Update system
pacman -Syu --noconfirm

# Install packages
pacman -S --noconfirm "${PACKAGES[@]}"

# Install PHP separately with PEAR prompts auto-answered
if [ "$MODE" = "all" ] || [ "$MODE" = "ci" ] || [ "$MODE" = "languages" ]; then
    echo "Installing PHP (with silent PEAR setup)..."
    yes '' | pacman -S --noconfirm "${PHP_PACKAGE[@]}" || true
fi

# Clean up package cache to reduce image size
if [ "$MODE" = "all" ] || [ "$MODE" = "dev" ]; then
    pacman -Scc --noconfirm
fi

echo "✓ System dependencies installed successfully"
