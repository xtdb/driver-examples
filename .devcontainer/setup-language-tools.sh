#!/bin/bash
# Shared language-specific tools installation script
# Installs Clojure CLI, Elixir mix/hex/rebar, and PHP Composer
# Used by both .devcontainer/Dockerfile.dev and .github/workflows/test.yml

set -e

MODE="${1:-all}"

setup_clojure() {
    echo "Installing Clojure CLI..."
    cd /tmp
    curl -L -O https://github.com/clojure/brew-install/releases/latest/download/linux-install.sh
    chmod +x linux-install.sh
    sudo ./linux-install.sh
    rm linux-install.sh
    echo "✓ Clojure CLI installed"
}

setup_elixir() {
    echo "Setting up Elixir (mix local.hex and local.rebar)..."
    mix local.hex --force
    mix local.rebar --force
    echo "✓ Elixir tools installed"
}

setup_php_composer() {
    echo "Installing PHP Composer..."
    cd /tmp
    curl -sS https://getcomposer.org/installer | php
    sudo mv composer.phar /usr/local/bin/composer
    chmod +x /usr/local/bin/composer
    echo "✓ PHP Composer installed"
}

case "$MODE" in
    clojure)
        setup_clojure
        ;;
    elixir)
        setup_elixir
        ;;
    php)
        setup_php_composer
        ;;
    all)
        setup_clojure
        setup_elixir
        setup_php_composer
        ;;
    *)
        echo "Usage: $0 [clojure|elixir|php|all]"
        echo "  clojure - Install Clojure CLI"
        echo "  elixir  - Setup Elixir mix/hex/rebar"
        echo "  php     - Install PHP Composer"
        echo "  all     - Install all language tools (default)"
        exit 1
        ;;
esac

echo "✓ Language tools setup complete"
