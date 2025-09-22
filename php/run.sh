#!/bin/bash

# Check if PHP is installed
if ! command -v php &> /dev/null; then
    echo "PHP is not installed. Please install PHP first."
    exit 1
fi

# Get PHP version
PHP_VERSION=$(php -r "echo PHP_MAJOR_VERSION.'.'.PHP_MINOR_VERSION;")
echo "Using PHP version: $PHP_VERSION"

# Check if PostgreSQL extension is installed
if ! php -m | grep -q "pgsql"; then
    echo "PostgreSQL PHP extension not installed for PHP $PHP_VERSION."

    # If this is the codespace environment with custom PHP
    if [[ $(which php) == */codespace/.php/* ]]; then
        echo "Using system PHP 7.4 instead which has php-pgsql installed..."

        # Check if system PHP 7.4 exists and has pgsql
        if [ -f /usr/bin/php7.4 ] && /usr/bin/php7.4 -m | grep -q "pgsql"; then
            echo "Running XTDB PHP example with PHP 7.4..."
            /usr/bin/php7.4 XtdbHelloWorld.php
            exit 0
        else
            # Install system PHP with pgsql
            echo "Installing system PHP with PostgreSQL extension..."
            echo -e "nameserver 8.8.8.8\nnameserver 1.1.1.1" | sudo tee -a /etc/resolv.conf > /dev/null
            sudo apt-get update
            sudo apt-get install -y php7.4-cli php7.4-pgsql

            if [ -f /usr/bin/php7.4 ] && /usr/bin/php7.4 -m | grep -q "pgsql"; then
                echo "Running XTDB PHP example with PHP 7.4..."
                /usr/bin/php7.4 XtdbHelloWorld.php
                exit 0
            fi
        fi
    else
        # Standard installation
        echo "Installing php-pgsql..."
        echo -e "nameserver 8.8.8.8\nnameserver 1.1.1.1" | sudo tee -a /etc/resolv.conf > /dev/null
        sudo apt-get update
        sudo apt-get install -y php-pgsql
    fi

    # Final check
    if ! php -m | grep -q "pgsql"; then
        echo "Failed to install php-pgsql extension."
        exit 1
    fi
fi

# Run the example
echo "Running XTDB PHP example..."
php XtdbHelloWorld.php
