#!/usr/bin/env bash

set -e

echo "Installing Go dependencies..."
go mod download

echo "Running XTDB Go example..."
go run main.go