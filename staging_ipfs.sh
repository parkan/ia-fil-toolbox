#!/bin/bash

# Simple script to run a staging IPFS node for metadata crawling
# Uses port 5009 for API to avoid conflicts with default IPFS daemon

REPO_DIR=".ipfs_staging"
export IPFS_PATH="$REPO_DIR"

# Initialize repo if it doesn't exist
if [ ! -d "$REPO_DIR" ]; then
    echo "Initializing IPFS repo at $REPO_DIR..."
    ipfs init --profile=pebbleds
fi

# Configure for optimal staging performance
echo "Configuring staging IPFS node..."

# Set API port to 5009 to avoid conflicts
ipfs config Addresses.API /ip4/127.0.0.1/tcp/5009

# Disable HTTP gateway (not needed for staging)
ipfs config Addresses.Gateway ""


echo "Starting staging IPFS daemon on port 5009 (gateway disabled, optimized for staging)..."
echo "Press Ctrl+C to stop"
ipfs daemon --offline
