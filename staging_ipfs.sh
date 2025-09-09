#!/bin/bash

# Simple script to run a staging IPFS node for metadata crawling
# Uses port 5002 for API to avoid conflicts with default IPFS daemon

REPO_DIR="./staging_ipfs"
export IPFS_PATH="$REPO_DIR"

# Initialize repo if it doesn't exist
if [ ! -d "$REPO_DIR" ]; then
    echo "Initializing IPFS repo at $REPO_DIR..."
    ipfs init
fi

# Set API port to 5002 to avoid conflicts
ipfs config Addresses.API /ip4/127.0.0.1/tcp/5009

echo "Starting staging IPFS daemon on port 5009..."
echo "Press Ctrl+C to stop"
ipfs daemon --offline
