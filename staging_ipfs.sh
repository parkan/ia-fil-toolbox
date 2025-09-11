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

# Routing: use a custom router set
ipfs config Routing.Type custom

# Router: local delegated router at 127.0.0.1:8190
ipfs config --json Routing.Routers.local \
'{"Type":"http","Parameters":{"Endpoint":"http://127.0.0.1:8190/routing/v1"}}'

# Methods: all required methods mapped to "local"
ipfs config --json Routing.Methods \
'{"find-providers":{"RouterName":"local"},
  "provide":{"RouterName":"local"},
  "find-peers":{"RouterName":"local"},
  "get-ipns":{"RouterName":"local"},
  "put-ipns":{"RouterName":"local"}}'

# HTTP Retrieval: enable
ipfs config --json HTTPRetrieval.Enabled true

# Reprovider: only provide pinned content (no unpinned CIDs)
ipfs config --json Reprovider.Strategy '"pinned"'

echo "Starting staging IPFS daemon on port 5009 (gateway disabled, custom routing configured)..."
echo "Press Ctrl+C to stop"
ipfs daemon --offline
