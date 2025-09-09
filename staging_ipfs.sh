#!/bin/bash

# Simple script to run a staging IPFS node for metadata crawling
# Uses port 5009 for API to avoid conflicts with default IPFS daemon

REPO_DIR="./staging_ipfs"
export IPFS_PATH="$REPO_DIR"

# Initialize repo if it doesn't exist
if [ ! -d "$REPO_DIR" ]; then
    echo "Initializing IPFS repo at $REPO_DIR..."
    ipfs init
fi

# Configure for optimal staging performance
echo "Configuring staging IPFS node..."

# Set API port to 5009 to avoid conflicts
ipfs config Addresses.API /ip4/127.0.0.1/tcp/5009

# Disable HTTP gateway (not needed for staging)
ipfs config Addresses.Gateway ""

# Disable swarm listening (offline mode anyway)
ipfs config Addresses.Swarm "[]"

# Disable announcements and discovery
ipfs config Addresses.Announce "[]"
ipfs config Addresses.NoAnnounce "[]"

# Use badger datastore for better performance
ipfs config Datastore.Spec.type badger

# Optimize garbage collection
ipfs config Datastore.GCPeriod "10m"

# Disable bandwidth metrics for slight performance gain
ipfs config Swarm.DisableBandwidthMetrics true

# Disable connection manager (not needed offline)
ipfs config Swarm.ConnMgr.Type none

# Optimize block service
ipfs config Experimental.GraphsyncEnabled false
ipfs config Experimental.Libp2pStreamMounting false

echo "Starting staging IPFS daemon on port 5009 (gateway disabled, optimized for staging)..."
echo "Press Ctrl+C to stop"
ipfs daemon --offline
