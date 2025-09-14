#!/usr/bin/env python3

import os
import sys
import subprocess
import time
import signal
from pathlib import Path


def initialize_repo():
    """Initialize IPFS repo if it doesn't exist"""
    repo_dir = ".ipfs_staging"
    
    if not Path(repo_dir).exists() or not Path(repo_dir, "config").exists():
        print("Initializing IPFS repo...", file=sys.stderr)
        # Remove any partial/corrupted repo directory
        if Path(repo_dir).exists():
            import shutil
            shutil.rmtree(repo_dir)
        
        env = os.environ.copy()
        env['IPFS_PATH'] = repo_dir
        
        result = subprocess.run(['ipfs', 'init', '--profile=pebbleds'], env=env, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"Failed to initialize IPFS repo: {result.stderr}", file=sys.stderr)
            return False
        
        print("IPFS repo initialized", file=sys.stderr)
    
    return True


def configure_ipfs():
    """Configure IPFS for staging use"""
    repo_dir = ".ipfs_staging"
    env = os.environ.copy()
    env['IPFS_PATH'] = repo_dir
    
    print("Configuring staging IPFS node...", file=sys.stderr)
    
    configs = [
        # Set API port to 5009 to avoid conflicts
        (['config', 'Addresses.API', '/ip4/127.0.0.1/tcp/5009'], "API port"),
        
        # Disable HTTP gateway (not needed for staging)
        (['config', 'Addresses.Gateway', ''], "HTTP gateway"),
        
        # Routing: use a custom router set
        (['config', 'Routing.Type', 'custom'], "Custom routing"),
        
        # Router: local delegated router at 127.0.0.1:8190
        (['config', '--json', 'Routing.Routers.local', 
          '{"Type":"http","Parameters":{"Endpoint":"http://127.0.0.1:8190/routing/v1"}}'], "Delegated router"),
        
        # Methods: all required methods mapped to "local"
        (['config', '--json', 'Routing.Methods',
          '{"find-providers":{"RouterName":"local"},'
          '"provide":{"RouterName":"local"},'
          '"find-peers":{"RouterName":"local"},'
          '"get-ipns":{"RouterName":"local"},'
          '"put-ipns":{"RouterName":"local"}}'], "Routing methods"),
        
        # HTTP Retrieval: enable
        (['config', '--json', 'HTTPRetrieval.Enabled', 'true'], "HTTP retrieval"),
        
        # Reprovider: only provide pinned content
        (['config', '--json', 'Reprovider.Strategy', '"pinned"'], "Reprovider strategy"),
        
        # Use CIDv1 by default for Filecoin compatibility
        (['config', '--json', 'Datastore.StorageMax', '"10GB"'], "Storage limit"),
        (['config', '--json', 'Import.CidVersion', '1'], "CIDv1 default"),
    ]
    
    for cmd, description in configs:
        result = subprocess.run(['ipfs'] + cmd, env=env, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"Warning: Failed to configure {description}: {result.stderr}", file=sys.stderr)
    
    print("IPFS configuration complete", file=sys.stderr)


def start_daemon():
    """Start the IPFS daemon"""
    repo_dir = ".ipfs_staging"
    env = os.environ.copy()
    env['IPFS_PATH'] = repo_dir
    
    print("Starting IPFS daemon on port 5009...", file=sys.stderr)
    
    # Start daemon in background
    daemon_process = subprocess.Popen(
        ['ipfs', 'daemon'],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    
    # Wait for daemon to be ready
    for i in range(30):
        try:
            result = subprocess.run(
                ['ipfs', '--api', '/ip4/127.0.0.1/tcp/5009', 'id'],
                capture_output=True,
                text=True,
                timeout=2
            )
            if result.returncode == 0:
                print("IPFS daemon is ready", file=sys.stderr)
                return daemon_process.pid
        except (subprocess.TimeoutExpired, subprocess.CalledProcessError):
            pass
        
        # Check if process is still alive
        if daemon_process.poll() is not None:
            stdout, stderr = daemon_process.communicate()
            print(f"Daemon failed to start: {stderr}", file=sys.stderr)
            return None
            
        time.sleep(1)
    
    # If we get here, daemon failed to start
    daemon_process.terminate()
    print("Timeout waiting for daemon to start", file=sys.stderr)
    return None


def stop_daemon():
    """Stop the IPFS daemon"""
    try:
        # Try graceful shutdown first
        result = subprocess.run(
            ['ipfs', '--api', '/ip4/127.0.0.1/tcp/5009', 'shutdown'],
            capture_output=True,
            text=True,
            timeout=10
        )
        if result.returncode == 0:
            print("IPFS daemon stopped gracefully", file=sys.stderr)
            return True
    except (subprocess.TimeoutExpired, subprocess.CalledProcessError):
        pass
    
    # Fall back to killing processes
    try:
        # Find and kill IPFS daemon processes
        result = subprocess.run(['pkill', '-f', 'ipfs daemon'], capture_output=True)
        if result.returncode == 0:
            print("IPFS daemon stopped (forced)", file=sys.stderr)
            return True
        else:
            print("No IPFS daemon processes found", file=sys.stderr)
            return False
    except Exception as e:
        print(f"Error stopping daemon: {e}", file=sys.stderr)
        return False


def run_start_daemons():
    """Start all required daemons"""
    # Initialize repo if needed
    if not initialize_repo():
        sys.exit(1)
    
    # Configure IPFS
    configure_ipfs()
    
    # Start IPFS daemon
    pid = start_daemon()
    if pid:
        print(f"IPFS daemon started (PID: {pid})")
        print("Daemon is ready for processing")
    else:
        print("Failed to start IPFS daemon", file=sys.stderr)
        sys.exit(1)


def run_stop_daemons():
    """Stop all daemons"""
    if stop_daemon():
        print("All daemons stopped")
    else:
        print("No daemons were running", file=sys.stderr)


def run_daemon_status():
    """Check daemon status"""
    try:
        result = subprocess.run(
            ['ipfs', '--api', '/ip4/127.0.0.1/tcp/5009', 'id'],
            capture_output=True,
            text=True,
            timeout=2
        )
        if result.returncode == 0:
            print("IPFS daemon is running")
            # Parse the ID output to show basic info
            import json
            info = json.loads(result.stdout)
            print(f"Peer ID: {info.get('ID', 'Unknown')}")
            print(f"Version: {info.get('AgentVersion', 'Unknown')}")
        else:
            print("IPFS daemon is not responding")
    except Exception:
        print("IPFS daemon is not running")
