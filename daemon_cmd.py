#!/usr/bin/env python3

import os
import sys
import subprocess
import time
import signal
from pathlib import Path

# Global variables to track daemon processes and log files
_daemon_process_obj = None
_daemon_log_files = None
_someguy_process_obj = None
_someguy_log_files = None


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
    
    print("Configuring staging IPFS node...", end="", file=sys.stderr)
    
    # Import here to avoid circular imports
    from shared import MFS_FLUSH_LIMIT
    
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
        
        # Provide: only provide pinned content (migrated from Reprovider.Strategy)
        (['config', '--json', 'Provide.Strategy', '"pinned"'], "Provide strategy"),
        
        # Use CIDv1 by default for Filecoin compatibility
        (['config', '--json', 'Datastore.StorageMax', '"10GB"'], "Storage limit"),
        (['config', '--json', 'Import.CidVersion', '1'], "CIDv1 default"),
        
        # Increase MFS unflushed operations limit for large directories
        (['config', '--json', 'Internal.MFSNoFlushLimit', str(MFS_FLUSH_LIMIT)], "MFS flush limit"),
    ]
    
    for cmd, description in configs:
        result = subprocess.run(['ipfs'] + cmd, env=env, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"Warning: Failed to configure {description}: {result.stderr}", file=sys.stderr)
    
    print(" complete", file=sys.stderr)


def start_daemon():
    """Start the IPFS daemon"""
    import tempfile
    import atexit
    import signal
    
    repo_dir = ".ipfs_staging"
    env = os.environ.copy()
    env['IPFS_PATH'] = repo_dir
    
    print("Starting IPFS daemon on port 5009...", end="", file=sys.stderr)
    
    # Create temporary log files for daemon output
    stdout_log = tempfile.NamedTemporaryFile(
        mode='w+', 
        prefix='ipfs_daemon_stdout_', 
        suffix='.log',
        delete=False  # We'll manage deletion ourselves
    )
    stderr_log = tempfile.NamedTemporaryFile(
        mode='w+', 
        prefix='ipfs_daemon_stderr_', 
        suffix='.log',
        delete=False
    )
    
    # Register cleanup for normal operation
    def cleanup_logs():
        try:
            stdout_log.close()
            stderr_log.close()
            os.unlink(stdout_log.name)
            os.unlink(stderr_log.name)
        except:
            pass
    
    atexit.register(cleanup_logs)
    
    def diagnose_process_death(returncode):
        """Provide human-readable description of how process died"""
        if returncode == 0:
            return "Process exited successfully"
        elif returncode > 0:
            return f"Process failed with exit code {returncode}"
        elif returncode < 0:
            signal_num = -returncode
            try:
                signal_name = signal.Signals(signal_num).name
                return f"Process killed by signal {signal_num} ({signal_name})"
            except ValueError:
                return f"Process killed by unknown signal {signal_num}"
    
    # Start daemon in background, logging to temp files
    daemon_process = subprocess.Popen(
        ['ipfs', 'daemon'],
        env=env,
        stdout=stdout_log,
        stderr=stderr_log,
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
                print(" ready", file=sys.stderr)
                
                # Store the process object globally for proper cleanup
                global _daemon_process_obj, _daemon_log_files
                _daemon_process_obj = daemon_process
                _daemon_log_files = (stdout_log.name, stderr_log.name)
                
                # Only show log paths in debug mode
                from shared import DEBUG
                if DEBUG:
                    print(f"IPFS logs: stdout={stdout_log.name}, stderr={stderr_log.name}", file=sys.stderr)
                return daemon_process.pid
        except (subprocess.TimeoutExpired, subprocess.CalledProcessError):
            pass
        
        # Check if process is still alive
        if daemon_process.poll() is not None:
            returncode = daemon_process.returncode
            
            # Don't clean up logs - we need them for debugging
            atexit.unregister(cleanup_logs)
            
            print(f"IPFS daemon failed: {diagnose_process_death(returncode)}", file=sys.stderr)
            print(f"Check logs for details:", file=sys.stderr)
            print(f"  stdout: {stdout_log.name}", file=sys.stderr)
            print(f"  stderr: {stderr_log.name}", file=sys.stderr)
            
            return None
            
        time.sleep(1)
    
    # If we get here, daemon failed to start within timeout
    daemon_process.terminate()
    
    # Wait a bit for termination, then check final status
    time.sleep(1)
    returncode = daemon_process.poll()
    
    # Don't clean up logs - we need them for debugging
    atexit.unregister(cleanup_logs)
    
    print(f"IPFS daemon startup timeout (30 seconds)", file=sys.stderr)
    if returncode is not None:
        print(f"Final status: {diagnose_process_death(returncode)}", file=sys.stderr)
    print(f"Check logs for details:", file=sys.stderr)
    print(f"  stdout: {stdout_log.name}", file=sys.stderr)
    print(f"  stderr: {stderr_log.name}", file=sys.stderr)
    
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


# someguy daemon management functions

def start_someguy():
    """Start someguy daemon with Internet Archive endpoints"""
    global _someguy_process_obj, _someguy_log_files
    import tempfile
    import atexit
    
    # Check if someguy is already running
    try:
        import urllib.request
        urllib.request.urlopen("http://127.0.0.1:8190/version", timeout=2)
        print("someguy daemon is already running", file=sys.stderr)
        return -1  # Special value to indicate "already running"
    except:
        pass  # Not running, proceed to start it
    
    print("Starting someguy daemon on port 8190...", end="", file=sys.stderr)
    
    # Create temporary log files for someguy output
    stdout_log = tempfile.NamedTemporaryFile(
        mode='w+', 
        prefix='someguy_stdout_', 
        suffix='.log',
        delete=False
    )
    stderr_log = tempfile.NamedTemporaryFile(
        mode='w+', 
        prefix='someguy_stderr_', 
        suffix='.log',
        delete=False
    )
    
    # Register cleanup for normal operation
    def cleanup_someguy_logs():
        try:
            stdout_log.close()
            stderr_log.close()
            os.unlink(stdout_log.name)
            os.unlink(stderr_log.name)
        except:
            pass
    
    atexit.register(cleanup_someguy_logs)
    
    # Generate throwaway peer IDs for endpoints (following run-someguy.sh pattern)
    endpoints = ['https://ia.dcentnetworks.nl']
    peer_ids = []
    
    # Ensure keystore directory exists
    keystore_dir = ".ipfs_staging/keystore"
    os.makedirs(keystore_dir, exist_ok=True)
    
    for i, endpoint in enumerate(endpoints):
        # Generate temporary key to get peer ID
        key_name = f"tmp-throwaway-{os.getpid()}_{i}"
        try:
            # Use IPFS_PATH instead of --api for key generation to avoid keystore access issues
            env = os.environ.copy()
            env['IPFS_PATH'] = '.ipfs_staging'
            result = subprocess.run(
                ['ipfs', 'key', 'gen', '-t', 'ed25519', key_name],
                capture_output=True, text=True, env=env
            )
            if result.returncode == 0:
                peer_id = result.stdout.strip().split('\n')[-1]
                peer_ids.append(peer_id)
                # Clean up the key
                subprocess.run(['ipfs', 'key', 'rm', key_name], capture_output=True, env=env)
            else:
                print(f"Failed to generate peer ID for {endpoint}: {result.stderr}", file=sys.stderr)
                return None
        except Exception as e:
            print(f"Error generating peer ID: {e}", file=sys.stderr)
            return None
    
    # Build someguy command arguments
    cmd_args = ['someguy', 'start']
    cmd_args.extend(['--listen-address', '127.0.0.1:8190'])
    cmd_args.extend(['--dht', 'disabled'])
    
    # Add endpoints and peer IDs
    for endpoint in endpoints:
        cmd_args.extend(['--http-block-provider-endpoints', endpoint])
    for peer_id in peer_ids:
        cmd_args.extend(['--http-block-provider-peerids', peer_id])
    
    # Start someguy daemon
    try:
        # Set up environment with debug logging
        env = os.environ.copy()
        env['GOLOG_LOG_LEVEL'] = 'debug'
        
        someguy_process = subprocess.Popen(
            cmd_args,
            stdout=stdout_log,
            stderr=stderr_log,
            text=True,
            env=env
        )
        
        # Wait a moment for startup
        time.sleep(2)
        
        # Check if process is still alive
        if someguy_process.poll() is not None:
            # Don't clean up logs - we need them for debugging
            atexit.unregister(cleanup_someguy_logs)
            
            returncode = someguy_process.returncode
            print(f"someguy daemon failed to start (exit code {returncode})", file=sys.stderr)
            print(f"Check logs for details:", file=sys.stderr)
            print(f"  stdout: {stdout_log.name}", file=sys.stderr)
            print(f"  stderr: {stderr_log.name}", file=sys.stderr)
            return None
        
        # Store process object and log files globally
        _someguy_process_obj = someguy_process
        _someguy_log_files = (stdout_log.name, stderr_log.name)
        
        print(" ready", file=sys.stderr)
        # Only show log paths in debug mode
        from shared import DEBUG
        if DEBUG:
            print(f"someguy logs: stdout={stdout_log.name}, stderr={stderr_log.name}", file=sys.stderr)
        return someguy_process.pid
        
    except FileNotFoundError:
        print("Error: 'someguy' command not found. Please install someguy.", file=sys.stderr)
        return None
    except Exception as e:
        print(f"Error starting someguy: {e}", file=sys.stderr)
        return None

def stop_someguy():
    """Stop the someguy daemon"""
    global _someguy_process_obj
    
    if _someguy_process_obj:
        try:
            _someguy_process_obj.terminate()
            _someguy_process_obj.wait(timeout=5)
            print("someguy daemon stopped", file=sys.stderr)
        except:
            try:
                _someguy_process_obj.kill()
                print("someguy daemon stopped (forced)", file=sys.stderr)
            except:
                print("Failed to stop someguy daemon", file=sys.stderr)
        finally:
            _someguy_process_obj = None

def ensure_someguy_running():
    """Check if someguy is running, start if needed"""
    try:
        # Test if someguy is responding on its default port
        import urllib.request
        urllib.request.urlopen("http://127.0.0.1:8190/version", timeout=2)
        return True  # Already running
    except:
        # Start someguy
        return start_someguy() is not None

def run_persistent_daemons(someguy=True):
    """Run persistent IPFS and optionally someguy daemons until interrupted"""
    import signal
    import sys
    import os
    
    # Initialize and start IPFS
    if not initialize_repo():
        print("Failed to initialize IPFS repository", file=sys.stderr)
        sys.exit(1)
    
    configure_ipfs()
    
    ipfs_pid = start_daemon()
    if not ipfs_pid:
        print("Failed to start IPFS daemon", file=sys.stderr)
        sys.exit(1)
    
    print(f"IPFS daemon started (PID: {ipfs_pid})")
    
    # Start someguy if requested
    someguy_pid = None
    someguy_external = False
    if someguy:
        someguy_pid = start_someguy()
        if someguy_pid == -1:
            # Already running externally
            someguy_external = True
            print("Using external someguy daemon")
        elif someguy_pid:
            print(f"someguy daemon started (PID: {someguy_pid})")
        else:
            print("Error: Failed to start someguy daemon", file=sys.stderr)
            print("Use --no-someguy to disable if someguy is not needed.", file=sys.stderr)
            # Stop IPFS before exiting
            stop_daemon()
            sys.exit(1)
    
    # Set up signal handlers for clean shutdown
    def signal_handler(signum, frame):
        print("\nShutting down daemons...", file=sys.stderr)
        
        # Stop someguy first (only if we started it)
        if someguy_pid and someguy_pid != -1:
            stop_someguy()
        elif someguy_external:
            print("Note: External someguy daemon left running", file=sys.stderr)
        
        # Stop IPFS
        stop_daemon()
        
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    print("Daemons running. Press Ctrl+C to stop.")
    
    # Monitor daemon health
    try:
        while True:
            time.sleep(5)
            
            # Check IPFS daemon
            if _daemon_process_obj and _daemon_process_obj.poll() is not None:
                print("IPFS daemon died unexpectedly!", file=sys.stderr)
                if _daemon_log_files:
                    print(f"Check logs: {_daemon_log_files[0]} {_daemon_log_files[1]}", file=sys.stderr)
                sys.exit(1)
            
            # Check someguy daemon (only if we started it)
            if someguy and not someguy_external and _someguy_process_obj and _someguy_process_obj.poll() is not None:
                print("someguy daemon died unexpectedly!", file=sys.stderr)
                if _someguy_log_files:
                    print(f"Check logs: {_someguy_log_files[0]} {_someguy_log_files[1]}", file=sys.stderr)
                sys.exit(1)
                
    except KeyboardInterrupt:
        signal_handler(signal.SIGINT, None)
