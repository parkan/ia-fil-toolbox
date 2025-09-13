#!/usr/bin/env python3

import subprocess
import sys
import os
import tempfile
import csv
import time
import signal
from pathlib import Path

def run_cmd(cmd, **kwargs):
    """Run command and return result"""
    print(f"Running: {' '.join(cmd)}")
    env = os.environ.copy()
    env['IPFS_API'] = "http://127.0.0.1:5009"
    env['IPFS_PATH'] = ".ipfs_staging"
    result = subprocess.run(cmd, capture_output=True, text=True, env=env, **kwargs)
    if result.returncode != 0:
        print(f"Command failed: {result.stderr}", file=sys.stderr)
        return None
    return result.stdout.strip()

def start_staging_ipfs():
    """Start the staging IPFS daemon"""
    print("Starting staging IPFS daemon...")
    
    # Start the staging IPFS script in background
    proc = subprocess.Popen(
        ["./staging_ipfs.sh"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    
    # Wait for daemon to be ready
    print("Waiting for IPFS daemon to be ready...")
    for i in range(30):  # Wait up to 30 seconds
        try:
            # Test if IPFS API is responding
            result = subprocess.run(
                ["ipfs", "id"],
                env={**os.environ, "IPFS_API": "http://127.0.0.1:5009", "IPFS_PATH": ".ipfs_staging"},
                capture_output=True,
                text=True,
                timeout=2
            )
            if result.returncode == 0:
                print("Staging IPFS daemon is ready")
                return proc
        except subprocess.TimeoutExpired:
            pass
        
        time.sleep(1)
        print(f"  Waiting... ({i+1}s)")
        
        # Check if process died
        if proc.poll() is not None:
            stdout, stderr = proc.communicate()
            print(f"IPFS daemon failed to start:")
            print(f"STDOUT: {stdout}")
            print(f"STDERR: {stderr}")
            return None
    
    print("Timeout waiting for IPFS daemon to start")
    proc.terminate()
    return None

def stop_staging_ipfs(proc):
    """Stop the staging IPFS daemon"""
    if proc and proc.poll() is None:
        print("Stopping staging IPFS daemon...")
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            print("Force killing IPFS daemon...")
            proc.kill()
            proc.wait()
        print("Staging IPFS daemon stopped")

def test_pipeline():
    """Test the complete files pipeline"""
    
    print("=== Testing Files Pipeline ===")
    
    # Step 0: Start staging IPFS
    print("\n0. Starting staging IPFS daemon...")
    ipfs_proc = start_staging_ipfs()
    if not ipfs_proc:
        print("Failed to start staging IPFS daemon")
        return False, None
    
    try:
        # Step 1: Add test fixtures to IPFS
        print("\n1. Adding test fixtures to IPFS...")
        fixtures_dir = Path("test_fixtures")
        if not fixtures_dir.exists():
            print("Error: test_fixtures directory not found!")
            return False, ipfs_proc
    
        # Add the test directory to IPFS
        result = run_cmd(["ipfs", "add", "-r", "--cid-version=1", str(fixtures_dir)])
        if not result:
            return False, ipfs_proc
        
        # Extract the root CID from the output
        lines = result.split('\n')
        root_cid = None
        for line in lines:
            if line.strip().endswith('test_fixtures'):
                parts = line.split()
                if len(parts) >= 2:
                    root_cid = parts[1]
                    break
        
        if not root_cid:
            print("Error: Could not extract root CID from ipfs add output")
            return False, ipfs_proc
        
        print(f"Root CID: {root_cid}")
        
        # Step 2: Run the files command
        print(f"\n2. Running files command on {root_cid}...")
        
        # Capture the CSV output
        result = run_cmd(["python3", "fil_crawler.py", "files", root_cid])
        if not result:
            return False, ipfs_proc
        
        print("Files command output:")
        print(result)
        
        # Parse the CSV output
        lines = result.strip().split('\n')
        if len(lines) < 2:  # Header + at least one data line
            print("Error: Expected at least header + 1 data line in CSV output")
            return False, ipfs_proc
        
        # Parse CSV
        csv_reader = csv.reader(lines)
        header = next(csv_reader)
        
        if header != ['identifier', 'synthetic_cid']:
            print(f"Error: Expected header ['identifier', 'synthetic_cid'], got {header}")
            return False, ipfs_proc
        
        synthetic_dirs = {}
        for row in csv_reader:
            if len(row) == 2:
                identifier, synthetic_cid = row
                synthetic_dirs[identifier] = synthetic_cid
                print(f"  {identifier} -> {synthetic_cid}")
        
        # Step 3: Validate the synthetic directories
        print(f"\n3. Validating {len(synthetic_dirs)} synthetic directories...")
        
        expected_identifiers = {'item1', 'item2'}
        if set(synthetic_dirs.keys()) != expected_identifiers:
            print(f"Error: Expected identifiers {expected_identifiers}, got {set(synthetic_dirs.keys())}")
            return False, ipfs_proc
        
        # Validate each synthetic directory
        for identifier, synthetic_cid in synthetic_dirs.items():
            print(f"\nValidating {identifier} ({synthetic_cid})...")
            
            # List contents of synthetic directory
            ls_result = run_cmd(["ipfs", "ls", synthetic_cid])
            if not ls_result:
                print(f"  Error: Could not list {synthetic_cid}")
                return False, ipfs_proc
            
            # Parse the file list
            files_in_dir = []
            for line in ls_result.split('\n'):
                if line.strip():
                    parts = line.split()
                    if len(parts) >= 3:
                        files_in_dir.append(parts[2])  # filename is the third part (hash size name)
            
            print(f"  Files in synthetic directory: {files_in_dir}")
            
            # Check expected files based on identifier
            if identifier == 'item1':
                expected_files = {'item1_doc.pdf', 'item1_data.txt'}
            elif identifier == 'item2':
                expected_files = {'item2_image.jpg', 'item2_notes.md'}
            else:
                print(f"  Error: Unknown identifier {identifier}")
                return False, ipfs_proc
            
            if set(files_in_dir) != expected_files:
                print(f"  Error: Expected files {expected_files}, got {set(files_in_dir)}")
                return False, ipfs_proc
            
            print(f"  ✓ {identifier} contains expected files")
            
            # Test that we can actually access the files
            for filename in expected_files:
                cat_result = run_cmd(["ipfs", "cat", f"{synthetic_cid}/{filename}"])
                if not cat_result:
                    print(f"  Error: Could not cat {synthetic_cid}/{filename}")
                    return False, ipfs_proc
                print(f"  ✓ Can access {filename} ({len(cat_result)} bytes)")
        
        print("\n🎉 All tests passed!")
        return True, ipfs_proc
    
    except Exception as e:
        print(f"Test error: {e}")
        return False, ipfs_proc

def cleanup():
    """Clean up test artifacts"""
    print("\n4. Cleaning up...")
    # Remove test fixtures from local filesystem
    # (IPFS content will remain until GC)
    
if __name__ == "__main__":
    ipfs_proc = None
    try:
        success, ipfs_proc = test_pipeline()
        if success:
            print("\n✅ Pipeline test completed successfully")
            sys.exit(0)
        else:
            print("\n❌ Pipeline test failed")
            sys.exit(1)
    except KeyboardInterrupt:
        print("\n⚠️ Test interrupted")
        sys.exit(1)
    except Exception as e:
        print(f"\n💥 Test error: {e}")
        sys.exit(1)
    finally:
        if ipfs_proc:
            stop_staging_ipfs(ipfs_proc)
        cleanup()
