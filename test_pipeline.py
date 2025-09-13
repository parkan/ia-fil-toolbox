#!/usr/bin/env python3

import subprocess
import sys
import os
import tempfile
import csv
import time
import signal
import unittest
import shutil
from pathlib import Path

def run_cmd(cmd, **kwargs):
    """Run command and return result"""
    # Use IPFS_PATH for setup/teardown operations (no daemon needed)
    env = os.environ.copy()
    env['IPFS_PATH'] = ".ipfs_staging"
    result = subprocess.run(cmd, capture_output=True, text=True, env=env, **kwargs)
    if result.returncode != 0:
        # Return both stdout and stderr for unittest to handle
        return None, result.stderr
    # Always return stderr for debugging, even on success
    return result.stdout.strip(), result.stderr.strip() if result.stderr.strip() else None

# Daemon management is now handled automatically by the CLI

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
        print(f"\n2. Running extract-items command on {root_cid}...")
        
        # Capture the CSV output
        result = run_cmd(["python3", "ia_fil.py", "extract-items", root_cid])
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
            ls_result = run_cmd(["ipfs", "ls", "--size=false", "--resolve-type=false", synthetic_cid])
            if not ls_result:
                print(f"  Error: Could not list {synthetic_cid}")
                return False, ipfs_proc
            
            # Parse the file list
            files_in_dir = []
            for line in ls_result.split('\n'):
                if line.strip():
                    parts = line.split()
                    if len(parts) >= 2:
                        files_in_dir.append(parts[1])  # filename is the second part (hash name)
            
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
        
        # Step 4: Test merge-roots command
        print(f"\n4. Testing merge-roots command...")
        
        # We'll create a second test CID and then merge both
        # Create another test directory with overlapping and unique files
        test_dir2 = Path("test_fixtures2")
        test_dir2.mkdir(exist_ok=True)
        
        # Create some files that overlap with test_fixtures and some unique ones
        (test_dir2 / "item1_doc.pdf").write_text("This is another PDF content")  # Same name, different content
        (test_dir2 / "item3_data.csv").write_text("col1,col2\na,1\nb,2")  # Unique file
        (test_dir2 / "shared_file.txt").write_text("This file exists in both")
        
        # Add the overlapping content to the first directory too
        (Path("test_fixtures") / "shared_file.txt").write_text("This file exists in both")
        
        print("  Adding second test directory to IPFS...")
        result2 = run_cmd(["ipfs", "add", "-r", "--cid-version=1", str(test_dir2)])
        if not result2:
            return False, ipfs_proc
        
        # Extract the second root CID
        lines2 = result2.split('\n')
        root_cid2 = None
        for line in lines2:
            if line.strip().endswith('test_fixtures2'):
                root_cid2 = line.split()[1]
                break
        
        if not root_cid2:
            print("  Error: Could not find root CID for test_fixtures2")
            return False, ipfs_proc
        
        print(f"  Second root CID: {root_cid2}")
        
        # Now re-add the first directory with the shared file
        result1_updated = run_cmd(["ipfs", "add", "-r", "--cid-version=1", "test_fixtures"])
        if not result1_updated:
            return False, ipfs_proc
        
        # Extract the updated first root CID
        lines1_updated = result1_updated.split('\n')
        root_cid1_updated = None
        for line in lines1_updated:
            if line.strip().endswith('test_fixtures'):
                root_cid1_updated = line.split()[1]
                break
        
        if not root_cid1_updated:
            print("  Error: Could not find updated root CID for test_fixtures")
            return False, ipfs_proc
        
        print(f"  Updated first root CID: {root_cid1_updated}")
        
        # Test merge-roots command
        print(f"  Running merge-roots on {root_cid1_updated} and {root_cid2}...")
        merge_result = run_cmd(["python3", "ia_fil.py", "merge-roots", root_cid1_updated, root_cid2])
        if not merge_result:
            print("  Error: merge-roots command failed")
            return False, ipfs_proc
        
        merged_cid = merge_result.strip()
        print(f"  ✓ Merged CID: {merged_cid}")
        
        # Validate the merged directory
        print("  Validating merged directory...")
        ls_merged = run_cmd(["ipfs", "ls", "--size=false", "--resolve-type=false", merged_cid])
        if not ls_merged:
            print("  Error: Could not list merged CID")
            return False, ipfs_proc
        
        merged_files = []
        for line in ls_merged.split('\n'):
            if line.strip():
                parts = line.split()
                if len(parts) >= 2:
                    merged_files.append(parts[1])
        
        print(f"  Files in merged directory: {merged_files}")
        
        # Check that we have files from both directories
        expected_files = {
            "item1_data.txt", "item1_doc.pdf", "item1_files.xml", "item1_meta.xml",
            "item2_files.xml", "item2_image.jpg", "item2_meta.xml", "item2_notes.md",
            "item3_data.csv", "shared_file.txt"
        }
        
        actual_files = set(merged_files)
        if not expected_files.issubset(actual_files):
            missing = expected_files - actual_files
            print(f"  Error: Missing files in merged directory: {missing}")
            return False, ipfs_proc
        
        print(f"  ✓ All expected files present in merged directory")
        
        # Test that conflicting files (same name, different content) are handled
        # The merge should keep the first occurrence and log a warning
        print("  Testing file access in merged directory...")
        test_file = run_cmd(["ipfs", "cat", f"{merged_cid}/shared_file.txt"])
        if not test_file:
            print("  Error: Could not access shared_file.txt in merged directory")
            return False, ipfs_proc
        print(f"  ✓ Can access shared file in merged directory")
        
        # Clean up test_fixtures2
        import shutil
        shutil.rmtree(test_dir2, ignore_errors=True)
        # Remove shared_file.txt from test_fixtures
        (Path("test_fixtures") / "shared_file.txt").unlink(missing_ok=True)
        
        print("\n🎉 All tests passed!")
        return True, ipfs_proc
    
    except Exception as e:
        print(f"Test error: {e}")
        return False, ipfs_proc

def cleanup():
    """Clean up test artifacts"""
    print("\n5. Cleaning up...")
    # Remove test fixtures from local filesystem
    # (IPFS content will remain until GC)
    
class TestIAFilToolbox(unittest.TestCase):
    """Test suite for ia-fil-toolbox"""
    
    @classmethod
    def setUpClass(cls):
        """Setup for all tests - CLI will handle daemon management"""
        print("\n=== Test Setup (CLI handles daemon) ===")
    
    @classmethod  
    def tearDownClass(cls):
        """Cleanup after all tests"""
        print("\n=== Test Cleanup ===")
        # Kill any leftover daemons from CLI (belt and suspenders)
        import subprocess
        subprocess.run(["pkill", "-f", "ipfs daemon"], capture_output=True)
    
    def setUp(self):
        """Clean up before each test"""
        # Remove any leftover files from previous tests
        (Path("test_fixtures") / "shared_file.txt").unlink(missing_ok=True)
        import shutil
        shutil.rmtree("test_fixtures2", ignore_errors=True)
    
    def tearDown(self):
        """Clean up after each test"""
        # Remove any files created during the test
        (Path("test_fixtures") / "shared_file.txt").unlink(missing_ok=True)
        import shutil
        shutil.rmtree("test_fixtures2", ignore_errors=True)
    
    def test_extract_items_command(self):
        """Test the extract-items command creates synthetic directories correctly"""
        # Add test fixtures to IPFS (using direct repo access, no daemon needed)
        result, error = run_cmd(["ipfs", "add", "-r", "--cid-version=1", "test_fixtures"])
        self.assertIsNotNone(result, f"Failed to add test fixtures: {error}")
        
        # Extract root CID
        root_cid = None
        for line in result.split('\n'):
            if line.strip().endswith('test_fixtures'):
                root_cid = line.split()[1]
                break
        self.assertIsNotNone(root_cid, "Could not find root CID")
        
        # Run extract-items command  
        result, error = run_cmd(["python3", "ia_fil.py", "extract-items", root_cid])
        self.assertIsNotNone(result, f"extract-items command failed: {error}")
        
        # Parse CSV output - filter out non-CSV lines (logging output)
        lines = result.strip().split('\n')
        csv_lines = [line for line in lines if ',' in line and not line.startswith('  ')]
        
        # Show actual output if CSV parsing fails
        if len(csv_lines) < 2:
            print(f"\n=== DEBUG: Full command output ===")
            print(f"Command: python3 ia_fil.py extract-items {root_cid}")
            print(f"STDOUT:\n{result}")
            if error:
                print(f"STDERR:\n{error}")
            print(f"Parsed CSV lines: {csv_lines}")
            print(f"=== END DEBUG ===\n")
        
        self.assertGreaterEqual(len(csv_lines), 2, "Expected header + data lines in CSV output")
        self.assertEqual(csv_lines[0], "identifier,synthetic_cid", "Invalid CSV header")
        
        # Validate synthetic directories were created
        synthetic_dirs = {}
        for line in csv_lines[1:]:
            identifier, synthetic_cid = line.split(',', 1)
            synthetic_dirs[identifier] = synthetic_cid
        
        self.assertEqual(set(synthetic_dirs.keys()), {'item1', 'item2'}, 
                        f"Expected synthetic directories for item1 and item2, got {set(synthetic_dirs.keys())}")
        
        # Validate each synthetic directory contains expected files
        for identifier, synthetic_cid in synthetic_dirs.items():
            with self.subTest(identifier=identifier):
                # List directory contents
                ls_result, ls_error = run_cmd(["ipfs", "ls", "--size=false", "--resolve-type=false", synthetic_cid])
                self.assertIsNotNone(ls_result, f"Failed to list {synthetic_cid}: {ls_error}")
                
                # Parse file list  
                files_in_dir = []
                for line in ls_result.split('\n'):
                    if line.strip():
                        parts = line.split()
                        if len(parts) >= 2:
                            files_in_dir.append(parts[1])
                
                # Check expected files
                if identifier == 'item1':
                    expected = {'item1_data.txt', 'item1_doc.pdf'}
                else:  # item2
                    expected = {'item2_image.jpg', 'item2_notes.md'}
                
                self.assertEqual(set(files_in_dir), expected,
                               f"Directory {identifier} has wrong files")
    
    def test_merge_roots_command(self):
        """Test the merge-roots command merges multiple CIDs correctly"""
        # Create two test directories with some overlapping files
        test_dir2 = Path("test_fixtures2")
        test_dir2.mkdir(exist_ok=True)
        
        try:
            # Create some files that overlap with test_fixtures and some unique ones
            (test_dir2 / "item1_doc.pdf").write_text("This is another PDF content")  # Same name, different content
            (test_dir2 / "item3_data.csv").write_text("col1,col2\na,1\nb,2")  # Unique file
            (test_dir2 / "shared_file.txt").write_text("This file exists in both")
            
            # Add the overlapping content to the first directory too
            (Path("test_fixtures") / "shared_file.txt").write_text("This file exists in both")
            
            # Add both directories to IPFS
            result1, error1 = run_cmd(["ipfs", "add", "-r", "--cid-version=1", "test_fixtures"])
            self.assertIsNotNone(result1, f"Failed to add test_fixtures: {error1}")
            
            result2, error2 = run_cmd(["ipfs", "add", "-r", "--cid-version=1", str(test_dir2)])
            self.assertIsNotNone(result2, f"Failed to add test_fixtures2: {error2}")
            
            # Extract root CIDs
            root_cid1 = None
            for line in result1.split('\n'):
                if line.strip().endswith('test_fixtures'):
                    root_cid1 = line.split()[1]
                    break
            
            root_cid2 = None  
            for line in result2.split('\n'):
                if line.strip().endswith('test_fixtures2'):
                    root_cid2 = line.split()[1]
                    break
            
            self.assertIsNotNone(root_cid1, "Could not find root CID for test_fixtures")
            self.assertIsNotNone(root_cid2, "Could not find root CID for test_fixtures2")
            
            # Run merge-roots command
            merge_result, merge_error = run_cmd(["python3", "ia_fil.py", "merge-roots", root_cid1, root_cid2])
            self.assertIsNotNone(merge_result, f"merge-roots command failed: {merge_error}")
            
            merged_cid = merge_result.strip()
            self.assertTrue(merged_cid.startswith(('Qm', 'bafy')), f"Invalid CID format: {merged_cid}")
            
            # Validate the merged directory contains files from both directories
            ls_result, ls_error = run_cmd(["ipfs", "ls", "--size=false", "--resolve-type=false", merged_cid])
            self.assertIsNotNone(ls_result, f"Failed to list merged directory {merged_cid}: {ls_error}")
            
            merged_files = []
            for line in ls_result.split('\n'):
                if line.strip():
                    parts = line.split()
                    if len(parts) >= 2:
                        merged_files.append(parts[1])
            
            # Check that we have files from both directories
            expected_files = {
                "item1_data.txt", "item1_doc.pdf", "item1_files.xml", "item1_meta.xml",
                "item2_files.xml", "item2_image.jpg", "item2_meta.xml", "item2_notes.md", 
                "item3_data.csv", "shared_file.txt"
            }
            
            actual_files = set(merged_files)
            self.assertTrue(expected_files.issubset(actual_files), 
                          f"Missing files in merged directory. Expected {expected_files}, got {actual_files}")
            
            # Test file access in merged directory
            test_result, test_error = run_cmd(["ipfs", "cat", f"{merged_cid}/shared_file.txt"])
            self.assertIsNotNone(test_result, f"Could not access file in merged directory: {test_error}")
            
        finally:
            # tearDown() will handle cleanup
            pass

if __name__ == "__main__":
    unittest.main(verbosity=2)
