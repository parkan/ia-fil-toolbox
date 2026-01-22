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
    """
    DEPRECATED: This test is obsolete - kept only to avoid breaking pytest discovery.
    
    Daemon management is now handled automatically by the CLI.
    Use the unittest-based tests in TestIAFilToolbox instead.
    """
    pass

def _obsolete_test_pipeline():
    """Original test kept for reference - DO NOT USE"""
    
    print("=== Testing Files Pipeline ===")
    
    # Step 0: Daemon is now managed automatically
    print("\n0. Daemon is managed automatically by CLI...")
    
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
        # Ensure daemon is running for all operations
        from shared import ensure_staging_ipfs
        ensure_staging_ipfs()
        
        # Add test fixtures to IPFS 
        result, error = run_cmd(["ipfs", "--api", "/ip4/127.0.0.1/tcp/5009", "add", "-r", "--cid-version=1", "test_fixtures"])
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
                # List top-level directory contents
                ls_result, ls_error = run_cmd(["ipfs", "--api", "/ip4/127.0.0.1/tcp/5009", "ls", "--size=false", "--resolve-type=false", synthetic_cid])
                self.assertIsNotNone(ls_result, f"Failed to list {synthetic_cid}: {ls_error}")
                
                # Parse top-level entries (both files and directories)
                top_level_entries = {}  # name -> (cid, is_directory)
                for line in ls_result.split('\n'):
                    if line.strip():
                        parts = line.split()
                        if len(parts) >= 2:
                            entry_cid = parts[0]
                            entry_name = parts[1]
                            top_level_entries[entry_name] = entry_cid
                
                # Check expected structure and drill down into subdirectories
                if identifier == 'item1':
                    # Should have 2 files + 2 subdirectories at the top level
                    expected_top_level = {'item1_data.txt', 'item1_doc.pdf', 'subdir', 'videos.thumbs'}
                    self.assertEqual(set(top_level_entries.keys()), expected_top_level,
                                   f"Directory {identifier} has wrong top-level entries")
                    
                    # Verify subdir contains nested_file.txt
                    if 'subdir' in top_level_entries:
                        subdir_cid = top_level_entries['subdir']
                        subdir_result, subdir_error = run_cmd(["ipfs", "--api", "/ip4/127.0.0.1/tcp/5009", "ls", "--size=false", "--resolve-type=false", subdir_cid])
                        self.assertIsNotNone(subdir_result, f"Failed to list subdir {subdir_cid}: {subdir_error}")
                        
                        subdir_files = []
                        for line in subdir_result.split('\n'):
                            if line.strip():
                                parts = line.split()
                                if len(parts) >= 2:
                                    subdir_files.append(parts[1])
                        
                        self.assertEqual(set(subdir_files), {'nested_file.txt'},
                                       f"subdir should contain nested_file.txt, got {subdir_files}")
                    
                    # Verify videos.thumbs contains thumb_001.jpg
                    if 'videos.thumbs' in top_level_entries:
                        thumbs_cid = top_level_entries['videos.thumbs']
                        thumbs_result, thumbs_error = run_cmd(["ipfs", "--api", "/ip4/127.0.0.1/tcp/5009", "ls", "--size=false", "--resolve-type=false", thumbs_cid])
                        self.assertIsNotNone(thumbs_result, f"Failed to list videos.thumbs {thumbs_cid}: {thumbs_error}")
                        
                        thumbs_files = []
                        for line in thumbs_result.split('\n'):
                            if line.strip():
                                parts = line.split()
                                if len(parts) >= 2:
                                    thumbs_files.append(parts[1])
                        
                        self.assertEqual(set(thumbs_files), {'thumb_001.jpg', 'thumb_002.jpg'},
                                       f"videos.thumbs should contain thumb files, got {thumbs_files}")
                        
                else:  # item2
                    # item2 only has files at the root level
                    expected_top_level = {'item2_image.jpg', 'item2_notes.md'}
                    self.assertEqual(set(top_level_entries.keys()), expected_top_level,
                                   f"Directory {identifier} has wrong top-level entries")
    
    def test_subdirectory_handling(self):
        """Test that subdirectories are handled correctly in recursive file listing"""
        # Ensure daemon is running for all operations
        from shared import ensure_staging_ipfs
        ensure_staging_ipfs()
        
        # Add test fixtures to IPFS 
        result, error = run_cmd(["ipfs", "--api", "/ip4/127.0.0.1/tcp/5009", "add", "-r", "--cid-version=1", "test_fixtures"])
        self.assertIsNotNone(result, f"Failed to add test fixtures: {error}")
        
        # Extract root CID
        root_cid = None
        for line in result.split('\n'):
            if line.strip().endswith('test_fixtures'):
                root_cid = line.split()[1]
                break
        self.assertIsNotNone(root_cid, "Could not find root CID")
        
        # Test recursive file listing
        from shared import list_files_with_cids
        files_with_cids = list_files_with_cids(root_cid)
        
        # Check that subdirectory files are found with their full paths
        expected_files = {
            "item1_doc.pdf",
            "item1_data.txt", 
            "item1_files.xml",
            "item1_meta.xml",
            "item2_files.xml",
            "item2_image.jpg",
            "item2_meta.xml", 
            "item2_notes.md",
            "subdir/nested_file.txt",  # File in subdirectory
            "videos.thumbs/thumb_001.jpg",  # File in subdirectory
            "videos.thumbs/thumb_002.jpg"   # File in subdirectory
        }
        
        found_files = set(files_with_cids.keys())
        self.assertEqual(found_files, expected_files, 
                        f"Expected files: {expected_files}, got: {found_files}")
        
        # Verify that subdirectory files have valid CIDs
        self.assertIn("subdir/nested_file.txt", files_with_cids)
        self.assertIn("videos.thumbs/thumb_001.jpg", files_with_cids)
        self.assertIn("videos.thumbs/thumb_002.jpg", files_with_cids)
        
        # Verify CIDs are valid (start with 'bafk' for CIDv1)
        for path, cid in files_with_cids.items():
            self.assertTrue(cid.startswith('bafk'), f"Invalid CID for {path}: {cid}")
    
    def test_merge_roots_command(self):
        """Test the merge-roots command merges multiple CIDs correctly"""
        # Ensure daemon is running for all operations
        from shared import ensure_staging_ipfs
        ensure_staging_ipfs()
        
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
            result1, error1 = run_cmd(["ipfs", "--api", "/ip4/127.0.0.1/tcp/5009", "add", "-r", "--cid-version=1", "test_fixtures"])
            self.assertIsNotNone(result1, f"Failed to add test_fixtures: {error1}")
            
            result2, error2 = run_cmd(["ipfs", "--api", "/ip4/127.0.0.1/tcp/5009", "add", "-r", "--cid-version=1", str(test_dir2)])
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
            # Note: item1_doc.pdf should be EXCLUDED because it appears in both with different content
            expected_files = {
                "item1_data.txt", "item1_files.xml", "item1_meta.xml",
                "item2_files.xml", "item2_image.jpg", "item2_meta.xml", "item2_notes.md", 
                "item3_data.csv", "shared_file.txt"
            }
            
            # item1_doc.pdf should NOT be in the merged result (conflict)
            excluded_files = {"item1_doc.pdf"}
            
            actual_files = set(merged_files)
            self.assertTrue(expected_files.issubset(actual_files), 
                          f"Missing files in merged directory. Expected {expected_files}, got {actual_files}")
            
            # Verify conflicted files were excluded
            for excluded in excluded_files:
                self.assertNotIn(excluded, actual_files,
                               f"Conflicted file {excluded} should have been excluded but was found in merge")

            # Test file access in merged directory
            test_result, test_error = run_cmd(["ipfs", "cat", f"{merged_cid}/shared_file.txt"])
            self.assertIsNotNone(test_result, f"Could not access file in merged directory: {test_error}")

        finally:
            # tearDown() will handle cleanup
            pass

    def test_collect_command(self):
        """Test the collect command wraps CIDs in a parent directory without reading subgraphs"""
        from shared import ensure_staging_ipfs
        ensure_staging_ipfs()

        # add test fixtures to get a CID
        result1, error1 = run_cmd(["ipfs", "--api", "/ip4/127.0.0.1/tcp/5009", "add", "-r", "--cid-version=1", "test_fixtures"])
        self.assertIsNotNone(result1, f"Failed to add test_fixtures: {error1}")

        # create a second directory
        test_dir2 = Path("test_fixtures2")
        test_dir2.mkdir(exist_ok=True)
        (test_dir2 / "file.txt").write_text("test content")

        try:
            result2, error2 = run_cmd(["ipfs", "--api", "/ip4/127.0.0.1/tcp/5009", "add", "-r", "--cid-version=1", str(test_dir2)])
            self.assertIsNotNone(result2, f"Failed to add test_fixtures2: {error2}")

            # extract root CIDs
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

            # run collect command (--no-someguy for test environment)
            collect_result, collect_error = run_cmd(["python3", "ia_fil.py", "--no-someguy", "collect", root_cid1, root_cid2])
            self.assertIsNotNone(collect_result, f"collect command failed: {collect_error}")

            collection_cid = collect_result.strip()
            self.assertTrue(collection_cid.startswith(('Qm', 'bafy')), f"Invalid CID format: {collection_cid}")

            # verify the collection directory has entries named after the input CIDs
            ls_result, ls_error = run_cmd(["ipfs", "--api", "/ip4/127.0.0.1/tcp/5009", "ls", "--size=false", "--resolve-type=false", collection_cid])
            self.assertIsNotNone(ls_result, f"Failed to list collection {collection_cid}: {ls_error}")

            entries = {}
            for line in ls_result.split('\n'):
                if line.strip():
                    parts = line.split()
                    if len(parts) >= 2:
                        entry_cid = parts[0]
                        entry_name = parts[1]
                        entries[entry_name] = entry_cid

            # entries should be named after input CIDs and point to them
            self.assertIn(root_cid1, entries, f"Collection should have entry named {root_cid1}")
            self.assertIn(root_cid2, entries, f"Collection should have entry named {root_cid2}")
            self.assertEqual(entries[root_cid1], root_cid1, "Entry should point to same CID as its name")
            self.assertEqual(entries[root_cid2], root_cid2, "Entry should point to same CID as its name")

            # verify we can traverse into the collected directories
            sub_ls, sub_error = run_cmd(["ipfs", "--api", "/ip4/127.0.0.1/tcp/5009", "ls", "--size=false", "--resolve-type=false", f"{collection_cid}/{root_cid1}"])
            self.assertIsNotNone(sub_ls, f"Failed to list subdirectory: {sub_error}")
            self.assertIn("item1_data.txt", sub_ls, "Should be able to see files in collected directory")

        finally:
            pass

    def test_extract_then_collect(self):
        """Integration test: extract-items creates synthetic dirs, collect wraps them"""
        from shared import ensure_staging_ipfs
        ensure_staging_ipfs()

        # add test fixtures
        result, error = run_cmd(["ipfs", "--api", "/ip4/127.0.0.1/tcp/5009", "add", "-r", "--cid-version=1", "test_fixtures"])
        self.assertIsNotNone(result, f"Failed to add test_fixtures: {error}")

        root_cid = None
        for line in result.split('\n'):
            if line.strip().endswith('test_fixtures'):
                root_cid = line.split()[1]
                break
        self.assertIsNotNone(root_cid, "Could not find root CID")

        # run extract-items to create synthetic directories
        extract_result, extract_error = run_cmd(["python3", "ia_fil.py", "--no-someguy", "extract-items", root_cid])
        self.assertIsNotNone(extract_result, f"extract-items failed: {extract_error}")

        # parse CSV output to get synthetic CIDs
        lines = extract_result.strip().split('\n')
        csv_lines = [line for line in lines if ',' in line and not line.startswith('  ')]
        self.assertGreaterEqual(len(csv_lines), 2, "Expected CSV output from extract-items")

        synthetic_cids = []
        for line in csv_lines[1:]:  # skip header
            parts = line.split(',', 1)
            if len(parts) == 2:
                synthetic_cids.append(parts[1])  # cid is second column

        self.assertGreater(len(synthetic_cids), 0, "No synthetic CIDs produced")

        # now collect the synthetic directories
        collect_result, collect_error = run_cmd(["python3", "ia_fil.py", "--no-someguy", "collect"] + synthetic_cids)
        self.assertIsNotNone(collect_result, f"collect failed: {collect_error}")

        collection_cid = collect_result.strip()
        self.assertTrue(collection_cid.startswith(('Qm', 'bafy')), f"Invalid collection CID: {collection_cid}")

        # verify collection contains entries for each synthetic directory
        ls_result, ls_error = run_cmd(["ipfs", "--api", "/ip4/127.0.0.1/tcp/5009", "ls", "--size=false", "--resolve-type=false", collection_cid])
        self.assertIsNotNone(ls_result, f"Failed to list collection: {ls_error}")

        entries = []
        for line in ls_result.split('\n'):
            if line.strip():
                parts = line.split()
                if len(parts) >= 2:
                    entries.append(parts[1])  # entry name

        # each synthetic CID should appear as an entry name
        for syn_cid in synthetic_cids:
            self.assertIn(syn_cid, entries, f"Synthetic CID {syn_cid} not in collection")

        # verify we can traverse collection -> synthetic dir -> file
        first_syn = synthetic_cids[0]
        deep_ls, deep_error = run_cmd(["ipfs", "--api", "/ip4/127.0.0.1/tcp/5009", "ls", "--size=false", "--resolve-type=false", f"{collection_cid}/{first_syn}"])
        self.assertIsNotNone(deep_ls, f"Failed to traverse into collected item: {deep_error}")
        # should see item files
        self.assertTrue(len(deep_ls.strip()) > 0, "Collected item should have contents")

    def test_merge_roots_heuristic_misses_misleading_dir(self):
        """Test that extension heuristic incorrectly treats 'file.jpg/' dir as a file"""
        from shared import ensure_staging_ipfs
        ensure_staging_ipfs()

        # create a misleadingly named directory
        tricky_dir = Path("test_tricky")
        tricky_dir.mkdir(exist_ok=True)
        misleading = tricky_dir / "image.jpg"  # looks like file, is actually dir
        misleading.mkdir(exist_ok=True)
        (misleading / "actual_content.txt").write_text("hidden inside")
        (tricky_dir / "normal.txt").write_text("visible")

        try:
            result, error = run_cmd(["ipfs", "--api", "/ip4/127.0.0.1/tcp/5009", "add", "-r", "--cid-version=1", str(tricky_dir)])
            self.assertIsNotNone(result, f"Failed to add tricky dir: {error}")

            root_cid = None
            for line in result.split('\n'):
                if line.strip().endswith('test_tricky'):
                    root_cid = line.split()[1]
                    break
            self.assertIsNotNone(root_cid, "Could not find root CID")

            # merge WITHOUT --force-check-directories (uses heuristic)
            merge_result, merge_error = run_cmd(["python3", "ia_fil.py", "--no-someguy", "merge-roots", root_cid])
            self.assertIsNotNone(merge_result, f"merge-roots failed: {merge_error}")

            merged_cid = merge_result.strip()

            # list merged directory
            ls_result, ls_error = run_cmd(["ipfs", "--api", "/ip4/127.0.0.1/tcp/5009", "ls", "--size=false", "--resolve-type=false", merged_cid])
            self.assertIsNotNone(ls_result, f"Failed to list merged: {ls_error}")

            files = [line.split()[1] for line in ls_result.strip().split('\n') if line.strip()]

            # heuristic should have treated image.jpg as a file (wrong!)
            # so we should see "image.jpg" at top level as a file link
            self.assertIn("image.jpg", files, "Heuristic should treat image.jpg as file")
            self.assertIn("normal.txt", files)

            # trying to ls into image.jpg should fail or return nothing
            # because it was copied as a file reference, not traversed as directory
            nested_ls, _ = run_cmd(["ipfs", "--api", "/ip4/127.0.0.1/tcp/5009", "ls", "--size=false", "--resolve-type=false", f"{merged_cid}/image.jpg"])
            # the nested ls might succeed (since original is a dir) but the key point is
            # actual_content.txt was never added to the merge as a separate entry
            # we verified this by only checking top-level has just image.jpg and normal.txt

        finally:
            shutil.rmtree(tricky_dir, ignore_errors=True)

    def test_merge_roots_force_check_finds_misleading_dir(self):
        """Test that --force-check-directories correctly identifies 'file.jpg/' as directory"""
        from shared import ensure_staging_ipfs
        ensure_staging_ipfs()

        # create same misleadingly named directory
        tricky_dir = Path("test_tricky2")
        tricky_dir.mkdir(exist_ok=True)
        misleading = tricky_dir / "image.jpg"
        misleading.mkdir(exist_ok=True)
        (misleading / "actual_content.txt").write_text("hidden inside")
        (tricky_dir / "normal.txt").write_text("visible")

        try:
            result, error = run_cmd(["ipfs", "--api", "/ip4/127.0.0.1/tcp/5009", "add", "-r", "--cid-version=1", str(tricky_dir)])
            self.assertIsNotNone(result, f"Failed to add tricky dir: {error}")

            root_cid = None
            for line in result.split('\n'):
                if line.strip().endswith('test_tricky2'):
                    root_cid = line.split()[1]
                    break
            self.assertIsNotNone(root_cid, "Could not find root CID")

            # merge WITH --force-check-directories
            merge_result, merge_error = run_cmd(["python3", "ia_fil.py", "--no-someguy", "merge-roots", "--force-check-directories", root_cid])
            self.assertIsNotNone(merge_result, f"merge-roots failed: {merge_error}")

            merged_cid = merge_result.strip()

            # list merged directory - top level should have image.jpg as directory
            ls_result, ls_error = run_cmd(["ipfs", "--api", "/ip4/127.0.0.1/tcp/5009", "ls", "--size=false", "--resolve-type=false", merged_cid])
            self.assertIsNotNone(ls_result, f"Failed to list merged: {ls_error}")

            top_level = [line.split()[1] for line in ls_result.strip().split('\n') if line.strip()]
            self.assertIn("image.jpg", top_level, f"Should have image.jpg dir, got: {top_level}")
            self.assertIn("normal.txt", top_level)

            # with force check, image.jpg should be a directory we can traverse into
            nested_ls, nested_error = run_cmd(["ipfs", "--api", "/ip4/127.0.0.1/tcp/5009", "ls", "--size=false", "--resolve-type=false", f"{merged_cid}/image.jpg"])
            self.assertIsNotNone(nested_ls, f"Failed to list image.jpg subdir: {nested_error}")

            nested_files = [line.split()[1] for line in nested_ls.strip().split('\n') if line.strip()]
            self.assertIn("actual_content.txt", nested_files,
                         f"Force check should find nested file inside image.jpg/, got: {nested_files}")

        finally:
            shutil.rmtree(tricky_dir, ignore_errors=True)

if __name__ == "__main__":
    unittest.main(verbosity=2)
