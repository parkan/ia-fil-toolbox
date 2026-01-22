import sys
from typing import List, Dict, Set
from shared import list_files_with_cids, log_errors, run_ipfs_cmd, create_directory_via_mfs

def merge_root_cids(cids: List[str], force_check_directories: bool = False) -> str:
    """
    Merge multiple root CIDs into a single synthetic directory.
    
    Files with the same name but different CIDs are considered conflicts and
    are excluded from the merge to avoid ambiguity.
    
    Args:
        cids: List of root CIDs to merge
        force_check_directories: If False (default), use file extension heuristics to skip expensive checks
        
    Returns:
        CID of the new synthetic directory containing all non-conflicting files
    """
    print(f"Merging {len(cids)} root CIDs...", file=sys.stderr)
    
    if not force_check_directories:
        print(f"  Using file extension heuristics to skip directory checks", file=sys.stderr)
    
    all_files = {}  # filename -> file_cid
    conflicted_files = set()  # Track files with conflicts to exclude them
    errors = []
    
    # Collect all files from all root CIDs
    for cid in cids:
        print(f"  Listing files in {cid}...", file=sys.stderr)
        files_with_cids = list_files_with_cids(cid, force_check_directories=force_check_directories)
        
        if not files_with_cids:
            error_msg = f"{cid}\t*\tIPFS_ERROR\tNo files found or failed to list files"
            errors.append(error_msg)
            continue
            
        print(f"    Found {len(files_with_cids)} files", file=sys.stderr)
        
        # Check for filename conflicts
        for filename, file_cid in files_with_cids.items():
            if filename in all_files:
                if all_files[filename] != file_cid:
                    # Conflict detected - mark for exclusion
                    conflicted_files.add(filename)
                    error_msg = f"{cid}\t{filename}\tDATA_ERROR\tFilename conflict: {filename} exists with different CID"
                    errors.append(error_msg)
                    print(f"    ⚠️ Conflict: {filename} (will be excluded from merge)", file=sys.stderr)
                else:
                    print(f"    ✓ {filename} (duplicate, same CID)", file=sys.stderr)
            else:
                all_files[filename] = file_cid
    
    # Remove all conflicted files
    if conflicted_files:
        print(f"  Removing {len(conflicted_files)} conflicted files from merge...", file=sys.stderr)
        for filename in conflicted_files:
            if filename in all_files:
                del all_files[filename]
                print(f"    ✗ Excluded: {filename}", file=sys.stderr)
    
    if errors:
        log_errors(errors)
    
    if not all_files:
        print("  Error: No files found in any of the provided CIDs", file=sys.stderr)
        return None
    
    print(f"  Total unique files: {len(all_files)}", file=sys.stderr)
    
    print(f"  Creating merged directory with {len(all_files)} files...", file=sys.stderr)
    
    try:
        # Use MFS for all directory creation - automatically handles dag-pb and HAMT sharding
        print(f"  Using MFS (auto-HAMT, dag-pb preservation)...", file=sys.stderr)
        merged_cid = create_directory_via_mfs(all_files, "merge")
        print(f"  ✓ Created merged directory: {merged_cid}", file=sys.stderr)
        return merged_cid
            
    except Exception as e:
        print(f"  ✗ Error creating merged directory: {e}", file=sys.stderr)
        error_msg = f"*\t*\tIPFS_ERROR\tError creating merged directory: {str(e)}"
        log_errors([error_msg])
        return None

def run_merge_roots(cids: List[str], force_check_directories: bool = False):
    """
    Main entry point for merge-roots command.
    
    Args:
        cids: List of root CIDs to merge
        force_check_directories: If False (default), use file extension heuristics to skip expensive checks
    """
    if not cids:
        print("Error: No CIDs provided", file=sys.stderr)
        return
    
    print(f"Starting merge-roots for {len(cids)} CIDs", file=sys.stderr)
    
    merged_cid = merge_root_cids(cids, force_check_directories=force_check_directories)
    
    if merged_cid:
        print(merged_cid)  # Output just the CID for easy scripting
        
        # Generate shallow CAR file with just the directory structure
        from shared import generate_shallow_car_file
        car_filename = f"merge_roots_{merged_cid}.car"
        
        # For merged roots, we don't have child CIDs to include, just the root
        generate_shallow_car_file(merged_cid, [], car_filename)
    else:
        print("Error: Failed to create merged directory", file=sys.stderr)
        sys.exit(1)
