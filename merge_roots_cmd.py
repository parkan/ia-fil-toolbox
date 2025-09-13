import json
import sys
from typing import List, Dict, Set
from shared import list_files_with_cids, log_errors, run_ipfs_cmd, pin_cid, gc_repo

def merge_root_cids(cids: List[str]) -> str:
    """
    Merge multiple root CIDs into a single synthetic directory.
    
    Args:
        cids: List of root CIDs to merge
        
    Returns:
        CID of the new synthetic directory containing all files
    """
    print(f"Merging {len(cids)} root CIDs...", file=sys.stderr)
    
    all_files = {}  # filename -> file_cid
    errors = []
    
    # Collect all files from all root CIDs
    for cid in cids:
        print(f"  Listing files in {cid}...", file=sys.stderr)
        files_with_cids = list_files_with_cids(cid)
        
        if not files_with_cids:
            error_msg = f"{cid}\t*\tIPFS_ERROR\tNo files found or failed to list files"
            errors.append(error_msg)
            continue
            
        print(f"    Found {len(files_with_cids)} files", file=sys.stderr)
        
        # Check for filename conflicts
        for filename, file_cid in files_with_cids.items():
            if filename in all_files:
                if all_files[filename] != file_cid:
                    error_msg = f"{cid}\t{filename}\tDATA_ERROR\tFilename conflict: {filename} exists with different CID"
                    errors.append(error_msg)
                    print(f"    ⚠️ Conflict: {filename} (keeping first occurrence)", file=sys.stderr)
                    continue
                else:
                    print(f"    ✓ {filename} (duplicate, same CID)", file=sys.stderr)
            else:
                all_files[filename] = file_cid
                print(f"    ✓ {filename} -> {file_cid}", file=sys.stderr)
    
    if errors:
        log_errors(errors)
    
    if not all_files:
        print("  Error: No files found in any of the provided CIDs", file=sys.stderr)
        return None
    
    print(f"  Total unique files: {len(all_files)}", file=sys.stderr)
    
    # Create links for the synthetic directory
    links = []
    for filename, file_cid in sorted(all_files.items()):
        links.append({
            "Name": filename,
            "Hash": {"/": file_cid}
        })
    
    # Create the DAG-JSON structure
    dag_json = {
        "Data": {"/": {"bytes": "CAE"}},
        "Links": links
    }
    
    print(f"  Creating merged directory with {len(links)} files...", file=sys.stderr)
    
    try:
        # Use ipfs dag put to create the directory
        result = run_ipfs_cmd([
            'dag', 'put',
            '--store-codec=dag-pb',
            '--input-codec=dag-json'
        ], input=json.dumps(dag_json), capture_output=True, text=True)
        
        if result.returncode == 0:
            merged_cid = result.stdout.strip()
            print(f"  ✓ Created merged directory: {merged_cid}", file=sys.stderr)
            # Pin the merged directory to prevent GC
            pin_cid(merged_cid)
            return merged_cid
        else:
            print(f"  ✗ Failed to create merged directory: {result.stderr}", file=sys.stderr)
            error_msg = f"*\t*\tIPFS_ERROR\tFailed to create merged directory: {result.stderr}"
            log_errors([error_msg])
            return None
            
    except Exception as e:
        print(f"  ✗ Error creating merged directory: {e}", file=sys.stderr)
        error_msg = f"*\t*\tIPFS_ERROR\tError creating merged directory: {str(e)}"
        log_errors([error_msg])
        return None

def run_merge_roots(cids: List[str]):
    """
    Main entry point for merge-roots command.
    
    Args:
        cids: List of root CIDs to merge
    """
    if not cids:
        print("Error: No CIDs provided", file=sys.stderr)
        return
    
    print(f"Starting merge-roots for {len(cids)} CIDs", file=sys.stderr)
    
    merged_cid = merge_root_cids(cids)
    
    if merged_cid:
        print(merged_cid)  # Output just the CID for easy scripting
        # Clean up temporary blocks after pinning what we want to keep
        gc_repo()
    else:
        print("Error: Failed to create merged directory", file=sys.stderr)
        sys.exit(1)
