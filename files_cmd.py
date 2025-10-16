import sys
import subprocess
from typing import List, Dict, Any, Tuple, Optional
from lxml import etree
from shared import (read_cids_from_file, list_files, list_files_with_cids, log_errors, xml_to_dict,
                   fetch_xml_files_parallel, validate_xml_completeness, run_ipfs_cmd, pin_cid, gc_repo, 
                   create_directory_via_mfs, DEBUG)

def parse_files_xml(xml_content: bytes) -> List[Dict[str, Any]]:
    files_dict = xml_to_dict(xml_content)
    files_data = []
    
    if 'files' in files_dict and 'file' in files_dict['files']:
        file_entries = files_dict['files']['file']
        if not isinstance(file_entries, list):
            file_entries = [file_entries]
        
        for file_entry in file_entries:
            if isinstance(file_entry, dict) and '@attributes' in file_entry:
                file_info = {
                    'name': file_entry['@attributes'].get('name', ''),
                    'source': file_entry['@attributes'].get('source', ''),
                }
                
                for field in ['mtime', 'size', 'md5', 'crc32', 'sha1', 'format']:
                    if field in file_entry:
                        file_info[field] = file_entry[field]
                
                files_data.append(file_info)
    
    return files_data

def create_synthetic_directory(cid: str, identifier: str, files_data: List[Dict[str, Any]], available_files: Dict[str, str]) -> str:
    """
    Create a synthetic UnixFS directory node containing all files from files.xml
    
    Returns:
        CID of the created directory, or None if failed
    """
    print(f"      Creating synthetic directory for {identifier}", file=sys.stderr)
    
    # Build the DAG-JSON structure for the directory
    links = []
    
    for file_info in files_data:
        file_name = file_info.get('name', '')
        if not file_name:
            continue
            
        if file_name not in available_files:
            print(f"        ✗ {file_name} (not found in root CID, skipping)")
            error_msg = f"{cid}\t{identifier}\tDATA_ERROR\tFile not found in root CID: {file_name}"
            log_errors([error_msg])
            continue
        
        # Use the actual individual file CID from ipfs ls output
        file_cid = available_files[file_name]
        
        links.append({
            "Name": file_name,
            "Hash": {"/": file_cid}
        })
        print(f"        ✓ {file_name} -> {file_cid}", file=sys.stderr)
    
    if not links:
        print(f"        No valid files found for {identifier}", file=sys.stderr)
        return None
    
    print(f"      Creating directory with {len(links)} files...", file=sys.stderr)
    
    try:
        # Convert links to files_dict for MFS
        files_dict = {}
        for link in links:
            files_dict[link["Name"]] = link["Hash"]["/"]
        
        # Use MFS to create directory - automatically handles dag-pb and HAMT sharding
        dir_cid = create_directory_via_mfs(files_dict, f"item_{identifier}")
        print(f"      ✓ Created synthetic directory: {dir_cid}", file=sys.stderr)
        
        # Pin the synthetic directory to prevent GC
        # Temporarily disabled - pinning can hang when fetching remote blocks
        # pin_cid(dir_cid)
        return dir_cid
            
    except Exception as e:
        print(f"      ✗ Error creating directory: {e}")
        error_msg = f"{cid}\t{identifier}\tIPFS_ERROR\tError creating synthetic directory: {str(e)}"
        log_errors([error_msg])
        return None

def process_file_list(cid: str, identifier: str, files_data: List[Dict[str, Any]], available_files: Dict[str, str]) -> Optional[str]:
    """
    Process the parsed file list by creating a synthetic UnixFS directory.
    
    Args:
        cid: Root CID being processed
        identifier: Identifier for this files.xml
        files_data: Parsed file information from files.xml
        available_files: Dict mapping filename -> file_cid in the root CID
        
    Returns:
        CID of synthetic directory if successful, None if failed
    """
    print(f"      Processing {len(files_data)} files for {identifier}", file=sys.stderr)
    
    synthetic_cid = create_synthetic_directory(cid, identifier, files_data, available_files)
    
    if synthetic_cid:
        print(f"      🎯 Synthetic directory created: {synthetic_cid}", file=sys.stderr)
        return synthetic_cid
    else:
        print(f"      ⚠ Failed to create synthetic directory for {identifier}", file=sys.stderr)
        return None

def process_cid_files(cid: str) -> List[Tuple[str, str]]:
    """
    Process files for a CID and return list of (identifier, synthetic_cid) tuples
    """
    print(f"\nProcessing files for CID: {cid}", file=sys.stderr)
    results = []
    
    print("  Finding XML files...", file=sys.stderr)
    # First pass: minimal listing to find XML files only (no optimization needed)
    # We'll use the simple list_files function which doesn't do recursive traversal
    from shared import list_files
    xml_files = list_files(cid)
    if not xml_files:
        error_msg = f"{cid}\t*\tIPFS_ERROR\tNo files found or failed to list files"
        log_errors([error_msg])
        return results
    
    meta_files = [f for f in xml_files if f.endswith('_meta.xml')]
    files_files = [f for f in xml_files if f.endswith('_files.xml')]
    
    if not meta_files and not files_files:
        error_msg = f"{cid}\t*\tIPFS_ERROR\tNo XML files found"
        log_errors([error_msg])
        return results
    
    meta_identifiers = {f.replace('_meta.xml', '') for f in meta_files}
    files_identifiers = {f.replace('_files.xml', '') for f in files_files}
    all_identifiers = list(meta_identifiers | files_identifiers)
    
    print(f"  Found {len(meta_files)} meta files and {len(files_files)} files.xml", file=sys.stderr)
    print(f"  Processing {len(all_identifiers)} identifiers", file=sys.stderr)
    
    print("  Fetching XML files...", file=sys.stderr)
    # We need to get CIDs for the XML files - but we can do this with simple top-level listing
    # Use the simple list_files_with_cids but only for the root directory (no recursion)
    from shared import run_ipfs_cmd
    result = run_ipfs_cmd(['ls', '--resolve-type=false', '--size=false', cid], capture_output=True, text=True)
    if result.returncode != 0:
        error_msg = f"{cid}\t*\tIPFS_ERROR\tFailed to list root directory: {result.stderr}"
        log_errors([error_msg])
        return results
    
    # Parse the simple listing to get XML file CIDs
    xml_files_with_cids = {}
    for line in result.stdout.strip().split('\n'):
        if not line:
            continue
        parts = line.split(maxsplit=1)
        if len(parts) == 2:
            file_cid = parts[0]
            filename = parts[1]
            if filename in meta_files + files_files:
                xml_files_with_cids[filename] = file_cid
    
    # Fetch XML files directly by CID
    xml_results = {}
    for identifier in all_identifiers:
        xml_results[identifier] = {}
        for xml_type in ['meta', 'files']:
            filename = f"{identifier}_{xml_type}.xml"
            if filename in xml_files_with_cids:
                file_cid = xml_files_with_cids[filename]
                try:
                    result = run_ipfs_cmd(['cat', file_cid], capture_output=True)
                    if result.returncode == 0:
                        xml_results[identifier][xml_type] = result.stdout
                        print(f"    ✓ Fetched {filename}", file=sys.stderr)
                    else:
                        print(f"    ✗ Failed to fetch {filename}: {result.stderr}", file=sys.stderr)
                except Exception as e:
                    print(f"    ✗ Exception fetching {filename}: {e}", file=sys.stderr)
    
    # Validate completeness early - no point processing if we don't have complete pairs
    print("  Validating completeness...", file=sys.stderr)
    valid_identifiers = validate_xml_completeness(cid, all_identifiers, xml_results, {'meta', 'files'})
    
    if not valid_identifiers:
        print("  No complete meta/files pairs found", file=sys.stderr)
        return results
    
    # Extract all known filenames from files.xml for optimization
    # Only process valid identifiers
    print("  Extracting known filenames from files.xml...", file=sys.stderr)
    known_files = set()
    for identifier in valid_identifiers:
        xml_data = xml_results[identifier]
        if 'files' in xml_data:
            try:
                files_data = parse_files_xml(xml_data['files'])
                for file_info in files_data:
                    filename = file_info.get('name', '')
                    if filename:
                        known_files.add(filename)
            except Exception as e:
                if DEBUG:
                    print(f"  DEBUG: Failed to parse files.xml for {identifier}: {e}", file=sys.stderr)
    
    print(f"  Found {len(known_files)} known filenames from files.xml", file=sys.stderr)
    if DEBUG and known_files:
        print(f"  DEBUG: Sample known files: {list(sorted(known_files))[:10]}", file=sys.stderr)
    
    # Second pass: comprehensive listing with known filenames for optimization
    print("  Listing all files with optimization...", file=sys.stderr)
    all_files = list_files_with_cids(cid, known_files)
    
    print(f"  Processing {len(valid_identifiers)} complete pairs...", file=sys.stderr)
    
    for i, identifier in enumerate(valid_identifiers, 1):
        print(f"    Processing {identifier} ({i}/{len(valid_identifiers)})...", file=sys.stderr)
        
        files_content = xml_results[identifier]['files']
        
        try:
            files_data = parse_files_xml(files_content)
            print(f"      Found {len(files_data)} files in files.xml", file=sys.stderr)
            
            synthetic_cid = process_file_list(cid, identifier, files_data, all_files)
            if synthetic_cid:
                results.append((identifier, synthetic_cid))
                print(f"      ✓ Completed {identifier}", file=sys.stderr)
            else:
                print(f"      ✗ Failed to create synthetic directory for {identifier}", file=sys.stderr)
                
        except etree.XMLSyntaxError as e:
            error_msg = f"{cid}\t{identifier}\tDATA_ERROR\tXML parse error: {str(e)}"
            log_errors([error_msg])
        except Exception as e:
            error_msg = f"{cid}\t{identifier}\tDATA_ERROR\t{str(e)}"
            log_errors([error_msg])
    
    print(f"  Completed processing all identifiers for {cid}", file=sys.stderr)
    return results

def run_files(cids: List[str]):
    all_results = []
    
    for cid in cids:
        try:
            results = process_cid_files(cid)
            all_results.extend(results)
        except Exception as e:
            print(f"Error processing {cid}: {e}", file=sys.stderr)
    
    # Only print CSV header and results if we have any
    if all_results:
        print("identifier,synthetic_cid")  # CSV header
        for identifier, synthetic_cid in all_results:
            print(f"{identifier},{synthetic_cid}")
        
        # Create container directory with all synthetic directories
        print("\nCreating container directory...", file=sys.stderr)
        container_files = {}
        child_cids = []
        for identifier, synthetic_cid in all_results:
            container_files[identifier] = synthetic_cid
            child_cids.append(synthetic_cid)
        
        from shared import create_directory_via_mfs, generate_shallow_car_file, pin_cid
        container_cid = create_directory_via_mfs(container_files, "extract_items_container")
        
        if container_cid:
            print(f"\n📁 Container directory (MFS root): {container_cid}", file=sys.stderr)
            print(container_cid)  # Also print to stdout for easy access
            
            # Pin the container directory
            # Temporarily disabled - pinning can hang when fetching remote blocks
            # pin_cid(container_cid)
            
            # Generate shallow CAR file with just the directory structure
            car_filename = f"extract_items_{container_cid}.car"
            generate_shallow_car_file(container_cid, child_cids, car_filename)
        
        # Clean up temporary blocks after pinning what we want to keep
        # Temporarily disabled for rapid iteration - keeping blocks in blockstore
        # gc_repo()
