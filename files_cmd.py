import sys
import subprocess
from typing import List, Dict, Any, Tuple, Optional
from lxml import etree
from shared import (read_cids_from_file, list_files, list_files_with_cids, log_errors, xml_to_dict,
                   fetch_xml_files_parallel, validate_xml_completeness, run_ipfs_cmd, pin_cid, gc_repo, 
                   create_directory_via_mfs)

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
    print(f"      Creating synthetic directory for {identifier}")
    
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
        print(f"        ✓ {file_name} -> {file_cid}")
    
    if not links:
        print(f"        No valid files found for {identifier}")
        return None
    
    print(f"      Creating directory with {len(links)} files...")
    
    try:
        # Convert links to files_dict for MFS
        files_dict = {}
        for link in links:
            files_dict[link["Name"]] = link["Hash"]["/"]
        
        # Use MFS to create directory - automatically handles dag-pb and HAMT sharding
        dir_cid = create_directory_via_mfs(files_dict, f"item_{identifier}")
        print(f"      ✓ Created synthetic directory: {dir_cid}")
        
        # Pin the synthetic directory to prevent GC
        pin_cid(dir_cid)
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
    print(f"      Processing {len(files_data)} files for {identifier}")
    
    synthetic_cid = create_synthetic_directory(cid, identifier, files_data, available_files)
    
    if synthetic_cid:
        print(f"      🎯 Synthetic directory created: {synthetic_cid}")
        return synthetic_cid
    else:
        print(f"      ⚠ Failed to create synthetic directory for {identifier}")
        return None

def process_cid_files(cid: str) -> List[Tuple[str, str]]:
    """
    Process files for a CID and return list of (identifier, synthetic_cid) tuples
    """
    print(f"\nProcessing files for CID: {cid}")
    results = []
    
    print("  Listing files...")
    all_files = list_files_with_cids(cid)
    if not all_files:
        error_msg = f"{cid}\t*\tIPFS_ERROR\tNo files found or failed to list files"
        log_errors([error_msg])
        return results
    
    meta_files = [f for f in all_files.keys() if f.endswith('_meta.xml')]
    files_files = [f for f in all_files.keys() if f.endswith('_files.xml')]
    
    if not meta_files and not files_files:
        error_msg = f"{cid}\t*\tIPFS_ERROR\tNo XML files found"
        log_errors([error_msg])
        return results
    
    meta_identifiers = {f.replace('_meta.xml', '') for f in meta_files}
    files_identifiers = {f.replace('_files.xml', '') for f in files_files}
    all_identifiers = list(meta_identifiers | files_identifiers)
    
    print(f"  Found {len(meta_files)} meta files and {len(files_files)} files.xml")
    print(f"  Processing {len(all_identifiers)} identifiers")
    
    print("  Fetching XML files...")
    xml_results = fetch_xml_files_parallel(cid, all_identifiers, {'meta', 'files'})
    
    print("  Validating completeness...")
    valid_identifiers = validate_xml_completeness(cid, all_identifiers, xml_results, {'meta', 'files'})
    
    if not valid_identifiers:
        print("  No complete meta/files pairs found")
        return results
    
    print(f"  Processing {len(valid_identifiers)} complete pairs...")
    
    for identifier in valid_identifiers:
        print(f"    Processing {identifier}...")
        
        files_content = xml_results[identifier]['files']
        
        try:
            files_data = parse_files_xml(files_content)
            print(f"      Found {len(files_data)} files in files.xml")
            
            synthetic_cid = process_file_list(cid, identifier, files_data, all_files)
            if synthetic_cid:
                results.append((identifier, synthetic_cid))
                
        except etree.XMLSyntaxError as e:
            error_msg = f"{cid}\t{identifier}\tDATA_ERROR\tXML parse error: {str(e)}"
            log_errors([error_msg])
        except Exception as e:
            error_msg = f"{cid}\t{identifier}\tDATA_ERROR\t{str(e)}"
            log_errors([error_msg])
    
    print(f"  Completed {cid}")
    return results

def run_files(cids: List[str]):
    print("identifier,synthetic_cid")  # CSV header
    
    any_results = False
    for cid in cids:
        try:
            results = process_cid_files(cid)
            for identifier, synthetic_cid in results:
                print(f"{identifier},{synthetic_cid}")
                any_results = True
        except Exception as e:
            print(f"Error processing {cid}: {e}", file=sys.stderr)
    
    # Clean up temporary blocks after pinning what we want to keep
    if any_results:
        gc_repo()
