import os
import sys
import csv
import subprocess
import sqlite3
import datetime
import concurrent.futures
import time
import signal
import atexit
from typing import List, Optional, Dict, Any, Tuple, Set
from lxml import etree

def read_cids_from_file(file_path: str) -> List[str]:
    cids = []
    
    with open(file_path, 'r') as f:
        first_line = f.readline().strip()
        f.seek(0)
        
        if ',' in first_line and ('cid' in first_line.lower() or 'CID' in first_line):
            reader = csv.DictReader(f)
            cid_column = None
            
            for column in reader.fieldnames:
                if column.lower() == 'cid':
                    cid_column = column
                    break
            
            if not cid_column:
                raise ValueError("CSV file must have a column named 'cid' (case-insensitive)")
            
            for row in reader:
                cid = row[cid_column].strip()
                if cid and not cid.startswith('#'):
                    cids.append(cid)
        else:
            f.seek(0)
            for line in f:
                cid = line.strip()
                if cid and not cid.startswith('#'):
                    cids.append(cid)
    
    return cids

def run_ipfs_cmd(cmd_args: List[str], **kwargs) -> subprocess.CompletedProcess:
    # Use --api flag to connect to staging daemon instead of accessing repo directly
    return subprocess.run(['ipfs', '--api', '/ip4/127.0.0.1/tcp/5009'] + cmd_args, **kwargs)

def list_files(cid: str) -> List[str]:
    result = run_ipfs_cmd(
        ['ls', '--resolve-type=false', '--size=false', cid],
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        print(f"  Failed to list {cid}: {result.stderr}", file=sys.stderr)
        return []
    out = []
    for line in result.stdout.strip().split('\n'):
        if not line:
            continue
        parts = line.split(maxsplit=1)
        if len(parts) == 2:
            filename = parts[1]
            out.append(filename)
    return out

def list_files_with_cids(cid: str) -> Dict[str, str]:
    """
    List files in a CID with their individual CIDs
    
    Returns:
        Dict mapping filename -> file_cid
    """
    result = run_ipfs_cmd(
        ['ls', '--resolve-type=false', '--size=false', cid],
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        print(f"  Failed to list {cid}: {result.stderr}", file=sys.stderr)
        return {}
    
    files = {}
    for line in result.stdout.strip().split('\n'):
        if not line:
            continue
        parts = line.split(maxsplit=1)
        if len(parts) == 2:
            file_cid = parts[0]
            filename = parts[1]
            files[filename] = file_cid
    return files

def fetch_file(cid: str, filename: str) -> Optional[bytes]:
    try:
        result = run_ipfs_cmd(
            ['cat', f'/ipfs/{cid}/{filename}'],
            capture_output=True
        )
        if result.returncode != 0:
            return None
        return result.stdout
    except Exception:
        return None

def log_errors(errors: List[str]):
    with open('fil_crawler_errors.log', 'a') as f:
        timestamp = datetime.datetime.now().isoformat()
        for error in errors:
            f.write(f"{timestamp}\t{error}\n")

def xml_to_dict(xml_content: bytes) -> Dict[str, Any]:
    root = etree.fromstring(xml_content)
    def element_to_dict(elem):
        result: Dict[str, Any] = {}
        if elem.attrib:
            result['@attributes'] = dict(elem.attrib)
        text = (elem.text or '').strip()
        if text and len(elem) == 0:
            return text
        if text:
            result['#text'] = text
        for child in elem:
            child_data = element_to_dict(child)
            tag = child.tag
            if tag in result:
                if not isinstance(result[tag], list):
                    result[tag] = [result[tag]]
                result[tag].append(child_data)
            else:
                result[tag] = child_data
        return result if result else None
    return {root.tag: element_to_dict(root)}

class MetadataFetcher:
    def __init__(self, db_path: str = "metadata.db"):
        self.db_path = db_path
        self.init_db()

    def init_db(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS metadata (
                identifier TEXT PRIMARY KEY,
                cid TEXT,
                meta JSON,
                fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()
        conn.close()

def fetch_xml_files_parallel(cid: str, identifiers: List[str], xml_types: Set[str]) -> Dict[str, Dict[str, bytes]]:
    """
    Fetch XML files for given identifiers and types in parallel.
    
    Args:
        cid: Root CID to fetch from
        identifiers: List of identifiers (without _meta.xml or _files.xml suffix)
        xml_types: Set of XML types to fetch ('meta', 'files', or both)
    
    Returns:
        Dict mapping identifier -> xml_type -> content
        Only includes successfully fetched files
    """
    results = {}
    errors = []
    
    # First, get the mapping of filenames to their CIDs
    files_with_cids = list_files_with_cids(cid)
    
    def fetch_single_xml(identifier: str, xml_type: str) -> Tuple[str, str, Optional[bytes]]:
        filename = f"{identifier}_{xml_type}.xml"
        try:
            # Get the specific file CID
            if filename not in files_with_cids:
                error_msg = f"{cid}\t{filename}\tDATA_ERROR\tFile not found in directory"
                errors.append(error_msg)
                return identifier, xml_type, None
            
            file_cid = files_with_cids[filename]
            
            # Fetch directly by file CID via HTTP API
            result = run_ipfs_cmd(['cat', file_cid], capture_output=True)
            if result.returncode != 0:
                error_msg = f"{cid}\t{filename}\tIPFS_ERROR\tFailed to fetch: {result.stderr}"
                errors.append(error_msg)
                return identifier, xml_type, None
            
            return identifier, xml_type, result.stdout
        except Exception as e:
            error_msg = f"{cid}\t{filename}\tIPFS_ERROR\t{str(e)}"
            errors.append(error_msg)
            return identifier, xml_type, None
    
    tasks = []
    for identifier in identifiers:
        for xml_type in xml_types:
            tasks.append((identifier, xml_type))
    
    # Use ThreadPoolExecutor for parallel fetching via HTTP API
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(fetch_single_xml, ident, xml_type): (ident, xml_type) 
                  for ident, xml_type in tasks}
        
        for future in concurrent.futures.as_completed(futures):
            ident, xml_type = futures[future]
            try:
                identifier_result, xml_type_result, content = future.result()
                if content:
                    if identifier_result not in results:
                        results[identifier_result] = {}
                    results[identifier_result][xml_type_result] = content
                    print(f"    ✓ Fetched {identifier_result}_{xml_type_result}.xml", file=sys.stderr)
                else:
                    print(f"    ✗ Failed to fetch {identifier_result}_{xml_type_result}.xml", file=sys.stderr)
            except Exception as e:
                print(f"    ✗ Exception fetching {ident}_{xml_type}.xml: {e}", file=sys.stderr)
                errors.append(f"{cid}\t{ident}_{xml_type}.xml\tTHREAD_ERROR\tException in thread: {str(e)}")
    
    if errors:
        log_errors(errors)
    
    return results

def validate_xml_completeness(cid: str, identifiers: List[str], results: Dict[str, Dict[str, bytes]], 
                            required_types: Set[str]) -> List[str]:
    """
    Validate that all required XML types were fetched for each identifier.
    
    Returns:
        List of identifiers that have all required XML types
    """
    valid_identifiers = []
    errors = []
    
    print(f"    Validating {len(identifiers)} identifiers: {identifiers}", file=sys.stderr)
    print(f"    Required types: {required_types}", file=sys.stderr)
    print(f"    Found results for: {list(results.keys())}", file=sys.stderr)
    
    for identifier in identifiers:
        if identifier not in results:
            print(f"    ✗ {identifier}: No XML files found", file=sys.stderr)
            errors.append(f"{cid}\t{identifier}\tDATA_ERROR\tNo XML files found for identifier")
            continue
        
        available_types = set(results[identifier].keys())
        missing_types = required_types - available_types
        print(f"    {identifier}: has {available_types}, missing {missing_types}", file=sys.stderr)
        
        if missing_types:
            missing_list = ', '.join(f"{identifier}_{t}.xml" for t in missing_types)
            errors.append(f"{cid}\t{identifier}\tDATA_ERROR\tMissing XML files: {missing_list}")
        else:
            valid_identifiers.append(identifier)
            print(f"    ✓ {identifier}: complete", file=sys.stderr)
    
    if errors:
        log_errors(errors)
    
    return valid_identifiers

# Global variable to track daemon process
_daemon_process = None

def start_staging_ipfs():
    """Start the staging IPFS daemon and wait for it to be ready"""
    global _daemon_process
    
    if _daemon_process and _daemon_process.poll() is None:
        # Daemon is already running
        return _daemon_process
    
    print("Starting staging IPFS daemon...", file=sys.stderr)
    
    # Check for existing lock file and warn user
    import os
    lock_file = ".ipfs_staging/repo.lock"
    if os.path.exists(lock_file):
        print(f"Warning: Lock file exists at {lock_file}", file=sys.stderr)
        print("This may indicate another IPFS daemon is running or was not shut down cleanly.", file=sys.stderr)
        print("If you're sure no other daemon is running, you can manually remove the lock file.", file=sys.stderr)
        raise RuntimeError(f"IPFS repository is locked. Lock file: {lock_file}")
    
    # Start the staging IPFS script
    script_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "staging_ipfs.sh")
    
    _daemon_process = subprocess.Popen(
        [script_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        preexec_fn=os.setsid  # Create new process group for clean shutdown
    )

    # Register cleanup function
    atexit.register(stop_staging_ipfs)
    
    # Wait for daemon to be ready by checking API directly (not using our wrapper to avoid recursion)
    for i in range(30):  # Wait up to 30 seconds
        try:
            result = subprocess.run(
                ['ipfs', '--api', '/ip4/127.0.0.1/tcp/5009', 'id'],
                capture_output=True,
                text=True,
                timeout=2
            )
            if result.returncode == 0:
                print("Staging IPFS daemon ready", file=sys.stderr)
                return _daemon_process
        except (subprocess.TimeoutExpired, subprocess.CalledProcessError):
            pass
        
        # Check if process is still alive
        if _daemon_process.poll() is not None:
            break
            
        time.sleep(1)
    
    # If we get here, daemon failed to start
    stop_staging_ipfs()
    raise RuntimeError("Failed to start staging IPFS daemon")

def stop_staging_ipfs():
    """Stop the staging IPFS daemon"""
    global _daemon_process
    
    if _daemon_process and _daemon_process.poll() is None:
        print("Stopping staging IPFS daemon...", file=sys.stderr)
        try:
            # Send SIGTERM to the process group
            os.killpg(os.getpgid(_daemon_process.pid), signal.SIGTERM)
            _daemon_process.wait(timeout=10)
        except (subprocess.TimeoutExpired, ProcessLookupError, OSError):
            # Force kill if graceful shutdown fails
            try:
                os.killpg(os.getpgid(_daemon_process.pid), signal.SIGKILL)
            except (ProcessLookupError, OSError):
                pass
        _daemon_process = None

def ensure_staging_ipfs():
    """Ensure staging IPFS daemon is running, start if needed"""
    import os
    
    try:
        # Test if daemon is already running by using ipfs id --api directly
        result = subprocess.run(
            ['ipfs', '--api', '/ip4/127.0.0.1/tcp/5009', 'id'],
            capture_output=True,
            text=True,
            timeout=2
        )
        if result.returncode == 0:
            return  # Already running
    except (subprocess.TimeoutExpired, subprocess.CalledProcessError):
        pass
    
    # Check if there's a lock file but no responding daemon
    lock_file = ".ipfs_staging/repo.lock"
    if os.path.exists(lock_file):
        print(f"Warning: Cannot connect to IPFS daemon but lock file exists: {lock_file}", file=sys.stderr)
        print("This suggests a daemon may be running but not responding, or was not shut down cleanly.", file=sys.stderr)
    
    # Start the daemon
    start_staging_ipfs()

def pin_cid(cid: str) -> bool:
    """Pin a CID in the staging IPFS node"""
    try:
        result = run_ipfs_cmd(['pin', 'add', cid], capture_output=True, text=True)
        if result.returncode == 0:
            print(f"  ✓ Pinned {cid}", file=sys.stderr)
            return True
        else:
            print(f"  ⚠️ Failed to pin {cid}: {result.stderr}", file=sys.stderr)
            return False
    except Exception as e:
        print(f"  ⚠️ Error pinning {cid}: {e}", file=sys.stderr)
        return False

def gc_repo():
    """Run garbage collection on the staging IPFS repo"""
    try:
        result = run_ipfs_cmd(['repo', 'gc', '--quiet'], capture_output=True, text=True)
        if result.returncode == 0:
            print("  ✓ Cleaned up temporary blocks", file=sys.stderr)
        else:
            print(f"  ⚠️ GC warning: {result.stderr}", file=sys.stderr)
    except Exception as e:
        print(f"  ⚠️ GC error: {e}", file=sys.stderr)

def create_directory_via_mfs(files_dict: Dict[str, str], name_prefix: str = "dir") -> str:
    """
    Create a directory using MFS, which automatically handles HAMT sharding for large directories.
    MFS preserves the important dag-pb codec properties while handling optimization automatically.
    
    Args:
        files_dict: Dict mapping filename -> file_cid
        name_prefix: Prefix for the temporary MFS directory name
        
    Returns:
        CID of the created directory (with HAMT sharding if needed)
    """
    import uuid
    
    # Use unique MFS path to avoid conflicts
    mfs_path = f'/tmp/{name_prefix}_{uuid.uuid4().hex[:8]}'
    
    try:
        # Create MFS directory
        result = run_ipfs_cmd([
            'files', 'mkdir', '-p', mfs_path
        ], capture_output=True, text=True)
        
        if result.returncode != 0:
            raise RuntimeError(f"Failed to create MFS directory: {result.stderr}")
        
        # Copy each file to MFS - MFS automatically handles HAMT sharding and uses dag-pb
        for filename, file_cid in files_dict.items():
            result = run_ipfs_cmd([
                'files', 'cp', f'/ipfs/{file_cid}', f'{mfs_path}/{filename}'
            ], capture_output=True, text=True)
            
            if result.returncode != 0:
                print(f"    ⚠️ Warning: Failed to add {filename}: {result.stderr}", file=sys.stderr)
        
        # Get the final directory CID - MFS will have automatically used dag-pb and sharded if needed
        result = run_ipfs_cmd([
            'files', 'stat', '--hash', mfs_path
        ], capture_output=True, text=True)
        
        if result.returncode != 0:
            raise RuntimeError(f"Failed to get directory CID: {result.stderr}")
        
        dir_cid = result.stdout.strip()
        
        return dir_cid
        
    finally:
        # Clean up MFS (ignore errors since it might not exist)
        run_ipfs_cmd(['files', 'rm', '-r', mfs_path], capture_output=True)
