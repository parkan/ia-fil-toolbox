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

# MFS configuration constants
MFS_FLUSH_LIMIT = 1024

# Debug configuration
DEBUG = os.environ.get('DEBUG', '').lower() in ('1', 'true', 'yes', 'on')


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

def list_files_with_cids(cid: str, known_files: Optional[Set[str]] = None) -> Dict[str, str]:
    """
    List files in a CID with their individual CIDs, recursively walking subdirectories
    
    Args:
        cid: Root CID to list
        known_files: Optional set of known filenames (from files.xml) to avoid probing
    
    Returns:
        Dict mapping full_path -> file_cid (e.g., "subdir/file.txt" -> "bafk...")
    """
    def walk_directory(dir_cid: str, path_prefix: str = "", known_files: Optional[Set[str]] = None) -> Dict[str, str]:
        """Recursively walk an IPFS directory"""
        if DEBUG:
            print(f"  DEBUG: Listing directory {dir_cid} (prefix: {path_prefix})", file=sys.stderr)
        try:
            result = run_ipfs_cmd(
                ['ls', '--resolve-type=false', '--size=false', dir_cid],
                capture_output=True,
                text=True,
                timeout=30  # Add timeout to prevent hanging
            )
            if result.returncode != 0:
                print(f"  Failed to list {dir_cid}: {result.stderr}", file=sys.stderr)
                return {}
        except subprocess.TimeoutExpired:
            print(f"  Timeout listing {dir_cid} after 30 seconds", file=sys.stderr)
            return {}
        
        files = {}
        for line in result.stdout.strip().split('\n'):
            if not line:
                continue
            parts = line.split(maxsplit=1)
            if len(parts) == 2:
                item_cid = parts[0]
                item_name = parts[1]
                full_path = f"{path_prefix}{item_name}" if path_prefix else item_name
                
                # Check if this is a known file from files.xml
                if known_files and full_path in known_files:
                    # Skip directory check for files listed in files.xml
                    files[full_path] = item_cid
                    continue
                
                # Check if this is a directory by trying to list it
                # If it fails, it's a file
                if DEBUG:
                    print(f"  DEBUG: Checking if {item_name} ({item_cid}) is directory", file=sys.stderr)
                try:
                    subdir_result = run_ipfs_cmd(
                        ['ls', '--resolve-type=false', '--size=false', item_cid],
                        capture_output=True,
                        text=True,
                        timeout=10  # Shorter timeout for type detection
                    )
                except subprocess.TimeoutExpired:
                    if DEBUG:
                        print(f"  DEBUG: Timeout checking {item_name}, assuming it's a file", file=sys.stderr)
                    # Assume it's a file if we can't determine the type
                    files[full_path] = item_cid
                    continue
                
                if subdir_result.returncode == 0 and subdir_result.stdout.strip():
                    # It's a directory, recurse into it
                    subdir_files = walk_directory(item_cid, f"{full_path}/", known_files=known_files)
                    files.update(subdir_files)
                else:
                    # It's a file
                    files[full_path] = item_cid
        
        return files
    
    return walk_directory(cid, known_files=known_files)

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

def start_staging_ipfs(someguy=False):
    """Start the staging IPFS daemon (and optionally someguy) and wait for it to be ready"""
    global _daemon_process
    
    if _daemon_process and _daemon_process.poll() is None:
        # Daemon is already running
        if someguy:
            from daemon_cmd import ensure_someguy_running
            ensure_someguy_running()
        return _daemon_process
    
    print("Starting staging IPFS daemon...", end="", file=sys.stderr)
    
    # Use the Python-based daemon startup
    from daemon_cmd import initialize_repo, configure_ipfs, start_daemon
    
    # Initialize repo if needed
    if not initialize_repo():
        raise RuntimeError("Failed to initialize IPFS repository")
    
    # Configure IPFS
    configure_ipfs()
    
    # Start daemon and get PID
    daemon_pid = start_daemon()
    if not daemon_pid:
        raise RuntimeError("Failed to start staging IPFS daemon")
    
    # Wait for daemon to be fully ready before starting someguy
    import time
    for i in range(10):  # Wait up to 5 seconds
        try:
            result = subprocess.run(
                ['ipfs', '--api', '/ip4/127.0.0.1/tcp/5009', 'id'],
                capture_output=True,
                text=True,
                timeout=1
            )
            if result.returncode == 0:
                break
        except (subprocess.TimeoutExpired, subprocess.CalledProcessError):
            pass
        time.sleep(0.5)
    else:
        raise RuntimeError("IPFS daemon failed to become ready")
    
    print(" ready", file=sys.stderr)
    
    # Start someguy if requested
    if someguy:
        from daemon_cmd import start_someguy
        someguy_result = start_someguy()
        if someguy_result is None:
            # someguy failed to start
            print("Error: Failed to start someguy daemon", file=sys.stderr)
            print("Use --no-someguy to disable if someguy is not needed.", file=sys.stderr)
            # Stop IPFS before exiting
            stop_staging_ipfs()
            sys.exit(1)
    
    # Create a dummy process object to maintain compatibility with existing code
    class DaemonProcess:
        def __init__(self, pid):
            self.pid = pid
        def poll(self):
            try:
                os.kill(self.pid, 0)
                return None  # Process is running
            except OSError:
                return 1  # Process is dead
    
    _daemon_process = DaemonProcess(daemon_pid)
    
    # Register cleanup function
    atexit.register(stop_staging_ipfs)
    
    return _daemon_process

def stop_staging_ipfs():
    """Stop the staging IPFS daemon and someguy"""
    global _daemon_process
    
    # Stop someguy first if it's running
    try:
        from daemon_cmd import stop_someguy
        stop_someguy()
    except ImportError:
        pass  # daemon_cmd not available
    
    if _daemon_process and _daemon_process.poll() is None:
        print("Stopping staging IPFS daemon...", file=sys.stderr)
        try:
            # Use ipfs shutdown command for graceful shutdown
            result = subprocess.run(
                ['ipfs', '--api', '/ip4/127.0.0.1/tcp/5009', 'shutdown'],
                capture_output=True,
                text=True,
                timeout=10
            )
            # Wait for the daemon process to actually exit
            # Use polling since DaemonProcess doesn't have wait()
            import time
            for _ in range(10):  # 5 seconds total
                if _daemon_process.poll() is not None:
                    break
                time.sleep(0.5)
            else:
                # Process still running after 5 seconds, continue to force kill
                pass
        except (subprocess.TimeoutExpired, subprocess.CalledProcessError, ProcessLookupError, OSError):
            # Fall back to signal-based shutdown if ipfs shutdown fails
            try:
                os.killpg(os.getpgid(_daemon_process.pid), signal.SIGTERM)
                # Wait for the daemon process to actually exit
                for _ in range(10):  # 5 seconds total
                    if _daemon_process.poll() is not None:
                        break
                    time.sleep(0.5)
                else:
                    # Process still running after 5 seconds, continue to force kill
                    pass
            except (subprocess.TimeoutExpired, ProcessLookupError, OSError):
                # Force kill if graceful shutdown fails
                try:
                    os.killpg(os.getpgid(_daemon_process.pid), signal.SIGKILL)
                except (ProcessLookupError, OSError):
                    pass
        
        _daemon_process = None

def ensure_staging_ipfs(someguy=False):
    """Ensure staging IPFS daemon (and optionally someguy) is running, start if needed"""
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
            # IPFS is running, check if we need to start someguy
            if someguy:
                from daemon_cmd import ensure_someguy_running
                if not ensure_someguy_running():
                    print("Error: Failed to start someguy daemon", file=sys.stderr)
                    print("Use --no-someguy to disable if someguy is not needed.", file=sys.stderr)
                    sys.exit(1)
            return  # Already running
    except (subprocess.TimeoutExpired, subprocess.CalledProcessError):
        pass
    
    # Check if there's a lock file but no responding daemon
    lock_file = ".ipfs_staging/repo.lock"
    if os.path.exists(lock_file):
        print(f"Warning: Cannot connect to IPFS daemon but lock file exists: {lock_file}", file=sys.stderr)
        print("This suggests a daemon may be running but not responding, or was not shut down cleanly.", file=sys.stderr)
    
    # Start the daemon(s)
    start_staging_ipfs(someguy=someguy)

def pin_cid(cid: str) -> bool:
    """Pin a CID in the staging IPFS node"""
    try:
        if DEBUG:
            print(f"  DEBUG: Pinning {cid}...", file=sys.stderr)
        
        result = run_ipfs_cmd(
            ['pin', 'add', '--progress=false', cid], 
            capture_output=True, 
            text=True,
            timeout=300  # 5 minute timeout
        )
        if result.returncode == 0:
            if DEBUG:
                print(f"  ✓ Pinned {cid}", file=sys.stderr)
            return True
        else:
            print(f"  ⚠️ Failed to pin {cid}: {result.stderr}", file=sys.stderr)
            return False
    except subprocess.TimeoutExpired:
        print(f"  ⚠️ Timeout pinning {cid} after 5 minutes", file=sys.stderr)
        return False
    except Exception as e:
        print(f"  ⚠️ Error pinning {cid}: {e}", file=sys.stderr)
        return False

def generate_shallow_car_file(root_cid: str, child_cids: List[str], output_path: str) -> bool:
    """
    Generate a shallow CAR file containing only the root block and immediate child blocks.
    Fetches only the directory blocks, not the files they reference.
    Uses go-car put-block command to create proper CAR files.
    
    Args:
        root_cid: The root CID (container directory)
        child_cids: List of child CIDs (item directories) to include
        output_path: Path where to save the CAR file
        
    Returns:
        True if successful, False otherwise
    """
    try:
        print(f"  Generating shallow CAR file: {output_path}", file=sys.stderr)
        print(f"  Root CID: {root_cid}", file=sys.stderr)
        print(f"  Including {len(child_cids)} child directory blocks", file=sys.stderr)
        
        # 1. Create CAR file with root block
        root_block_result = run_ipfs_cmd(['block', 'get', root_cid], capture_output=True)
        if root_block_result.returncode != 0:
            print(f"  ⚠️ Failed to get root block: {root_block_result.stderr}", file=sys.stderr)
            return False
        
        # Use dag-pb codec for UnixFS directories, CARv2 for append support
        car_cmd = [
            'car', 'put-block',
            '--codec=dag-pb',
            '--set-root',
            '--version=2',
            output_path
        ]
        
        car_result = subprocess.run(
            car_cmd,
            input=root_block_result.stdout,
            capture_output=True
        )
        
        if car_result.returncode != 0:
            stderr_text = car_result.stderr.decode('utf-8', errors='replace')
            print(f"  ⚠️ Failed to create CAR with root block: {stderr_text}", file=sys.stderr)
            return False
        
        # Verify the CID matches (car put-block outputs the CID)
        output_cid = car_result.stdout.decode('utf-8').strip()
        if output_cid != root_cid:
            print(f"  ⚠️ CID mismatch! Expected {root_cid}, got {output_cid}", file=sys.stderr)
            return False
        
        if DEBUG:
            print(f"  DEBUG: Root block added, CID verified: {output_cid}", file=sys.stderr)
        
        # 2. Append child blocks
        for i, child_cid in enumerate(child_cids, 1):
            if DEBUG:
                print(f"  DEBUG: Adding child block {i}/{len(child_cids)}: {child_cid}", file=sys.stderr)
            
            child_block_result = run_ipfs_cmd(['block', 'get', child_cid], capture_output=True)
            if child_block_result.returncode != 0:
                print(f"  ⚠️ Failed to get block {child_cid}: {child_block_result.stderr}", file=sys.stderr)
                continue
            
            # Append to existing CAR file (no --set-root)
            car_cmd = [
                'car', 'put-block',
                '--codec=dag-pb',
                '--version=2',
                output_path
            ]
            
            car_result = subprocess.run(
                car_cmd,
                input=child_block_result.stdout,
                capture_output=True
            )
            
            if car_result.returncode != 0:
                stderr_text = car_result.stderr.decode('utf-8', errors='replace')
                print(f"  ⚠️ Failed to add block {child_cid}: {stderr_text}", file=sys.stderr)
                continue
            
            # Verify CID
            output_cid = car_result.stdout.decode('utf-8').strip()
            if output_cid != child_cid:
                print(f"  ⚠️ CID mismatch for child! Expected {child_cid}, got {output_cid}", file=sys.stderr)
                continue
        
        # Get file size for user feedback
        file_size = os.path.getsize(output_path)
        size_kb = file_size / 1024
        print(f"  ✓ Shallow CAR file created: {output_path} ({size_kb:.1f} KB, {len(child_cids) + 1} blocks)", file=sys.stderr)
        
        return True
        
    except Exception as e:
        print(f"  ⚠️ Error generating shallow CAR file: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return False

def generate_car_file(root_cid: str, output_path: str) -> bool:
    """
    Generate a CAR file for the given root CID.
    
    Args:
        root_cid: The root CID to export
        output_path: Path where to save the CAR file
        
    Returns:
        True if successful, False otherwise
        
    Note: This exports the complete DAG. IPFS dag export doesn't support
    depth limiting, so the entire DAG structure will be included.
    For large HAMT-sharded directories, this may result in large CAR files.
    """
    try:
        print(f"  Generating CAR file: {output_path}", file=sys.stderr)
        print(f"  Root CID: {root_cid} (full DAG)", file=sys.stderr)
        
        # Use ipfs dag export to create CAR file
        result = run_ipfs_cmd([
            'dag', 'export', 
            '--progress=false',  # Disable progress to keep output clean
            root_cid
        ], capture_output=True)
        
        if result.returncode != 0:
            print(f"  ⚠️ Failed to export CAR: {result.stderr}", file=sys.stderr)
            return False
        
        # Write the CAR data to file
        with open(output_path, 'wb') as f:
            f.write(result.stdout)
        
        # Get file size for user feedback
        import os
        file_size = os.path.getsize(output_path)
        size_mb = file_size / (1024 * 1024)
        print(f"  ✓ CAR file created: {output_path} ({size_mb:.1f} MB)", file=sys.stderr)
        
        return True
        
    except Exception as e:
        print(f"  ⚠️ Error generating CAR file: {e}", file=sys.stderr)
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
    
    Uses --flush=false for performance during bulk operations, then flushes manually at the end.
    
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
        # Create MFS directory (no need for --flush=false on mkdir)
        result = run_ipfs_cmd([
            'files', 'mkdir', '-p', '--cid-version=1', mfs_path
        ], capture_output=True, text=True)
        
        if result.returncode != 0:
            raise RuntimeError(f"Failed to create MFS directory: {result.stderr}")
        
        # Copy each file to MFS with --flush=false for performance
        # MFS automatically handles HAMT sharding and uses dag-pb
        # Use --parents to automatically create intermediate directories
        # Flush periodically to avoid hitting the unflushed operations limit
        operation_count = 0
        total_files = len(files_dict)
        for i, (filename, file_cid) in enumerate(files_dict.items(), 1):
            if DEBUG:
                print(f"    DEBUG: Adding file {i}/{total_files}: {filename} ({file_cid})", file=sys.stderr)
            
            result = run_ipfs_cmd([
                'files', 'cp', '--flush=false', '--parents', f'/ipfs/{file_cid}', f'{mfs_path}/{filename}'
            ], capture_output=True, text=True)
            
            if result.returncode != 0:
                print(f"    ⚠️ Warning: Failed to add {filename}: {result.stderr}", file=sys.stderr)
            else:
                operation_count += 1
                
                # Flush every (MFS_FLUSH_LIMIT - 1) operations to stay under the limit
                if operation_count >= (MFS_FLUSH_LIMIT - 1):
                    print(f"    Flushing after {operation_count} operations...", file=sys.stderr)
                    flush_result = run_ipfs_cmd([
                        'files', 'flush', mfs_path
                    ], capture_output=True, text=True)
                    
                    if flush_result.returncode != 0:
                        print(f"    ⚠️ Warning: Failed to flush MFS directory: {flush_result.stderr}", file=sys.stderr)
                    
                    operation_count = 0
        
        # Manually flush the directory to ensure consistency and get final CID
        result = run_ipfs_cmd([
            'files', 'flush', mfs_path
        ], capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"    ⚠️ Warning: Failed to flush MFS directory: {result.stderr}", file=sys.stderr)
        
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
