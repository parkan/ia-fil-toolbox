import os
import sys
import csv
import subprocess
import sqlite3
import datetime
import concurrent.futures
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
    env = os.environ.copy()
    env['IPFS_API'] = "http://127.0.0.1:5009"
    env['IPFS_PATH'] = ".ipfs_staging"
    return subprocess.run(['ipfs'] + cmd_args, env=env, **kwargs)

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
    
    def fetch_single_xml(identifier: str, xml_type: str) -> Tuple[str, str, Optional[bytes]]:
        filename = f"{identifier}_{xml_type}.xml"
        try:
            content = fetch_file(cid, filename)
            if content:
                return identifier, xml_type, content
            else:
                errors.append(f"{cid}\t{filename}\tIPFS_ERROR\tFailed to fetch XML file")
                return identifier, xml_type, None
        except Exception as e:
            errors.append(f"{cid}\t{filename}\tIPFS_ERROR\t{str(e)}")
            return identifier, xml_type, None
    
    tasks = []
    for identifier in identifiers:
        for xml_type in xml_types:
            tasks.append((identifier, xml_type))
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(fetch_single_xml, ident, xml_type): (ident, xml_type) 
                  for ident, xml_type in tasks}
        
        for future in concurrent.futures.as_completed(futures):
            identifier, xml_type, content = future.result()
            if content:
                if identifier not in results:
                    results[identifier] = {}
                results[identifier][xml_type] = content
                print(f"    ✓ Fetched {identifier}_{xml_type}.xml")
            else:
                print(f"    ✗ Failed to fetch {identifier}_{xml_type}.xml", file=sys.stderr)
    
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
