#!/usr/bin/env python3

import sys
import os
import requests
import subprocess
import tempfile
import sqlite3
import json
import argparse
import csv
import datetime
import concurrent.futures
from typing import Dict, Any, List, Optional, Tuple
from lxml import etree

class MetadataFetcher:
    def __init__(self, db_path: str = "metadata.db"):
        self.db_path = db_path
        self.ipfs_api_url = "http://127.0.0.1:5009"  # Staging IPFS API
        self.gateways = [
#            "https://trustless-gateway.link",
            "https://ia.dcentnetworks.nl"
        ]
        self.init_db()
    
    def _log_errors(self, errors: List[str]):
        """Log CID-scoped errors to file in tab-separated format"""
        with open('ia_fetch_errors.log', 'a') as f:
            timestamp = datetime.datetime.now().isoformat()
            for error in errors:
                f.write(f"{timestamp}\t{error}\n")
    
    def _run_ipfs_cmd(self, cmd_args: List[str], **kwargs) -> subprocess.CompletedProcess:
        """Run an IPFS command using the staging API endpoint"""
        env = os.environ.copy()
        env['IPFS_API'] = self.ipfs_api_url
        return subprocess.run(['ipfs'] + cmd_args, env=env, **kwargs)
    
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
    
    def fetch_car(self, relative_path: str, output_path: str) -> bool:
        headers = {'Accept': 'application/vnd.ipld.car'}
        for gateway in self.gateways:
            full_url = f"{gateway.rstrip('/')}/{relative_path.lstrip('/')}"
            try:
                print(f"  Fetching from {full_url}...", file=sys.stderr)
                r = requests.get(full_url, headers=headers, stream=True, timeout=30)
                if r.status_code == 200:
                    with open(output_path, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            f.write(chunk)
                    return True
                else:
                    print(f"  Gateway {gateway} returned {r.status_code}", file=sys.stderr)
            except Exception as e:
                print(f"  Gateway {gateway} failed: {e}", file=sys.stderr)
        return False
    
    def import_dag(self, car_path: str) -> bool:
        result = self._run_ipfs_cmd(
            ['dag', 'import', '--stats=true', '--pin-roots=false', car_path],
            capture_output=True,
            text=True
        )
        if result.returncode != 0:
            print(f"  Import failed: {result.stderr}", file=sys.stderr)
            return False
        return True
    
    def list_files(self, cid: str) -> List[str]:
        result = self._run_ipfs_cmd(
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
                if filename.endswith('_meta.xml'):
                    out.append(filename)
        return out
    
    def fetch_file_via_car(self, cid: str, filepath: str) -> Optional[bytes]:
        with tempfile.NamedTemporaryFile(suffix='.car', delete=False) as tmp_car:
            car_path = tmp_car.name
        try:
            rel = f"ipfs/{cid}/{filepath}?format=car&dag-scope=entity"
            if not self.fetch_car(rel, car_path):
                print(f"  Failed to fetch {filepath}", file=sys.stderr)
                return None
            if not self.import_dag(car_path):
                return None
            cat = self._run_ipfs_cmd(
                ['cat', f'/ipfs/{cid}/{filepath}'],
                capture_output=True
            )
            if cat.returncode != 0:
                print(f"  ipfs cat failed for {cid}/{filepath}: {cat.stderr.decode('utf-8', errors='ignore')}", file=sys.stderr)
                return None
            return cat.stdout
        finally:
            os.unlink(car_path)
    
    def xml_to_dict(self, xml_content: bytes) -> Dict[str, Any]:
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
    
    def _fetch_and_import_root_dag(self, cid: str) -> bool:
        """Fetch and import the root DAG for a CID"""
        print("  Fetching root DAG...")
        with tempfile.NamedTemporaryFile(suffix='.car', delete=False) as tmp:
            dag_car_path = tmp.name
        
        try:
            dag_rel = f"ipfs/{cid}?dag-scope=entity&format=car"
            if not self.fetch_car(dag_rel, dag_car_path):
                error_msg = f"{cid}\t*\tGATEWAY_ERROR\tFailed to fetch root DAG"
                self._log_errors([error_msg])
                return False
            
            print("  Importing DAG...")
            if not self.import_dag(dag_car_path):
                error_msg = f"{cid}\t*\tIPFS_ERROR\tFailed to import root DAG"
                self._log_errors([error_msg])
                return False
            
            return True
        finally:
            if os.path.exists(dag_car_path):
                os.unlink(dag_car_path)
    
    def _filter_existing_meta_files(self, meta_files: List[str]) -> List[str]:
        """Filter out meta files that already exist in the database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            meta_files_to_fetch = []
            for meta_file in meta_files:
                identifier = meta_file.replace('_meta.xml', '')
                cursor.execute('SELECT 1 FROM metadata WHERE identifier = ?', (identifier,))
                if not cursor.fetchone():
                    meta_files_to_fetch.append(meta_file)
                else:
                    print(f"    Already exists, skipping {identifier}")
            return meta_files_to_fetch
        finally:
            conn.close()
    
    def _fetch_meta_files_parallel(self, cid: str, meta_files: List[str]) -> Dict[str, bytes]:
        """Fetch multiple meta files in parallel, return successful ones"""
        results = {}
        errors = []
        
        def fetch_single_meta(meta_file: str) -> Tuple[str, Optional[bytes]]:
            try:
                content = self.fetch_file_via_car(cid, meta_file)
                if content:
                    print(f"    ✓ Downloaded {meta_file}")
                    return meta_file, content
                else:
                    errors.append(f"{cid}\t{meta_file}\tGATEWAY_ERROR\tFailed to fetch meta file")
                    return meta_file, None
            except Exception as e:
                errors.append(f"{cid}\t{meta_file}\tGATEWAY_ERROR\t{str(e)}")
                return meta_file, None
        
        # Limit concurrency to avoid overwhelming gateways
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            futures = {executor.submit(fetch_single_meta, mf): mf for mf in meta_files}
            
            for future in concurrent.futures.as_completed(futures):
                meta_file, content = future.result()
                if content:
                    results[meta_file] = content
        
        # Log any errors that occurred
        if errors:
            self._log_errors(errors)
        
        return results
    
    def _process_meta_files_to_db(self, cid: str, meta_file_data: Dict[str, bytes]):
        """Process downloaded meta files and write to DB in single transaction"""
        if not meta_file_data:
            print(f"  No meta files successfully downloaded for {cid}")
            return
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        errors = []
        
        try:
            for meta_file, content in meta_file_data.items():
                identifier = meta_file.replace('_meta.xml', '')
                
                try:
                    meta_dict = self.xml_to_dict(content)
                    cursor.execute('''
                        INSERT INTO metadata (identifier, cid, meta)
                        VALUES (?, ?, ?)
                    ''', (identifier, cid, json.dumps(meta_dict)))
                    print(f"    ✓ Inserted {identifier}")
                    
                except etree.XMLSyntaxError as e:
                    errors.append(f"{cid}\t{identifier}\tDATA_ERROR\tXML syntax error: {str(e)}")
                except Exception as e:
                    errors.append(f"{cid}\t{identifier}\tDATA_ERROR\t{str(e)}")
            
            conn.commit()
            
        except Exception as e:
            conn.rollback()
            errors.append(f"{cid}\t*\tDB_ERROR\t{str(e)}")
            raise
        finally:
            conn.close()
            if errors:
                self._log_errors(errors)
    
    def process_cid(self, cid: str):
        print(f"\nProcessing CID: {cid}")
        
        # Sequential: fetch and import root DAG
        if not self._fetch_and_import_root_dag(cid):
            return
        
        # Sequential: list meta files
        print("  Listing files...")
        meta_files = self.list_files(cid)
        if not meta_files:
            error_msg = f"{cid}\t*\tIPFS_ERROR\tNo meta files found or failed to list files"
            self._log_errors([error_msg])
            return
        
        print(f"  Found {len(meta_files)} meta files")
        
        # Pre-filter: check which meta files we don't already have
        meta_files_to_fetch = self._filter_existing_meta_files(meta_files)
        if not meta_files_to_fetch:
            print("  All meta files already exist, skipping")
            print(f"  Completed {cid}")
            return
        
        print(f"  Need to fetch {len(meta_files_to_fetch)} new meta files")
        
        # PARALLEL: fetch all meta file CARs concurrently
        print("  Downloading meta files...")
        meta_file_data = self._fetch_meta_files_parallel(cid, meta_files_to_fetch)
        
        # Sequential: parse and write to DB in single transaction
        print("  Processing to database...")
        self._process_meta_files_to_db(cid, meta_file_data)
        
        print(f"  Completed {cid}")

def read_cids_from_file(file_path: str) -> List[str]:
    """Read CIDs from a file, supporting both plain text and CSV formats"""
    cids = []
    
    with open(file_path, 'r') as f:
        # Try to detect if it's a CSV file by reading the first line
        first_line = f.readline().strip()
        f.seek(0)  # Reset file pointer
        
        # Check if the first line looks like a CSV header
        if ',' in first_line and ('cid' in first_line.lower() or 'CID' in first_line):
            # Handle as CSV
            reader = csv.DictReader(f)
            cid_column = None
            
            # Find the CID column (case-insensitive)
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
            # Handle as plain text (one CID per line)
            f.seek(0)  # Reset file pointer
            for line in f:
                cid = line.strip()
                if cid and not cid.startswith('#'):
                    cids.append(cid)
    
    return cids

def main():
    parser = argparse.ArgumentParser(description="Fetch and parse IA-style metadata from IPFS into SQLite")
    parser.add_argument('cids', nargs='*', help='CIDs to process')
    parser.add_argument('-f', '--file', help='File containing CIDs (plain text: one per line, or CSV with "cid" column)')
    parser.add_argument('--db', default='metadata.db', help='SQLite database path (default: metadata.db)')
    
    args = parser.parse_args()
    
    cids: List[str] = []
    if args.file:
        cids = read_cids_from_file(args.file)
    elif args.cids:
        cids = args.cids
    else:
        parser.error("Must provide either CIDs as arguments or use --file option")
    for cmd in ['ipfs', 'car']:
        if subprocess.run(['which', cmd], capture_output=True).returncode != 0:
            print(f"Error: {cmd} command not found in PATH")
            sys.exit(1)
    fetcher = MetadataFetcher(db_path=args.db)
    for cid in cids:
        try:
            fetcher.process_cid(cid)
        except Exception as e:
            print(f"Error processing {cid}: {e}", file=sys.stderr)
        finally:
            # Clean up IPFS repo after processing each CID
            print(f"  Running garbage collection for {cid}...")
            try:
                env = os.environ.copy()
                env['IPFS_API'] = fetcher.ipfs_api_url
                subprocess.run(['ipfs', 'repo', 'gc', '--quiet'], 
                             env=env, capture_output=True, check=True)
            except subprocess.CalledProcessError as gc_error:
                print(f"  Warning: IPFS GC failed: {gc_error}", file=sys.stderr)
    print(f"\nDatabase saved to: {fetcher.db_path}")
    conn = sqlite3.connect(fetcher.db_path)
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM metadata')
    count = cursor.fetchone()[0]
    conn.close()
    print(f"Total metadata entries: {count}")

if __name__ == "__main__":
    main()

