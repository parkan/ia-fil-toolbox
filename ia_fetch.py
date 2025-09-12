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
        self.ipfs_api_url = "http://127.0.0.1:5009"
        self.init_db()
    
    def _log_errors(self, errors: List[str]):
        with open('ia_fetch_errors.log', 'a') as f:
            timestamp = datetime.datetime.now().isoformat()
            for error in errors:
                f.write(f"{timestamp}\t{error}\n")
    
    def _run_ipfs_cmd(self, cmd_args: List[str], **kwargs) -> subprocess.CompletedProcess:
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
    
    def fetch_meta_file(self, cid: str, meta_file: str) -> Optional[bytes]:
        try:
            result = self._run_ipfs_cmd(
                ['cat', f'/ipfs/{cid}/{meta_file}'],
                capture_output=True
            )
            if result.returncode != 0:
                return None
            return result.stdout
        except Exception:
            return None
    
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
    
    def _filter_existing_meta_files(self, meta_files: List[str]) -> List[str]:
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
        results = {}
        errors = []
        
        def fetch_single_meta(meta_file: str) -> Tuple[str, Optional[bytes]]:
            try:
                content = self.fetch_meta_file(cid, meta_file)
                if content:
                    print(f"    ✓ Fetched {meta_file}")
                    return meta_file, content
                else:
                    errors.append(f"{cid}\t{meta_file}\tIPFS_ERROR\tFailed to fetch meta file")
                    return meta_file, None
            except Exception as e:
                errors.append(f"{cid}\t{meta_file}\tIPFS_ERROR\t{str(e)}")
                return meta_file, None
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            futures = {executor.submit(fetch_single_meta, mf): mf for mf in meta_files}
            
            for future in concurrent.futures.as_completed(futures):
                meta_file, content = future.result()
                if content:
                    results[meta_file] = content
        
        if errors:
            self._log_errors(errors)
        
        return results
    
    def _process_meta_files_to_db(self, cid: str, meta_file_data: Dict[str, bytes]):
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
        
        print("  Listing files...")
        meta_files = self.list_files(cid)
        if not meta_files:
            error_msg = f"{cid}\t*\tIPFS_ERROR\tNo meta files found or failed to list files"
            self._log_errors([error_msg])
            return
        
        print(f"  Found {len(meta_files)} meta files")
        
        meta_files_to_fetch = self._filter_existing_meta_files(meta_files)
        if not meta_files_to_fetch:
            print("  All meta files already exist, skipping")
            print(f"  Completed {cid}")
            return
        
        print(f"  Need to fetch {len(meta_files_to_fetch)} new meta files")
        
        print("  Fetching meta files...")
        meta_file_data = self._fetch_meta_files_parallel(cid, meta_files_to_fetch)
        
        print("  Processing to database...")
        self._process_meta_files_to_db(cid, meta_file_data)
        
        print(f"  Completed {cid}")

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

