#!/usr/bin/env python3

import sys
import os
import requests
import subprocess
import tempfile
import sqlite3
import json
import argparse
from typing import Dict, Any, List, Optional
from lxml import etree

class MetadataFetcher:
    def __init__(self, db_path: str = "metadata.db"):
        self.db_path = db_path
        self.ipfs_api_url = "http://127.0.0.1:5002"  # Staging IPFS API
        self.gateways = [
#            "https://trustless-gateway.link",
            "https://ia.dcentnetworks.nl"
        ]
        self.init_db()
    
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
    
    def process_cid(self, cid: str):
        print(f"\nProcessing CID: {cid}")
        print("  Fetching root DAG...")
        with tempfile.NamedTemporaryFile(suffix='.car', delete=False) as tmp:
            dag_car_path = tmp.name
        dag_rel = f"ipfs/{cid}?dag-scope=entity&format=car"
        if not self.fetch_car(dag_rel, dag_car_path):
            print(f"  Failed to fetch root DAG for {cid}", file=sys.stderr)
            return
        print("  Importing DAG...")
        if not self.import_dag(dag_car_path):
            os.unlink(dag_car_path)
            return
        os.unlink(dag_car_path)
        print("  Listing files...")
        meta_files = self.list_files(cid)
        print(f"  Found {len(meta_files)} meta files")
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        for meta_file in meta_files:
            identifier = meta_file.replace('_meta.xml', '')
            print(f"  Processing {identifier}...")
            cursor.execute('SELECT 1 FROM metadata WHERE identifier = ?', (identifier,))
            if cursor.fetchone():
                print(f"    Already exists, skipping")
                continue
            content = self.fetch_file_via_car(cid, meta_file)
            if not content:
                print(f"    ✗ Failed to fetch {meta_file}")
                continue
            try:
                meta_dict = self.xml_to_dict(content)
            except etree.XMLSyntaxError as e:
                print(f"    ⚠ XML parse error for {meta_file}: {e}. Skipping insert.", file=sys.stderr)
                continue
            cursor.execute('''
                INSERT OR REPLACE INTO metadata (identifier, cid, meta)
                VALUES (?, ?, ?)
            ''', (identifier, cid, json.dumps(meta_dict)))
            print(f"    ✓ Inserted {identifier}")
        conn.commit()
        conn.close()
        print(f"  Completed {cid}")

def main():
    parser = argparse.ArgumentParser(description="Fetch and parse IA-style metadata from IPFS into SQLite")
    parser.add_argument('cids', nargs='*', help='CIDs to process')
    parser.add_argument('-f', '--file', help='File containing CIDs (one per line)')
    parser.add_argument('--db', default='metadata.db', help='SQLite database path (default: metadata.db)')
    
    args = parser.parse_args()
    
    cids: List[str] = []
    if args.file:
        with open(args.file, 'r') as f:
            cids = [line.strip() for line in f if line.strip() and not line.startswith('#')]
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

