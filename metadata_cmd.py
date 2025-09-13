import json
import sqlite3
import concurrent.futures
from typing import Dict, List, Optional, Tuple
from lxml import etree
from shared import (read_cids_from_file, list_files, log_errors, xml_to_dict, MetadataFetcher,
                   fetch_xml_files_parallel, validate_xml_completeness)

class MetadataProcessor(MetadataFetcher):
    def filter_existing_meta_files(self, meta_files: List[str]) -> List[str]:
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


    def process_meta_files_to_db(self, cid: str, meta_file_data: Dict[str, bytes]):
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
                    meta_dict = xml_to_dict(content)
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
                log_errors(errors)

    def process_cid(self, cid: str):
        print(f"\nProcessing CID: {cid}")
        
        print("  Listing files...")
        all_files = list_files(cid)
        if not all_files:
            error_msg = f"{cid}\t*\tIPFS_ERROR\tNo files found or failed to list files"
            log_errors([error_msg])
            return
        
        meta_files = [f for f in all_files if f.endswith('_meta.xml')]
        if not meta_files:
            error_msg = f"{cid}\t*\tIPFS_ERROR\tNo meta files found"
            log_errors([error_msg])
            return
        
        print(f"  Found {len(meta_files)} meta files")
        
        meta_files_to_fetch = self.filter_existing_meta_files(meta_files)
        if not meta_files_to_fetch:
            print("  All meta files already exist, skipping")
            print(f"  Completed {cid}")
            return
        
        print(f"  Need to fetch {len(meta_files_to_fetch)} new meta files")
        
        print("  Fetching meta files...")
        identifiers_to_fetch = [f.replace('_meta.xml', '') for f in meta_files_to_fetch]
        xml_results = fetch_xml_files_parallel(cid, identifiers_to_fetch, {'meta'})
        
        meta_file_data = {}
        for identifier, xml_data in xml_results.items():
            if 'meta' in xml_data:
                meta_file_data[f"{identifier}_meta.xml"] = xml_data['meta']
        
        print("  Processing to database...")
        self.process_meta_files_to_db(cid, meta_file_data)
        
        print(f"  Completed {cid}")

def run_metadata(cids: List[str], db_path: str):
    processor = MetadataProcessor(db_path=db_path)
    for cid in cids:
        try:
            processor.process_cid(cid)
        except Exception as e:
            print(f"Error processing {cid}: {e}", file=sys.stderr)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM metadata')
    count = cursor.fetchone()[0]
    conn.close()
    print(f"\nTotal metadata entries: {count}")
