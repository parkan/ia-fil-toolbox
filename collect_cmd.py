import sys
from typing import List, Dict
from shared import run_ipfs_cmd, create_directory_via_mfs, generate_shallow_car_file

def collect_cids(cids: List[str], names: Dict[str, str] = None) -> str:
    """
    Collect multiple CIDs into a single parent directory without reading their contents.

    Unlike merge-roots which flattens inputs, this preserves the structure of each
    input CID as a subdirectory entry.

    IMPORTANT: This is a shallow operation. Only the root block of each input CID
    is fetched (to create the directory link). The subgraphs are NOT traversed.
    No ipfs ls or recursive fetches are performed on the input CIDs.

    Args:
        cids: List of CIDs to collect
        names: Optional dict mapping CID -> name for directory entries.
               If not provided, CIDs are used as entry names.

    Returns:
        CID of the new collection directory
    """
    print(f"Collecting {len(cids)} CIDs into directory...", file=sys.stderr)

    # build name -> cid mapping
    if names:
        entries = {names.get(cid, cid): cid for cid in cids}
    else:
        entries = {cid: cid for cid in cids}

    # check for duplicate names
    if len(entries) != len(cids):
        print("  Warning: duplicate names detected, some entries may be overwritten", file=sys.stderr)

    try:
        collection_cid = create_directory_via_mfs(entries, "collect")
        print(f"  Created collection: {collection_cid}", file=sys.stderr)
        return collection_cid
    except Exception as e:
        print(f"  Error creating collection: {e}", file=sys.stderr)
        return None


def run_collect(cids: List[str], names: Dict[str, str] = None):
    """
    Main entry point for collect command.

    Args:
        cids: List of CIDs to collect
        names: Optional dict mapping CID -> name for directory entries
    """
    if not cids:
        print("Error: No CIDs provided", file=sys.stderr)
        return

    print(f"Starting collect for {len(cids)} CIDs", file=sys.stderr)

    collection_cid = collect_cids(cids, names=names)

    if collection_cid:
        print(collection_cid)

        # generate shallow CAR with just the root directory block
        # input CIDs are children but we don't fetch their blocks
        car_filename = f"collect_{collection_cid}.car"
        generate_shallow_car_file(collection_cid, [], car_filename)
    else:
        print("Error: Failed to create collection", file=sys.stderr)
        sys.exit(1)
