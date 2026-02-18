# ia-fil-toolbox

Internet Archive & Filecoin toolbox for metadata crawling, item extraction, and root merging on IPFS DAGs.

## Container (Podman)

The included Containerfile bundles the full stack: IPFS (Kubo), go-car, someguy, storacha CLI, and the Python toolbox.

### Build

```bash
podman build -t ia-fil-toolbox .
```

### Interactive shell

Starts IPFS and someguy daemons, then drops into a tab-completing `ia>` prompt:

```bash
podman run -it \
  -v ./work:/work:Z \
  -v ./config:/config:Z \
  ia-fil-toolbox
```

- `/work` — working directory for CID lists, CAR files, metadata DBs, error logs
- `/config` — persistent IPFS repo and storacha secrets (survives container restarts)

### One-shot commands

Pass arguments directly to skip the shell:

```bash
podman run --rm \
  -v ./work:/work:Z \
  -v ./config:/config:Z \
  ia-fil-toolbox extract-items -f /work/cids.txt
```

## Commands

```bash
# Extract items from _files.xml into synthetic directories
ia-fil extract-items <cid> [<cid> ...]

# Fetch and parse metadata into SQLite
ia-fil metadata <cid> [<cid> ...] [--db metadata.db]

# Merge multiple root CIDs into a single directory (flattens contents)
ia-fil merge-roots <cid1> <cid2> [--force-check-directories]

# Collect CIDs into parent directory (shallow, preserves structure)
ia-fil collect <cid1> <cid2> [<cid3> ...]

# Read CIDs from file (plain text or CSV with "cid" column)
ia-fil extract-items -f cids.txt
ia-fil metadata -f cids.csv

# Pin output CAR to storacha after generation
ia-fil --pin merge-roots <cid1> <cid2>

# Check daemon status
ia-fil daemon-status
```

### Output

- **extract-items**: Shallow CAR with synthetic directory blocks → `extract_items_<cid>.car`
- **merge-roots**: Shallow CAR with merged directory → `merge_roots_<cid>.car`
- **collect**: Shallow CAR with parent directory → `collect_<cid>.car`
- **metadata**: SQLite database (default `metadata.db`)

All commands print the resulting CID to stdout; progress and errors go to stderr.

### Global flags

- `--pin` — upload generated CAR to storacha (requires `storacha login` + `storacha space use`)

### Environment variables

- **`DEBUG=1`** — verbose output
- **`FETCH_TIMEOUT=30`** — timeout in seconds for IPFS fetch operations (supports decimals)

## Development

```bash
uv sync
uv run python test_pipeline.py
```
