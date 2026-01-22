# ia-fil-toolbox

Internet Archive & Filecoin toolbox for metadata crawling, item extraction, and root merging on IPFS DAGs.

## Prerequisites

- **IPFS** (Kubo): `ipfs` command must be available in PATH
- **go-car**: For CAR file generation - install with `go install github.com/ipld/go-car/cmd/car@latest`
- **someguy** (optional): For delegated routing - install from https://github.com/ipfs-shipyard/someguy

## Installation

```bash
# Install globally
uv tool install .

# Or for development
uv sync
```

## Usage

### Basic Commands

```bash
# Extract items from _files.xml into synthetic directories
ia-fil extract-items <cid> [<cid> ...]

# Fetch metadata files
ia-fil metadata <cid> [<cid> ...]

# Merge multiple root CIDs into a single directory
ia-fil merge-roots <cid1> <cid2> [<cid3> ...]

# Force expensive directory checks (default: uses file extension heuristics)
ia-fil merge-roots --force-check-directories <cid1> <cid2>

# Use with file input (plain text or CSV with "cid" column)
ia-fil extract-items -f cids.txt
ia-fil metadata -f cids.csv
```

### Daemon Management

The tool automatically manages a staging IPFS daemon on port 5009. For persistent daemon:

```bash
# Run persistent daemons (IPFS + someguy)
ia-fil run-daemons

# Run without someguy delegated routing
ia-fil --no-someguy run-daemons

# Check daemon status
ia-fil daemon-status
```

### Environment Variables

- **`DEBUG=1`**: Enable verbose debug output showing detailed operation progress
- **`SOMEGUY_ENABLED=0`**: Disable someguy delegated routing (auto-disabled in test environments)

### Performance Options

- **`--force-check-directories`** (merge-roots): Forces expensive directory type checks via `ipfs ls` for every entry. By default (False), the tool uses file extension heuristics to skip these checks for known file types, significantly speeding up operations on flat directories.

### Output

- **extract-items**: Creates synthetic directories and generates shallow CAR files containing only directory blocks
  - Prints container CID to stdout
  - Generates `extract_items_<cid>.car` file
  
- **merge-roots**: Merges CIDs into a single directory and generates a CAR file
  - Prints merged root CID to stdout
  - Generates `merged_root_<cid>.car` file
  - Files with conflicting names (same name, different CID) are excluded from the merge

- **metadata**: Saves metadata to SQLite database (default: `metadata.db`)

### Examples

```bash
# Extract with debug output
DEBUG=1 ia-fil extract-items bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi

# Run without someguy
ia-fil --no-someguy extract-items <cid>

# Development mode
uv run ia_fil.py extract-items <cid>
```

## Development

### Running Tests

```bash
# Run all tests
pytest test_pipeline.py -v

# Run specific test class
pytest test_pipeline.py::TestIAFilToolbox -v
```

### Shell Completion

```bash
# Install bash completion
ia-fil completion install bash

# Install zsh completion
ia-fil completion install zsh

# Install fish completion
ia-fil completion install fish
```
