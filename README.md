# ia-fil-toolbox

Internet Archive & Filecoin toolbox for metadata crawling, item extraction, and root merging on IPFS DAGs.

## Installation

```bash
# Install globally
uv tool install .

# Or for development
uv sync
```

## Usage

```bash
# After global install
ia-fil metadata bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi
ia-fil extract-items bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi
ia-fil merge-roots bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi bafybeihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku

# For development (no install needed)
uv run ia_fil.py metadata bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi

# Use with file input
ia-fil metadata -f cids.txt
```
