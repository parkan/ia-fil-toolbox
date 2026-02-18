# MFS flush limits: misleading documentation

## Background

Kubo's `Internal.MFSNoFlushLimit` (default 256 in Kubo 0.39.0) and the
surrounding documentation imply that MFS will consume excessive memory if
you don't flush frequently when building large directories. This led us
to implement periodic flushing every 1024 ops when building directories
with 300k+ entries.

## What actually happened

- Periodic flushes every 1024 ops **hurt** performance — each flush
  serializes the entire HAMT (O(n) work per flush, O(n²) total)
- Both test directories stalled at exactly **5956 entries** — the
  deterministic count proved this was a data problem, not a memory problem
- Root cause: `ipfs files cp /ipfs/{cid}` blocks indefinitely on
  unresolvable CIDs with no timeout
- Fix: added `FETCH_TIMEOUT` (subprocess timeout on `files cp`)
- With timeout in place, builds sailed past 6k entries with no issues

## Memory reality

IPFS resident memory stayed **under 100MB** for 300k directory entries.
Each entry is ~50–100 bytes (CID link + name). 300k entries ≈ 30MB of
actual data. The flush limit "protection" against OOM is solving a
problem that doesn't exist at any practical scale.

## Correct approach for large MFS directories

1. **Don't flush periodically** — set a high flush limit or disable
   periodic flushing entirely; do a single final flush
2. **Add timeouts to `files cp`** — the real failure mode is network
   I/O blocking forever on missing/unretrievable CIDs
3. **Collect failures** — log CIDs that time out and continue building;
   report them at the end

## References

- Kubo source: `core/coreapi/unixfs.go`, `MFSNoFlushLimit`
- Republisher runs on 300ms/3s timers independently, does NOT hold the
  directory lock (so it's not the bottleneck either)
