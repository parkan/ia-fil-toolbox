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

## Crash recovery changes the calculus

After disabling periodic flushes (MFS_FLUSH_LIMIT=500000), a ~200k entry
build completed the `files cp` loop but the daemon crashed before the
final flush. Unflushed MFS state is in-memory only — the entire build
was lost. No OOM killer in dmesg; cause unknown.

Periodic flushing at 1024 ops is cheap insurance. With `FETCH_TIMEOUT`
in place, each `files cp` is bounded, so the O(n) flush cost every 1024
ops is negligible compared to network fetches. Worst case on crash: you
lose the last 1023 ops, not the entire build.

## Correct approach for large MFS directories

1. **Keep periodic flushing** (every 1024 ops) — not for memory, but
   for crash recovery of unflushed in-memory state
2. **Add timeouts to `files cp`** — the real failure mode is network
   I/O blocking forever on missing/unretrievable CIDs
3. **Collect failures** — log CIDs that time out and continue building;
   report them at the end

## References

- Kubo source: `core/coreapi/unixfs.go`, `MFSNoFlushLimit`
- Republisher runs on 300ms/3s timers independently, does NOT hold the
  directory lock (so it's not the bottleneck either)
