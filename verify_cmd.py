import sys
import subprocess
import concurrent.futures
import time
from typing import List, Dict, Optional, Tuple
from shared import run_ipfs_cmd, FETCH_TIMEOUT


def verify_cid(cid: str, timeout: float) -> Dict[str, object]:
    """
    Perform the lightest possible retrievability check for a CID.

    Uses `ipfs block stat`, which fetches only the root block (not the subgraph)
    and returns its size + codec. If the daemon can resolve the block from the
    network within the timeout, the CID is considered retrievable.

    Returns:
        Dict with keys: cid, ok (bool), size (Optional[int]), elapsed (float),
        error (Optional[str]).
    """
    start = time.monotonic()
    try:
        result = run_ipfs_cmd(
            ['block', 'stat', cid],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        return {
            'cid': cid,
            'ok': False,
            'size': None,
            'elapsed': time.monotonic() - start,
            'error': f'timeout after {timeout}s',
        }
    except Exception as e:
        return {
            'cid': cid,
            'ok': False,
            'size': None,
            'elapsed': time.monotonic() - start,
            'error': str(e),
        }

    elapsed = time.monotonic() - start

    if result.returncode != 0:
        err = (result.stderr or '').strip().splitlines()
        msg = err[-1] if err else f'exit {result.returncode}'
        return {
            'cid': cid,
            'ok': False,
            'size': None,
            'elapsed': elapsed,
            'error': msg,
        }

    # Parse "Size: N" from `ipfs block stat` output.
    size: Optional[int] = None
    for line in result.stdout.splitlines():
        line = line.strip()
        if line.lower().startswith('size:'):
            try:
                size = int(line.split(':', 1)[1].strip())
            except ValueError:
                pass
            break

    return {
        'cid': cid,
        'ok': True,
        'size': size,
        'elapsed': elapsed,
        'error': None,
    }


def _format_size(size: Optional[int]) -> str:
    if size is None:
        return '-'
    return str(size)


def _print_table(results: List[Dict[str, object]]):
    headers = ('CID', 'STATUS', 'SIZE', 'TIME', 'ERROR')
    rows = []
    for r in results:
        rows.append((
            str(r['cid']),
            'OK' if r['ok'] else 'FAIL',
            _format_size(r['size']),
            f"{r['elapsed']:.2f}s",
            r['error'] or '',
        ))

    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            if len(cell) > widths[i]:
                widths[i] = len(cell)

    def fmt(row):
        return '  '.join(cell.ljust(widths[i]) for i, cell in enumerate(row))

    print(fmt(headers))
    print('  '.join('-' * w for w in widths))
    for row in rows:
        print(fmt(row))


def run_verify(cids: List[str], timeout: float = FETCH_TIMEOUT, workers: int = 8):
    """
    Verify retrievability of each CID by fetching only its root block.

    Args:
        cids: List of CIDs to verify
        timeout: Per-CID timeout in seconds
        workers: Max parallel checks
    """
    if not cids:
        print("Error: No CIDs provided", file=sys.stderr)
        sys.exit(1)

    print(f"Verifying {len(cids)} CID(s) (timeout={timeout}s, workers={workers})...", file=sys.stderr)

    results: List[Dict[str, object]] = []
    results_by_index: Dict[int, Dict[str, object]] = {}

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        # Key futures by input index so duplicate CIDs don't collide.
        futures = {
            executor.submit(verify_cid, cid, timeout): i
            for i, cid in enumerate(cids)
        }
        done = 0
        for future in concurrent.futures.as_completed(futures):
            idx = futures[future]
            cid = cids[idx]
            done += 1
            try:
                res = future.result()
            except Exception as e:
                res = {
                    'cid': cid,
                    'ok': False,
                    'size': None,
                    'elapsed': 0.0,
                    'error': f'unexpected: {e}',
                }
            results_by_index[idx] = res
            mark = '✓' if res['ok'] else '✗'
            extra = '' if res['ok'] else f" ({res['error']})"
            print(f"  [{done}/{len(cids)}] {mark} {cid}{extra}", file=sys.stderr)

    # Preserve input order in the output table.
    for i in range(len(cids)):
        results.append(results_by_index[i])

    ok_count = sum(1 for r in results if r['ok'])
    print(
        f"Done: {ok_count}/{len(cids)} retrievable, {len(cids) - ok_count} failed",
        file=sys.stderr,
    )

    _print_table(results)

    if ok_count < len(cids):
        sys.exit(2)
