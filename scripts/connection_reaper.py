#!/usr/bin/env python3
"""
  # Kill all TCP connections:
  python connection_reaper.py
"""

import argparse
import json
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# ── Defaults ──────────────────────────────────────────────────────────────────

DEFAULT_ROUTER_URL   = "amqp://127.0.0.1:5672"
CONN_TYPE            = "io.skupper.router.connection"
TCP_CONTAINER        = "TcpAdaptor"
EGRESS_DISPATCH_HOST = "egress-dispatch"


#Helpers

def log(msg: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def fmt_seconds(secs) -> str:
    if secs is None:
        return "never"
    secs = int(secs)
    h, rem = divmod(secs, 3600)
    m, s   = divmod(rem, 60)
    if h:
        return f"{h}h{m:02d}m{s:02d}s"
    if m:
        return f"{m}m{s:02d}s"
    return f"{s}s"


# ── Management interface ──────────────────────────────────────────────────────

"""Run skmanage and return stdout. Raises on failure."""
def skmanage(args_list: list, skmanage_bin: str = "skmanage", timeout: int = 30) -> str:
    cmd = [skmanage_bin] + args_list
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if result.returncode != 0:
        raise RuntimeError(f"skmanage failed (rc={result.returncode}): {result.stderr.strip()}")
    return result.stdout


"""Return a list of connection dicts from the router."""
def query_connections(url: str, skmanage_bin: str) -> list:
    out = skmanage(
        ["--bus", url, "QUERY", f"--type={CONN_TYPE}"],
        skmanage_bin=skmanage_bin,
    )
    return json.loads(out)

"""Force-close a connection via UPDATE adminStatus=deleted. Returns (success, error_msg)."""
def delete_connection(url: str, identity: str, skmanage_bin: str) -> tuple[bool, str]:
    try:
        skmanage(
            [
                "--bus", url,
                "UPDATE",
                f"--type={CONN_TYPE}",
                f"--identity={identity}",
                "adminStatus=deleted",
            ],
            skmanage_bin=skmanage_bin,
        )
        return True, ""
    except RuntimeError as e:
        return False, str(e)


# ── Detection ─────────────────────────────────────────────────────────────────

def is_tcp_adaptor_conn(conn: dict) -> bool:
    """True if this connection belongs to the TCP adaptor (not inter-router etc.)."""
    if conn.get("container") != TCP_CONTAINER:
        return False
    # The egress-dispatch pseudo-connection is an internal dispatcher; skip it.
    if conn.get("host") == EGRESS_DISPATCH_HOST:
        return False
    return True


# ── Main loop ─────────────────────────────────────────────────────────────────

def run_once(url: str, skmanage_bin: str, dry_run: bool) -> int:
    """
    Query connections, print a report, kill all TCP adaptor connections (unless dry_run to be used later when filtering out which are half closed or not).
    Returns the number of connections killed.
    """
    try:
        all_conns = query_connections(url, skmanage_bin)
    except Exception as exc:
        log(f"ERROR: could not query router at {url}: {exc}")
        return 0

    tcp_conns = [c for c in all_conns if is_tcp_adaptor_conn(c)]

    log(f"Connections — total:{len(all_conns)}  tcp-adaptor:{len(tcp_conns)}")

    if not tcp_conns:
        log("No TCP adaptor connections to kill.")
        return 0

    action = "would kill" if dry_run else "killing"
    log(f"--- {action.upper()} {len(tcp_conns)} TCP connection(s) ---")

    if dry_run:
        for conn in tcp_conns:
            log(f"  [DRY-RUN] id={conn.get('identity','?')}  host={conn.get('host','?')}  "
                f"dir={conn.get('dir','?')}  uptime={fmt_seconds(conn.get('uptimeSeconds'))}")
        return len(tcp_conns)

    def _kill(conn):
        ident    = conn.get("identity", "?")
        ok, err  = delete_connection(url, ident, skmanage_bin)
        return ident, conn.get("host", "?"), conn.get("dir", "?"), conn.get("uptimeSeconds"), ok, err

     #Runs the script in parallel. I have no clue how many threads other machines can handle so 
     #I just kept it at 20 for now. Ask Andy / TED abt how much this should be later
    with ThreadPoolExecutor(max_workers=20) as pool:
        futures = {pool.submit(_kill, c): c for c in tcp_conns}
        for fut in as_completed(futures):
            ident, host, direction, uptime, ok, err = fut.result()
            status = "killed" if ok else f"failed: {err}"
            log(f"  id={ident}  host={host}  dir={direction}  uptime={fmt_seconds(uptime)}  → {status}")

    return len(tcp_conns)


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="connection_reaper.py",
        description="Detect and kill idle half-closed TCP connections in Skupper router.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--url",
        default=DEFAULT_ROUTER_URL,
        help=f"Router management URL (default: {DEFAULT_ROUTER_URL})",
    )
    parser.add_argument(
        "--skmanage",
        default="skmanage",
        metavar="PATH",
        help="Path to the skmanage binary (default: skmanage, found via PATH)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print orphaned connections but do NOT kill them",
    )
    args = parser.parse_args()

    log(f"router : {args.url}")
    log(f"mode   : {'dry-run (no kills)' if args.dry_run else 'active (will kill all TCP connections)'}")
    print(flush=True)

    run_once(args.url, args.skmanage, args.dry_run)


if __name__ == "__main__":
    main()
