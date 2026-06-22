#!/usr/bin/env python3
"""
half_close_sim.py - Simulate half-closed TCP connections to observe FD/connection
leaks in the Skupper router.


When a client half-closes (sends FIN via shutdown(SHUT_WR)), a router that does
not propagate the close will remain in CLOSE_WAIT indefinitely, leaking file
descriptors and memory.


Modes:
 server   Run a local TCP echo server that simulates the CLOSE_WAIT bug
 client   Connect to a target, send data, half-close, and hold the connection open
 both     Run server and clients together in one process


Examples:


 # Local loopback test — server simulates the CLOSE_WAIT bug, client half-closes:
 python half_close_sim.py both --port 9090 --count 5


 # Against a real Skupper listener (router must already be running):
 python half_close_sim.py client --host <skupper-listener-ip> --port 9090 --count 5


Inspecting connections on Linux:
 ss -tnp | grep -E 'CLOSE_WAIT|FIN_WAIT|9090|9091'


Inspecting via Skupper router:
 skstat -c    # list router connections
 skstat -l    # list links and data transfer stats
"""


import argparse
import signal
import socket
import sys
import threading
import time
from datetime import datetime




def log(msg: str) -> None:
   ts = datetime.now().strftime("%H:%M:%S")
   print(f"[{ts}] {msg}", flush=True)




# ── Echo server ────────────────────────────────────────────────────────────────



def _close_wait_handler(conn: socket.socket, addr: tuple) -> None:
   """
   Simulates the Skupper router CLOSE_WAIT bug: echoes data back but does NOT
   close after receiving the client's FIN. The connection hangs in CLOSE_WAIT
   until the process exits (or the 4-hour killer fires).
   """
   log(f"  SERVER: accepted connection from {addr[0]}:{addr[1]}")
   try:
       while True:
           data = conn.recv(4096)
           if not data:
               log(
                   f"  SERVER [CLOSE_WAIT]: got FIN from {addr[0]}:{addr[1]} "
                   f"— NOT closing (simulating stuck router)"
               )
               # Hold the socket open without sending our own FIN.
               # Server is now in CLOSE_WAIT; client is in FIN_WAIT_2.
               time.sleep(86400)
               return
           conn.sendall(data)
   except OSError:
       pass
   # Intentionally no conn.close() — the finally block is omitted on purpose
   # so Python's GC decides when to release (simulating the leak).




def run_server(host: str, port: int) -> None:
   srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
   srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
   srv.bind((host, port))
   srv.listen(128)


   log(f"Echo server listening on {host}:{port}  [CLOSE_WAIT simulation]")


   while True:
       try:
           conn, addr = srv.accept()
           t = threading.Thread(target=_close_wait_handler, args=(conn, addr), daemon=True)
           t.start()
       except OSError as e:
           log(f"Server error: {e}")
           break




# ── Half-close client ──────────────────────────────────────────────────────────


def _half_close_worker(
   host: str, port: int, client_id: int, stop_event: threading.Event
) -> None:
   """
   Connect to host:port, send a probe payload, then call shutdown(SHUT_WR).


   After shutdown(SHUT_WR):
     - This side sends a FIN and enters FIN_WAIT_1 → FIN_WAIT_2
     - The server side receives the FIN and enters CLOSE_WAIT
     - If the server (or router) never sends its own FIN back, both sides
       hold the connection open forever — the half-close / FD leak scenario.
   """
   sock = None
   try:
       sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
       sock.connect((host, port))
       local = sock.getsockname()
       remote = sock.getpeername()
       log(f"  CLIENT {client_id}: connected  {local[0]}:{local[1]} -> {remote[0]}:{remote[1]}")


       payload = (
           f"half-close probe {client_id} pid={sys.argv[0]} "
           f"ts={datetime.now().isoformat()}\n"
       ).encode()
       sock.sendall(payload)
       log(f"  CLIENT {client_id}: sent {len(payload)}-byte probe")


       # Half-close: shut down the write side only. This sends a FIN to the
       # server without closing the socket or the read side.
       sock.shutdown(socket.SHUT_WR)
       log(
           f"  CLIENT {client_id}: shutdown(SHUT_WR) done — FIN sent\n"
           f"    client {local[0]}:{local[1]}  -> FIN_WAIT_2\n"
           f"    server {remote[0]}:{remote[1]} -> CLOSE_WAIT (if router is stuck)"
       )


       # Keep the read side open. If the server eventually sends a FIN back
       # we will see it here; otherwise we hold until stop_event is set.
       sock.settimeout(1.0)
       while not stop_event.is_set():
           try:
               data = sock.recv(4096)
               if data:
                   log(f"  CLIENT {client_id}: received {len(data)} bytes (echo)")
               else:
                   log(f"  CLIENT {client_id}: server sent FIN — connection fully closed")
                   return
           except socket.timeout:
               continue
           except OSError:
               break


   except OSError as e:
       log(f"  CLIENT {client_id}: error — {e}")
   finally:
       if sock:
           sock.close()
       log(f"  CLIENT {client_id}: socket released")




def run_clients(host: str, port: int, count: int, hold_secs: int) -> None:
   log(f"Opening {count} half-closed connection(s) to {host}:{port}  (pid={os.getpid()})")


   stop_event = threading.Event()
   threads = []


   for i in range(1, count + 1):
       t = threading.Thread(
           target=_half_close_worker,
           args=(host, port, i, stop_event),
           daemon=True,
       )
       t.start()
       threads.append(t)
       time.sleep(0.05)


   # Give all workers a moment to connect before printing the banner.
   time.sleep(0.5)


   hold_label = f"{hold_secs}s" if hold_secs < 3600 else f"{hold_secs // 3600}h"
   print(flush=True)
   print("─" * 64, flush=True)
   print(f"  {count} connection(s) half-closed.  Holding for {hold_label}.", flush=True)
   print(f"  Process PID: {os.getpid()}", flush=True)
   print(flush=True)
   print("  Inspect on Linux:", flush=True)
   print("    ss -tnp | grep -E 'CLOSE_WAIT|FIN_WAIT|9090|9091'", flush=True)
   print(flush=True)
   print("  Inspect via Skupper router:", flush=True)
   print("    skstat -c    # connections", flush=True)
   print("    skstat -l    # links / data transfer", flush=True)
   print("─" * 64, flush=True)


   try:
       time.sleep(hold_secs)
   except KeyboardInterrupt:
       log("Interrupted — releasing connections")


   stop_event.set()
   for t in threads:
       t.join(timeout=3)
   log("All connections released.")




# ── CLI ────────────────────────────────────────────────────────────────────────


import os  # noqa: E402 (imported late to keep top-of-file doc clean)




def _add_server_args(p: argparse.ArgumentParser) -> None:
   p.add_argument("--host", default="0.0.0.0", help="Bind address (default: 0.0.0.0)")
   p.add_argument("--port", type=int, default=9090, help="Port to listen on (default: 9090)")




def _add_client_args(p: argparse.ArgumentParser) -> None:
   p.add_argument("--host", default="127.0.0.1", help="Target host (default: 127.0.0.1)")
   p.add_argument("--port", type=int, default=9090, help="Target port (default: 9090)")
   p.add_argument("--count", type=int, default=5, help="Number of connections (default: 5)")
   p.add_argument(
       "--hold",
       type=int,
       default=3600,
       help="Seconds to hold connections open (default: 3600)",
   )




def main() -> None:
   parser = argparse.ArgumentParser(
       prog="half_close_sim.py",
       description="Simulate half-closed TCP connections to expose FD/connection leaks.",
       formatter_class=argparse.RawDescriptionHelpFormatter,
       epilog=__doc__,
   )
   sub = parser.add_subparsers(dest="mode", required=True)


   sp = sub.add_parser("server", help="Run a local TCP echo server")
   _add_server_args(sp)


   cp = sub.add_parser("client", help="Create half-closed connections to a target")
   _add_client_args(cp)


   bp = sub.add_parser("both", help="Run server and clients in one process")
   _add_server_args(bp)
   bp.add_argument("--count", type=int, default=5, help="Number of client connections (default: 5)")
   bp.add_argument(
       "--hold",
       type=int,
       default=3600,
       help="Seconds to hold connections open (default: 3600)",
   )


   args = parser.parse_args()


   def _sigint(_sig, _frame):
       print("\nInterrupted.")
       sys.exit(0)


   signal.signal(signal.SIGINT, _sigint)


   if args.mode == "server":
       run_server(args.host, args.port)


   elif args.mode == "client":
       run_clients(args.host, args.port, args.count, args.hold)


   elif args.mode == "both":
       server_thread = threading.Thread(
           target=run_server,
           args=("0.0.0.0", args.port),
           daemon=True,
       )
       server_thread.start()
       time.sleep(0.3)  # wait for server to bind
       run_clients(args.host, args.port, args.count, args.hold)




if __name__ == "__main__":
   main()




