#!/usr/bin/env python3
"""
Simulates half closed connections by making them stuck on Close_Wait for one side and Fin_Wait_2 for the other side.
Makes a connection from Client -> Router A (port 9090) -> Backend (port 9091)

Adjust file pathing here as needed
Terminal 1 — start the router:
  /root/skupper-router/build/router/skrouterd -c /root/skupper-router/build/a.conf

Terminal 2 — start backend and clients:
  python3 /root/skupper-router/half_close_sim.py --port 9090 --count 5

Terminal 3 — inspect connections:
  ss -tnp | grep -E '9090|9091'
  . /root/skupper-router/build/config.sh && skstat -c

  skstat -g | grep -E "VmSize|RSS"  - This command shows memory usage so you can adjust terminal 2 count 5 to be like 500 
  and see how big of an impact more half-closed connections are
"""

import argparse
import os
import signal
import socket
import sys
import threading
import time


#Simulates the bug by calling sleeping instead of doing conn.close() which leaves the connection stuck in CLOSE_WAIT.
#This is triggered after shutdown(SHUT_WR) sends the FIN from client side.
def _close_wait_handler(conn: socket.socket, addr: tuple) -> None:
    try:
        while True:
            data = conn.recv(4096)
            if not data:
                time.sleep(86400)
                return
            conn.sendall(data)
    except OSError:
        pass


#Listens for incoming TCP connections and spawns a _close_wait_handler thread for each one.
def run_server(host: str, port: int) -> None:
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((host, port))
    srv.listen(128)
    while True:
        try:
            conn, addr = srv.accept()
            threading.Thread(target=_close_wait_handler, args=(conn, addr), daemon=True).start()
        except OSError:
            break


def _half_close_worker(host: str, port: int, stop_event: threading.Event) -> None:
    # Opens the TCP connection between the listener and connector, sends a small
    # probe so the tunnel is properly established, then shutdown(SHUT_WR) makes
    # the half-close by sending a FIN without closing the socket. After that it
    # just waits. If the router correctly propagates the FIN
    # the backend sends one back and the connection fully closes. If the router
    # is buggy nothing comes back and it stays stuck open indefinitely.
    sock = None
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))
        sock.sendall(b"half-close probe\n")
        sock.shutdown(socket.SHUT_WR)
        sock.settimeout(1.0)
        while not stop_event.is_set():
            try:
                if not sock.recv(4096):
                    return
            except socket.timeout:
                continue
            except OSError:
                break
    except OSError:
        pass
    finally:
        if sock:
            sock.close()


def main() -> None:
    parser = argparse.ArgumentParser(prog="half_close_sim.py")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9090)
    parser.add_argument("--count", type=int, default=5)
    parser.add_argument("--hold", type=int, default=3600)

    args = parser.parse_args()
    # Exit cleanly on Ctrl+C
    signal.signal(signal.SIGINT, lambda _s, _f: sys.exit(0))

    # Start the backend server on port+1 before spawning clients
    threading.Thread(target=run_server, args=("0.0.0.0", args.port + 1), daemon=True).start()
    time.sleep(0.3)
    # Spawn client threads staggered by 50ms to avoid jams
    stop_event = threading.Event()
    threads = []
    for _ in range(args.count):
        t = threading.Thread(target=_half_close_worker, args=(args.host, args.port, stop_event), daemon=True)
        t.start()
        threads.append(t)
        time.sleep(0.05)
    # Hold connections open for hold_secs, then signal all threads to stop
    try:
        time.sleep(args.hold)
    except KeyboardInterrupt:
        pass
    stop_event.set()
    for t in threads:
        t.join(timeout=3)


if __name__ == "__main__":
    main()
