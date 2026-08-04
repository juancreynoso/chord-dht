"""
Runs a single Chord node as a process listening on a TCP port.

Usage:
    python3 server.py --port 9000                      # first node, creates its own ring
    python3 server.py --port 9001 --join 127.0.0.1:9000 # joins an existing ring through 9000
"""
import argparse
import socketserver
import threading
import time
import sys

import rpc
from identifiers import short_id
from node import Node
from network import Network

STABILIZE_INTERVAL = 1.0
FIX_FINGERS_INTERVAL = 1.0
CHECK_PREDECESSOR_INTERVAL = 1.0

ALLOWED_METHODS = {
    "find_successor", "get_predecessor", "get_successor_list", "notify", "ping",
    "receive_keys", "store_replica", "drop_replica",
    "write", "read", "write_local", "read_local",
    "get_debug_state",
}


class ChordRequestHandler(socketserver.BaseRequestHandler):
    def handle(self):
        try:
            line = rpc.read_line(self.request)
            if not line:
                return
            msg = rpc.loads(line)
            method_name = msg["method"]
            args = msg.get("args", [])
            node = self.server.node
            if method_name not in ALLOWED_METHODS:
                raise PermissionError(f"method not exposed over RPC: {method_name}")
            method = getattr(node, method_name)
            result = method(*args)
            response = {"result": result}
        except Exception as e:
            response = {"error": f"{type(e).__name__}: {e}"}
        try:
            self.request.sendall((rpc.dumps(response) + "\n").encode("utf-8"))
        except OSError:
            pass 


class ChordServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


def periodic(name, fn, interval, log):
    """Call fn() every interval seconds forever."""
    while True:
        try:
            fn()
        except Exception as e:
            log(f"{name} error: {e}")
        time.sleep(interval)


def start_maintenance(node, log):
    jobs = [
        ("stabilize", node.stabilize, STABILIZE_INTERVAL),
        ("fix_fingers", node.fix_fingers, FIX_FINGERS_INTERVAL),
        ("check_predecessor", node.check_predecessor, CHECK_PREDECESSOR_INTERVAL),
    ]
    for name, fn, interval in jobs:
        threading.Thread(target=periodic, args=(name, fn, interval, log), daemon=True).start()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--join", default=None, help="address of an existing node to join through")
    parser.add_argument("--m", type=int, default=160)
    parser.add_argument("--replicas", type=int, default=2)
    args = parser.parse_args()

    address = f"{args.host}:{args.port}"

    def log(msg):
        print(f"[{address}] {msg}", flush=True)

    network = Network()
    node = Node(address, network, m=args.m, num_replicas=args.replicas, log_fn=log)

    server = ChordServer((args.host, args.port), ChordRequestHandler)
    server.node = node

    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    log(f"listening (id={short_id(node.id)})")

    # give the socket a moment before start hammering it with our own join request
    time.sleep(0.2)

    if args.join:
        node.join(args.join)
        log(f"joined ring through {args.join} -> successor={short_id(node.successor.id)}")
    else:
        node.join(address)
        log("creating new ring")

    start_maintenance(node, log)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        sys.exit(0)


if __name__ == "__main__":
    main()