"""
JSON encode/decode utilities for sending Chord RPC messages through real sockets.
"""
import json
from node import NodeRef


def encode_hook(obj):
    if isinstance(obj, NodeRef):
        return {"__noderef__": True, "address": obj.address, "id": obj.id}
    raise TypeError(f"{type(obj).__name__} is not JSON serializable")

def decode_hook(obj):
    if obj.get("__noderef__"):
        ref = NodeRef.__new__(NodeRef)
        ref.address = obj["address"]
        ref.id = obj["id"]
        return ref
    return obj

def dumps(obj):
    return json.dumps(obj, default=encode_hook)

def loads(data):
    return json.loads(data, object_hook=decode_hook)

def read_line(sock):
    """Read from a socket until a newline, return the decoded line (without the newline)."""
    chunks = []
    while True:
        chunk = sock.recv(4096)
        if not chunk:
            break
        chunks.append(chunk)
        if b"\n" in chunk:
            break
    return b"".join(chunks).split(b"\n", 1)[0].decode("utf-8")
