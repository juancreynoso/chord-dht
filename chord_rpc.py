"""
JSON encode/decode utilities for sending Chord RPC messages through real sockets.
"""
import json
from node import NodeRef


class ChordJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, NodeRef):
            return {"__noderef__": True, "address": obj.address, "id": obj.id}
        return super().default(obj)

def decode_hook(d):
    if d.get("__noderef__"):
        ref = NodeRef.__new__(NodeRef)
        ref.address = d["address"]
        ref.id = d["id"]
        return ref
    return d

def dumps(obj):
    return json.dumps(obj, cls=ChordJSONEncoder)

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
