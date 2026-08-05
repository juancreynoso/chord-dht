# Chord DHT — Distributed User Presence System

Implementation of a distributed hash table (DHT) based on **Chord**, built for the
qualifying project of *Telecomunicaciones y Sistemas Distribuidos (UNRC)*: keeping
user status (`connected`/`disconnected`) distributed across nodes, with logarithmic
lookup cost and fault tolerance without data loss.

The full write-up on architecture, algorithms, message format and the demonstration
of the required properties is in [`documentacion.pdf`](./documentacion.pdf) (in Spanish).

## Requirements

- Python 3.10+.

## Project structure

| File | Role |
|---|---|
| `identifiers.py` | Consistent hashing (SHA-1), identifiers and circular intervals |
| `node.py` | Chord protocol logic: `find_successor`, `join`, `stabilize`, `fix_fingers`, replication, `check_predecessor` |
| `network.py` | Transport over TCP sockets (`send(address, method, *args)`) |
| `rpc.py` | JSON serialization of RPC messages |
| `server.py` | Runs a node as an independent process, listening on a port |

## Running it

Each process prints its own events as they happen (join, successor/predecessor
change, failure detection, promoting a replica to primary data). No extra tooling
is needed to see the ring correcting itself.

```bash
# Terminal 1: first node, starts its own ring
python3 server.py --port 9000

# Terminal 2: joins through the first one
python3 server.py --port 9001 --join 127.0.0.1:9000

# Terminal 3, 4, 5...: each joins through any existing node
python3 server.py --port 9002 --join 127.0.0.1:9000
```

To stop a node, or simulate a crash: `Ctrl+C`, or `kill -9 <pid>` from another terminal.

### Talking to the ring

The most direct way is a one-line Python call from a new terminal:

```bash
python3 -c "
from network import Network
net = Network()
net.send('127.0.0.1:9000', 'write', 'juan_cruz', 'connected')
print(net.send('127.0.0.1:9001', 'read', 'juan_cruz'))
connected
"
```

## Testing

A full demonstration was carried out (forming a seven-node ring, writes and reads
from arbitrary nodes, hot-joining a node, and the crash of the node owning a key),
documented in the *Demostración de ejecución* (execution walkthrough) section of
[`documentacion.pdf`](./documentacion.pdf).
