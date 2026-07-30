"""
Network transport for Chord, over TCP sockets.
"""
import socket
import chord_rpc


class Network:
    def __init__(self, timeout: float = 2.0):
        self.timeout = timeout

    def send(self, address: str, method_name: str, *args):
        host, port_str = address.rsplit(":", 1)
        port = int(port_str)

        try:
            sock = socket.create_connection((host, port), timeout=self.timeout)
        except (ConnectionRefusedError, OSError, socket.timeout):
            raise ConnectionError(f"Node at {address} is unreachable")

        try:
            request = chord_rpc.dumps({"method": method_name, "args": list(args)}) + "\n"
            sock.sendall(request.encode("utf-8"))
            response_line = chord_rpc.read_line(sock)
        except (ConnectionResetError, OSError, socket.timeout):
            raise ConnectionError(f"Node at {address} is unreachable")
        finally:
            sock.close()

        response = chord_rpc.loads(response_line)
        if "error" in response:
            raise RuntimeError(f"Remote error from {address}: {response['error']}")
        return response.get("result")