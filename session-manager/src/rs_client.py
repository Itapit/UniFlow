import socket
import struct

from pb import rs_helper_pb2


class ReconstructionError(Exception):
    """Raised when rs_helper reports a failed reconstruction."""


class RSClient:
    """Persistent client to the rs_helper Go subprocess. rs_helper handles
    each connection's requests strictly in order — send one, read its
    response, then send the next; there's no request ID to multiplex on."""

    def __init__(self, sock_path="/tmp/uniflow_rs_helper.sock"):
        self.sock_path = sock_path
        self._sock = None

    def connect(self):
        self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._sock.connect(self.sock_path)

    def close(self):
        if self._sock is not None:
            self._sock.close()
            self._sock = None

    def reconstruct(self, file_hash: int, block_id: int, k_symbols: int,
                     n_symbols: int, shards: dict) -> list:
        """shards maps symbol_id -> content for every shard actually
        received for this block. Returns the k_symbols data shards, in
        order. Raises ReconstructionError if too many shards were missing."""
        request = rs_helper_pb2.ReconstructRequest(
            file_hash=file_hash, block_id=block_id,
            k_symbols=k_symbols, n_symbols=n_symbols,
        )
        for symbol_id, content in shards.items():
            request.shards.add(symbol_id=symbol_id, content=content)

        self._send_framed(request)
        response = self._recv_framed(rs_helper_pb2.ReconstructResponse)

        if not response.ok:
            raise ReconstructionError(
                f"rs_helper failed for file_hash={file_hash} block_id={block_id}: {response.error}"
            )
        return list(response.data_shards)

    def _send_framed(self, message) -> None:
        payload = message.SerializeToString()
        self._sock.sendall(struct.pack(">I", len(payload)) + payload)

    def _recv_framed(self, message_cls):
        header = self._recv_exact(4)
        payload = self._recv_exact(struct.unpack(">I", header)[0])
        message = message_cls()
        message.ParseFromString(payload)
        return message

    def _recv_exact(self, n: int) -> bytes:
        chunks = []
        remaining = n
        while remaining > 0:
            chunk = self._sock.recv(remaining)
            if not chunk:
                raise ConnectionError("rs_helper closed the connection unexpectedly")
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)