import os
import socket
import struct
import threading

from pb import rx_ipc_pb2


class ReceiverServer:
    # accepts connections from the  3 receivers processes on a
    #single Unix Domain Socket and parses incoming SymbolBatch messages.
    #All three dial the same socket path — SymbolBatch.receiver_id indicates 
    #the connection

    #on_batch is called from whichever connection's thread received the
    #batch, so once it does real work (not just printing) it needs to be
    #thread-safe against being called concurrently from up to 3 threads.
    

    def __init__(self, sock_path: str, on_batch):
        self.sock_path = sock_path
        self._on_batch = on_batch
        self._listener = None
        self._accept_thread = None
        self._stop_event = threading.Event()

    def start(self) -> None:
        if os.path.exists(self.sock_path):
            os.remove(self.sock_path)

        self._listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._listener.bind(self.sock_path)
        self._listener.listen(5)
        self._listener.settimeout(0.5)  # lets the accept loop notice stop() promptly

        self._accept_thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._accept_thread.start()
        print(f"[ReceiverServer] listening on {self.sock_path}")

    def stop(self) -> None:
        # this stops accepting new connections but doesn't force-close
        # ones already open — each of those threads exits once its peer
        # disconnects. 
        self._stop_event.set()
        if self._accept_thread is not None:
            self._accept_thread.join(timeout=2.0)
        if self._listener is not None:
            self._listener.close()
        if os.path.exists(self.sock_path):
            os.remove(self.sock_path)

    def _accept_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                conn, _ = self._listener.accept()
            except socket.timeout:
                continue
            except OSError:
                return  # listener closed during stop()
            threading.Thread(target=self._handle_connection, args=(conn,), daemon=True).start()

    def _handle_connection(self, conn: socket.socket) -> None:
        print("[ReceiverServer] a receiver connected")
        try:
            while True:
                batch = self._read_batch(conn)
                if batch is None:
                    break
                self._on_batch(batch)
        except (ConnectionError, OSError):
            pass
        finally:
            conn.close()
            print("[ReceiverServer] a receiver disconnected")

    def _read_batch(self, conn: socket.socket):
        header = self._recv_exact(conn, 4)
        if header is None:
            return None
        length = struct.unpack(">I", header)[0]
        payload = self._recv_exact(conn, length)
        if payload is None:
            return None
        batch = rx_ipc_pb2.SymbolBatch()
        batch.ParseFromString(payload)
        return batch

    def _recv_exact(self, conn: socket.socket, n: int):
        chunks = []
        remaining = n
        while remaining > 0:
            chunk = conn.recv(remaining)
            if not chunk:
                return None  # peer closed
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)