import os
import socket
import struct
import sys
from pathlib import Path

FILE_MONITOR_DIR = Path(__file__).resolve().parent.parent

# Add 'file-monitor' and 'file-monitor/src' to sys.path
sys.path.insert(0, str(FILE_MONITOR_DIR))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from pb import ipc_pb2
from task_scheduler import SenderTask

class IPCClient:
    # dispatches protobuf TaskAssignment messages using UNIX sockets to the senders

    @staticmethod
    def send_task(task: SenderTask) -> bool:
        # serializes and transmits a single SenderTask to its designated sender socket.
        # returns True if successful, False if the target sender is unavailable.
        socket_path = task.socket_path

        # check if the socket file exists on disk
        if not os.path.exists(socket_path):
            print(f"[IPC Error] Socket path does not exist: {socket_path}")
            return False

        #  map SenderTask into protobuf message
        msg = ipc_pb2.TaskAssignment()
        msg.file_path = task.metadata.file_path
        msg.file_name = task.metadata.file_name
        msg.file_hash = task.metadata.file_hash
        msg.file_size = task.metadata.file_size
        msg.total_blocks = task.metadata.total_blocks
        msg.k_symbols = task.metadata.k_symbols
        msg.n_symbols = task.metadata.n_symbols
        msg.symbol_size = task.metadata.symbol_size
        msg.assigned_blocks.extend(task.assigned_blocks)

        if task.is_multicast_session:
            msg.mode = ipc_pb2.DISTRIBUTED
        else:
            msg.mode = ipc_pb2.SINGLE_SENDER

        # serialize protobuf to binary payload
        payload = msg.SerializeToString()

        #  create 4-byte length prefix (Big-Endian unsigned 32-bit int)
        # the struct module is python’s bridge between python high-level values and raw binary bytes
        # > specifies big indian I specifies a C unsigned int
        header = struct.pack(">I", len(payload))
        wire_data = header + payload

        #  connect and send over Unix Domain Socket
        client_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            client_sock.connect(socket_path)
            client_sock.sendall(wire_data)
            print(f"[IPC Success] Dispatched task for '{msg.file_name}' to Sender {task.sender_id} ({len(task.assigned_blocks)} blocks)")
            return True
        except socket.error as err:
            print(f"[IPC Failure] Failed to send task to {socket_path}: {err}")
            return False
        finally:
            client_sock.close()
            
            
if __name__ == "__main__":
    from file_processor import FileMetadata

    test_meta = FileMetadata(
        file_path="/tmp/test.bin",
        file_name="test.bin",
        file_size=1024,
        file_hash=999888777,
        total_blocks=1,
        k_symbols=10,
        n_symbols=15,
        symbol_size=1400
    )

    test_task = SenderTask(
        sender_id=1,
        socket_path="/tmp/uniflow_sender_1.sock",
        metadata=test_meta,
        assigned_blocks=[0],
        is_multicast_session=False
    )

    # Convert to message and inspect wire serialization
    msg = ipc_pb2.TaskAssignment(
        file_path=test_task.metadata.file_path,
        file_name=test_task.metadata.file_name,
        file_hash=test_task.metadata.file_hash,
        file_size=test_task.metadata.file_size,
        total_blocks=test_task.metadata.total_blocks,
        k_symbols=test_task.metadata.k_symbols,
        n_symbols=test_task.metadata.n_symbols,
        symbol_size=test_task.metadata.symbol_size,
        mode=ipc_pb2.SINGLE_SENDER
    )
    msg.assigned_blocks.extend(test_task.assigned_blocks)
    
    serialized = msg.SerializeToString()
    header = struct.pack(">I", len(serialized))
    
    print(f"Serialized Protobuf payload length: {len(serialized)} bytes")
    print(f"Framed header hex: {header.hex()} (Length: {struct.unpack('>I', header)[0]})")