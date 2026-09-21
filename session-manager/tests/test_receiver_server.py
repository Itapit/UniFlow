import socket
import struct
import time

from pb import rx_ipc_pb2
from src.config import SESSION_SOCKET_PATH
from src.receiver_server import ReceiverServer


def on_batch(batch):
    print(f"[Test] received batch: receiver_id={batch.receiver_id} packet_count={len(batch.packets)}")
    for packet in batch.packets:
        print(f"    file_hash={packet.file_hash} block_id={packet.block_id} symbol_id={packet.symbol_id}")


server = ReceiverServer(SESSION_SOCKET_PATH, on_batch)
server.start()
time.sleep(0.2)

conn = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
conn.connect(SESSION_SOCKET_PATH)

batch = rx_ipc_pb2.SymbolBatch(receiver_id=1)
for symbol_id in range(3):
    batch.packets.add(
        file_hash=999, block_id=0, total_blocks=1,
        symbol_id=symbol_id, k_symbols=100, n_symbols=150,
        file_size=100000, content=b"x" * 1344, packet_crc=0,
    )
payload = batch.SerializeToString()
conn.sendall(struct.pack(">I", len(payload)) + payload)

time.sleep(0.5)
conn.close()
time.sleep(0.2)
server.stop()