import selectors
import socket
import struct
from pb import tx_ipc_pb2 
from src.config import SENDERS_CONFIG

class IPCManager:
    def __init__(self, sender_states):
        # the selector handles non-blocking IO multiplexing
        self.selector = selectors.DefaultSelector()
        self.sockets = {}  # maps socket objects back to their ID
        self.sender_states = sender_states  # reference to the orchestrator's state dictionary

    def connect_to_senders(self):
        # establishes connections to the Go Senders and registers them for reading
        for sender_id, params in SENDERS_CONFIG.items():
            sock_path = params["socket_path"]
            
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.setblocking(False) # prevents the socket's functions from freezing the script
            
            try:
                sock.connect(sock_path)
                self.sockets[sock] = sender_id
                
                # register this socket with the OS to tell us when data is ready to read
                self.selector.register(sock, selectors.EVENT_READ, self._handle_read)
                print(f"[IPC] Connected to Sender {sender_id} at {sock_path}")
            except (FileNotFoundError, ConnectionRefusedError):
                print(f"[IPC Error] Could not connect to Sender {sender_id}.")

    def send_task(self, sender_id: int, file_path: str, file_hash: int):
        # frames and dispatches a TaskAssignment to a specific Sender.
        msg = tx_ipc_pb2.TaskAssignment(file_path=file_path, file_hash=file_hash)
        self._send_framed(sender_id, msg)

    def ping_all(self):
        # sends an empty Ping message to all connected Senders.
        ping_msg = tx_ipc_pb2.Ping()
        for sock, sender_id in self.sockets.items():
            self._send_framed(sender_id, ping_msg, sock)

    def _send_framed(self, sender_id: int, pb_msg, target_sock=None):
        # serializes the protobuf message and prepends the 4-byte length header.
        # Find the socket if not explicitly provided
        target_sock = None
        for s, sid in self.sockets.items():
           if sid == sender_id:
             target_sock = s
             break
            
        payload = pb_msg.SerializeToString()
        header = struct.pack(">I", len(payload))
        
        try:
            target_sock.sendall(header + payload)
        except BlockingIOError:
            pass # Socket buffer full, handle retry logic later

    def _handle_read(self, sock):
        # triggered automatically by the selector when a Sender sends data. in the selctor.register function
        sender_id = self.sockets[sock]
        
        try:
            # read the 4-byte length header
            header = sock.recv(4)
            if not header:
                return # connection closed
                
            payload_len = struct.unpack(">I", header)[0]
            
            # read the exact length of the Protobuf payload
            payload = sock.recv(payload_len)
            
            # parse the Heartbeat and update the global state dictionary
            heartbeat = tx_ipc_pb2.Heartbeat()
            heartbeat.ParseFromString(payload)
            
            # State 0 is IDLE, State 1 is WORKING
            self.sender_states[sender_id] = "WORKING" if heartbeat.state == 1 else "IDLE" #TODO: make an enum
            
        except (BlockingIOError, ConnectionResetError):
            pass # handle broken pipes

    def poll(self, timeout=1.0):
        # checks all connected sockets for incoming data and triggers the read process if any is found.
        events = self.selector.select(timeout=timeout)
        for key, mask in events:
            callback = key.data
            callback(key.fileobj) # Triggers _handle_read