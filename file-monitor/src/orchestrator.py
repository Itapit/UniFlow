import time
import queue
from src.config import SENDERS_CONFIG
from src.config import TEN_MB_BYTES 

class Orchestrator:
    def __init__(self, task_queue: queue.Queue, ipc_manager, sender_states: dict):
        self.task_queue = task_queue
        self.ipc_manager = ipc_manager
        self.sender_states = sender_states
        
        # tracks active transfers for each file path there is a list contains all the working senders
        self.active_transfers = {}
        
        # holds a large file if we need newly IDLE senders to join it later
        self.current_large_file = None 
        
        self.finished_large_file_senders = set() # Track who is done with the current large file

    def run(self):
        print("[Orchestrator] Event loop started.")
        
        while True:
            # catch incoming heartbeats (non-blocking)
            self.ipc_manager.poll(timeout=0.1)
            
            # sync active transfers with the latest Sender states
            self._sync_states()
            
            # assign tasks to any senders that are currently IDLE
            self._dispatch_tasks()
            
            time.sleep(0.05)

    def _sync_states(self):
        # removes IDLE senders from active tracking, If a file has no active senders, it's finished."
        completed_files = []
        
        for file_path, assigned_sockets in self.active_transfers.items():
            active_sockets = []
            for sock in assigned_sockets:
                if self.sender_states[sock] == "WORKING":
                    active_sockets.append(sock)
                else:
                    # if they are IDLE and this is the large file, they are permanently done with it
                    if self.current_large_file and file_path == self.current_large_file.file_path:
                        self.finished_large_file_senders.add(sock)
            
            self.active_transfers[file_path] = active_sockets
            
            if not active_sockets:
                completed_files.append(file_path)
                
        for file_path in completed_files:
            del self.active_transfers[file_path]
            print(f"[Orchestrator] Completed transmission for: {file_path}")
            
            # if this was our tracked large file, clear it so we can move to the next one
            if self.current_large_file and self.current_large_file.file_path == file_path:
                self.current_large_file = None
                self.finished_large_file_senders.clear()
    def _dispatch_tasks(self):
        # greedily assigns files to IDLE senders.
        idle_sockets = [
            sock for sock, state in self.sender_states.items() 
            if state == "IDLE"
        ]
        
        if not idle_sockets:
            return # No available workers, back to polling
            
        # strategy A: join an in-progress large file
        if self.current_large_file:
            for sock in idle_sockets:
                # check if the sender already finished this specific large file
                if sock not in self.finished_large_file_senders:
                 self._assign_task(sock, self.current_large_file)
            return

        # strategy B: pull a new file from the queue
        if not self.task_queue.empty():
            metadata = self.task_queue.get()
            
            if metadata.file_size < TEN_MB_BYTES:
                # Small file: Assign to the first available IDLE sender
                target_sock = idle_sockets[0]
                self._assign_task(target_sock, metadata)
            else:
                # Large file: Assign to ALL currently IDLE senders and set as current
                self.current_large_file = metadata
                for sock in idle_sockets:
                    self._assign_task(sock, metadata)

    def _assign_task(self, sock_path: str, metadata):
        # triggers the IPC manager to send the Protobuf message and updates local state.
        print(f"[Orchestrator] Assigning {metadata.file_name} to Sender at {sock_path}")
        
        # dispatch via IPC Manager
        self.ipc_manager.send_task(sock_path, metadata.file_path, metadata.file_hash)
        
        # immediately assume WORKING to prevent duplicate assignments in the same loop
        self.sender_states[sock_path] = "WORKING"
        
        # track the active transfer
        if metadata.file_path not in self.active_transfers:
            self.active_transfers[metadata.file_path] = []
        self.active_transfers[metadata.file_path].append(sock_path)