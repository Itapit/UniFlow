import os
import time
import queue
from src.config import SENDERS_CONFIG
from src.config import TEN_MB_BYTES
from src.log_setup import get_logger

log = get_logger("orchestrator")

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
        log.info("event=loop_started")
        last_ping = 0.0
        ping_interval = 1.0

        while True:
            self.ipc_manager.poll(timeout=0.1)

            now = time.time()
            if now - last_ping >= ping_interval:
                self.ipc_manager.ping_all()
                last_ping = now

            self._sync_states()
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
            log.info("event=file_complete path=%s", file_path)
            try:
                self._cleanup_counter(file_path)
            except Exception as e:
                log.warning("event=counter_cleanup_failed path=%s err=%s", file_path, e)

            # if this was our tracked large file, clear it so we can move to the next one
            if self.current_large_file and self.current_large_file.file_path == file_path:
                self.current_large_file = None
                self.finished_large_file_senders.clear()

    @staticmethod
    def _counter_path_for_hash(file_hash: int):
        from pathlib import Path
        counter_dir = Path(__file__).resolve().parent.parent.parent / "shared" / "counter"
        return counter_dir / f"counter_{file_hash}.bin"

    @staticmethod
    def _remove_counter_file(counter_path, file_hash: int, reason: str) -> None:
        try:
            os.remove(counter_path)
            log.info("event=counter_deleted counter_path=%s file_hash=%d reason=%s",
                     str(counter_path), file_hash, reason)
        except FileNotFoundError:
            log.debug("event=counter_already_gone counter_path=%s file_hash=%d reason=%s",
                      str(counter_path), file_hash, reason)

    @staticmethod
    def _reset_counter_for_new_file(file_hash: int) -> None:
        """Delete any stale shared-counter before a fresh transfer starts.

        Counter files are keyed only by file_hash, so a re-transfer of the
        same content would otherwise reuse the old value (>=1) and the
        sender would claim zero blocks (startBlock >= totalBlocks).
        Must be called exactly once per new file, BEFORE the first
        send_task — never for large-file joiners while others count.
        """
        counter_path = Orchestrator._counter_path_for_hash(file_hash)
        Orchestrator._remove_counter_file(counter_path, file_hash, reason="pre_dispatch_reset")

    @staticmethod
    def _cleanup_counter(file_path: str) -> None:
        """Remove leftover shared-counter files for a finished transfer.

        Counter files are named counter_<file_hash>.bin and live under
        shared/counter/. The senders only Close() them, so without this
        the directory accumulates one stale .bin per transfer. We hash here
        rather than trust in-memory metadata so a restart still cleans up.
        """
        from src.file_processor import compute_file_hash

        try:
            file_hash = compute_file_hash(file_path)
        except OSError:
            # Source file already moved/deleted — fall back to sweeping
            # only files that look like orphaned counters is unsafe, so skip.
            log.debug("event=counter_cleanup_skipped path=%s reason=hash_failed", file_path)
            return
        counter_path = Orchestrator._counter_path_for_hash(file_hash)
        Orchestrator._remove_counter_file(counter_path, file_hash, reason="post_complete_cleanup")

    def _dispatch_tasks(self):
        # greedily assigns files to IDLE senders.
        idle_sockets = [
            sock for sock, state in self.sender_states.items()
            if state == "IDLE"
        ]

        if not idle_sockets:
            return # No available workers, back to polling

        log.debug("event=dispatch_poll idle=%d queued=%d active_files=%d",
                  len(idle_sockets), self.task_queue.qsize(), len(self.active_transfers))

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
                # Small file: Assign to the first available IDLE sender.
                # Reset once before dispatch so a stale counter_<hash>.bin
                # from a previous run/crash can't cause zero-block transfer.
                self._reset_counter_for_new_file(metadata.file_hash)
                target_sock = idle_sockets[0]
                self._assign_task(target_sock, metadata)
            else:
                # Large file: Assign to ALL currently IDLE senders and set as current.
                # Reset exactly once BEFORE the first sender starts counting;
                # joiners (strategy A) must never reset mid-transfer.
                self._reset_counter_for_new_file(metadata.file_hash)
                self.current_large_file = metadata
                for sock in idle_sockets:
                    self._assign_task(sock, metadata)

    def _assign_task(self, sock_path: str, metadata):
        # triggers the IPC manager to send the Protobuf message and updates local state.
        strategy = "large_join" if (self.current_large_file and metadata.file_path == self.current_large_file.file_path) else "single"
        log.info("event=assigned file=%s file_hash=%d size=%d sock=%s strategy=%s",
                 metadata.file_name, metadata.file_hash, metadata.file_size, sock_path, strategy)

        # dispatch via IPC Manager
        self.ipc_manager.send_task(sock_path, metadata.file_path, metadata.file_hash)

        # immediately assume WORKING to prevent duplicate assignments in the same loop
        self.sender_states[sock_path] = "WORKING"

        # track the active transfer
        if metadata.file_path not in self.active_transfers:
            self.active_transfers[metadata.file_path] = []
        self.active_transfers[metadata.file_path].append(sock_path)
