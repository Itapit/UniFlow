import threading

from src.log_setup import get_logger

log = get_logger("file_tracker")


class FileTracker:
    #tracks which blocks of each file have arrived, and fires
    #on_file_complete exactly once, with the full ordered bytes, the
    #moment every block 0..total_blocks-1 has been assembled.

    #handle_block_assembled is called from whichever receiver-connection
    #thread's block finished reconstruction in block_assembler.py — so,
    #same as Aggregator, this can be entered concurrently by up to 3
    #threads, for different blocks of the same or different files.

    # just tracks all the file by the file hash and checks if all the blocks had arraived already so it can set it as complete.


    def __init__(self, on_file_complete):
        self._on_file_complete = on_file_complete
        self._lock = threading.Lock()
        self._blocks = {}
        self._receivers_seen = {}   # file_hash -> set of receiver_ids across all its blocks
        self._completed_files = set()

    def handle_block_assembled(self, file_hash, block_id, total_blocks, block_bytes, contributing_receivers):
        with self._lock:
            if file_hash in self._completed_files:
                log.debug("event=duplicate_block_after_complete file_hash=%s block_id=%s",
                          file_hash, block_id)
                return

            blocks = self._blocks.setdefault(file_hash, {})
            blocks[block_id] = block_bytes

            receivers_seen = self._receivers_seen.setdefault(file_hash, set())
            receivers_seen.update(contributing_receivers)

            if len(blocks) < total_blocks:
                log.debug("event=block_assembled file_hash=%s block_id=%s done=%d/%d",
                          file_hash, block_id, len(blocks), total_blocks)
                return

            self._completed_files.add(file_hash)
            del self._blocks[file_hash]
            del self._receivers_seen[file_hash]

        ordered_bytes = b"".join(blocks[i] for i in range(total_blocks))
        log.info("event=file_complete file_hash=%s total_blocks=%d bytes=%d receivers=%s",
                 file_hash, total_blocks, len(ordered_bytes), sorted(receivers_seen))
        self._on_file_complete(file_hash=file_hash, total_blocks=total_blocks,
                                file_bytes=ordered_bytes, contributing_receivers=receivers_seen)
