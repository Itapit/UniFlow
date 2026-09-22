import threading


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
        self._file_names = {}       # file_hash -> (file_name, file_ext), first-seen wins
        self._completed_files = set()

    def handle_block_assembled(self, file_hash, block_id, total_blocks, block_bytes, contributing_receivers,
                               file_name="", file_ext=""):
        with self._lock:
            if file_hash in self._completed_files:
                return

            blocks = self._blocks.setdefault(file_hash, {})
            blocks[block_id] = block_bytes

            receivers_seen = self._receivers_seen.setdefault(file_hash, set())
            receivers_seen.update(contributing_receivers)

            if file_hash not in self._file_names:
                self._file_names[file_hash] = (file_name, file_ext)

            if len(blocks) < total_blocks:
                return

            self._completed_files.add(file_hash)
            del self._blocks[file_hash]
            del self._receivers_seen[file_hash]
            saved_name, saved_ext = self._file_names.pop(file_hash)

        ordered_bytes = b"".join(blocks[i] for i in range(total_blocks))
        self._on_file_complete(file_hash=file_hash, total_blocks=total_blocks,
                                file_bytes=ordered_bytes, contributing_receivers=receivers_seen,
                                file_name=saved_name, file_ext=saved_ext)