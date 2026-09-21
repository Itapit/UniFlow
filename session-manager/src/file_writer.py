import os
import time

from src.integrity import verify_file
from src.log_setup import get_logger

log = get_logger("file_writer")


class FileWriter:
    # verifies a completed file against its file_hash and writes it to
    # disk, named by that hash

    # called from file_tracker.py's on_file_complete — same thread that
    # finished assembling that file.

    def __init__(self, output_dir=None, on_file_written=None, on_file_corrupted=None):
        self._output_dir = output_dir
        self._on_file_written = on_file_written
        self._on_file_corrupted = on_file_corrupted
        os.makedirs(self._output_dir, exist_ok=True)
        log.info("event=output_ready output_dir=%s", self._output_dir)

    def handle_file_complete(self, file_hash, total_blocks, file_bytes, contributing_receivers):
        start = time.monotonic()
        if not verify_file(file_hash, file_bytes):
            log.error("event=integrity_failure file_hash=%s bytes=%d total_blocks=%d",
                      file_hash, len(file_bytes), total_blocks)
            if self._on_file_corrupted:
                self._on_file_corrupted(file_hash=file_hash, file_bytes=file_bytes)
            return

        output_path = os.path.join(self._output_dir, f"{file_hash}.bin")
        try:
            with open(output_path, "wb") as f:
                f.write(file_bytes)
        except OSError as e:
            # Deliberately caught here rather than left to propagate: an
            # uncaught exception this deep would silently kill whichever
            # receiver-connection thread happened to finish this file,
            # in receiver_server.py, with no other thread the wiser.
            log.error("event=write_failed path=%s file_hash=%s err=%s",
                      output_path, file_hash, e)
            return

        duration_ms = int((time.monotonic() - start) * 1000)
        log.info("event=file_written path=%s file_hash=%s bytes=%d total_blocks=%d duration_ms=%d receivers=%s",
                 output_path, file_hash, len(file_bytes), total_blocks,
                 duration_ms, sorted(contributing_receivers))

        if self._on_file_written:
            self._on_file_written(file_hash=file_hash, output_path=output_path,
                                   size=len(file_bytes), contributing_receivers=contributing_receivers)
