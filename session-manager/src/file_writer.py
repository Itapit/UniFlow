import os

from src.integrity import verify_file


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

    def handle_file_complete(self, file_hash, total_blocks, file_bytes, contributing_receivers):
        if not verify_file(file_hash, file_bytes):
            print(f"[FileWriter] INTEGRITY FAILURE: file_hash={file_hash} — "
                  f"reconstructed bytes do not match the source hash")
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
            print(f"[FileWriter] failed writing {output_path}: {e}")
            return

        print(f"[FileWriter] wrote verified file: {output_path} "
              f"({len(file_bytes)} bytes, receivers={sorted(contributing_receivers)})")

        if self._on_file_written:
            self._on_file_written(file_hash=file_hash, output_path=output_path,
                                   size=len(file_bytes), contributing_receivers=contributing_receivers)