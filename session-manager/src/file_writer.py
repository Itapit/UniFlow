import os
import re

from src.integrity import verify_file

# Anything outside alphanumerics, dash, underscore and dot is unsafe in
# a reconstructed file name — replaced with "_". Path separators never
# survive because we basename() first, but this also kills control
# characters and NUL bytes.
_UNSAFE_CHARS = re.compile(r"[^A-Za-z0-9._-]")
_MAX_BASENAME_LEN = 255


def sanitize_basename(file_name: str, file_ext: str, file_hash: int) -> str:
    # Builds a safe output basename from the transmitted stem + ext.
    # Falls back to "{file_hash}.bin" when nothing usable remains.
    raw = f"{file_name or ''}{file_ext or ''}"
    # Never allow directory traversal: keep only the final component.
    base = os.path.basename(raw).strip()
    # Remove NUL bytes explicitly (basename keeps them) and surrounding dots.
    base = base.replace("\x00", "")
    if not base or base in (".", ".."):
        return f"{file_hash}.bin"
    safe = _UNSAFE_CHARS.sub("_", base)
    if not safe or safe in (".", ".."):
        return f"{file_hash}.bin"
    if len(safe) > _MAX_BASENAME_LEN:
        stem, dot, ext = safe.rpartition(".")
        if dot and len(ext) <= 16:
            keep = _MAX_BASENAME_LEN - len(ext) - 1
            safe = f"{stem[:keep]}.{ext}"
        else:
            safe = safe[:_MAX_BASENAME_LEN]
    return safe


def _dedupe_path(output_dir: str, basename: str, file_hash: int) -> str:
    # Returns a non-colliding path: exact name when free, otherwise
    # "{stem}_{hash8}{ext}", then numeric suffixes as a last resort.
    candidate = os.path.join(output_dir, basename)
    if not os.path.exists(candidate):
        return candidate
    stem, dot, ext = basename.rpartition(".")
    if not dot:
        stem, ext_suffix = basename, ""
    else:
        ext_suffix = f".{ext}"
    suffixed = f"{stem}_{file_hash & 0xFFFFFFFF:08x}{ext_suffix}"
    candidate = os.path.join(output_dir, suffixed)
    if not os.path.exists(candidate):
        return candidate
    counter = 2
    while True:
        numbered = f"{stem}_{file_hash & 0xFFFFFFFF:08x}_{counter}{ext_suffix}"
        candidate = os.path.join(output_dir, numbered)
        if not os.path.exists(candidate):
            return candidate
        counter += 1


class FileWriter:
    # verifies a completed file against its file_hash and writes it to
    # disk under its original name (sanitized), falling back to the
    # legacy "{file_hash}.bin" when no usable name was transmitted.

    # called from file_tracker.py's on_file_complete — same thread that
    # finished assembling that file.

    def __init__(self, output_dir=None, on_file_written=None, on_file_corrupted=None):
        self._output_dir = output_dir
        self._on_file_written = on_file_written
        self._on_file_corrupted = on_file_corrupted
        os.makedirs(self._output_dir, exist_ok=True)

    def handle_file_complete(self, file_hash, total_blocks, file_bytes, contributing_receivers,
                             file_name="", file_ext=""):
        if not verify_file(file_hash, file_bytes):
            print(f"[FileWriter] INTEGRITY FAILURE: file_hash={file_hash} — "
                  f"reconstructed bytes do not match the source hash")
            if self._on_file_corrupted:
                self._on_file_corrupted(file_hash=file_hash, file_bytes=file_bytes)
            return

        basename = sanitize_basename(file_name, file_ext, file_hash)
        output_path = _dedupe_path(self._output_dir, basename, file_hash)
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
                                   size=len(file_bytes), contributing_receivers=contributing_receivers,
                                   file_name=file_name, file_ext=file_ext)