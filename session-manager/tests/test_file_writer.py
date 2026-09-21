import os
import tempfile

from src.file_writer import FileWriter
from src.integrity import compute_reference_hash


def test_writes_verified_file_named_by_hash():
    file_bytes = b"correctly reconstructed content"
    file_hash = compute_reference_hash(file_bytes)

    with tempfile.TemporaryDirectory() as output_dir:
        written = []
        writer = FileWriter(output_dir, on_file_written=lambda **kw: written.append(kw))

        writer.handle_file_complete(file_hash=file_hash, total_blocks=1,
                                     file_bytes=file_bytes, contributing_receivers={1, 2})

        expected_path = os.path.join(output_dir, f"{file_hash}.bin")
        assert os.path.exists(expected_path)
        with open(expected_path, "rb") as f:
            assert f.read() == file_bytes

        assert len(written) == 1
        assert written[0]["output_path"] == expected_path
        assert written[0]["size"] == len(file_bytes)


def test_corrupted_file_is_not_written():
    file_bytes = b"content that got corrupted somewhere along the way"
    wrong_hash = compute_reference_hash(b"different content entirely")

    with tempfile.TemporaryDirectory() as output_dir:
        written, corrupted = [], []
        writer = FileWriter(output_dir,
                             on_file_written=lambda **kw: written.append(kw),
                             on_file_corrupted=lambda **kw: corrupted.append(kw))

        writer.handle_file_complete(file_hash=wrong_hash, total_blocks=1,
                                     file_bytes=file_bytes, contributing_receivers={1})

        assert written == []
        assert len(corrupted) == 1
        assert os.listdir(output_dir) == []  # nothing written at all


def test_creates_output_dir_if_missing():
    file_bytes = b"content"
    file_hash = compute_reference_hash(file_bytes)

    with tempfile.TemporaryDirectory() as parent:
        nested_dir = os.path.join(parent, "does", "not", "exist", "yet")
        writer = FileWriter(nested_dir)
        writer.handle_file_complete(file_hash=file_hash, total_blocks=1,
                                     file_bytes=file_bytes, contributing_receivers={1})
        assert os.path.exists(os.path.join(nested_dir, f"{file_hash}.bin"))


if __name__ == "__main__":
    test_writes_verified_file_named_by_hash()
    print("test_writes_verified_file_named_by_hash: OK")
    test_corrupted_file_is_not_written()
    print("test_corrupted_file_is_not_written: OK")
    test_creates_output_dir_if_missing()
    print("test_creates_output_dir_if_missing: OK")