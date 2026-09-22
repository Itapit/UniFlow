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
    test_writes_file_under_original_name()
    print("test_writes_file_under_original_name: OK")
    test_sanitizes_traversal_in_name()
    print("test_sanitizes_traversal_in_name: OK")
    test_dedupes_colliding_names()
    print("test_dedupes_colliding_names: OK")
    test_empty_name_falls_back_to_hash()
    print("test_empty_name_falls_back_to_hash: OK")


def test_writes_file_under_original_name():
    file_bytes = b"some pdf content"
    file_hash = compute_reference_hash(file_bytes)

    with tempfile.TemporaryDirectory() as output_dir:
        written = []
        writer = FileWriter(output_dir, on_file_written=lambda **kw: written.append(kw))

        writer.handle_file_complete(file_hash=file_hash, total_blocks=1,
                                     file_bytes=file_bytes, contributing_receivers={1},
                                     file_name="report", file_ext=".pdf")

        expected_path = os.path.join(output_dir, "report.pdf")
        assert os.path.exists(expected_path)
        with open(expected_path, "rb") as f:
            assert f.read() == file_bytes
        assert written[0]["output_path"] == expected_path


def test_sanitizes_traversal_in_name():
    file_bytes = b"evil content"
    file_hash = compute_reference_hash(file_bytes)

    with tempfile.TemporaryDirectory() as output_dir:
        writer = FileWriter(output_dir)
        writer.handle_file_complete(file_hash=file_hash, total_blocks=1,
                                     file_bytes=file_bytes, contributing_receivers={1},
                                     file_name="../../etc/passwd", file_ext="")

        assert os.path.exists(os.path.join(output_dir, "passwd"))
        # Nothing escaped the output dir: only the sanitized file is there.
        assert os.listdir(output_dir) == ["passwd"]


def test_dedupes_colliding_names():
    first_bytes = b"first file"
    second_bytes = b"second file"
    first_hash = compute_reference_hash(first_bytes)
    second_hash = compute_reference_hash(second_bytes)

    with tempfile.TemporaryDirectory() as output_dir:
        writer = FileWriter(output_dir)
        writer.handle_file_complete(file_hash=first_hash, total_blocks=1,
                                     file_bytes=first_bytes, contributing_receivers={1},
                                     file_name="report", file_ext=".pdf")
        writer.handle_file_complete(file_hash=second_hash, total_blocks=1,
                                     file_bytes=second_bytes, contributing_receivers={1},
                                     file_name="report", file_ext=".pdf")

        assert os.path.exists(os.path.join(output_dir, "report.pdf"))
        suffixed = os.path.join(output_dir, f"report_{second_hash & 0xFFFFFFFF:08x}.pdf")
        assert os.path.exists(suffixed)
        with open(suffixed, "rb") as f:
            assert f.read() == second_bytes


def test_empty_name_falls_back_to_hash():
    file_bytes = b"nameless content"
    file_hash = compute_reference_hash(file_bytes)

    with tempfile.TemporaryDirectory() as output_dir:
        writer = FileWriter(output_dir)
        writer.handle_file_complete(file_hash=file_hash, total_blocks=1,
                                     file_bytes=file_bytes, contributing_receivers={1},
                                     file_name="", file_ext="")

        assert os.path.exists(os.path.join(output_dir, f"{file_hash}.bin"))