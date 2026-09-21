import hashlib

from src.integrity import compute_reference_hash, verify_file


def test_matches_file_processors_algorithm_exactly():
    # Mirrors file_processor.py's compute_file_hash byte-for-byte —
    # this breaks loudly if the two implementations ever drift apart.
    file_bytes = b"some file content for testing"
    expected = int.from_bytes(hashlib.sha256(file_bytes).digest()[:8], byteorder="big", signed=False)
    assert compute_reference_hash(file_bytes) == expected


def test_verify_file_true_for_matching_content():
    file_bytes = b"identical content"
    assert verify_file(compute_reference_hash(file_bytes), file_bytes) is True


def test_verify_file_false_for_corrupted_content():
    reference = compute_reference_hash(b"identical content")
    assert verify_file(reference, b"identicalXcontent") is False


if __name__ == "__main__":
    test_matches_file_processors_algorithm_exactly()
    print("test_matches_file_processors_algorithm_exactly: OK")
    test_verify_file_true_for_matching_content()
    print("test_verify_file_true_for_matching_content: OK")
    test_verify_file_false_for_corrupted_content()
    print("test_verify_file_false_for_corrupted_content: OK")