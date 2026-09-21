import hashlib


def compute_reference_hash(file_bytes: bytes) -> int:
    
    digest_bytes = hashlib.sha256(file_bytes).digest()[:8]
    return int.from_bytes(digest_bytes, byteorder="big", signed=False)


def verify_file(file_hash: int, file_bytes: bytes) -> bool:
    return compute_reference_hash(file_bytes) == file_hash