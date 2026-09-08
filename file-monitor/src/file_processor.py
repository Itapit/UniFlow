import os
import hashlib
from dataclasses import dataclass

# 64 KB buffer ensures we don't blow up RAM when hashing 1GB files
BUFFER_CHUNK_SIZE = 64 * 1024  

# build in python class to save values it has build in comperators etc.
@dataclass
class FileMetadata:
    file_path: str
    file_name: str
    file_size: int
    file_hash: int

def compute_file_hash(file_path: str) -> int:
    # computes a 64-bit from SHA256 hash for Protobuf compatibility.
    hasher = hashlib.sha256()
    
    # Using OS-level file descriptors is generally faster for raw byte processing
    file_descriptor = os.open(file_path, os.O_RDONLY)
    try:
        while True:
            chunk = os.read(file_descriptor, BUFFER_CHUNK_SIZE)
            if not chunk:
                break
            hasher.update(chunk)
    finally:
        os.close(file_descriptor)

    # Slice the first 8 bytes of the digest and convert to a standard unsigned integer
    digest_bytes = hasher.digest()[:8]
    return int.from_bytes(digest_bytes, byteorder="big", signed=False)

def process_file(file_path: str) -> FileMetadata:
    # extracts required metadata and computes the file hash.
    file_size = os.path.getsize(file_path)
    file_name = os.path.basename(file_path)
    file_hash = compute_file_hash(file_path)

    return FileMetadata(
        file_path=file_path,
        file_name=file_name,
        file_size=file_size,
        file_hash=file_hash
    )

if __name__ == "__main__":
    # Quick standalone test
    test_file = "dummy_test.bin"
    with open(test_file, "wb") as f:
        f.write(os.urandom(1024 * 1024)) # Write 1MB of random data
        
    meta = process_file(test_file)
    print(f"File: {meta.file_name}")
    print(f"Size: {meta.file_size} bytes")
    print(f"Hash (uint64): {meta.file_hash}")
    
    os.remove(test_file)