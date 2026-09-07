import hashlib
import math
import os
from dataclasses import dataclass
from config import SYMBOL_SIZE_BYTES, K_SOURCE_SYMBOLS, M_PARITY_SYMBOLS, BUFFER_CHUNK_SIZE


# data class is a python object dedicted for storing data without all the init functions (has special functionalities like comperision).
@dataclass
class FileMetadata:
    file_path: str
    file_name: str
    file_size: int
    file_hash: int          # 64-bit integer digest for Protobuf
    total_blocks: int
    k_symbols: int
    n_symbols: int
    symbol_size: int

# The purpose of the function is to read 8 bits of the file update the state of the hash and then take the next 8 bits
def compute_file_hash(file_path: str) -> int:
    hasher = hashlib.sha256()
    
    # use OS file descriptor
    file_descriptor = os.open(file_path, os.O_RDONLY)
    try:
        while True:
            chunk = os.read(file_descriptor, BUFFER_CHUNK_SIZE)
            if not chunk:
                break
            hasher.update(chunk)
    finally:
        os.close(file_descriptor)

    # take first 8 bytes of the digest to fit protobuf uint64
    digest_bytes = hasher.digest()[:8]
    return int.from_bytes(digest_bytes, byteorder="big", signed=False)

def process_file(file_path: str) -> FileMetadata:
    """Inspects file parameters and calculates transmission dimensions."""
    file_size = os.path.getsize(file_path)
    file_name = os.path.basename(file_path)
    
    
    file_hash = compute_file_hash(file_path)
    
    # block dimension math:
    # block contains (K * SYMBOL_SIZE) data bytes
    bytes_per_block = K_SOURCE_SYMBOLS * SYMBOL_SIZE_BYTES
    total_blocks = math.ceil(file_size / bytes_per_block) if file_size > 0 else 1
    
    n_symbols = K_SOURCE_SYMBOLS + M_PARITY_SYMBOLS

    return FileMetadata(
        file_path=file_path,
        file_name=file_name,
        file_size=file_size,
        file_hash=file_hash,
        total_blocks=total_blocks,
        k_symbols=K_SOURCE_SYMBOLS,
        n_symbols=n_symbols,
        symbol_size=SYMBOL_SIZE_BYTES
    )

if __name__ == "__main__":
    import sys
    test_path =  "test.txt"
    if not os.path.exists(test_path):
        with open(test_path, "wb") as f:
            f.write(b"UniFlow test content" * 100)
            
    meta = process_file(test_path)
    print(f"File: {meta.file_name}")
    print(f"Size: {meta.file_size} bytes")
    print(f"Hash (uint64): {meta.file_hash}")
    print(f"Total Blocks: {meta.total_blocks}")