SYMBOL_SIZE_BYTES = 1400  # MTU compliant payload limit
K_SOURCE_SYMBOLS = 10     # source symbols per block
M_PARITY_SYMBOLS = 2      # reed-Solomon parity symbols per block
BUFFER_CHUNK_SIZE = 64 * 1024  # 64 KB read buffer for hashing