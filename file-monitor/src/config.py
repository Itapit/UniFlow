SYMBOL_SIZE_BYTES = 1400  # MTU compliant payload limit
K_SOURCE_SYMBOLS = 10     # source symbols per block
M_PARITY_SYMBOLS = 2      # reed-Solomon parity symbols per block
BUFFER_CHUNK_SIZE = 64 * 1024  # 64 KB read buffer for hashing

# thresholds (in bytes)
TEN_MB_BYTES = 10 * 1024 * 1024
ONE_GB_BYTES = 1024 * 1024 * 1024

# unix domain socket endpoints for the 3 Senders (this is an example for thier adresses)
SENDER_SOCKETS = {
    1: "/tmp/uniflow_sender_1.sock",
    2: "/tmp/uniflow_sender_2.sock",
    3: "/tmp/uniflow_sender_3.sock"
}