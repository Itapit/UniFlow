from pathlib import Path

# defenition of repo root 
REPO_ROOT = Path(__file__).resolve().parent.parent.parent

# Central inbox folder
WATCH_DIR = str(REPO_ROOT / "data" / "tx_inbox")

TEN_MB_BYTES = 10 * 1024 * 1024

TARGET_IP = "192.168.1.100"

# dictionary mapping sender IDs to their specific execution parameters
SENDERS_CONFIG = {
    1: {"port": 5001, "socket_path": "/tmp/uniflow_sender_1.sock"},
    2: {"port": 5002, "socket_path": "/tmp/uniflow_sender_2.sock"},
    3: {"port": 5003, "socket_path": "/tmp/uniflow_sender_3.sock"}
}

sender_states = {
    "/tmp/uniflow_sender_1.sock": "IDLE",
    "/tmp/uniflow_sender_2.sock": "IDLE",
    "/tmp/uniflow_sender_3.sock": "IDLE"
}