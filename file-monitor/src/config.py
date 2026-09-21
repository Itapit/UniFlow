from pathlib import Path

# defenition of repo root 
REPO_ROOT = Path(__file__).resolve().parent.parent.parent

# Central inbox folder
WATCH_DIR = str(REPO_ROOT / "data" / "tx_inbox")

TEN_MB_BYTES = 10 * 1024 * 1024

TARGET_IP = "127.0.0.1"  # same-machine testing; swap for the RX machine's real IP later

SENDERS_CONFIG = {
    1: {"port": 1400, "socket_path": "/tmp/uniflow_sender_1.sock"},
    2: {"port": 1401, "socket_path": "/tmp/uniflow_sender_2.sock"},
    3: {"port": 1402, "socket_path": "/tmp/uniflow_sender_3.sock"},
}

sender_states = {
    "/tmp/uniflow_sender_1.sock": "IDLE",
    "/tmp/uniflow_sender_2.sock": "IDLE",
    "/tmp/uniflow_sender_3.sock": "IDLE"
}