RECEIVERS_CONFIG = {
    1: {"listen": "127.0.0.1:1400"},
    2: {"listen": "127.0.0.1:1401"},
    3: {"listen": "127.0.0.1:1402"},
}

SESSION_SOCKET_PATH = "/tmp/uniflow_session.sock"
RS_HELPER_SOCKET_PATH = "/tmp/uniflow_rs_helper.sock"

RECEIVER_BINARY = "./bin/receiver.exe"
RS_HELPER_BINARY = "./bin/rs_helper.exe"

BLOCK_SIZE = 134400

REPO_ROOT = Path(__file__).resolve().parent.parent.parent

# Central inbox folder
OUTPUT_DIR = str(REPO_ROOT / "data" / "rx_inbox")
