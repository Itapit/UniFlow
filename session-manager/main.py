import subprocess
import time

from src.config import (
    RECEIVERS_CONFIG,
    SESSION_SOCKET_PATH,
    RS_HELPER_SOCKET_PATH,
    RECEIVER_BINARY,
    RS_HELPER_BINARY,
    OUTPUT_DIR,
)
from src.log_setup import get_logger
from src.receiver_server import ReceiverServer
from src.batch_handler import BatchHandler
from src.aggregator import Aggregator
from src.block_assembler import BlockAssembler
from src.file_tracker import FileTracker
from src.file_writer import FileWriter

log = get_logger("main")


def boot_rs_helper():
    log.info("event=booting_rs_helper binary=%s socket_path=%s", RS_HELPER_BINARY, RS_HELPER_SOCKET_PATH)
    proc = subprocess.Popen([RS_HELPER_BINARY, "-sock", RS_HELPER_SOCKET_PATH])
    time.sleep(0.5)
    return proc


def boot_receivers():
    processes = []
    log.info("event=booting_receivers count=%d", len(RECEIVERS_CONFIG))
    for receiver_id, params in RECEIVERS_CONFIG.items():
        cmd = [
            RECEIVER_BINARY,
            "-listen", params["listen"],
            "-receiver-id", str(receiver_id),
            "-session-socket", SESSION_SOCKET_PATH,
        ]
        proc = subprocess.Popen(cmd)
        processes.append(proc)
        log.info("event=receiver_booted receiver_id=%s listen=%s pid=%s",
                 receiver_id, params['listen'], proc.pid)
    time.sleep(1)
    return processes


if __name__ == "__main__":
    log.info("event=starting output_dir=%s session_socket=%s", OUTPUT_DIR, SESSION_SOCKET_PATH)

    # Wired bottom-up: each stage's constructor takes the next stage's
    # handler as its callback, so construction order runs opposite to
    # the direction data actually flows once everything is running.
    file_writer = FileWriter(OUTPUT_DIR)
    file_tracker = FileTracker(on_file_complete=file_writer.handle_file_complete)
    block_assembler = BlockAssembler(
        on_block_assembled=file_tracker.handle_block_assembled,
        rs_helper_socket_path=RS_HELPER_SOCKET_PATH,
    )
    aggregator = Aggregator(on_block_ready=block_assembler.handle_block_ready)
    batch_handler = BatchHandler(aggregator)
    receiver_server = ReceiverServer(SESSION_SOCKET_PATH, on_batch=batch_handler.handle_batch)

    rs_helper_proc = boot_rs_helper()
    receiver_server.start()
    receiver_processes = boot_receivers()

    log.info("event=pipeline_ready")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("event=interrupted")
    finally:
        log.info("event=terminating count=%d", len(receiver_processes))
        for p in receiver_processes:
            p.terminate()
            p.wait()
        rs_helper_proc.terminate()
        rs_helper_proc.wait()
        receiver_server.stop()
        log.info("event=offline")
