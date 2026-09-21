import time
import queue
import subprocess
import sys
from src.config import TARGET_IP, SENDERS_CONFIG ,WATCH_DIR
from src.log_setup import get_logger

log = get_logger("main")

from src.watcher import FileWatcher
from src.ipc_manager import IPCManager
from src.orchestrator import Orchestrator

def boot_senders():
    processes = []
    log.info("event=booting_senders count=%d", len(SENDERS_CONFIG))

    for sender_id, params in SENDERS_CONFIG.items():
        cmd = [
            "./bin/sender.exe",
            "-target", f"{TARGET_IP}:{params['port']}",
            "-socket", params["socket_path"],
            "-sender-id", str(sender_id),
        ]
        proc = subprocess.Popen(cmd)
        processes.append(proc)
        log.info("event=sender_booted sender_id=%s target=%s:%s socket_path=%s pid=%s",
                 sender_id, TARGET_IP, params['port'], params['socket_path'], proc.pid)

    time.sleep(1)
    return processes

if __name__ == "__main__":
    log.info("event=starting watch_dir=%s", WATCH_DIR)

    # initialize Shared Resources
    task_queue = queue.Queue()
    watch_folder = WATCH_DIR

    # boot Go Executables
    sender_processes = boot_senders()

    # start the Background Watcher Thread
    watcher_thread = FileWatcher(watch_dir=watch_folder, task_queue=task_queue)
    watcher_thread.start()

    # initialize State Tracking and IPC
    # dynamically build the initial state dictionary mapping socket paths to "IDLE"
    # initialize State Tracking and IPC
    initial_states = {sender_id: "IDLE" for sender_id in SENDERS_CONFIG.keys()}
    ipc = IPCManager(sender_states=initial_states)
    ipc.connect_to_senders()


    orchestrator = Orchestrator(
        task_queue=task_queue,
        ipc_manager=ipc,
        sender_states=initial_states
    )

    try:
        orchestrator.run()
    except KeyboardInterrupt:
        log.info("event=interrupted")
    finally:
        log.info("event=terminating_senders count=%d", len(sender_processes))
        for p in sender_processes:
            p.terminate()
            p.wait() # Ensure they fully close
        log.info("event=offline")
