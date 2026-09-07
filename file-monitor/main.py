import os
import sys
import time
from pathlib import Path

# Configure paths for internal imports
BASE_DIR = Path(__file__).resolve().parent
SRC_DIR = BASE_DIR / "src"
PB_DIR = BASE_DIR / "pb"

sys.path.insert(0, str(SRC_DIR))
sys.path.insert(0, str(PB_DIR))

from config import WATCH_DIR
from file_processor import process_file
from src.task_scheduler import TaskScheduler
from ipc_client import IPCClient
from src.watcher import watch_directory

def run_monitor(watch_folder: str):
    # main pipeline orchestrator.
    # detectes new files from the dedicted folder. pass watcher generator, calculates metadata/FEC params,
    # generates sender tasks, and dispatches them via IPC.
    abs_watch_folder = os.path.abspath(watch_folder)
    print(f"=== UniFlow File Monitor Daemon Started ===")
    print(f"Watching directory: {abs_watch_folder}")

    scheduler = TaskScheduler()

    # Consuming the generator stream
    for ready_file_path in watch_directory(abs_watch_folder):
        print(f"\n[Pipeline Triggered] Working file: {ready_file_path}")

        # metadata extraction and hashing
        try:
            metadata = process_file(ready_file_path)
            print(f"[File Processor] Hash: {metadata.file_hash} , Size: {metadata.file_size} bytes , Blocks: {metadata.total_blocks}")
        except Exception as err:
            print(f"[Pipeline Error] Failed to process file {ready_file_path}: {err}")
            continue

        # scheduling and task allocation
        try:
            tasks = scheduler.schedule(metadata)
            print(f"[Scheduler] Generated {len(tasks)} tasks for '{metadata.file_name}'")
        except ValueError as err:
            print(f"[Pipeline Warning] File skipped by scheduler: {err}")
            continue

        # IPC dispatch to senders
        for task in tasks:
            success = IPCClient.send_task(task)
            if not success:
                print(f"[Pipeline Error] Delivery to Sender {task.sender_id} failed on socket {task.socket_path}")

