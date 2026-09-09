import time
import queue
import subprocess
import sys
from src.config import TARGET_IP, SENDERS_CONFIG

from src.watcher import FileWatcher
from src.ipc_manager import IPCManager
from src.orchestrator import Orchestrator

def boot_senders():
    processes = []
    print("[System] Booting Go Senders...")
    
    for sender_id, params in SENDERS_CONFIG.items():
        # Command mapped to the Go flags we agreed upon
        cmd = [
            "./bin/sender.exe",
            #sys.executable, "mock_sender.py", 
            "-ip", TARGET_IP,
            "-port", str(params["port"]),
            "-sock", params["socket_path"]
        ]
        
        # runs the Go binary in the background
        proc = subprocess.Popen(cmd)
        processes.append(proc)
        print(f" Booted Sender (Port: {params['port']}, Sock: {params['socket_path']})")
        
    # give the OS 1 second to initialize the Unix Domain Socket files
    time.sleep(1) 
    return processes

if __name__ == "__main__":
    print(" UniFlow File Monitor Starting ")
    
    # initialize Shared Resources
    task_queue = queue.Queue()
    watch_folder = "./data/tx_inbox"
    
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
        print("\n[System] Interrupted by user. ")
    finally:
        print("[System] Terminating Go Sender processes")
        for p in sender_processes:
            p.terminate()
            p.wait() # Ensure they fully close
        print(" UniFlow Offline ")