import subprocess
import time
import socket
from src.config import TARGET_IP, SENDERS_CONFIG

def wait_for_sockets(socket_paths, timeout=5.0):
    # checking the sockets and polls them untill they active or timeout is reached
    start_time = time.time()
    pending_sockets = list(socket_paths)

    while pending_sockets and (time.time() - start_time < timeout):
        sockets_to_check = pending_sockets.copy()
        for sock_path in sockets_to_check:
            try:
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
                    s.connect(sock_path)
                pending_sockets.remove(sock_path)
            except (FileNotFoundError, ConnectionRefusedError):
                pass 
        
        if pending_sockets:
            time.sleep(0.05) 

    if pending_sockets:
        raise TimeoutError(f"Senders failed to boot within {timeout}s. Unready sockets: {pending_sockets}")

def boot_senders():
    processes = []
    socket_paths = []
    
    print("[Orchestrator] Booting Go Senders...")
    for sender_id, params in SENDERS_CONFIG.items():
        cmd = [
            "./bin/sender.exe",
            "-ip", TARGET_IP,
            "-port", str(params["port"]),
            "-sock", params["socket_path"]
        ]
        
        proc = subprocess.Popen(cmd)
        processes.append(proc)
        socket_paths.append(params["socket_path"])
        print(f"Booted Sender {sender_id} (Port: {params['port']}, Sock: {params['socket_path']})")
        
    print("[Orchestrator] Waiting for sockets to become ready...")
    wait_for_sockets(socket_paths, timeout=3.0)
    print("[Orchestrator] All senders are fully booted and listening!")
    
    return processes