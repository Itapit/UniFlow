import subprocess
import time

from src.config import (
    RECEIVERS_CONFIG,
    SESSION_SOCKET_PATH,
    RS_HELPER_SOCKET_PATH,
    RECEIVER_BINARY,
    RS_HELPER_BINARY,
)


def boot_rs_helper():
    print("[System] Booting rs_helper")
    proc = subprocess.Popen([RS_HELPER_BINARY, "-sock", RS_HELPER_SOCKET_PATH])
    time.sleep(0.5)  # give it time to bind before anything tries to dial it
    return proc


def boot_receivers():
    processes = []
    print("[System] Booting Go Receivers...")
    for receiver_id, params in RECEIVERS_CONFIG.items():
        cmd = [
            RECEIVER_BINARY,
            "-listen", params["listen"],
            "-receiver-id", str(receiver_id),
            "-session-socket", SESSION_SOCKET_PATH,
        ]
        proc = subprocess.Popen(cmd)
        processes.append(proc)
        print(f"  Booted Receiver {receiver_id} (listen={params['listen']})")
    time.sleep(1)
    return processes


if __name__ == "__main__":
    print("UniFlow Session Manager Starting")

    rs_helper_proc = boot_rs_helper()
    receiver_processes = boot_receivers()

    # receiver_server.py — accepting these 3 connections and parsing
    # incoming SymbolBatch messages — plugs in right here once it exists.
    # For now this just proves all four subprocesses start and stay up.
    print("[System] All subprocesses booted. Press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[System] Interrupted by user.")
    finally:
        print("[System] Terminating subprocesses")
        for p in receiver_processes:
            p.terminate()
            p.wait()
        rs_helper_proc.terminate()
        rs_helper_proc.wait()
        print("UniFlow Session Manager Offline")