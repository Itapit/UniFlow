import os
import sys
from inotify_simple import INotify, flags

def watch_directory(target_dir: str):
    # ensure the data directory exists
    target_path = os.path.abspath(target_dir)
    if not os.path.isdir(target_path):
        os.makedirs(target_path, exist_ok=True)

    inotify = INotify()

    #creating the file descriptor watcher that triggered when the folder is attched to is changed by the flags below:
    watch_flags = flags.CLOSE_WRITE | flags.MOVED_TO
    watch_descriptor = inotify.add_watch(target_path, watch_flags)

    print(f"[File Monitor] Listening for new/modified files in: {target_path}")

    try:
        while True:
            events = inotify.read()
            for event in events:
                if event.mask & flags.ISDIR:
                    continue

                filename = event.name
                
                full_path = os.path.join(target_path, filename)

                if os.path.isfile(full_path):
                    file_size = os.path.getsize(full_path)
                    print(f"[Event Detected] Ready: {full_path} ({file_size} bytes)")
                    
                    # Next step in pipeline: hand the full path to the senders.
                    #TODO: inside the tx folder in creation mode event of a file with 0 bytes is sent need to check special condition.

    except KeyboardInterrupt:
        print("\n[File Monitor] Stopping watcher...")
    finally:
        inotify.rm_watch(watch_descriptor)
        inotify.close()

if __name__ == "__main__":
    folder_to_watch = "./data/tx_inbox"
    watch_directory(folder_to_watch)