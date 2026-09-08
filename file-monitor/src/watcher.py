import os
import threading
import queue
from inotify_simple import INotify, flags

# Assuming file_processor.py is in the same or src directory
from file_processor import process_file 

class FileWatcher(threading.Thread):
    def __init__(self, watch_dir: str, task_queue: queue.Queue):
        # daemon=True ensures this thread closes when main.py exits
        super().__init__(daemon=True) 
        self.watch_dir = watch_dir
        self.task_queue = task_queue
        
        if not os.path.isdir(self.watch_dir):
            os.makedirs(self.watch_dir, exist_ok=True)

    def run(self):
        inotify = INotify()

        # create the file descriptor watcher
        watch_flags = flags.CLOSE_WRITE | flags.MOVED_TO
        watch_descriptor = inotify.add_watch(self.watch_dir, watch_flags)

        print(f"[Watcher] Listening for new/modified files in: {self.watch_dir}")

        try:
            while True:
                events = inotify.read()
                for event in events:
                    if event.mask & flags.ISDIR:
                        continue

                    filename = event.name
                    full_path = os.path.join(self.watch_dir, filename)

                    if os.path.isfile(full_path):
                        file_size = os.path.getsize(full_path)
                        
                        # filter out 0-byte creation artifacts
                        if file_size == 0:
                            continue
                        
                        print(f"[Watcher Detected] Ready: {full_path} ({file_size} bytes)")
                        
                        # Process hash and size
                        try:
                            metadata = process_file(full_path)
                            # Push to the thread-safe queue for the orchestrator
                            self.task_queue.put(metadata)
                            print(f"[Watcher] Queued: {event.name}")
                        except Exception as e:
                            print(f"[Watcher Error] Failed to process {event.name}: {e}")
                            
        except Exception as e:
            # Catch general thread exceptions since KeyboardInterrupt goes to the main thread
            print(f"\n[Watcher] Thread stopping: {e}")
        finally:
            inotify.rm_watch(watch_descriptor)
            inotify.close()

if __name__ == "__main__":
    import time
    
    # Dummy test block for the new class
    folder_to_watch = "./data/tx_inbox"
    test_queue = queue.Queue()
    
    watcher_thread = FileWatcher(folder_to_watch, test_queue)
    watcher_thread.start()
    
    try:
        while True:
            if not test_queue.empty():
                meta = test_queue.get()
                print(f"[Main Thread Test] Popped from queue: {meta.file_name}")
            time.sleep(1)
    except KeyboardInterrupt:
        print("Test stopped.")