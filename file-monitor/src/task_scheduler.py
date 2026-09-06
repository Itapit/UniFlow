import sys
from pathlib import Path
from dataclasses import dataclass
from typing import List, Dict, Optional
from config import TEN_MB_BYTES, ONE_GB_BYTES, SENDER_SOCKETS
from file_processor import FileMetadata

@dataclass
class SenderTask:
    sender_id: int
    socket_path: str
    metadata: FileMetadata
    assigned_blocks: List[int]  # Block IDs this sender is responsible for
    is_multicast_session: bool  # True if 3 senders share the file

class TaskScheduler:
    def __init__(self):
        # Round-robin cursor for distributing small files across idle senders
        self._next_small_file_sender_idx = 0
        self._sender_ids = list(SENDER_SOCKETS.keys())

    def schedule(self, metadata: FileMetadata) -> List[SenderTask]:
        
        # determine the send strategy based on the file size
        file_size = metadata.file_size

        if file_size > ONE_GB_BYTES:
            raise ValueError(
                f"File '{metadata.file_name}' ({file_size} bytes) exceeds the 1GB system limit."
            )

        if file_size < TEN_MB_BYTES:
            return self._schedule_small_file(metadata)
        else:
            return self._schedule_large_file(metadata)

    def _schedule_small_file(self, metadata: FileMetadata) -> List[SenderTask]:
        # routes small files to a single sender using round-robin rotation
        
        sender_id = self._sender_ids[self._next_small_file_sender_idx]
        self._next_small_file_sender_idx = (self._next_small_file_sender_idx + 1) % len(self._sender_ids)

        # The single sender is responsible for all blocks
        all_blocks = list(range(metadata.total_blocks))

        task = SenderTask(
            sender_id=sender_id,
            socket_path=SENDER_SOCKETS[sender_id],
            metadata=metadata,
            assigned_blocks=all_blocks,
            is_multicast_session=False
        )
        return [task]

    def _schedule_large_file(self, metadata: FileMetadata) -> List[SenderTask]:
        # dividing the exists block across the 3 senders
        tasks = []
        total_blocks = metadata.total_blocks

        for idx, sender_id in enumerate(self._sender_ids):
            # Interleaving stride:
            # Sender 1 (idx 0) gets blocks: 0, 3, 6, ...
            # Sender 2 (idx 1) gets blocks: 1, 4, 7, ...
            # Sender 3 (idx 2) gets blocks: 2, 5, 8, ...
            num_senders = len(self._sender_ids)
            assigned_blocks = []

            for block_id in range(total_blocks):
             # Check if this block belongs to the current sender
              if block_id % num_senders == idx:
                 assigned_blocks.append(block_id)

            tasks.append(
                SenderTask(
                    sender_id=sender_id,
                    socket_path=SENDER_SOCKETS[sender_id],
                    metadata=metadata,
                    assigned_blocks=assigned_blocks,
                    is_multicast_session=True
                    )
                  )
        return tasks

if __name__ == "__main__":
    from file_processor import process_file

    # Create dummy metadata to test routing
    scheduler = TaskScheduler()

    small_meta = FileMetadata(
        file_path="/tmp/small.txt",
        file_name="small.txt",
        file_size=5 * 1024 * 1024,  # 5 MB
        file_hash=12345678,
        total_blocks=376,
        k_symbols=10,
        n_symbols=15,
        symbol_size=1400
    )

    large_meta = FileMetadata(
        file_path="/tmp/large.bin",
        file_name="large.bin",
        file_size=50 * 1024 * 1024,  # 50 MB
        file_hash=87654321,
        total_blocks=3750,
        k_symbols=10,
        n_symbols=15,
        symbol_size=1400
    )

    print("--- Testing Small File (Round Robin) ---")
    plan1 = scheduler.schedule(small_meta)
    print(f"Task assigned to Sender {plan1[0].sender_id}, Total Blocks: {len(plan1[0].assigned_blocks)}")

    plan2 = scheduler.schedule(small_meta)
    print(f"Task assigned to Sender {plan2[0].sender_id}, Total Blocks: {len(plan2[0].assigned_blocks)}")

    print("\n--- Testing Large File (Interleaved Striding) ---")
    large_plan = scheduler.schedule(large_meta)
    for task in large_plan:
        print(f"Sender {task.sender_id} allocated {len(task.assigned_blocks)} blocks "
              f"(first 3: {task.assigned_blocks[:3]})")