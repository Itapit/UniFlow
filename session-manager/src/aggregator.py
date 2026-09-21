import threading
from collections import defaultdict

from src.log_setup import get_logger

log = get_logger("aggregator")


class Aggregator:
    # this aggregator purpose is to logically get rid of the reciver number and look at the packet that come with the symbol number.
    # pools shards by (file_hash, block_id), regardless of which receiver
    #reported them. Call add_packet() once per individual Packet
    #unpacked from an incoming SymbolBatch; receiver_id is metadata only,
    #it plays no part in which pool a shard lands in.

    # thread-safe: receiver_server.py calls this from one thread per
    # connected receiver (up to 3 concurrently). on_block_ready fires
    # exactly once per block, from whichever thread's packet completes it.

    # defaultdict is used to add keys when we try to access a key that is not exist

    def __init__(self, on_block_ready):
        self._on_block_ready = on_block_ready
        self._lock = threading.Lock() # mutual exclusion
        self._pending_shards = defaultdict(dict)          # (file_hash, block_id) -> {symbol_id: content}
        self._pending_meta = {}                            # (file_hash, block_id) -> k/n/file_size/total_blocks
        self._contributing_receivers = defaultdict(set)    # (file_hash, block_id) -> {receiver_id, ...}
        self._completed = set()                             # blocks already handed off — further shards are dropped

    def add_packet(self, receiver_id: int, packet) -> None:
        key = (packet.file_hash, packet.block_id)

        with self._lock:
            if key in self._completed:
                log.debug("event=post_complete_shard file_hash=%s block_id=%s symbol_id=%s receiver_id=%s",
                          packet.file_hash, packet.block_id, packet.symbol_id, receiver_id)
                return  # already reconstructed

            shards = self._pending_shards[key]
            if packet.symbol_id not in shards:
                shards[packet.symbol_id] = packet.content
                self._pending_meta[key] = {
                    "k_symbols": packet.k_symbols,
                    "n_symbols": packet.n_symbols,
                    "file_size": packet.file_size,
                    "total_blocks": packet.total_blocks,
                }
                log.debug("event=shard_added file_hash=%s block_id=%s symbol_id=%s pooled=%d/%d receivers=%s",
                          packet.file_hash, packet.block_id, packet.symbol_id,
                          len(shards), packet.k_symbols, sorted(self._contributing_receivers[key] | {receiver_id}))
            else:
                log.debug("event=duplicate_shard file_hash=%s block_id=%s symbol_id=%s receiver_id=%s",
                          packet.file_hash, packet.block_id, packet.symbol_id, receiver_id)
            self._contributing_receivers[key].add(receiver_id)

            if len(shards) < packet.k_symbols:
                return

            # Threshold reached — hand off and stop tracking this block.
            meta = self._pending_meta.pop(key)
            del self._pending_shards[key]
            receivers = self._contributing_receivers.pop(key)
            self._completed.add(key)

        # Call outside the lock so a slow reconstruction downstream
        # (block_assembler -> rs_client) doesn't block other receivers'
        # threads from making progress on unrelated blocks.
        file_hash, block_id = key
        log.info("event=block_ready file_hash=%s block_id=%s shards=%d k=%d receivers=%s",
                 file_hash, block_id, len(shards), meta["k_symbols"], sorted(receivers))
        self._on_block_ready(
            file_hash=file_hash,
            block_id=block_id,
            k_symbols=meta["k_symbols"],
            n_symbols=meta["n_symbols"],
            file_size=meta["file_size"],
            total_blocks=meta["total_blocks"],
            shards=shards,
            contributing_receivers=receivers,
        )
