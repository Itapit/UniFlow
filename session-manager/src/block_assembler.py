import queue
import threading
import time

from src.rs_client import RSClient, ReconstructionError
from src.config import RS_HELPER_SOCKET_PATH, BLOCK_SIZE
from src.log_setup import get_logger

log = get_logger("block_assembler")


class BlockAssembler:
    # turns one completed shard pool (from Aggregator.on_block_ready)
    # into real reconstructed bytes via rs_helper, then trims padding.
    #
    # Production path is decoupled: Aggregator calls submit_block_ready()
    # (fast enqueue, never blocks on RS decode), and a pool of background
    # workers drains the queue with one persistent RSClient each.
    # This keeps receiver_server connection threads (and therefore the
    # Go receivers' Unix-socket WriteTo + UDP intake queue) from stalling
    # behind 8-17ms synchronous decodes during large-file bursts.
    #
    # handle_block_ready() is kept synchronous for tests / single-shot use.

    def __init__(self, on_block_assembled, rs_helper_socket_path=RS_HELPER_SOCKET_PATH,
                 client_factory=RSClient, num_workers=4, max_queue=1024,
                 start_workers=True):
        self._on_block_assembled = on_block_assembled
        self._rs_helper_socket_path = rs_helper_socket_path
        self._client_factory = client_factory  # swappable in tests
        self._num_workers = max(1, num_workers)
        self._queue: queue.Queue = queue.Queue(maxsize=max_queue)
        self._workers: list[threading.Thread] = []
        self._clients: list = []
        self._stop_event = threading.Event()
        self._started = False
        if start_workers:
            self.start()

    def start(self):
        if self._started:
            return
        self._started = True
        self._stop_event.clear()
        for i in range(self._num_workers):
            client = self._client_factory(self._rs_helper_socket_path)
            self._clients.append(client)
            t = threading.Thread(
                target=self._worker_loop, args=(client,),
                name=f"block-assembler-{i}", daemon=True)
            t.start()
            self._workers.append(t)
        log.info("event=assembler_started workers=%d max_queue=%d",
                 self._num_workers, self._queue.maxsize)

    def stop(self, timeout=5.0):
        self._stop_event.set()
        for t in self._workers:
            t.join(timeout=timeout / max(1, len(self._workers)))
        for client in self._clients:
            try:
                client.close()
            except Exception:
                pass
        log.info("event=assembler_stopped pending=%d", self._queue.qsize())

    def pending(self) -> int:
        return self._queue.qsize()

    def submit_block_ready(self, file_hash, block_id, k_symbols, n_symbols,
                           file_size, total_blocks, shards, contributing_receivers):
        """Fast enqueue path for Aggregator — must never do RS I/O."""
        if not self._started:
            # Fall back to synchronous decode (tests / standalone use).
            self.handle_block_ready(
                file_hash=file_hash, block_id=block_id,
                k_symbols=k_symbols, n_symbols=n_symbols,
                file_size=file_size, total_blocks=total_blocks,
                shards=shards, contributing_receivers=contributing_receivers)
            return
        try:
            self._queue.put_nowait({
                "file_hash": file_hash, "block_id": block_id,
                "k_symbols": k_symbols, "n_symbols": n_symbols,
                "file_size": file_size, "total_blocks": total_blocks,
                "shards": shards,
                "contributing_receivers": set(contributing_receivers),
            })
        except queue.Full:
            log.warning("event=assembler_queue_full file_hash=%s block_id=%s pending=%d",
                        file_hash, block_id, self._queue.qsize())
            # Bounded backpressure: block briefly rather than drop —
            # still far cheaper than stalling on a full RS decode.
            self._queue.put({
                "file_hash": file_hash, "block_id": block_id,
                "k_symbols": k_symbols, "n_symbols": n_symbols,
                "file_size": file_size, "total_blocks": total_blocks,
                "shards": shards,
                "contributing_receivers": set(contributing_receivers),
            })

    def _worker_loop(self, client):
        try:
            client.connect()
        except OSError as e:
            log.error("event=rs_connect_failed socket_path=%s err=%s",
                      self._rs_helper_socket_path, e)
            # Worker still runs: reconstruct() will retry via ensure_connected.
        while not self._stop_event.is_set():
            try:
                job = self._queue.get(timeout=0.2)
            except queue.Empty:
                continue
            try:
                self._decode_with_client(client, **job)
            finally:
                self._queue.task_done()

    def _decode_with_client(self, client, file_hash, block_id, k_symbols,
                            n_symbols, file_size, total_blocks, shards,
                            contributing_receivers):
        start = time.monotonic()
        try:
            data_shards = client.reconstruct(
                file_hash=file_hash, block_id=block_id,
                k_symbols=k_symbols, n_symbols=n_symbols, shards=shards,
            )
        except ReconstructionError as e:
            log.error("event=reconstruct_failed file_hash=%s block_id=%s shards_rx=%d k=%d duration_ms=%d err=%s",
                      file_hash, block_id, len(shards), k_symbols,
                      int((time.monotonic() - start) * 1000), e)
            return
        except (ConnectionError, OSError) as e:
            log.error("event=rs_io_failed file_hash=%s block_id=%s err=%s",
                      file_hash, block_id, e)
            return
        self._emit_assembled(
            data_shards, file_hash, block_id, file_size, total_blocks,
            len(shards), contributing_receivers, start)

    def handle_block_ready(self, file_hash, block_id, k_symbols, n_symbols,
                            file_size, total_blocks, shards, contributing_receivers):
        """Synchronous decode (tests + fallback when workers not started)."""
        start = time.monotonic()
        client = self._client_factory(self._rs_helper_socket_path)
        try:
            client.connect()
        except OSError as e:
            log.error("event=rs_connect_failed file_hash=%s block_id=%s socket_path=%s err=%s",
                      file_hash, block_id, self._rs_helper_socket_path, e)
            return
        try:
            data_shards = client.reconstruct(
                file_hash=file_hash, block_id=block_id,
                k_symbols=k_symbols, n_symbols=n_symbols, shards=shards,
            )
        except ReconstructionError as e:
            log.error("event=reconstruct_failed file_hash=%s block_id=%s shards_rx=%d k=%d duration_ms=%d err=%s",
                      file_hash, block_id, len(shards), k_symbols,
                      int((time.monotonic() - start) * 1000), e)
            return
        except (ConnectionError, OSError) as e:
            log.error("event=rs_io_failed file_hash=%s block_id=%s err=%s",
                      file_hash, block_id, e)
            return
        finally:
            try:
                client.close()
            except Exception:
                pass

        self._emit_assembled(
            data_shards, file_hash, block_id, file_size, total_blocks,
            len(shards), contributing_receivers, start)

    def _emit_assembled(self, data_shards, file_hash, block_id, file_size,
                        total_blocks, shards_rx, contributing_receivers, start):
        block_bytes = b"".join(data_shards)

        # only the LAST block of a file can be shorter than BLOCK_SIZE —
        # fec.go zero-pads every other block to exactly BLOCK_SIZE bytes.
        if block_id == total_blocks - 1:
            real_bytes = file_size - (total_blocks - 1) * BLOCK_SIZE
            block_bytes = block_bytes[:real_bytes]

        duration_ms = int((time.monotonic() - start) * 1000)
        log.info("event=block_ok file_hash=%s block_id=%s bytes=%d shards_rx=%d duration_ms=%d receivers=%s",
                 file_hash, block_id, len(block_bytes), shards_rx,
                 duration_ms, sorted(contributing_receivers))

        self._on_block_assembled(
            file_hash=file_hash, block_id=block_id, total_blocks=total_blocks,
            block_bytes=block_bytes, contributing_receivers=contributing_receivers,
        )
