import time

from src.rs_client import RSClient, ReconstructionError
from src.config import RS_HELPER_SOCKET_PATH, BLOCK_SIZE
from src.log_setup import get_logger

log = get_logger("block_assembler")


class BlockAssembler:
    # turns one completed shard pool (from Aggregator.on_block_ready)
    #into real reconstructed bytes via rs_helper, then trims padding.

    #opens a fresh RSClient connection per call rather than sharing one —
    #on_block_ready can fire this from any of the (up to 3) receiver
    #threads concurrently.

    def __init__(self, on_block_assembled, rs_helper_socket_path=RS_HELPER_SOCKET_PATH,
                 client_factory=RSClient):
        self._on_block_assembled = on_block_assembled
        self._rs_helper_socket_path = rs_helper_socket_path
        self._client_factory = client_factory  # swappable in tests

    def handle_block_ready(self, file_hash, block_id, k_symbols, n_symbols,
                            file_size, total_blocks, shards, contributing_receivers):
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

        block_bytes = b"".join(data_shards)

        # only the LAST block of a file can be shorter than BLOCK_SIZE —
        # fec.go zero-pads every other block to exactly BLOCK_SIZE bytes.
        if block_id == total_blocks - 1:
            real_bytes = file_size - (total_blocks - 1) * BLOCK_SIZE
            block_bytes = block_bytes[:real_bytes]

        duration_ms = int((time.monotonic() - start) * 1000)
        log.info("event=block_ok file_hash=%s block_id=%s bytes=%d shards_rx=%d duration_ms=%d receivers=%s",
                 file_hash, block_id, len(block_bytes), len(shards),
                 duration_ms, sorted(contributing_receivers))

        self._on_block_assembled(
            file_hash=file_hash, block_id=block_id, total_blocks=total_blocks,
            block_bytes=block_bytes, contributing_receivers=contributing_receivers,
        )
