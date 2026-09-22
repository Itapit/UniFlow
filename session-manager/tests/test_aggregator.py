from pb import packet_pb2
from src.aggregator import Aggregator
import threading


def make_packet(file_hash, block_id, symbol_id, k_symbols=100, n_symbols=150,
                 file_size=100000, total_blocks=1, content=b"x",
                 file_name="report", file_ext=".pdf"):
    return packet_pb2.Packet(
        file_hash=file_hash, block_id=block_id, symbol_id=symbol_id,
        k_symbols=k_symbols, n_symbols=n_symbols,
        file_size=file_size, total_blocks=total_blocks, content=content,
        file_name=file_name, file_ext=file_ext,
    )


def test_fires_exactly_at_threshold():
    ready_calls = []
    aggregator = Aggregator(on_block_ready=lambda **kwargs: ready_calls.append(kwargs))

    for symbol_id in range(99):
        aggregator.add_packet(1, make_packet(file_hash=1, block_id=0, symbol_id=symbol_id))
    assert not ready_calls, "should not fire before k_symbols shards arrive"

    aggregator.add_packet(1, make_packet(file_hash=1, block_id=0, symbol_id=99))
    assert len(ready_calls) == 1
    assert len(ready_calls[0]["shards"]) == 100

    aggregator.add_packet(1, make_packet(file_hash=1, block_id=0, symbol_id=120))
    assert len(ready_calls) == 1, "an extra shard afterward must not retrigger it"


def test_pools_shards_across_receivers_despite_misrouting():
    ready_calls = []
    aggregator = Aggregator(on_block_ready=lambda **kwargs: ready_calls.append(kwargs))

    for symbol_id in range(100):
        receiver_id = (symbol_id % 3) + 1  # deliberately spread across all 3 receivers
        aggregator.add_packet(receiver_id, make_packet(file_hash=42, block_id=3, symbol_id=symbol_id))

    assert len(ready_calls) == 1
    call = ready_calls[0]
    assert call["file_hash"] == 42 and call["block_id"] == 3
    assert call["contributing_receivers"] == {1, 2, 3}
    assert set(call["shards"].keys()) == set(range(100))


def test_duplicate_symbol_does_not_overwrite_first_content():
    ready_calls = []
    aggregator = Aggregator(on_block_ready=lambda **kwargs: ready_calls.append(kwargs))

    aggregator.add_packet(1, make_packet(file_hash=1, block_id=0, symbol_id=0, content=b"first"))
    aggregator.add_packet(2, make_packet(file_hash=1, block_id=0, symbol_id=0, content=b"second"))
    for symbol_id in range(1, 100):
        aggregator.add_packet(1, make_packet(file_hash=1, block_id=0, symbol_id=symbol_id))

    assert ready_calls[0]["shards"][0] == b"first"


def test_thread_safety_fires_exactly_once_under_concurrency():
    ready_calls = []
    append_lock = threading.Lock()

    def on_ready(**kwargs):
        with append_lock:
            ready_calls.append(kwargs)

    aggregator = Aggregator(on_block_ready=on_ready)

    def feed(receiver_id, symbol_ids):
        for symbol_id in symbol_ids:
            aggregator.add_packet(receiver_id, make_packet(file_hash=7, block_id=0, symbol_id=symbol_id))

    threads = [
        threading.Thread(target=feed, args=(1, range(0, 40))),
        threading.Thread(target=feed, args=(2, range(40, 70))),
        threading.Thread(target=feed, args=(3, range(70, 100))),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5)

    assert len(ready_calls) == 1, f"expected exactly one callback, got {len(ready_calls)}"


if __name__ == "__main__":
    test_fires_exactly_at_threshold()
    print("test_fires_exactly_at_threshold: OK")
    test_pools_shards_across_receivers_despite_misrouting()
    print("test_pools_shards_across_receivers_despite_misrouting: OK")
    test_duplicate_symbol_does_not_overwrite_first_content()
    print("test_duplicate_symbol_does_not_overwrite_first_content: OK")
    test_thread_safety_fires_exactly_once_under_concurrency()
    print("test_thread_safety_fires_exactly_once_under_concurrency: OK")
    test_forwards_file_name_and_ext()
    print("test_forwards_file_name_and_ext: OK")
    test_name_conflict_keeps_first_seen()
    print("test_name_conflict_keeps_first_seen: OK")


def test_forwards_file_name_and_ext():
    ready_calls = []
    aggregator = Aggregator(on_block_ready=lambda **kwargs: ready_calls.append(kwargs))

    for symbol_id in range(100):
        aggregator.add_packet(1, make_packet(file_hash=9, block_id=0, symbol_id=symbol_id,
                                             file_name="vacation", file_ext=".jpg"))

    assert len(ready_calls) == 1
    assert ready_calls[0]["file_name"] == "vacation"
    assert ready_calls[0]["file_ext"] == ".jpg"


def test_name_conflict_keeps_first_seen():
    ready_calls = []
    aggregator = Aggregator(on_block_ready=lambda **kwargs: ready_calls.append(kwargs))

    aggregator.add_packet(1, make_packet(file_hash=9, block_id=0, symbol_id=0,
                                         file_name="original", file_ext=".txt"))
    # Conflicting name on a NEW symbol exercises the first-seen-wins path.
    aggregator.add_packet(2, make_packet(file_hash=9, block_id=0, symbol_id=1,
                                         file_name="renamed", file_ext=".txt"))
    for symbol_id in range(2, 100):
        aggregator.add_packet(1, make_packet(file_hash=9, block_id=0, symbol_id=symbol_id,
                                             file_name="renamed", file_ext=".txt"))

    assert len(ready_calls) == 1
    assert ready_calls[0]["file_name"] == "original"
    assert ready_calls[0]["file_ext"] == ".txt"