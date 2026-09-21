import threading

from src.file_tracker import FileTracker


def test_fires_exactly_once_when_all_blocks_present():
    results = []
    tracker = FileTracker(on_file_complete=lambda **kw: results.append(kw))

    tracker.handle_block_assembled(file_hash=1, block_id=0, total_blocks=2, block_bytes=b"AAAA", contributing_receivers={1})
    assert not results, "should not fire with only 1 of 2 blocks"

    tracker.handle_block_assembled(file_hash=1, block_id=1, total_blocks=2, block_bytes=b"BBBB", contributing_receivers={2})
    assert len(results) == 1
    assert results[0]["file_bytes"] == b"AAAABBBB"


def test_orders_blocks_correctly_regardless_of_arrival_order():
    results = []
    tracker = FileTracker(on_file_complete=lambda **kw: results.append(kw))

    tracker.handle_block_assembled(file_hash=2, block_id=2, total_blocks=3, block_bytes=b"CCCC", contributing_receivers={1})
    tracker.handle_block_assembled(file_hash=2, block_id=0, total_blocks=3, block_bytes=b"AAAA", contributing_receivers={1})
    tracker.handle_block_assembled(file_hash=2, block_id=1, total_blocks=3, block_bytes=b"BBBB", contributing_receivers={1})

    assert results[0]["file_bytes"] == b"AAAABBBBCCCC"


def test_stray_block_after_completion_is_ignored():
    results = []
    tracker = FileTracker(on_file_complete=lambda **kw: results.append(kw))
    tracker.handle_block_assembled(file_hash=4, block_id=0, total_blocks=1, block_bytes=b"DONE", contributing_receivers={1})
    assert len(results) == 1

    tracker.handle_block_assembled(file_hash=4, block_id=0, total_blocks=1, block_bytes=b"DONE", contributing_receivers={1})
    assert len(results) == 1


def test_thread_safety_fires_exactly_once_under_concurrency():
    results = []
    append_lock = threading.Lock()

    def on_complete(**kw):
        with append_lock:
            results.append(kw)

    tracker = FileTracker(on_file_complete=on_complete)

    def deliver(block_id):
        tracker.handle_block_assembled(file_hash=5, block_id=block_id, total_blocks=50,
                                        block_bytes=bytes([block_id]), contributing_receivers={1})

    threads = [threading.Thread(target=deliver, args=(i,)) for i in range(50)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5)

    assert len(results) == 1, f"expected exactly one callback, got {len(results)}"
    assert results[0]["file_bytes"] == bytes(range(50))


if __name__ == "__main__":
    test_fires_exactly_once_when_all_blocks_present()
    print("test_fires_exactly_once_when_all_blocks_present: OK")
    test_orders_blocks_correctly_regardless_of_arrival_order()
    print("test_orders_blocks_correctly_regardless_of_arrival_order: OK")
    test_stray_block_after_completion_is_ignored()
    print("test_stray_block_after_completion_is_ignored: OK")
    test_thread_safety_fires_exactly_once_under_concurrency()
    print("test_thread_safety_fires_exactly_once_under_concurrency: OK")