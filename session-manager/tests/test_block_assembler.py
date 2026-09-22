from src.block_assembler import BlockAssembler
from src.rs_client import ReconstructionError


class FakeRSClient:
    def __init__(self, sock_path, data_shards=None, raise_error=None):
        self._data_shards = data_shards
        self._raise_error = raise_error

    def connect(self):
        pass

    def reconstruct(self, **kwargs):
        if self._raise_error:
            raise self._raise_error
        return self._data_shards

    def close(self):
        pass


def make_factory(data_shards=None, raise_error=None):
    return lambda sock_path: FakeRSClient(sock_path, data_shards, raise_error)


def test_trims_padding_on_last_block():
    data_shards = [b"a" * 1344 for _ in range(99)]
    data_shards.append(b"a" * (100000 - 99 * 1344) + b"\x00" * 1344)
    data_shards[-1] = data_shards[-1][:1344]

    results = []
    assembler = BlockAssembler(lambda **kw: results.append(kw), client_factory=make_factory(data_shards))
    assembler.handle_block_ready(file_hash=1, block_id=0, k_symbols=100, n_symbols=150,
                                  file_size=100000, total_blocks=1, shards={}, contributing_receivers={1})
    assert len(results[0]["block_bytes"]) == 100000


def test_does_not_trim_a_non_last_block():
    data_shards = [b"a" * 1344 for _ in range(100)]
    results = []
    assembler = BlockAssembler(lambda **kw: results.append(kw), client_factory=make_factory(data_shards))
    assembler.handle_block_ready(file_hash=1, block_id=0, k_symbols=100, n_symbols=150,
                                  file_size=500000, total_blocks=4, shards={}, contributing_receivers={1})
    assert len(results[0]["block_bytes"]) == 134400


def test_reconstruction_failure_does_not_call_back_or_raise():
    results = []
    assembler = BlockAssembler(lambda **kw: results.append(kw),
                                client_factory=make_factory(raise_error=ReconstructionError("too few shards")))
    assembler.handle_block_ready(file_hash=1, block_id=0, k_symbols=100, n_symbols=150,
                                  file_size=100000, total_blocks=1, shards={}, contributing_receivers={1})
    assert results == []


if __name__ == "__main__":
    test_trims_padding_on_last_block()
    print("test_trims_padding_on_last_block: OK")
    test_does_not_trim_a_non_last_block()
    print("test_does_not_trim_a_non_last_block: OK")
    test_reconstruction_failure_does_not_call_back_or_raise()
    print("test_reconstruction_failure_does_not_call_back_or_raise: OK")
    test_forwards_file_name_to_block_assembled()
    print("test_forwards_file_name_to_block_assembled: OK")


def test_forwards_file_name_to_block_assembled():
    data_shards = [b"a" * 1344 for _ in range(100)]
    results = []
    assembler = BlockAssembler(lambda **kw: results.append(kw), client_factory=make_factory(data_shards))
    assembler.handle_block_ready(file_hash=1, block_id=0, k_symbols=100, n_symbols=150,
                                  file_size=500000, total_blocks=4, shards={}, contributing_receivers={1},
                                  file_name="movie", file_ext=".mkv")
    assert results[0]["file_name"] == "movie"
    assert results[0]["file_ext"] == ".mkv"