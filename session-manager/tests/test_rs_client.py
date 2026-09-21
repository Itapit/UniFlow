from src.rs_client import RSClient, ReconstructionError

client = RSClient("/tmp/uniflow_rs_helper.sock")
client.connect()
try:
    client.reconstruct(
        file_hash=123, block_id=0, k_symbols=100, n_symbols=150,
        shards={i: b"x" * 1344 for i in range(90)},  # 10 short of the 100 required
    )
    print("UNEXPECTED: reconstruction succeeded with only 90 shards")
except ReconstructionError as e:
    print(f"Got the expected failure back from rs_helper: {e}")
finally:
    client.close()