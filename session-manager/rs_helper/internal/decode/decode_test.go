package decode_test

import (
	"bytes"
	"crypto/rand"
	"testing"

	"common/erasure"

	"rs_helper/internal/decode"
	"rs_helper/pb"
)

// TestReconstructMatchesOriginalData encodes with the exact same code the
// Senders use, drops shards the way real packet loss would, and confirms
// decode.FromRequest — the function rs_helper's main.go actually calls —
// returns byte-identical data. This has to pass before anything upstream
// (block_assembler.py, aggregator.py) gets built on top of this helper.
func TestReconstructMatchesOriginalData(t *testing.T) {
	const dataShards = 100
	const parityShards = 50
	const totalShards = dataShards + parityShards

	original := make([]byte, 100000)
	if _, err := rand.Read(original); err != nil {
		t.Fatalf("generate random block: %v", err)
	}

	encoded, err := erasure.EncodeBlock(original)
	if err != nil {
		t.Fatalf("EncodeBlock failed: %v", err)
	}
	if len(encoded) != totalShards {
		t.Fatalf("want %d shards, got %d", totalShards, len(encoded))
	}

	// Drop the maximum tolerable 50 shards, spread across data and parity
	// rather than only ever dropping a clean parity block.
	dropped := map[int]bool{}
	for i := 0; len(dropped) < parityShards; i += 3 {
		dropped[i%totalShards] = true
	}

	req := &pb.ReconstructRequest{FileHash: 42, BlockId: 0, KSymbols: dataShards, NSymbols: totalShards}
	for symbolID, shard := range encoded {
		if dropped[symbolID] {
			continue
		}
		req.Shards = append(req.Shards, &pb.Shard{SymbolId: uint32(symbolID), Content: shard})
	}

	resp := decode.FromRequest(req)
	if !resp.GetOk() {
		t.Fatalf("reconstruction reported failure: %s", resp.GetError())
	}
	if len(resp.GetDataShards()) != dataShards {
		t.Fatalf("want %d data shards back, got %d", dataShards, len(resp.GetDataShards()))
	}

	var reconstructed bytes.Buffer
	for _, shard := range resp.GetDataShards() {
		reconstructed.Write(shard)
	}
	if restored := reconstructed.Bytes()[:len(original)]; !bytes.Equal(original, restored) {
		t.Fatal("round trip through decode.FromRequest produced different bytes than the original block")
	}
}

// TestReconstructFailsClearlyBelowThreshold: "too much loss to recover" is
// a real, expected outcome here, not just an edge case — confirm it comes
// back as Ok:false with a message, not a panic.
func TestReconstructFailsClearlyBelowThreshold(t *testing.T) {
	const dataShards = 100
	const parityShards = 50

	original := make([]byte, 50000)
	_, _ = rand.Read(original)
	encoded, err := erasure.EncodeBlock(original)
	if err != nil {
		t.Fatalf("EncodeBlock failed: %v", err)
	}

	req := &pb.ReconstructRequest{KSymbols: dataShards, NSymbols: dataShards + parityShards}
	for symbolID := 0; symbolID < 90; symbolID++ { // 10 short of the 100 required
		req.Shards = append(req.Shards, &pb.Shard{SymbolId: uint32(symbolID), Content: encoded[symbolID]})
	}

	resp := decode.FromRequest(req)
	if resp.GetOk() {
		t.Fatal("expected failure with only 90 of 100 required shards, got Ok:true")
	}
	if resp.GetError() == "" {
		t.Fatal("expected a non-empty error message")
	}
}