package erasure_test

import (
	"bytes"
	"crypto/rand"
	"testing"

	"senders/internal/constants"
	rs "senders/internal/erasure"
	"senders/internal/pb"

	"github.com/klauspost/reedsolomon"
)

func TestEncodeAndVerifyIntegrity(t *testing.T) {
	originalSize := 50000
	rawBlock := make([]byte, originalSize)
	if _, err := rand.Read(rawBlock); err != nil {
		t.Fatalf("Failed to generate random block: %v", err)
	}

	encodedShards, err := rs.EncodeBlock(rawBlock)
	if err != nil {
		t.Fatalf("EncodeBlock failed: %v", err)
	}

	totalShards := constants.DefaultDataShrads + constants.DefaultParityShards
	if len(encodedShards) != totalShards {
		t.Fatalf("Expected %d shards, got %d", totalShards, len(encodedShards))
	}

	fileHash := uint64(0xABCDEF1234567890)
	blockId := uint32(0)
	totalBlocks := uint32(1)
	fileSize := uint64(originalSize)

	for shardIdx, shard := range encodedShards {
		crc := pb.CalculateCRC(
			fileHash,
			blockId,
			totalBlocks,
			uint32(shardIdx),
			uint32(constants.DefaultDataShrads),
			uint32(constants.DefaultParityShards),
			fileSize,
			shard,
		)
		if crc == 0 {
			t.Errorf("Shard %d calculated CRC returned 0", shardIdx)
		}
	}

	for i := 0; i < constants.DefaultParityShards; i++ {
		encodedShards[i] = nil
	}

	enc, err := reedsolomon.New(constants.DefaultDataShrads, constants.DefaultParityShards)
	if err != nil {
		t.Fatalf("Failed to initialize decoder: %v", err)
	}

	err = enc.Reconstruct(encodedShards)
	if err != nil {
		t.Fatalf("Reconstruct failed: %v", err)
	}

	var reconstructed bytes.Buffer
	for i := 0; i < constants.DefaultDataShrads; i++ {
		reconstructed.Write(encodedShards[i])
	}

	restoredData := reconstructed.Bytes()[:originalSize]
	if !bytes.Equal(rawBlock, restoredData) {
		t.Fatal("Data corruption: reconstructed bytes do not match original")
	}
}

func TestEncodeBlock_ExceedsMaxSize(t *testing.T) {
	oversized := make([]byte, constants.MaxBlockSize+1)
	_, err := rs.EncodeBlock(oversized)
	if err == nil {
		t.Errorf("Expected error when encoding block larger than MaxBlockSize, got nil")
	}
}