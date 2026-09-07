package codec

import (
	"bytes"
	"math/rand"
	"testing"

	"common/config"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	encoder, err := NewCodec()
	if err != nil {
		t.Fatal(err)
	}
	// Deterministic pseudo-random block, short of full size to test padding.
	random := rand.New(rand.NewSource(42))
	block := make([]byte, config.BlockSize-100)
	random.Read(block)

	shards, err := EncodeBlock(encoder, block)
	if err != nil {
		t.Fatal(err)
	}
	if len(shards) != config.NSymbols {
		t.Fatalf("want %d shards, got %d", config.NSymbols, len(shards))
	}

	// Simulate loss: drop symbols up to the parity budget.
	present := make([]bool, len(shards))
	for index := range present {
		present[index] = true
	}
	shardsWithLoss := make([][]byte, len(shards))
	copy(shardsWithLoss, shards)
	for dropped := 0; dropped < config.ParityShards; dropped++ {
		shardsWithLoss[dropped*3] = nil
		present[dropped*3] = false
	}

	decoder, err := NewCodec()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeBlock(decoder, shardsWithLoss, present)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded[:len(block)], block) {
		t.Fatal("decoded block does not match original")
	}
}

func TestDecodeTooFewSymbols(t *testing.T) {
	decoder, err := NewCodec()
	if err != nil {
		t.Fatal(err)
	}
	shards := make([][]byte, config.NSymbols)
	present := make([]bool, config.NSymbols)
	for index := 0; index < config.DataShards-1; index++ {
		shards[index] = make([]byte, config.ShardSize)
		present[index] = true
	}
	if _, err := DecodeBlock(decoder, shards, present); err == nil {
		t.Fatal("expected error with fewer than K symbols")
	}
}

func TestEncodeOversize(t *testing.T) {
	encoder, err := NewCodec()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := EncodeBlock(encoder, make([]byte, config.BlockSize+1)); err == nil {
		t.Fatal("expected error for oversize block")
	}
}
