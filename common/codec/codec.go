// Package codec hosts the shared Reed-Solomon interfaces and block
// helpers used by both senders (encode) and receivers (decode).
//
// The concrete algorithm is injected: callers pass an Encoder to
// EncodeBlock and a Decoder to DecodeBlock.
package codec

import (
	"fmt"

	"common/config"

	"github.com/klauspost/reedsolomon"
)

type Encoder interface {
	Encode(shards [][]byte) error
}

type Decoder interface {
	Reconstruct(shards [][]byte) error
	Verify(shards [][]byte) (bool, error)
}

type Codec interface {
	Encoder
	Decoder
}

// NewCodec builds the default reedsolomon implementation for the
// config ratio. Inject the result into EncodeBlock/DecodeBlock.
func NewCodec() (Codec, error) {
	enc, err := reedsolomon.New(config.DataShards, config.ParityShards)
	if err != nil {
		return nil, fmt.Errorf("codec: construct k=%d m=%d: %w", config.DataShards, config.ParityShards, err)
	}
	return enc, nil
}

// EncodeBlock splits block into DataShards shards of ShardSize bytes,
// computes parity, and returns all NSymbols shards.
// short final blocks are zero-padded.
func EncodeBlock(encoder Encoder, block []byte) ([][]byte, error) {
	if len(block) > config.BlockSize {
		return nil, fmt.Errorf("codec: block size %d exceeds max %d", len(block), config.BlockSize)
	}
	shards := make([][]byte, config.NSymbols)
	for index := range shards {
		shards[index] = make([]byte, config.ShardSize)
	}
	for index := 0; index < config.DataShards; index++ {
		start := index * config.ShardSize
		if start >= len(block) {
			break
		}
		end := start + config.ShardSize
		if end > len(block) {
			end = len(block)
		}
		copy(shards[index], block[start:end])
	}
	if err := encoder.Encode(shards); err != nil {
		return nil, fmt.Errorf("codec: encode parity: %w", err)
	}
	return shards, nil
}

// DecodeBlock reconstructs the original block from shards where
// present[i]==false marks a lost symbol (shards[i] must be nil then).
// It requires at least DataShards present symbols. The returned slice
// is the full padded block; callers truncate to the file size for the
// final block.
func DecodeBlock(decoder Decoder, shards [][]byte, present []bool) ([]byte, error) {
	if len(shards) != config.NSymbols || len(present) != config.NSymbols {
		return nil, fmt.Errorf("codec: want %d shards, got %d/%d", config.NSymbols, len(shards), len(present))
	}
	presentCount := 0
	for _, isPresent := range present {
		if isPresent {
			presentCount++
		}
	}
	if presentCount < config.DataShards {
		return nil, fmt.Errorf("codec: need %d symbols, have %d", config.DataShards, presentCount)
	}
	workingShards := make([][]byte, len(shards))
	for index := range shards {
		if present[index] {
			if len(shards[index]) != config.ShardSize {
				return nil, fmt.Errorf("codec: shard %d size %d, want %d", index, len(shards[index]), config.ShardSize)
			}
			workingShards[index] = shards[index]
		} else {
			workingShards[index] = nil
		}
	}
	if err := decoder.Reconstruct(workingShards); err != nil {
		return nil, fmt.Errorf("codec: reconstruct: %w", err)
	}
	if verified, err := decoder.Verify(workingShards); err != nil {
		return nil, fmt.Errorf("codec: verify: %w", err)
	} else if !verified {
		return nil, fmt.Errorf("codec: verify failed after reconstruct")
	}
	decodedBlock := make([]byte, 0, config.BlockSize)
	for index := 0; index < config.DataShards; index++ {
		decodedBlock = append(decodedBlock, workingShards[index]...)
	}
	return decodedBlock, nil
}
