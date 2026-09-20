package erasure

import (
	"fmt"

	"github.com/klauspost/reedsolomon"
)

// ReconstructBlock fills in any missing (nil) entries in shards via
// Reed-Solomon reconstruction and returns just the data shards, in order.
// dataShards/parityShards must be the exact values EncodeBlock used to
// produce these shards — reedsolomon.New builds a specific generator
// matrix from these two numbers, and mismatched values won't error,
// they'll silently return wrong bytes.
func ReconstructBlock(shards [][]byte, dataShards, parityShards int) ([][]byte, error) {
	enc, err := reedsolomon.New(dataShards, parityShards)
	if err != nil {
		return nil, fmt.Errorf("encoder failed to construct: %w", err)
	}
	if err := enc.Reconstruct(shards); err != nil {
		return nil, fmt.Errorf("reconstruct failed: %w", err)
	}
	return shards[:dataShards], nil
}