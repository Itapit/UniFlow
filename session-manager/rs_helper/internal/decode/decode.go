package decode

import (
	"fmt"

	"common/erasure"

	"rs_helper/pb"
)

// FromRequest expands the sparse set of received shards into the full
// n_symbols-length array Reconstruct expects (nil for anything missing),
// reconstructs via common/erasure — the same code the Senders already use
// to encode — and returns the k data shards.
func FromRequest(req *pb.ReconstructRequest) *pb.ReconstructResponse {
	k := int(req.GetKSymbols())
	n := int(req.GetNSymbols())
	if k <= 0 || n <= k {
		return &pb.ReconstructResponse{Ok: false, Error: fmt.Sprintf("invalid k_symbols=%d n_symbols=%d", k, n)}
	}

	shards := make([][]byte, n)
	for _, shard := range req.GetShards() {
		id := int(shard.GetSymbolId())
		if id < 0 || id >= n {
			return &pb.ReconstructResponse{Ok: false, Error: fmt.Sprintf("symbol_id %d out of range for n_symbols=%d", id, n)}
		}
		shards[id] = shard.GetContent()
	}

	dataShards, err := erasure.ReconstructBlock(shards, k, n-k)
	if err != nil {
		return &pb.ReconstructResponse{Ok: false, Error: err.Error()}
	}
	return &pb.ReconstructResponse{Ok: true, DataShards: dataShards}
}