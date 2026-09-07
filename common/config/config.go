// Package config hosts the shared variables for senders and receivers.
package config

import "time"

const (
	// DataShards (K) is the number of source data symbols per block.
	DataShards = 100
	// ParityShards (M) is the number of parity symbols per block.
	ParityShards = 50
	// ShardSize is the payload bytes per symbol. 1344 keeps the
	// protobuf packet under ~1400B on the wire.
	ShardSize = 1344
	// BlockSize is the max raw block bytes (DataShards*ShardSize).
	BlockSize = DataShards * ShardSize
	// NSymbols is the total symbols per block (DataShards+ParityShards).
	NSymbols = DataShards + ParityShards
	// MaxUDPPayload is the soft cap for a serialized Packet.
	MaxUDPPayload = 1400
	// StaleAfter is how long a coordinator claim lock without a done
	// marker is considered orphaned (holder crashed) and reclaimable.
	StaleAfter = 30 * time.Second
)
