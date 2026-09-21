package constants

import "time"

const (
	
	
	CounterFilePath     = "../../../shared/counter/"
	// File paths & formatting
	DefaultTargetAddr   = "127.0.0.1:1400"
	DefaultSocketPath   = "/tmp/monitor.sock"
	CounterFilePrefix   = "counter_"
	CounterFileExt      = ".bin"
	CounterFileTemplate = CounterFilePrefix + "%d" + CounterFileExt

	// File permissions
	DefaultFileMode = 0666

	// Erasure coding & Block structure
	BlockSize            = 134400
	DefaultDataShrads     = 100
	DefaultParityShards  = 50
	BlocksJump           = 8
	MaxShardSize        = 1344
	MaxBlockSize        = DefaultDataShrads * MaxShardSize
	ChunkSize           = 134400

	// Network & Socket buffer sizing
	UDPWriteBufferSize = 4 * 1024 * 1024 // 4MB socket send buffer
	PacingBatchThreshold = 16
	PacingInterval       = 50 * time.Microsecond
)