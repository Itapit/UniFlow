package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	ipc "senders/internal/IPC"
	"senders/internal/constants"
	"senders/internal/counter"
	rs "senders/internal/erasure"
	"senders/internal/logger"
	"senders/internal/pb"
	rd "senders/internal/reader"
)

const BlocksJump = 8

type EncodedBlock struct {
	blockIdx uint32
	shards   [][]byte
}

func main() {
	targetAddrStr := flag.String("target", "127.0.0.1:1400", "Destination UDP address (IP:Port)")
	socketPath := flag.String("socket", "/tmp/monitor.sock", "Path to Unix domain socket for IPC")
	senderID := flag.Uint("sender-id", 0, "Sender identity reported in logs")

	flag.Parse()

	log := logger.New("sender").With("sender_id", *senderID, "socket_path", *socketPath)

	var currentState atomic.Int32
	currentState.Store(int32(pb.SenderState_IDLE))

	channel := make(chan *pb.TaskAssignment)
	listener, err := ipc.StartUDSServer(*socketPath, log)
	if err != nil {
		log.Error("socket startup failed", "event", "socket_startup_failed", "err", err)
		os.Exit(1)
	}

	go ipc.HandleConn(listener, channel, func() pb.SenderState {
		return pb.SenderState(currentState.Load())
	}, log)

	defer listener.Close()
	defer func() {
		if err := os.Remove(*socketPath); err != nil && !os.IsNotExist(err) {
			log.Warn("socket cleanup failed", "event", "socket_remove_failed", "err", err)
		} else {
			log.Info("socket removed", "event", "socket_removed")
		}
	}()

	addr, err := net.ResolveUDPAddr("udp", *targetAddrStr)
	if err != nil {
		log.Error("resolve target failed", "event", "resolve_failed", "target", *targetAddrStr, "err", err)
		os.Exit(1)
	}
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		log.Error("dial target failed", "event", "dial_failed", "target", *targetAddrStr, "err", err)
		os.Exit(1)
	}
	defer conn.Close()

	log.Info("sender ready", "event", "sender_ready", "target", *targetAddrStr)

	for task := range channel {
		filePath := task.FilePath
		fileHash := task.FileHash
		taskLog := log.With("file_hash", fileHash, "file_path", filePath)

		// 1. Initialize a unique counter file per file transfer session using fileHash
		counterFileName := fmt.Sprintf("counter_%d.bin", fileHash)
		counterFilePath := filepath.Join(constants.CounterFilePath, counterFileName)
		absCounterPath, _ := filepath.Abs(counterFilePath)

		counterFile, created, err := counter.InitCounterFile(counterFilePath)
		if err != nil {
			taskLog.Error("counter init failed", "event", "counter_init_failed", "counter_path", absCounterPath, "err", err)
			continue
		}
		if created {
			taskLog.Info("counter created", "event", "counter_created", "counter_path", absCounterPath)
		} else {
			taskLog.Info("counter reused", "event", "counter_reused", "counter_path", absCounterPath)
		}

		reader, err := rd.OpenFile(filePath)
		if err != nil {
			taskLog.Error("open file failed", "event", "open_failed", "err", err)
			counterFile.Close()
			continue
		}

		readerSize := reader.Size()
		totalBlocks := uint32((readerSize + constants.BlockSize - 1) / constants.BlockSize)

		taskLog.Info("transfer started",
			"event", "transfer_start",
			"file_size", readerSize,
			"total_blocks", totalBlocks)

		currentState.Store(int32(pb.SenderState_WORKING))
		transferStart := time.Now()
		var claims uint64
		var packetsSent uint64
		var blocksRead uint64

		for {
			count, err := counter.Count(counterFile)
			if err != nil {
				taskLog.Error("counter read failed", "event", "counter_read_failed", "claims", claims, "err", err)
				break
			}
			claims++

			startBlock := uint32(count) * BlocksJump
			if startBlock >= totalBlocks {
				break
			}

			endBlock := startBlock + BlocksJump
			if endBlock > totalBlocks {
				endBlock = totalBlocks
			}

			taskLog.Debug("blocks claimed",
				"event", "blocks_claimed",
				"claim", count,
				"start_block", startBlock,
				"end_block", endBlock)

			var batchBlocks []EncodedBlock

			for blockIdx := startBlock; blockIdx < endBlock; blockIdx++ {
				offset := int64(blockIdx) * constants.BlockSize

				data, err := reader.ReadChunk(offset)
				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					}
					taskLog.Warn("read failed", "event", "read_failed", "block_id", blockIdx, "err", err)
					break
				}
				blocksRead++

				taskLog.Debug("block read",
					"event", "block_read",
					"block_id", blockIdx,
					"bytes", len(data),
					"offset", offset)

				contents, err := rs.EncodeBlock(data)
				if err != nil {
					taskLog.Error("encode failed", "event", "encode_failed", "block_id", blockIdx, "err", err)
					break
				}

				batchBlocks = append(batchBlocks, EncodedBlock{
					blockIdx: blockIdx,
					shards:   contents,
				})
			}

			totalShardsPerBlock := constants.DefaultDataShrads + constants.DefaultParityShards
			for shardIndex := 0; shardIndex < totalShardsPerBlock; shardIndex++ {
				for _, b := range batchBlocks {
					if shardIndex >= len(b.shards) {
						continue
					}

					content := b.shards[shardIndex]
					crc := pb.CalculateCRC(
						fileHash,
						b.blockIdx,
						totalBlocks,
						uint32(shardIndex),
						uint32(constants.DefaultDataShrads), // kSymbols
						uint32(constants.DefaultDataShrads+constants.DefaultParityShards), // nSymbols — 150
						uint64(readerSize),
						content,
					)

					serializedData, err := pb.FormatPacket(
						uint64(fileHash),
						b.blockIdx,
						totalBlocks,
						uint32(shardIndex),
						uint32(constants.DefaultDataShrads), // kSymbols
						uint32(constants.DefaultDataShrads+constants.DefaultParityShards), // nSymbols — 150
						uint64(readerSize),
						content,
						crc,
					)
					if err != nil {
						taskLog.Warn("format packet failed",
							"event", "format_failed",
							"block_id", b.blockIdx,
							"symbol_id", shardIndex,
							"err", err)
						continue
					}

					if _, err = conn.Write(serializedData); err != nil {
						taskLog.Warn("udp write failed",
							"event", "udp_write_failed",
							"block_id", b.blockIdx,
							"symbol_id", shardIndex,
							"err", err)
					} else {
						packetsSent++
					}
				}
			}
		}

		reader.Close()
		counterFile.Close()

		taskLog.Info("transfer finished",
			"event", "transfer_done",
			"claims", claims,
			"blocks_read", blocksRead,
			"packets_sent", packetsSent,
			"duration_ms", time.Since(transferStart).Milliseconds())

		currentState.Store(int32(pb.SenderState_IDLE))
	}
}
