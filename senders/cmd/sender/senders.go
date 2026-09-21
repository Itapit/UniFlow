package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"

	ipc "senders/internal/IPC"
	"senders/internal/constants"
	"senders/internal/counter"
	rs "senders/internal/erasure"
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

	flag.Parse()

	var currentState atomic.Int32
	currentState.Store(int32(pb.SenderState_IDLE))

	channel := make(chan *pb.TaskAssignment)
	listener, err := ipc.StartUDSServer(*socketPath)
	if err != nil {
		log.Fatal(err)
	}

	go ipc.HandleConn(listener, channel, func() pb.SenderState {
		return pb.SenderState(currentState.Load())
	})

	defer listener.Close()
	defer os.Remove(*socketPath)

	addr, err := net.ResolveUDPAddr("udp", *targetAddrStr)
	if err != nil {
		log.Fatal(err)
	}
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	for task := range channel {
		filePath := task.FilePath
		fileHash := task.FileHash

		// 1. Initialize a unique counter file per file transfer session using fileHash
		counterFileName := fmt.Sprintf("counter_%d.bin", fileHash)
		counterFilePath := filepath.Join(constants.CounterFilePath, counterFileName)

		counterFile, err := counter.InitCounterFile(counterFilePath)
		if err != nil {
			log.Fatalf("failed to initialize counter file for hash %d: %v", fileHash, err)
		}

		reader, err := rd.OpenFile(filePath)
		if err != nil {
			log.Fatalf("failed opening file %s: %v", filePath, err)
		}

		readerSize := reader.Size()
		totalBlocks := uint32((readerSize + constants.BlockSize - 1) / constants.BlockSize)

		currentState.Store(int32(pb.SenderState_WORKING))

		for {
			count, err := counter.Count(counterFile)
			if err != nil {
				log.Printf("failed reading counter: %v", err)
				break
			}

			startBlock := uint32(count) * BlocksJump
			if startBlock >= totalBlocks {
				break
			}

			endBlock := startBlock + BlocksJump
			if endBlock > totalBlocks {
				endBlock = totalBlocks
			}

			var batchBlocks []EncodedBlock

			for blockIdx := startBlock; blockIdx < endBlock; blockIdx++ {
				offset := int64(blockIdx) * constants.BlockSize

				data, err := reader.ReadChunk(offset)
				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					}
					log.Printf("Read error at block %d: %v", blockIdx, err)
					break
				}

				fmt.Printf("Successfully read Block #%d: %d bytes (Offset: %d)\n", blockIdx, len(data), offset)
				fmt.Println("------------------------------------------")

				contents, err := rs.EncodeBlock(data)
				if err != nil {
					log.Printf("failed encoding block %d: %v", blockIdx, err)
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
						uint32(constants.DefaultDataShrads),                              // kSymbols
                        uint32(constants.DefaultDataShrads+constants.DefaultParityShards), // nSymbols — 150
						uint64(readerSize),
						content,
					)

					serializedData, err := pb.FormatPacket(
						uint64(fileHash),
						b.blockIdx,
						totalBlocks,
						uint32(shardIndex),
						uint32(constants.DefaultDataShrads),                              // kSymbols
                        uint32(constants.DefaultDataShrads+constants.DefaultParityShards), // nSymbols — 150
						uint64(readerSize),
						content,
						crc,
					)
					if err != nil {
						log.Printf("Error formatting packet: %v", err)
						continue
					}

					_, err = conn.Write(serializedData)
					if err != nil {
						log.Printf("failed writing packet: %v", err)
					} else {
						log.Println("packet sent")
					}
				}
			}
		}

		reader.Close()
		counterFile.Close()

		currentState.Store(int32(pb.SenderState_IDLE))
	}
}