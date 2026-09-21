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
	"time"

	ipc "senders/internal/IPC"
	"senders/internal/constants"
	"senders/internal/counter"
	rs "senders/internal/erasure"
	"senders/internal/pb"
	rd "senders/internal/reader"
)

type EncodedBlock struct {
	blockIdx uint32
	shards   [][]byte
}

func main() {
	targetAddrStr := flag.String("target", constants.DefaultTargetAddr, "Destination UDP address (IP:Port)")
	socketPath := flag.String("socket", constants.DefaultSocketPath, "Path to Unix domain socket for IPC")

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

	_ = conn.SetWriteBuffer(constants.UDPWriteBufferSize)

	for task := range channel {
		filePath := task.FilePath
		fileHash := task.FileHash

		counterFileName := fmt.Sprintf(constants.CounterFileTemplate, fileHash)
		counterFilePath := filepath.Join(constants.CounterFilePath, counterFileName)

		counterFile, err := counter.InitCounterFile(counterFilePath)
		if err != nil {
			log.Fatalf("failed to initialize counter file for hash %d: %v", fileHash, err)
		}

		reader, err := rd.OpenFile(filePath)
		if err != nil {
			counterFile.Close()
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

			startBlock := uint32(count) * constants.BlocksJump
			if startBlock >= totalBlocks {
				break
			}

			endBlock := startBlock + constants.BlocksJump
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
			packetsSentInBatch := 0

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
						uint32(constants.DefaultDataShrads),
						uint32(constants.DefaultParityShards),
						uint64(readerSize),
						content,
					)

					serializedData, err := pb.FormatPacket(
						fileHash,
						b.blockIdx,
						totalBlocks,
						uint32(shardIndex),
						uint32(constants.DefaultDataShrads),
						uint32(constants.DefaultParityShards),
						uint64(readerSize),
						content,
						crc,
					)
					if err != nil {
						continue
					}

					if _, err = conn.Write(serializedData); err != nil {
						log.Printf("failed writing packet: %v", err)
					}

					packetsSentInBatch++
					if packetsSentInBatch%constants.PacingBatchThreshold == 0 {
						time.Sleep(constants.PacingInterval)
					}
				}
			}
		}

		reader.Close()
		counterFile.Close()

		currentState.Store(int32(pb.SenderState_IDLE))
	}
}