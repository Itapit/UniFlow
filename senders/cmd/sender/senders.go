package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	ipc "senders/internal/IPC"
	"senders/internal/constants"
	"senders/internal/counter"
	rs "senders/internal/erasure"
	"senders/internal/pb"
	rd "senders/internal/reader"
)

func main() {

	targetAddrStr := flag.String("target", "127.0.0.1:1400", "Destination UDP address (IP:Port)")
	socketPath := flag.String("socket", "/tmp/monitor.sock", "Path to Unix domain socket for IPC")
	counterFileName := flag.String("counter-file", "sender_counter.bin", "counter coordination file")
	
	flag.Parse()

	channel:=make(chan string)

	listener,err:=ipc.StartUDSServer(*socketPath)
	if err != nil {
    	log.Fatal(err)
	}
	go ipc.HandleConn(listener,channel)

	defer listener.Close()
	defer os.Remove(*socketPath)


	counterFile,_:=counter.InitCounterFile(constants.CounterFilePath+*counterFileName)
	//count,err:=counter.Count(counterFile)
	defer counterFile.Close()


	addr, err := net.ResolveUDPAddr("udp", *targetAddrStr)
	if err != nil {
		log.Fatal(err)
	}
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	for filePath := range channel{
		reader, err := rd.OpenFile(filePath)
		if err != nil {
			log.Fatalf("failed opening file %s: %v", filePath, err)
		}


		fileHash, err := rd.GenerateFileHash(filePath)
		if err != nil {
			log.Fatalf("failed generating file hash: %v", err)
		}

		offset := int64(0)
		blockIndex := 0
		readerSize := reader.Size()
		totalBlocks := uint32((readerSize + constants.BlockSize - 1) / constants.BlockSize)
		var contents [][]byte

		for offset < int64(readerSize) {
			data, err := reader.ReadChunk(offset)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				fmt.Printf("Read error at block %d: %v\n", blockIndex, err)
				break
			}
			fmt.Printf("Successfully read Block #%d: %d bytes (Offset: %d)\n", blockIndex, len(data), offset)
			fmt.Println("------------------------------------------")

			contents, err = rs.EncodeBlock(data)
			if err != nil {
				log.Printf("failed encoding block %d: %v", blockIndex, err)
				break
			}

			for shardIndex, content := range contents {
				crc := pb.CalculateCRC(content)
				serializedData, err := pb.FormatPacket(
					uint64(fileHash),
					uint32(blockIndex),
					totalBlocks,
					uint32(shardIndex),
					uint32(constants.DefaultDataShrads),
					uint32(constants.DefaultParityShards),
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

			offset += int64(len(data))
			blockIndex++
		}
		reader.Close()
	}
}