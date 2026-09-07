package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"senders/internal/constants"
	rs "senders/internal/erasure"
	"senders/internal/pb"
	rd "senders/internal/reader"
)



func main() {

	targetAddrStr := flag.String("target", "127.0.0.1:1400", "Destination UDP address (IP:Port)")
	flag.Parse()

	const filePath = "../test/big_test.txt"

	
	addr, err := net.ResolveUDPAddr("udp", *targetAddrStr)
	if err != nil {
		log.Fatal(err)
	}
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	reader, err := rd.OpenFile(filePath)
	if err != nil {
		log.Fatalf("failed opening file %s: %v", filePath, err)
	}
	defer reader.Close()

	fmt.Printf("File size from mmap: %d bytes\n", reader.Size())

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
				uint32(constants.DefaultParityShards),
				uint32(constants.DefaultDataShrads),
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
}