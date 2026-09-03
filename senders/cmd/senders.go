package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"senders/internal/pb"
	"time"

	"golang.org/x/exp/mmap"
)

func main() {
	targetAddrStr := flag.String("target", "127.0.0.1:1400", "Destination UDP address (IP:Port)")
	flag.Parse()

	mockPayload := []byte("Hello, UniFlow network!")

	serializedData, err := pb.FormatPacket(
		123456789,   // fileHash
		0,           // blockId
		10,          // totalBlocks
		1,           // symbolId
		200,         // kSymbols
		260,         // nSymbols
		102400,      // fileSize (100KB)
		mockPayload, // content
		987654321,   // packetCrc
	)

	if err != nil {
		log.Fatalf("Error formatting packet: %v", err)
	}

	fmt.Printf("Successfully serialized packet!\n")
	fmt.Printf("Wire Byte Size: %d bytes\n", len(serializedData))
	fmt.Printf("Raw Bytes: %v\n", serializedData)

	addr, err := net.ResolveUDPAddr("udp", *targetAddrStr)
	if err != nil {
		log.Fatal(err)
	}
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	for range 10 {
		_, err := conn.Write(serializedData)
		if err != nil {
			log.Println("failed writing")
		} else {
			log.Println("packet sent")
		}
		time.Sleep(1 * time.Second) // השהייה לצורך בדיקה נקייה

	}
	reader, err := mmap.Open("../test/test.txt")
	if err != nil {
		log.Fatal(err)
	}
	fileLength := reader.Len()
	fmt.Println(fileLength)
	buf := make([]byte, fileLength)
	_, err = reader.ReadAt(buf, 0)
	if err != nil {
	}
	err = reader.Close()
	fmt.Println(string(buf))
	if err != nil {
		return
	}
}
