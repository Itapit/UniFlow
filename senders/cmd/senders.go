package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	rd "senders/internal/reader"
)

func main() {
	data := bytes.Repeat([]byte("A quick brown fox jumps over the lazy dog. "), 12000)
_ = os.WriteFile("../test/big_test.txt", data, 0644)
	/*targetAddrStr := flag.String("target", "127.0.0.1:1400", "Destination UDP address (IP:Port)")
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

	}*/
	reader,err:= rd.OpenFile("../test/big_test.txt")
	if(err!=nil){
		return
	}
	fmt.Printf("File size from mmap: %d bytes\n", reader.Size())

	offset := int64(0)
	blockIndex := 0

	for offset < int64(reader.Size()) {
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

    	offset += int64(len(data))
    	blockIndex++
	}
	reader.Close()
}
