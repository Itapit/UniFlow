package pb

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"

	proto "google.golang.org/protobuf/proto"
)

func FormatPacket(
	fileHash      uint64 ,                        
	blockId       uint32 ,                
	totalBlocks   uint32 ,               
	symbolId      uint32 ,              
	kSymbols      uint32 ,                       
	nSymbols      uint32 ,                
	fileSize      uint64 ,                
	content       []byte ,                                             
	packetCrc     uint32 ,                
	fileName      string ,
	fileExt       string ,

  ) ([]byte,error){

	packet:=&Packet{
		FileHash:fileHash ,                        
		BlockId:blockId ,                
		TotalBlocks:totalBlocks ,               
		SymbolId:symbolId ,              
		KSymbols:kSymbols ,                       
		NSymbols:nSymbols ,                
		FileSize:fileSize ,                
		Content:content ,                                             
		PacketCrc:packetCrc ,  
		FileName:fileName ,
		FileExt:fileExt ,
	}
	data,err:=proto.Marshal(packet)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal packet: %w", err)
	}

	return data, nil
} 

func CalculateCRC(
	fileHash uint64,
	blockId uint32,
	totalBlocks uint32,
	symbolId uint32,
	kSymbols uint32,
	nSymbols uint32,
	fileSize uint64,
	fileName string,
	fileExt string,
	content []byte,
) uint32 {
	table := crc32.MakeTable(crc32.Castagnoli)
	h := crc32.New(table)

	// כתיבת ה-Metadata בסדר אחיד
	var metaBuf [36]byte
	binary.LittleEndian.PutUint64(metaBuf[0:8], fileHash)
	binary.LittleEndian.PutUint32(metaBuf[8:12], blockId)
	binary.LittleEndian.PutUint32(metaBuf[12:16], totalBlocks)
	binary.LittleEndian.PutUint32(metaBuf[16:20], symbolId)
	binary.LittleEndian.PutUint32(metaBuf[20:24], kSymbols)
	binary.LittleEndian.PutUint32(metaBuf[24:28], nSymbols)
	binary.LittleEndian.PutUint64(metaBuf[28:36], fileSize)

	h.Write(metaBuf[:])
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(fileName)))
	h.Write(lenBuf[:])
	h.Write([]byte(fileName))
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(fileExt)))
	h.Write(lenBuf[:])
	h.Write([]byte(fileExt))
	h.Write(content)

	return h.Sum32()
}