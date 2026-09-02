package pb

import (
	"fmt"

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
	}
	data,err:=proto.Marshal(packet)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal packet: %w", err)
	}

	return data, nil
} 