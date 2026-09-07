package erasure

import (
	"fmt"
	"senders/internal/constants"

	"github.com/klauspost/reedsolomon"
)



func EncodeBlock(block []byte) ([][]byte,error) {
	if len(block) > constants.MaxBlockSize {
		return nil, fmt.Errorf("block size: %d is to big, max size: %d",len(block),constants.MaxBlockSize)
	}
	enc, err:=reedsolomon.New(constants.DefaultDataShrads,constants.DefaultParityShards);

	if err !=nil{

		return nil, fmt.Errorf("encoder failed to construct: %w",err)

	}
	data:=make([][]byte,constants.DefaultDataShrads+constants.DefaultParityShards)

	for i :=range constants.DefaultDataShrads+constants.DefaultParityShards{
		data[i]=make([]byte, constants.MaxShardSize)
	}

	for i := range constants.DefaultDataShrads {
		start := i * constants.MaxShardSize
		if start >= len(block) {
			break 
		}

		end := start + constants.MaxShardSize
		if end > len(block) {
			end = len(block)
		}

		copy(data[i], block[start:end])
	}
	err = enc.Encode(data)
	if err !=nil{
		return nil, fmt.Errorf("failed to populate the parity shards: %w", err)
	}
	return data,nil
}