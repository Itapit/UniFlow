package erasure

import (
	"fmt"

	"github.com/klauspost/reedsolomon"
)

const (
	defaultDataShrads   = 128
	defaultParityShards = 64
	maxShardSize       = 1344
	maxBlockSize        = defaultDataShrads * maxShardSize
)

func EncodeBlock(block []byte) ([][]byte,error) {
	if len(block) > maxBlockSize {
		return nil, fmt.Errorf("block size: %d is to big, max size: %d",len(block),maxBlockSize)
	}
	enc, err:=reedsolomon.New(defaultDataShrads,defaultParityShards);

	if err !=nil{

		return nil, fmt.Errorf("encoder failed to construct: %w",err)

	}
	data:=make([][]byte,defaultDataShrads+defaultParityShards)

	for i :=range defaultDataShrads+defaultParityShards{
		data[i]=make([]byte, maxShardSize)
	}

	for i := range defaultDataShrads {
		start := i * maxShardSize
		if start >= len(block) {
			break 
		}

		end := start + maxShardSize
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