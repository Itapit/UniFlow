// Package integrity is responsible for the checksum algorithm for packets.
//
// Senders attach a packet-scoped checksum to every packet; receivers
// recompute it to detect bit flips in the metadata or the content
package integrity

import (
	"encoding/binary"
	"hash/crc32"
)

// castagnoliTable is the shared lookup table,
// built once and reused by every checksum call.
var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

// packetHeaderSize is the fixed encoded size of the packet metadata
// prefix: 8 + 4 + 4 + 4 + 4 + 4 + 8 bytes. It is part of the
// cross-implementation contract and must never change without a
// coordinated sender/receiver redeploy.
const packetHeaderSize = 36

// ChecksumPacket returns the CRC32-Castagnoli checksum over the packet
// metadata followed by the content. The metadata layout is fixed and
// is itself the contract any reimplementation (for example the sender
// side) must replicate byte for byte:
//
//	offset  size  field (little-endian)
//	0       8     file hash (packet.proto: file_hash)
//	8       4     block id (packet.proto: block_id)
//	12      4     total blocks (packet.proto: total_blocks)
//	16      4     symbol id (packet.proto: symbol_id)
//	20      4     data symbols per block (packet.proto: k_symbols)
//	24      4     total symbols per block (packet.proto: n_symbols)
//	28      8     file size (packet.proto: file_size)
//	36      ...   raw content bytes
//
// The packet_crc field itself is excluded: it carries the checksum.
func ChecksumPacket(
	fileHash uint64,
	blockID uint32,
	totalBlocks uint32,
	symbolID uint32,
	dataSymbols uint32,
	totalSymbols uint32,
	fileSize uint64,
	content []byte,
) uint32 {
	var header [packetHeaderSize]byte
	binary.LittleEndian.PutUint64(header[0:8], fileHash)
	binary.LittleEndian.PutUint32(header[8:12], blockID)
	binary.LittleEndian.PutUint32(header[12:16], totalBlocks)
	binary.LittleEndian.PutUint32(header[16:20], symbolID)
	binary.LittleEndian.PutUint32(header[20:24], dataSymbols)
	binary.LittleEndian.PutUint32(header[24:28], totalSymbols)
	binary.LittleEndian.PutUint64(header[28:36], fileSize)

	crc := crc32.Checksum(header[:], castagnoliTable)
	return crc32.Update(crc, castagnoliTable, content)
}

// VerifyPacket reports whether checksum is the valid ChecksumPacket of
// the given metadata and content.
func VerifyPacket(
	fileHash uint64,
	blockID uint32,
	totalBlocks uint32,
	symbolID uint32,
	dataSymbols uint32,
	totalSymbols uint32,
	fileSize uint64,
	content []byte,
	checksum uint32,
) bool {
	return ChecksumPacket(fileHash, blockID, totalBlocks, symbolID, dataSymbols, totalSymbols, fileSize, content) == checksum
}
