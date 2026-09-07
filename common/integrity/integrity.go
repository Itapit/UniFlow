// Package integrity pins the shared checksum algorithm for packet
// contents.
//
// Senders attach the checksum to every packet; receivers recompute it
// to detect bit flips before FEC decoding.
package integrity

import "hash/crc32"

// castagnoliTable is the shared lookup table,
// built once and reused by every Checksum call.
var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

// Checksum returns the CRC32-Castagnoli checksum of data.
func Checksum(data []byte) uint32 {
	return crc32.Checksum(data, castagnoliTable)
}

// Verify reports whether checksum is the valid Checksum of data.
func Verify(data []byte, checksum uint32) bool {
	return Checksum(data) == checksum
}
