package integrity

import (
	"testing"
)

// referencePacket returns one fixed set of packet inputs shared by the
// packet-scope tests.
func referencePacket() (uint64, uint32, uint32, uint32, uint32, uint32, uint64, []byte) {
	return 0x0102030405060708,
		11,
		77,
		5,
		100,
		150,
		1048576,
		[]byte("reference-symbol-payload")
}

func checksumReference(t *testing.T) uint32 {
	t.Helper()
	fileHash, blockID, totalBlocks, symbolID, dataSymbols, totalSymbols, fileSize, content := referencePacket()
	return ChecksumPacket(fileHash, blockID, totalBlocks, symbolID, dataSymbols, totalSymbols, fileSize, content)
}

func TestChecksumPacketDeterministic(t *testing.T) {
	first := checksumReference(t)
	second := checksumReference(t)
	if first != second {
		t.Fatalf("same inputs must give same checksum, got %#x and %#x", first, second)
	}
}

func TestChecksumPacketCoversEveryField(t *testing.T) {
	fileHash, blockID, totalBlocks, symbolID, dataSymbols, totalSymbols, fileSize, content := referencePacket()
	original := ChecksumPacket(fileHash, blockID, totalBlocks, symbolID, dataSymbols, totalSymbols, fileSize, content)

	variants := []struct {
		fieldName string
		checksum  uint32
	}{
		{"fileHash", ChecksumPacket(fileHash+1, blockID, totalBlocks, symbolID, dataSymbols, totalSymbols, fileSize, content)},
		{"blockID", ChecksumPacket(fileHash, blockID+1, totalBlocks, symbolID, dataSymbols, totalSymbols, fileSize, content)},
		{"totalBlocks", ChecksumPacket(fileHash, blockID, totalBlocks+1, symbolID, dataSymbols, totalSymbols, fileSize, content)},
		{"symbolID", ChecksumPacket(fileHash, blockID, totalBlocks, symbolID+1, dataSymbols, totalSymbols, fileSize, content)},
		{"dataSymbols", ChecksumPacket(fileHash, blockID, totalBlocks, symbolID, dataSymbols+1, totalSymbols, fileSize, content)},
		{"totalSymbols", ChecksumPacket(fileHash, blockID, totalBlocks, symbolID, dataSymbols, totalSymbols+1, fileSize, content)},
		{"fileSize", ChecksumPacket(fileHash, blockID, totalBlocks, symbolID, dataSymbols, totalSymbols, fileSize+1, content)},
	}
	for _, variant := range variants {
		if variant.checksum == original {
			t.Fatalf("changing %s did not change the checksum", variant.fieldName)
		}
	}

	flippedContent := append([]byte(nil), content...)
	flippedContent[0] ^= 0x01
	if flipped := ChecksumPacket(fileHash, blockID, totalBlocks, symbolID, dataSymbols, totalSymbols, fileSize, flippedContent); flipped == original {
		t.Fatal("flipping one content bit did not change the checksum")
	}
}

func TestVerifyPacketRoundTrip(t *testing.T) {
	fileHash, blockID, totalBlocks, symbolID, dataSymbols, totalSymbols, fileSize, content := referencePacket()
	checksum := ChecksumPacket(fileHash, blockID, totalBlocks, symbolID, dataSymbols, totalSymbols, fileSize, content)

	if !VerifyPacket(fileHash, blockID, totalBlocks, symbolID, dataSymbols, totalSymbols, fileSize, content, checksum) {
		t.Fatal("valid packet should verify")
	}
	if VerifyPacket(fileHash, blockID, totalBlocks, symbolID, dataSymbols, totalSymbols, fileSize, content, checksum+1) {
		t.Fatal("tampered checksum should not verify")
	}
	// In-range metadata corruption (the silent class content-only CRC
	// misses) must fail verification against the original checksum.
	if VerifyPacket(fileHash, blockID, totalBlocks, symbolID+1, dataSymbols, totalSymbols, fileSize, content, checksum) {
		t.Fatal("corrupted symbol id should not verify")
	}
	if VerifyPacket(fileHash, blockID+1, totalBlocks, symbolID, dataSymbols, totalSymbols, fileSize, content, checksum) {
		t.Fatal("corrupted block id should not verify")
	}
}

func TestChecksumPacketGoldenVector(t *testing.T) {
	// Pinned output of the documented layout. If this changes, the wire
	// format changed and senders/receivers must redeploy together.
	const expected = 0x38309A76
	if actual := checksumReference(t); actual != expected {
		t.Fatalf("golden vector: want %#x, got %#x", expected, actual)
	}
}
