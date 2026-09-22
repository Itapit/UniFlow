package pb_test

import (
	"bytes"
	"testing"

	"senders/internal/pb"

	"google.golang.org/protobuf/proto"
)

func TestFormatPacketAndCRC(t *testing.T) {
	fileHash := uint64(9999)
	blockId := uint32(0)
	totalBlocks := uint32(10)
	symbolId := uint32(1)
	kSymbols := uint32(100)
	nSymbols := uint32(50)
	fileSize := uint64(134400)
	fileName := "report"
	fileExt := ".pdf"
	payload := []byte("packet-payload-test")

	crc := pb.CalculateCRC(
		fileHash,
		blockId,
		totalBlocks,
		symbolId,
		kSymbols,
		nSymbols,
		fileSize,
		fileName,
		fileExt,
		payload,
	)

	serialized, err := pb.FormatPacket(
		fileHash,
		blockId,
		totalBlocks,
		symbolId,
		kSymbols,
		nSymbols,
		fileSize,
		payload,
		crc,
		fileName,
		fileExt,
	)
	if err != nil {
		t.Fatalf("FormatPacket returned error: %v", err)
	}

	var parsed pb.Packet
	if err := proto.Unmarshal(serialized, &parsed); err != nil {
		t.Fatalf("Failed to unmarshal packet: %v", err)
	}

	if parsed.FileHash != fileHash ||
		parsed.BlockId != blockId ||
		parsed.TotalBlocks != totalBlocks ||
		parsed.SymbolId != symbolId ||
		parsed.KSymbols != kSymbols ||
		parsed.NSymbols != nSymbols ||
		parsed.FileSize != fileSize ||
		parsed.FileName != fileName ||
		parsed.FileExt != fileExt ||
		parsed.PacketCrc != crc {
		t.Errorf("Packet metadata or CRC mismatch")
	}

	if !bytes.Equal(parsed.Content, payload) {
		t.Errorf("Payload mismatch")
	}

	recomputedCRC := pb.CalculateCRC(
		parsed.FileHash,
		parsed.BlockId,
		parsed.TotalBlocks,
		parsed.SymbolId,
		parsed.KSymbols,
		parsed.NSymbols,
		parsed.FileSize,
		parsed.FileName,
		parsed.FileExt,
		parsed.Content,
	)
	if recomputedCRC != parsed.PacketCrc {
		t.Errorf("Recomputed CRC mismatch: expected %d, got %d", parsed.PacketCrc, recomputedCRC)
	}

	corruptedCRC := pb.CalculateCRC(
		parsed.FileHash,
		parsed.BlockId+1, // סימולציה של שיבוש ב-blockId
		parsed.TotalBlocks,
		parsed.SymbolId,
		parsed.KSymbols,
		parsed.NSymbols,
		parsed.FileSize,
		parsed.FileName,
		parsed.FileExt,
		parsed.Content,
	)
	if corruptedCRC == parsed.PacketCrc {
		t.Errorf("CRC failed to detect metadata alteration")
	}

	corruptedNameCRC := pb.CalculateCRC(
		parsed.FileHash,
		parsed.BlockId,
		parsed.TotalBlocks,
		parsed.SymbolId,
		parsed.KSymbols,
		parsed.NSymbols,
		parsed.FileSize,
		parsed.FileName+"tampered",
		parsed.FileExt,
		parsed.Content,
	)
	if corruptedNameCRC == parsed.PacketCrc {
		t.Errorf("CRC failed to detect file name alteration")
	}
}