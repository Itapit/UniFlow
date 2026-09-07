package erasure_test

import (
	"bytes"
	"crypto/rand"
	"testing"

	"senders/internal/constants"
	rs "senders/internal/erasure"
	"senders/internal/pb"

	"github.com/klauspost/reedsolomon"
)

func TestEncodeAndVerifyIntegrity(t *testing.T) {
	// בלוק בגודל 50,000 בייטים כדי לוודא טיפול תקין ב-Padding
	originalSize := 50000
	rawBlock := make([]byte, originalSize)
	_, _ = rand.Read(rawBlock)

	encodedShards, err := rs.EncodeBlock(rawBlock)
	if err != nil {
		t.Fatalf("EncodeBlock failed: %v", err)
	}

	totalShards := constants.DefaultDataShrads + constants.DefaultParityShards
	if len(encodedShards) != totalShards {
		t.Fatalf("Expected %d shards, got %d", totalShards, len(encodedShards))
	}

	// בדיקת אימות CRC32 לכל שארד
	for i, shard := range encodedShards {
		crc := pb.CalculateCRC(shard)
		if crc == 0 {
			t.Errorf("Shard %d calculated CRC returned 0", i)
		}
	}

	// סימולציית איבוד של 50 שארדים ראשונים (מקסימום אפשרי)
	for i := 0; i < constants.DefaultParityShards; i++ {
		encodedShards[i] = nil
	}

	enc, err := reedsolomon.New(constants.DefaultDataShrads, constants.DefaultParityShards)
	if err != nil {
		t.Fatal(err)
	}

	err = enc.Reconstruct(encodedShards)
	if err != nil {
		t.Fatalf("Reconstruct failed: %v", err)
	}

	// איחוד שארדי ה-Data
	var reconstructed bytes.Buffer
	for i := 0; i < constants.DefaultDataShrads; i++ {
		reconstructed.Write(encodedShards[i])
	}

	// חיתוך ה-Padding לפי הגודל המקורי
	restoredData := reconstructed.Bytes()[:originalSize]
	if !bytes.Equal(rawBlock, restoredData) {
		t.Fatal("Data corruption: reconstructed bytes do not match original")
	}
}