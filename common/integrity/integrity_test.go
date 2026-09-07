package integrity

import (
	"bytes"
	"math/rand"
	"testing"
)

func TestStandardVector(t *testing.T) {
	// Published check value of CRC32-Castagnoli over "123456789".
	const expected = 0xE3069283
	if actual := Checksum([]byte("123456789")); actual != expected {
		t.Fatalf("standard vector: want %#x, got %#x", expected, actual)
	}
}

func TestVerifyRoundTrip(t *testing.T) {
	random := rand.New(rand.NewSource(7))
	payload := make([]byte, 1344)
	random.Read(payload)

	checksum := Checksum(payload)
	if !Verify(payload, checksum) {
		t.Fatal("valid payload should verify")
	}
	if Verify(payload, checksum+1) {
		t.Fatal("tampered checksum should not verify")
	}
}

func TestBitFlipDetected(t *testing.T) {
	original := bytes.Repeat([]byte{0xAB}, 512)
	checksum := Checksum(original)

	for _, bit := range []uint{0, 7, 2048, 4095} {
		corrupted := bytes.Clone(original)
		corrupted[bit/8] ^= 1 << (bit % 8)
		if Verify(corrupted, checksum) {
			t.Fatalf("single-bit flip at bit %d went undetected", bit)
		}
	}
}

func TestEmptyPayload(t *testing.T) {
	if !Verify([]byte{}, Checksum([]byte{})) {
		t.Fatal("empty payload should verify against its own checksum")
	}
	if !Verify(nil, Checksum(nil)) {
		t.Fatal("nil payload should verify against its own checksum")
	}
}
