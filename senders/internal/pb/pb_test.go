package pb_test

import (
	"bytes"
	"testing"

	"senders/internal/pb"

	"google.golang.org/protobuf/proto"
)

func TestFormatPacket(t *testing.T) {
	payload := []byte("packet-payload-test")
	crc := pb.CalculateCRC(payload)

	serialized, err := pb.FormatPacket(
		9999,
		0,
		10,
		1,
		100,
		50,
		134400,
		payload,
		crc,
	)
	if err != nil {
		t.Fatalf("FormatPacket returned error: %v", err)
	}

	var parsed pb.Packet
	if err := proto.Unmarshal(serialized, &parsed); err != nil {
		t.Fatalf("Failed to unmarshal packet: %v", err)
	}

	if parsed.FileHash != 9999 || parsed.BlockId != 0 || parsed.PacketCrc != crc {
		t.Errorf("Packet metadata mismatch")
	}

	if !bytes.Equal(parsed.Content, payload) {
		t.Errorf("Payload mismatch")
	}
}