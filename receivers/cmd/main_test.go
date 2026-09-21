package main

import (
	"net"
	"strings"
	"testing"
	"time"

	"common/integrity"
	"receivers/internal/stats"
	"receivers/pb"

	"google.golang.org/protobuf/proto"
)

func listenTestSocket(t *testing.T) (*net.UDPConn, *net.UDPAddr) {
	t.Helper()
	listenAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	udpConn, err := net.ListenUDP("udp", listenAddr)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	return udpConn, udpConn.LocalAddr().(*net.UDPAddr)
}

func sendDatagram(t *testing.T, target *net.UDPAddr, datagram []byte) {
	t.Helper()
	sender, err := net.DialUDP("udp", nil, target)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer sender.Close()
	if _, err := sender.Write(datagram); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func marshalPacket(t *testing.T, packet *pb.Packet) []byte {
	t.Helper()
	payload, err := proto.Marshal(packet)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return payload
}

// marshalValidPacket stamps a packet-scoped checksum over the packet's
// own metadata and content before marshaling, mirroring what a
// compliant sender puts on the wire.
func marshalValidPacket(t *testing.T, packet *pb.Packet) []byte {
	t.Helper()
	packet.PacketCrc = integrity.ChecksumPacket(
		packet.GetFileHash(),
		packet.GetBlockId(),
		packet.GetTotalBlocks(),
		packet.GetSymbolId(),
		packet.GetKSymbols(),
		packet.GetNSymbols(),
		packet.GetFileSize(),
		packet.GetContent(),
	)
	return marshalPacket(t, packet)
}

func TestReadLoopAcceptsValidPacket(t *testing.T) {
	udpConn, listenAddr := listenTestSocket(t)
	intake := make(chan *pb.Packet, 4)
	counters := &stats.Counters{}
	done := make(chan struct{})
	go func() {
		readLoop(udpConn, intake, counters)
		close(done)
	}()

	content := []byte("symbol-payload")
	sendDatagram(t, listenAddr, marshalValidPacket(t, &pb.Packet{
		FileHash: 999,
		BlockId:  3,
		SymbolId: 7,
		Content:  content,
	}))

	select {
	case packet := <-intake:
		if packet.GetFileHash() != 999 || packet.GetBlockId() != 3 || packet.GetSymbolId() != 7 {
			t.Fatalf("wrong packet forwarded: %+v", packet)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("valid packet never reached intake")
	}
	udpConn.Close()
	<-done

	snapshot := counters.Snapshot()
	for _, want := range []string{"received=1", "forwarded=0", "crc_dropped=0", "malformed=0"} {
		if !strings.Contains(snapshot, want) {
			t.Fatalf("counters %q missing %q", snapshot, want)
		}
	}
}

func TestReadLoopDropsBadPackets(t *testing.T) {
	udpConn, listenAddr := listenTestSocket(t)
	intake := make(chan *pb.Packet, 8)
	counters := &stats.Counters{}
	done := make(chan struct{})
	go func() {
		readLoop(udpConn, intake, counters)
		close(done)
	}()

	content := []byte("symbol-payload")
	// CRC mismatch: valid protobuf, checksum off by one over the same
	// metadata and content the receiver will verify.
	sendDatagram(t, listenAddr, marshalPacket(t, &pb.Packet{
		FileHash:  1,
		Content:   content,
		PacketCrc: integrity.ChecksumPacket(1, 0, 0, 0, 0, 0, 0, content) + 1,
	}))
	// Truncated varint: invalid protobuf.
	sendDatagram(t, listenAddr, []byte{0xff, 0xff, 0xff, 0xff, 0xff})
	// Oversized datagram.
	oversized := make([]byte, maxDatagramSize+500)
	for index := range oversized {
		oversized[index] = 0xAB
	}
	sendDatagram(t, listenAddr, oversized)

	time.Sleep(300 * time.Millisecond)
	udpConn.Close()
	<-done

	select {
	case packet := <-intake:
		t.Fatalf("no packet should reach intake, got %+v", packet)
	default:
	}
	snapshot := counters.Snapshot()
	for _, want := range []string{"received=3", "crc_dropped=1", "malformed=2"} {
		if !strings.Contains(snapshot, want) {
			t.Fatalf("counters %q missing %q", snapshot, want)
		}
	}
}

func TestReadLoopDropsMetadataCorruption(t *testing.T) {
	udpConn, listenAddr := listenTestSocket(t)
	intake := make(chan *pb.Packet, 8)
	counters := &stats.Counters{}
	done := make(chan struct{})
	go func() {
		readLoop(udpConn, intake, counters)
		close(done)
	}()

	// Stamp a valid checksum, then flip the symbol id in range. A
	// content-only checksum would still accept this packet; the
	// packet-scoped checksum must reject it.
	content := []byte("symbol-payload")
	corruptedPacket := &pb.Packet{
		FileHash: 5,
		BlockId:  2,
		SymbolId: 9,
		Content:  content,
	}
	corruptedPacket.PacketCrc = integrity.ChecksumPacket(
		corruptedPacket.GetFileHash(),
		corruptedPacket.GetBlockId(),
		corruptedPacket.GetTotalBlocks(),
		corruptedPacket.GetSymbolId(),
		corruptedPacket.GetKSymbols(),
		corruptedPacket.GetNSymbols(),
		corruptedPacket.GetFileSize(),
		corruptedPacket.GetContent(),
	)
	corruptedPacket.SymbolId = 10
	sendDatagram(t, listenAddr, marshalPacket(t, corruptedPacket))

	time.Sleep(300 * time.Millisecond)
	udpConn.Close()
	<-done

	select {
	case packet := <-intake:
		t.Fatalf("corrupted packet should not reach intake, got %+v", packet)
	default:
	}
	snapshot := counters.Snapshot()
	for _, want := range []string{"received=1", "crc_dropped=1"} {
		if !strings.Contains(snapshot, want) {
			t.Fatalf("counters %q missing %q", snapshot, want)
		}
	}
}

func TestReadLoopDropsWhenIntakeFull(t *testing.T) {	udpConn, listenAddr := listenTestSocket(t)
	intake := make(chan *pb.Packet) // nobody drains: always full
	counters := &stats.Counters{}
	done := make(chan struct{})
	go func() {
		readLoop(udpConn, intake, counters)
		close(done)
	}()

	content := []byte("x")
	sendDatagram(t, listenAddr, marshalValidPacket(t, &pb.Packet{Content: content}))
	time.Sleep(300 * time.Millisecond)
	udpConn.Close()
	<-done

	snapshot := counters.Snapshot()
	for _, want := range []string{"received=1", "intake_dropped=1"} {
		if !strings.Contains(snapshot, want) {
			t.Fatalf("counters %q missing %q", snapshot, want)
		}
	}
}
