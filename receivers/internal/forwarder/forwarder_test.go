package forwarder

import (
	"log"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"receivers/internal/stats"
	"receivers/pb"

	"google.golang.org/protobuf/proto"
)

// fakeSender records delivered payloads and optionally fails. It is
// safe for concurrent use between the forwarder goroutine and test
// assertions.
type fakeSender struct {
	mutex     sync.Mutex
	delivered [][]byte
	failNext  bool
}

func (sender *fakeSender) SendBatch(payload []byte) error {
	sender.mutex.Lock()
	defer sender.mutex.Unlock()
	if sender.failNext {
		sender.failNext = false
		return errFakeSend
	}
	sender.delivered = append(sender.delivered, payload)
	return nil
}

// deliveredCount returns how many payloads arrived so far.
func (sender *fakeSender) deliveredCount() int {
	sender.mutex.Lock()
	defer sender.mutex.Unlock()
	return len(sender.delivered)
}

// deliveredPayload returns a copy of one delivered payload.
func (sender *fakeSender) deliveredPayload(index int) []byte {
	sender.mutex.Lock()
	defer sender.mutex.Unlock()
	return append([]byte(nil), sender.delivered[index]...)
}

type fakeSendError struct{}

func (err fakeSendError) Error() string { return "fake send failure" }

var errFakeSend = fakeSendError{}

func testPacket(fileHash uint64, blockID uint32, symbolID uint32) *pb.Packet {
	return &pb.Packet{
		FileHash:    fileHash,
		BlockId:     blockID,
		TotalBlocks: 4,
		SymbolId:    symbolID,
		KSymbols:    100,
		NSymbols:    150,
		FileSize:    1024,
		Content:     []byte{byte(symbolID)},
		PacketCrc:   42,
	}
}

func runForwarder(
	batchSize int,
	flushInterval time.Duration,
	feed []*pb.Packet,
	sender *fakeSender,
	counters *stats.Counters,
) {
	intake := make(chan *pb.Packet, len(feed)+1)
	forward := NewForwarder(7, batchSize, flushInterval, intake, sender, counters, log.New(os.Stdout, "test ", 0))
	done := make(chan struct{})
	go func() {
		forward.Run(nil)
		close(done)
	}()
	for _, packet := range feed {
		intake <- packet
	}
	close(intake)
	<-done
}

func waitForBatches(sender *fakeSender, want int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if sender.deliveredCount() >= want {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

func TestFlushOnFullBatch(t *testing.T) {
	sender := &fakeSender{}
	counters := &stats.Counters{}
	feed := make([]*pb.Packet, 0, 30)
	for symbol := uint32(0); symbol < 30; symbol++ {
		feed = append(feed, testPacket(111, 0, symbol))
	}
	runForwarder(30, time.Minute, feed, sender, counters)

	if sender.deliveredCount() != 1 {
		t.Fatalf("want 1 batch, got %d", sender.deliveredCount())
	}
	batch := &pb.SymbolBatch{}
	if err := proto.Unmarshal(sender.deliveredPayload(0), batch); err != nil {
		t.Fatalf("unmarshal delivered batch: %v", err)
	}
	if batch.GetReceiverId() != 7 {
		t.Fatalf("want receiver 7, got %d", batch.GetReceiverId())
	}
	if len(batch.GetPackets()) != 30 {
		t.Fatalf("want 30 packets, got %d", len(batch.GetPackets()))
	}
}

func TestFlushPartialBatchOnInterval(t *testing.T) {
	sender := &fakeSender{}
	counters := &stats.Counters{}
	intake := make(chan *pb.Packet, 8)
	forward := NewForwarder(7, 30, 20*time.Millisecond, intake, sender, counters, log.New(os.Stdout, "test ", 0))
	done := make(chan struct{})
	go func() {
		forward.Run(nil)
		close(done)
	}()
	for symbol := uint32(0); symbol < 5; symbol++ {
		intake <- testPacket(222, 1, symbol)
	}
	if !waitForBatches(sender, 1, 2*time.Second) {
		t.Fatal("partial batch did not flush on interval")
	}
	batch := &pb.SymbolBatch{}
	if err := proto.Unmarshal(sender.deliveredPayload(0), batch); err != nil {
		t.Fatalf("unmarshal delivered batch: %v", err)
	}
	if len(batch.GetPackets()) != 5 {
		t.Fatalf("want 5 packets, got %d", len(batch.GetPackets()))
	}
	close(intake)
	<-done
}

func TestSendFailureCounted(t *testing.T) {
	sender := &fakeSender{failNext: true}
	counters := &stats.Counters{}
	feed := []*pb.Packet{testPacket(333, 2, 0)}
	runForwarder(30, time.Minute, feed, sender, counters)

	if sender.deliveredCount() != 0 {
		t.Fatalf("want 0 delivered batches, got %d", sender.deliveredCount())
	}
	snapshot := counters.Snapshot()
	for _, want := range []string{"forwarded=0", "batches=0", "ipc_dropped=1"} {
		if !strings.Contains(snapshot, want) {
			t.Fatalf("counters %q missing %q", snapshot, want)
		}
	}
}

func TestStopFlushesRemainder(t *testing.T) {
	sender := &fakeSender{}
	counters := &stats.Counters{}
	intake := make(chan *pb.Packet, 8)
	stop := make(chan struct{})
	forward := NewForwarder(7, 30, time.Minute, intake, sender, counters, log.New(os.Stdout, "test ", 0))
	done := make(chan struct{})
	go func() {
		forward.Run(stop)
		close(done)
	}()
	intake <- testPacket(444, 3, 0)
	intake <- testPacket(444, 3, 1)
	close(stop)
	<-done
	if sender.deliveredCount() != 1 {
		t.Fatalf("want 1 batch from remainder flush, got %d", sender.deliveredCount())
	}
	batch := &pb.SymbolBatch{}
	if err := proto.Unmarshal(sender.deliveredPayload(0), batch); err != nil {
		t.Fatalf("unmarshal delivered batch: %v", err)
	}
	if len(batch.GetPackets()) != 2 {
		t.Fatalf("want 2 packets, got %d", len(batch.GetPackets()))
	}
}
