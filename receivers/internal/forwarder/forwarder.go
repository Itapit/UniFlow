// Package forwarder accumulates validated packets into fixed-size
// batches and flushes them to the session-manager either when the
// batch is full or when the flush interval elapses, whichever comes
// first. A trailing partial batch never waits longer than one
// interval.
package forwarder

import (
	"log"
	"sync"
	"time"

	"receivers/internal/stats"
	"receivers/pb"

	"google.golang.org/protobuf/proto"
)

var PacketPool = sync.Pool{
	New: func() any {
		return &pb.Packet{}
	},
}

// BatchSender delivers one marshalled batch to the session-manager.
// *sessionClient.Client satisfies it; tests substitute fakes.
type BatchSender interface {
	SendBatch(payload []byte) error
}

// Forwarder owns the batch buffer for one receiver process.
type Forwarder struct {
	receiverID    uint32
	batchSize     int
	flushInterval time.Duration
	intake        <-chan *pb.Packet
	sender        BatchSender
	counters      *stats.Counters
	logger        *log.Logger
}

// NewForwarder builds a forwarder that drains intake and delivers
// batches of up to batchSize packets through sender.
func NewForwarder(
	receiverID uint32,
	batchSize int,
	flushInterval time.Duration,
	intake <-chan *pb.Packet,
	sender BatchSender,
	counters *stats.Counters,
	logger *log.Logger,
) *Forwarder {
	return &Forwarder{
		receiverID:    receiverID,
		batchSize:     batchSize,
		flushInterval: flushInterval,
		intake:        intake,
		sender:        sender,
		counters:      counters,
		logger:        logger,
	}
}

// Run drains the intake channel until the stop channel closes or the
// intake channel is closed, flushing any remainder before returning.
func (forwarder *Forwarder) Run(stopChannel <-chan struct{}) {
	flushTicker := time.NewTicker(forwarder.flushInterval)
	defer flushTicker.Stop()

	batch := make([]*pb.Packet, 0, forwarder.batchSize)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		forwarder.flushBatch(batch)
		batch = make([]*pb.Packet, 0, forwarder.batchSize)
	}

	for {
		select {
		case <-stopChannel:
			// Drain anything already queued so shutdown loses nothing
			// that was accepted, then flush the remainder.
			for {
				select {
				case packet, intakeOpen := <-forwarder.intake:
					if !intakeOpen {
						flush()
						return
					}
					batch = append(batch, packet)
				default:
					flush()
					return
				}
			}
		case packet, intakeOpen := <-forwarder.intake:
			if !intakeOpen {
				flush()
				return
			}
			batch = append(batch, packet)
			if len(batch) >= forwarder.batchSize {
				flush()
			}
		case <-flushTicker.C:
			flush()
		}
	}
}

// flushBatch marshals one SymbolBatch and hands it to the sender,
// accounting every packet as forwarded or IPC-dropped.
func (forwarder *Forwarder) flushBatch(batch []*pb.Packet) {

	defer func() {
		for _, packet := range batch {
			packet.Reset()
			PacketPool.Put(packet)
		}
	}()

	outgoing := &pb.SymbolBatch{
		ReceiverId: forwarder.receiverID,
		Packets:    batch,
	}

	payload, err := proto.Marshal(outgoing)
	if err != nil {
		forwarder.counters.AddIpcDropped()
		forwarder.logger.Printf("forwarder: marshal batch of %d packets: %v", len(batch), err)
		return
	}
	if err := forwarder.sender.SendBatch(payload); err != nil {
		forwarder.counters.AddIpcDropped()
		forwarder.logger.Printf("forwarder: send batch of %d packets: %v", len(batch), err)
		return
	}
	for range batch {
		forwarder.counters.AddForwarded()
	}
	forwarder.counters.AddBatchSent()
	forwarder.logger.Printf("forwarder: batch sent size=%d files=%v", len(batch), distinctFileHashes(batch))
}

// distinctFileHashes lists the file hashes present in a batch for the
// per-batch log line (multi-file visibility without per-packet logs).
func distinctFileHashes(batch []*pb.Packet) []uint64 {
	seen := make(map[uint64]struct{}, len(batch))
	hashes := make([]uint64, 0, len(batch))
	for _, packet := range batch {
		if _, alreadySeen := seen[packet.GetFileHash()]; alreadySeen {
			continue
		}
		seen[packet.GetFileHash()] = struct{}{}
		hashes = append(hashes, packet.GetFileHash())
	}
	return hashes
}
