// Package stats holds the receiver's atomically-updated counters and
// renders the periodic status line.
// received == forwarded + crcDropped + malformed + intakeDropped + (packets lost in ipcDropped batches)
//
// ipcDropped track whole batch

package stats

import (
	"fmt"
	"sync/atomic"
)

// Counters tracks per-process packet accounting. Use the Add* methods
// from the hot paths; they are safe for concurrent use.
type Counters struct {
	received      atomic.Uint64
	forwarded     atomic.Uint64
	crcDropped    atomic.Uint64
	malformed     atomic.Uint64
	intakeDropped atomic.Uint64
	ipcDropped    atomic.Uint64
	batchesSent   atomic.Uint64
}

// AddReceived records one datagram read from the UDP socket.
func (counters *Counters) AddReceived() {
	counters.received.Add(1)
}

// AddForwarded records one validated packet handed to a flushed batch.
func (counters *Counters) AddForwarded() {
	counters.forwarded.Add(1)
}

// AddCrcDropped records one packet rejected by checksum verification.
func (counters *Counters) AddCrcDropped() {
	counters.crcDropped.Add(1)
}

// AddMalformed records one datagram that failed protobuf unmarshaling
// or the size guard.
func (counters *Counters) AddMalformed() {
	counters.malformed.Add(1)
}

// AddIntakeDropped records one validated packet dropped because the
// intake channel to the forwarder was full.
func (counters *Counters) AddIntakeDropped() {
	counters.intakeDropped.Add(1)
}

// AddIpcDropped records one batch lost while the session socket was
// disconnected.
func (counters *Counters) AddIpcDropped() {
	counters.ipcDropped.Add(1)
}

// AddBatchSent records one batch successfully written to the session socket.
func (counters *Counters) AddBatchSent() {
	counters.batchesSent.Add(1)
}

// Snapshot renders the current counters as a single log line.
func (counters *Counters) Snapshot() string {
	return fmt.Sprintf(
		"stats received=%d forwarded=%d batches=%d crc_dropped=%d malformed=%d intake_dropped=%d ipc_dropped=%d",
		counters.received.Load(),
		counters.forwarded.Load(),
		counters.batchesSent.Load(),
		counters.crcDropped.Load(),
		counters.malformed.Load(),
		counters.intakeDropped.Load(),
		counters.ipcDropped.Load(),
	)
}
