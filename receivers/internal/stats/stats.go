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
// It returns the new total.
func (counters *Counters) AddCrcDropped() uint64 {
	return counters.crcDropped.Add(1)
}

// AddMalformed records one datagram that failed protobuf unmarshaling
// or the size guard. It returns the new total.
func (counters *Counters) AddMalformed() uint64 {
	return counters.malformed.Add(1)
}

// AddIntakeDropped records one validated packet dropped because the
// intake channel to the forwarder was full. It returns the new total.
func (counters *Counters) AddIntakeDropped() uint64 {
	return counters.intakeDropped.Add(1)
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
		counters.Received(),
		counters.Forwarded(),
		counters.BatchesSent(),
		counters.CrcDropped(),
		counters.Malformed(),
		counters.IntakeDropped(),
		counters.IpcDropped(),
	)
}

// Read-only getters for structured logging.
func (counters *Counters) Received() uint64      { return counters.received.Load() }
func (counters *Counters) Forwarded() uint64     { return counters.forwarded.Load() }
func (counters *Counters) BatchesSent() uint64   { return counters.batchesSent.Load() }
func (counters *Counters) CrcDropped() uint64    { return counters.crcDropped.Load() }
func (counters *Counters) Malformed() uint64     { return counters.malformed.Load() }
func (counters *Counters) IntakeDropped() uint64 { return counters.intakeDropped.Load() }
func (counters *Counters) IpcDropped() uint64    { return counters.ipcDropped.Load() }
