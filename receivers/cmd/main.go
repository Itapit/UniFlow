package main

import (
	"flag"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"common/integrity"
	"receivers/internal/forwarder"
	"receivers/internal/logger"
	"receivers/internal/sessionClient"
	"receivers/internal/stats"
	"receivers/pb"

	"google.golang.org/protobuf/proto"
)

var bufferPool = sync.Pool{
	New: func() any {
		b := make([]byte, maxDatagramSize)
		return &b
	},
}

// UDP and queue tuning.
const (
	// maxDatagramSize bounds accepted packets. Valid packets stay under
	// ~1400B on the wire; anything far larger is garbage worth dropping
	// early without paying protobuf unmarshal cost.
	maxDatagramSize = 2048
	// readDatagramSize fits any UDP datagram so oversized ones arrive
	// whole and are counted as malformed instead of being truncated.
	readDatagramSize = 65535
	// udpReadBuffer sizes the kernel receive buffer against bursts.
	// 32MB absorbs a full 17MB-file burst (~20k datagrams) even when the
	// session-manager briefly lags on RS decode.
	udpReadBuffer = 32 << 20
	// shutdownGrace caps how long the forwarder may block flushing (for
	// example while the session-manager is down) before shutdown forces
	// the session client closed.
	shutdownGrace = 10 * time.Second
	// dropLogEvery throttles per-drop warn lines: only every Nth drop
	// of each class is logged, the rest are counter-only.
	dropLogEvery = 1000
)

func main() {
	listenAddr := flag.String("listen", ":1400", "UDP address to listen on (IP:Port)")
	receiverID := flag.Uint("receiver-id", 1, "Receiver identity reported in every batch")
	sessionSocket := flag.String("session-socket", "/tmp/uniflow_session.sock", "Session-manager Unix socket path")
	batchSize := flag.Int("batch-size", 50, "Symbols per batch before forced flush")
	flushInterval := flag.Duration("flush-interval", 20*time.Millisecond, "Max delay before a partial batch flushes")
	queueDepth := flag.Int("queue-depth", 8192, "Validated-packet queue between UDP intake and forwarder")
	statsInterval := flag.Duration("stats-interval", 30*time.Second, "Interval between stats log lines")
	flag.Parse()

	baseLog := logger.New("receiver").With("receiver_id", *receiverID)

	counters := &stats.Counters{}

	udpAddr, err := net.ResolveUDPAddr("udp", *listenAddr)
	if err != nil {
		baseLog.Error("resolve failed", "event", "resolve_failed", "listen", *listenAddr, "err", err)
		os.Exit(1)
	}
	udpConn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		baseLog.Error("listen failed", "event", "listen_failed", "listen", *listenAddr, "err", err)
		os.Exit(1)
	}
	if err := udpConn.SetReadBuffer(udpReadBuffer); err != nil {
		baseLog.Warn("set read buffer failed, continuing", "event", "read_buffer_failed", "err", err)
	}

	intake := make(chan *pb.Packet, *queueDepth)
	sessionClient := sessionClient.NewClient(*sessionSocket, baseLog)
	batchForwarder := forwarder.NewForwarder(
		uint32(*receiverID),
		*batchSize,
		*flushInterval,
		intake,
		sessionClient,
		counters,
		baseLog,
	)

	var workerGroup sync.WaitGroup
	workerGroup.Add(2)
	// Intake is never closed by the forwarder; nil stop means shutdown
	// is driven by closing the intake channel below.
	go func() {
		defer workerGroup.Done()
		batchForwarder.Run(nil)
	}()
	go func() {
		defer workerGroup.Done()
		readLoop(udpConn, intake, counters, baseLog)
	}()
	go statsLoop(counters, baseLog, *statsInterval)

	baseLog.Info("receiver listening",
		"event", "receiver_listening",
		"listen", *listenAddr,
		"session_socket", *sessionSocket,
		"batch_size", *batchSize,
		"flush_interval_ms", flushInterval.Milliseconds(),
		"queue_depth", *queueDepth)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	baseLog.Info("shutting down", "event", "shutdown")

	// Stop intake first so nothing new enters the queue, then let the
	// forwarder drain and flush its remainder.
	_ = udpConn.Close()
	close(intake)

	shutdownDone := make(chan struct{})
	go func() {
		workerGroup.Wait()
		close(shutdownDone)
	}()
	select {
	case <-shutdownDone:
	case <-time.After(shutdownGrace):
		baseLog.Warn("flush grace expired, forcing session client closed", "event", "flush_grace_expired")
		_ = sessionClient.Close()
		<-shutdownDone
	}
	_ = sessionClient.Close()
	baseLog.Info("receiver stopped",
		"event", "receiver_stopped",
		"received", counters.Received(),
		"forwarded", counters.Forwarded(),
		"batches", counters.BatchesSent(),
		"crc_dropped", counters.CrcDropped(),
		"malformed", counters.Malformed(),
		"intake_dropped", counters.IntakeDropped(),
		"ipc_dropped", counters.IpcDropped())
}

// readLoop reads datagrams, keeps the valid CRC-checked packets, and
// drops everything else with accounting. It returns when udpConn is
// closed. It never blocks on the intake channel: a full queue means
// the session-manager path is the bottleneck, so packets drop here
// with a counter instead of stalling the socket.
func readLoop(udpConn *net.UDPConn, intake chan<- *pb.Packet, counters *stats.Counters, log interface {
	Info(string, ...any)
	Warn(string, ...any)
}) {
	datagram := make([]byte, readDatagramSize)
	for {
		packetSize, _, err := udpConn.ReadFromUDP(datagram)
		if err != nil {
			return
		}
		counters.AddReceived()
		if packetSize > maxDatagramSize {
			n := counters.AddMalformed()
			if n%dropLogEvery == 1 {
				log.Warn("datagram dropped",
					"event", "packet_dropped",
					"reason", "oversize",
					"packet_bytes", packetSize,
					"dropped_total", n)
			}
			continue
		}

		bufPtr := bufferPool.Get().(*[]byte)
		rawPacket := (*bufPtr)[:packetSize]
		copy(rawPacket, datagram[:packetSize])

		packet := forwarder.PacketPool.Get().(*pb.Packet)
		packet.Reset()

		if err := proto.Unmarshal(rawPacket, packet); err != nil {
			n := counters.AddMalformed()
			if n%dropLogEvery == 1 {
				log.Warn("datagram dropped",
					"event", "packet_dropped",
					"reason", "malformed",
					"dropped_total", n,
					"err", err)
			}
			forwarder.PacketPool.Put(packet)
			bufferPool.Put(bufPtr)
			continue
		}
		if !integrity.VerifyPacket(
			packet.GetFileHash(),
			packet.GetBlockId(),
			packet.GetTotalBlocks(),
			packet.GetSymbolId(),
			packet.GetKSymbols(),
			packet.GetNSymbols(),
			packet.GetFileSize(),
			packet.GetContent(),
			packet.GetPacketCrc(),
		) {
			n := counters.AddCrcDropped()
			if n%dropLogEvery == 1 {
				log.Warn("datagram dropped",
					"event", "packet_dropped",
					"reason", "crc",
					"file_hash", packet.GetFileHash(),
					"block_id", packet.GetBlockId(),
					"symbol_id", packet.GetSymbolId(),
					"dropped_total", n)
			}
			forwarder.PacketPool.Put(packet)
			bufferPool.Put(bufPtr)
			continue
		}

		ownedContent := make([]byte, len(packet.Content))
		copy(ownedContent, packet.Content)
		packet.Content = ownedContent
		bufferPool.Put(bufPtr)

		select {
		case intake <- packet:
		default:
			n := counters.AddIntakeDropped()
			if n%dropLogEvery == 1 {
				log.Warn("datagram dropped",
					"event", "packet_dropped",
					"reason", "intake_full",
					"file_hash", packet.GetFileHash(),
					"block_id", packet.GetBlockId(),
					"dropped_total", n)
			}
			packet.Reset()
			forwarder.PacketPool.Put(packet)
		}
	}
}

// statsLoop logs the counters snapshot until the process exits.
func statsLoop(counters *stats.Counters, log interface {
	Info(string, ...any)
}, statsInterval time.Duration) {
	statsTicker := time.NewTicker(statsInterval)
	defer statsTicker.Stop()
	for range statsTicker.C {
		log.Info("stats",
			"event", "stats",
			"received", counters.Received(),
			"forwarded", counters.Forwarded(),
			"batches", counters.BatchesSent(),
			"crc_dropped", counters.CrcDropped(),
			"malformed", counters.Malformed(),
			"intake_dropped", counters.IntakeDropped(),
			"ipc_dropped", counters.IpcDropped())
	}
}
