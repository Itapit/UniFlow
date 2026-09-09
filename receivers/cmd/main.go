package main

import (
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"common/integrity"
	"receivers/internal/forwarder"
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
	udpReadBuffer = 8 << 20
	// shutdownGrace caps how long the forwarder may block flushing (for
	// example while the session-manager is down) before shutdown forces
	// the session client closed.
	shutdownGrace = 10 * time.Second
)

func main() {
	logger := log.New(os.Stdout, "", log.LstdFlags)

	listenAddr := flag.String("listen", ":1400", "UDP address to listen on (IP:Port)")
	receiverID := flag.Uint("receiver-id", 1, "Receiver identity reported in every batch")
	sessionSocket := flag.String("session-socket", "/tmp/uniflow_session.sock", "Session-manager Unix socket path")
	batchSize := flag.Int("batch-size", 30, "Symbols per batch before forced flush")
	flushInterval := flag.Duration("flush-interval", 100*time.Millisecond, "Max delay before a partial batch flushes")
	queueDepth := flag.Int("queue-depth", 1024, "Validated-packet queue between UDP intake and forwarder")
	statsInterval := flag.Duration("stats-interval", 30*time.Second, "Interval between stats log lines")
	flag.Parse()

	counters := &stats.Counters{}

	udpAddr, err := net.ResolveUDPAddr("udp", *listenAddr)
	if err != nil {
		logger.Fatalf("receiver: resolve %s: %v", *listenAddr, err)
	}
	udpConn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		logger.Fatalf("receiver: listen %s: %v", *listenAddr, err)
	}
	if err := udpConn.SetReadBuffer(udpReadBuffer); err != nil {
		logger.Printf("receiver: set read buffer: %v (continuing)", err)
	}

	intake := make(chan *pb.Packet, *queueDepth)
	sessionClient := sessionClient.NewClient(*sessionSocket)
	batchForwarder := forwarder.NewForwarder(
		uint32(*receiverID),
		*batchSize,
		*flushInterval,
		intake,
		sessionClient,
		counters,
		logger,
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
		readLoop(udpConn, intake, counters)
	}()
	go statsLoop(counters, logger, *statsInterval)

	logger.Printf("receiver: id=%d listening on %s, session socket %s", *receiverID, *listenAddr, *sessionSocket)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	logger.Printf("receiver: shutting down")

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
		logger.Printf("receiver: flush grace expired, forcing session client closed")
		_ = sessionClient.Close()
		<-shutdownDone
	}
	_ = sessionClient.Close()
	logger.Printf("receiver: stopped. %s", counters.Snapshot())
}

// readLoop reads datagrams, keeps the valid CRC-checked packets, and
// drops everything else with accounting. It returns when udpConn is
// closed. It never blocks on the intake channel: a full queue means
// the session-manager path is the bottleneck, so packets drop here
// with a counter instead of stalling the socket.
func readLoop(udpConn *net.UDPConn, intake chan<- *pb.Packet, counters *stats.Counters) {
	datagram := make([]byte, readDatagramSize)
	for {
		packetSize, _, err := udpConn.ReadFromUDP(datagram)
		if err != nil {
			return
		}
		counters.AddReceived()
		if packetSize > maxDatagramSize {
			counters.AddMalformed()
			continue
		}

		bufPtr := bufferPool.Get().(*[]byte)
		rawPacket := (*bufPtr)[:packetSize]
		copy(rawPacket, datagram[:packetSize])

		packet := forwarder.PacketPool.Get().(*pb.Packet)
		packet.Reset()

		if err := proto.Unmarshal(rawPacket, packet); err != nil {
			counters.AddMalformed()
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
			counters.AddCrcDropped()
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
			counters.AddIntakeDropped()
			packet.Reset()
			forwarder.PacketPool.Put(packet)
		}
	}
}

// statsLoop logs the counters snapshot until the process exits.
func statsLoop(counters *stats.Counters, logger *log.Logger, statsInterval time.Duration) {
	statsTicker := time.NewTicker(statsInterval)
	defer statsTicker.Stop()
	for range statsTicker.C {
		logger.Printf("receiver: %s", counters.Snapshot())
	}
}
