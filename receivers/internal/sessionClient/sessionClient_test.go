package sessionClient

import (
	"encoding/binary"
	"net"
	"path/filepath"
	"testing"
	"time"
)

// startMockServer listens on a temp unix socket and delivers each
// framed payload to the received channel.
func startMockServer(t *testing.T, received chan<- []byte) (socketPath string, stop func()) {
	t.Helper()
	socketPath = filepath.Join(t.TempDir(), "session.sock")
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatalf("listen mock server: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			go serveMockConnection(connection, received)
		}
	}()
	return socketPath, func() {
		listener.Close()
		<-done
	}
}

func serveMockConnection(connection net.Conn, received chan<- []byte) {
	defer connection.Close()
	header := make([]byte, 4)
	for {
		if _, err := readFull(connection, header); err != nil {
			return
		}
		payload := make([]byte, binary.BigEndian.Uint32(header))
		if _, err := readFull(connection, payload); err != nil {
			return
		}
		received <- payload
	}
}

func readFull(connection net.Conn, buffer []byte) (int, error) {
	filled := 0
	for filled < len(buffer) {
		read, err := connection.Read(buffer[filled:])
		filled += read
		if err != nil {
			return filled, err
		}
	}
	return filled, nil
}

func TestSendBatchFramed(t *testing.T) {
	received := make(chan []byte, 4)
	socketPath, stop := startMockServer(t, received)
	defer stop()

	client := NewClient(socketPath)
	defer client.Close()

	payload := []byte("batch-payload-bytes")
	if err := client.SendBatch(payload); err != nil {
		t.Fatalf("send batch: %v", err)
	}
	select {
	case got := <-received:
		if string(got) != string(payload) {
			t.Fatalf("want %q, got %q", payload, got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for batch at mock server")
	}
}

func TestSendBatchMultipleOverOneConnection(t *testing.T) {
	received := make(chan []byte, 8)
	socketPath, stop := startMockServer(t, received)
	defer stop()

	client := NewClient(socketPath)
	defer client.Close()

	for index := 0; index < 3; index++ {
		if err := client.SendBatch([]byte{byte(index)}); err != nil {
			t.Fatalf("send batch %d: %v", index, err)
		}
	}
	for index := 0; index < 3; index++ {
		select {
		case got := <-received:
			if len(got) != 1 || got[0] != byte(index) {
				t.Fatalf("batch %d: want [%d], got %v", index, index, got)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for batch %d", index)
		}
	}
}

func TestSendBatchUnblocksOnClose(t *testing.T) {
	missingSocket := filepath.Join(t.TempDir(), "nobody-listens.sock")
	client := NewClient(missingSocket)

	sendResult := make(chan error, 1)
	go func() {
		sendResult <- client.SendBatch([]byte("lost"))
	}()

	select {
	case <-sendResult:
		t.Fatal("send should block in reconnect while nobody listens")
	case <-time.After(300 * time.Millisecond):
	}

	if err := client.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	select {
	case err := <-sendResult:
		if err == nil {
			t.Fatal("want error after close, got nil")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("send did not unblock after close")
	}

	if err := client.Close(); err != nil {
		t.Fatalf("second close should be harmless, got: %v", err)
	}
}
