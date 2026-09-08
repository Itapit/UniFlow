package ipc_test

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	ipc "senders/internal/IPC"
)
const SocketPath = "/tmp/monitor.sock"

func TestUDSServer(t *testing.T) {
	_ = os.Remove(SocketPath)
	defer os.Remove(SocketPath)

	listener, err := ipc.StartUDSServer(SocketPath)
	if err != nil {
		t.Fatalf("Failed to start UDS server: %v", err)
	}
	defer listener.Close()

	fileChan := make(chan string)
	go ipc.HandleConn(listener, fileChan)

	conn, err := net.Dial("unix", SocketPath)
	if err != nil {
		t.Fatalf("Failed connecting to socket: %v", err)
	}
	defer conn.Close()

	expectedPath := filepath.Clean("/tmp/sample_transfer_file.dat")
	_, err = fmt.Fprintf(conn, "%s\n", expectedPath)
	if err != nil {
		t.Fatalf("Failed to write to socket: %v", err)
	}

	select {
	case receivedPath := <-fileChan:
		if receivedPath != expectedPath {
			t.Errorf("Path mismatch: expected %s, got %s", expectedPath, receivedPath)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for path on channel")
	}
}