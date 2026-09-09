package ipc_test

import (
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	ipc "senders/internal/IPC"
	"senders/internal/pb"

	"google.golang.org/protobuf/proto"
)

const SocketPath = "/tmp/monitor_test.sock"

func TestUDSServer(t *testing.T) {
	_ = os.Remove(SocketPath)
	defer os.Remove(SocketPath)

	listener, err := ipc.StartUDSServer(SocketPath)
	if err != nil {
		t.Fatalf("Failed to start UDS server: %v", err)
	}
	defer listener.Close()

	taskChan := make(chan *pb.TaskAssignment)
	go ipc.HandleConn(listener, taskChan)

	conn, err := net.Dial("unix", SocketPath)
	if err != nil {
		t.Fatalf("Failed connecting to socket: %v", err)
	}
	defer conn.Close()

	expectedTask := &pb.TaskAssignment{
		FilePath: filepath.Clean("/tmp/sample_transfer_file.dat"),
		FileHash: 0xDEADBEEFCAFEBABE,
	}

	payload, err := proto.Marshal(expectedTask)
	if err != nil {
		t.Fatalf("Failed to marshal protobuf message: %v", err)
	}

	_, err = conn.Write(payload)
	if err != nil {
		t.Fatalf("Failed to write protobuf payload to socket: %v", err)
	}

	select {
	case receivedTask := <-taskChan:
		if receivedTask.GetFilePath() != expectedTask.GetFilePath() {
			t.Errorf("Path mismatch: expected %s, got %s", expectedTask.GetFilePath(), receivedTask.GetFilePath())
		}
		if receivedTask.GetFileHash() != expectedTask.GetFileHash() {
			t.Errorf("Hash mismatch: expected %d, got %d", expectedTask.GetFileHash(), receivedTask.GetFileHash())
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for task on channel")
	}
}