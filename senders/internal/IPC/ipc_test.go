package ipc_test

import (
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	ipc "senders/internal/IPC"
	"senders/internal/pb"

	"google.golang.org/protobuf/proto"
)

const SocketPath = "/tmp/monitor_test.sock"

func TestUDSServer_TaskAssignment(t *testing.T) {
	_ = os.Remove(SocketPath)
	defer os.Remove(SocketPath)

	listener, err := ipc.StartUDSServer(SocketPath)
	if err != nil {
		t.Fatalf("Failed to start UDS server: %v", err)
	}
	defer listener.Close()

	taskChan := make(chan *pb.TaskAssignment)
	// פונקציית סטטוס קבועה לצורך בדיקת קבלת משימה
	getState := func() pb.SenderState {
		return pb.SenderState_IDLE
	}

	go ipc.HandleConn(listener, taskChan, getState)

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

func TestUDSServer_PingHeartbeat(t *testing.T) {
	_ = os.Remove(SocketPath)
	defer os.Remove(SocketPath)

	listener, err := ipc.StartUDSServer(SocketPath)
	if err != nil {
		t.Fatalf("Failed to start UDS server: %v", err)
	}
	defer listener.Close()

	taskChan := make(chan *pb.TaskAssignment)
	var state atomic.Int32
	state.Store(int32(pb.SenderState_WORKING))

	getState := func() pb.SenderState {
		return pb.SenderState(state.Load())
	}

	go ipc.HandleConn(listener, taskChan, getState)

	conn, err := net.Dial("unix", SocketPath)
	if err != nil {
		t.Fatalf("Failed connecting to socket: %v", err)
	}
	defer conn.Close()

	// 1. שליחת הודעת Ping
	pingMsg := &pb.Ping{}
	pingBytes, err := proto.Marshal(pingMsg)
	if err != nil {
		t.Fatalf("Failed to marshal ping: %v", err)
	}

	if _, err := conn.Write(pingBytes); err != nil {
		t.Fatalf("Failed to write ping: %v", err)
	}

	// 2. קריאת תשובת ה-Heartbeat מה-Socket
	buf := make([]byte, 1024)
	_ = conn.SetReadDeadline(time.Now().Add(1 * time.Second))
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("Failed to read heartbeat response: %v", err)
	}

	var hb pb.Heartbeat
	if err := proto.Unmarshal(buf[:n], &hb); err != nil {
		t.Fatalf("Failed to unmarshal heartbeat response: %v", err)
	}

	if hb.GetState() != pb.SenderState_WORKING {
		t.Errorf("Expected state WORKING (%v), got %v", pb.SenderState_WORKING, hb.GetState())
	}
}