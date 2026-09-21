package ipc_test

import (
	"encoding/binary"
	"io"
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

// Helper to write length-prefixed messages in tests
func writeFramedTestMsg(w io.Writer, msg proto.Message) error {
	payload, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(payload)))
	if _, err := w.Write(header); err != nil {
		return err
	}
	_, err = w.Write(payload)
	return err
}

// Helper to read length-prefixed messages in tests
func readFramedTestMsg(r io.Reader) ([]byte, error) {
	header := make([]byte, 4)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}
	payloadLen := binary.BigEndian.Uint32(header)
	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func TestUDSServer_TaskAssignment(t *testing.T) {
	_ = os.Remove(SocketPath)
	defer os.Remove(SocketPath)

	listener, err := ipc.StartUDSServer(SocketPath)
	if err != nil {
		t.Fatalf("Failed to start UDS server: %v", err)
	}
	defer listener.Close()

	taskChan := make(chan *pb.TaskAssignment)
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

	if err := writeFramedTestMsg(conn, expectedTask); err != nil {
		t.Fatalf("Failed to write framed task: %v", err)
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

	// 1. Send framed Ping
	pingMsg := &pb.Ping{}
	if err := writeFramedTestMsg(conn, pingMsg); err != nil {
		t.Fatalf("Failed to write framed ping: %v", err)
	}

	// 2. Read framed Heartbeat response
	_ = conn.SetReadDeadline(time.Now().Add(1 * time.Second))
	payload, err := readFramedTestMsg(conn)
	if err != nil {
		t.Fatalf("Failed to read framed heartbeat: %v", err)
	}

	var hb pb.Heartbeat
	if err := proto.Unmarshal(payload, &hb); err != nil {
		t.Fatalf("Failed to unmarshal heartbeat response: %v", err)
	}

	if hb.GetState() != pb.SenderState_WORKING {
		t.Errorf("Expected state WORKING (%v), got %v", pb.SenderState_WORKING, hb.GetState())
	}
}