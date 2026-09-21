package main

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"common/erasure"

	"rs_helper/pb"
)

func TestServeReconstructsOverRealSocket(t *testing.T) {
	sockPath := filepath.Join(t.TempDir(), "rs_helper_test.sock")
	listener, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { listener.Close() })

	go serve(listener, log.New(os.Stdout, "test ", 0))

	conn, err := net.Dial("unix", sockPath)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	const dataShards = 100
	const parityShards = 50

	original := make([]byte, 100000)
	if _, err := rand.Read(original); err != nil {
		t.Fatalf("generate random block: %v", err)
	}
	encoded, err := erasure.EncodeBlock(original)
	if err != nil {
		t.Fatalf("EncodeBlock: %v", err)
	}

	req := &pb.ReconstructRequest{FileHash: 7, BlockId: 0, KSymbols: dataShards, NSymbols: dataShards + parityShards}
	for symbolID, shard := range encoded {
		if symbolID%3 == 0 { // drop every third shard — well within the 50-loss tolerance
			continue
		}
		req.Shards = append(req.Shards, &pb.Shard{SymbolId: uint32(symbolID), Content: shard})
	}

	if err := sendFramed(conn, req); err != nil {
		t.Fatalf("send request: %v", err)
	}

	resp := &pb.ReconstructResponse{}
	if err := recvFramed(conn, resp); err != nil {
		t.Fatalf("receive response: %v", err)
	}
	if !resp.GetOk() {
		t.Fatalf("reconstruction reported failure: %s", resp.GetError())
	}

	var reconstructed bytes.Buffer
	for _, shard := range resp.GetDataShards() {
		reconstructed.Write(shard)
	}
	if restored := reconstructed.Bytes()[:len(original)]; !bytes.Equal(original, restored) {
		t.Fatal("data returned over the real socket does not match the original block")
	}
}

// sendFramed/recvFramed mimic exactly what rs_client.py will do on the
// Python side — used here to prove the server's wire framing is correct
// from a real client's perspective, not just decode.FromRequest in isolation.
func sendFramed(conn net.Conn, msg proto.Message) error {
	payload, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(payload)))
	buffers := net.Buffers{header, payload}
	_, err = buffers.WriteTo(conn)
	return err
}

func recvFramed(conn net.Conn, msg proto.Message) error {
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	header := make([]byte, 4)
	if _, err := io.ReadFull(conn, header); err != nil {
		return err
	}
	payload := make([]byte, binary.BigEndian.Uint32(header))
	if _, err := io.ReadFull(conn, payload); err != nil {
		return err
	}
	return proto.Unmarshal(payload, msg)
}