package main

import (
	"encoding/binary"
	"flag"
	"io"
	"log"
	"net"
	"os"

	"google.golang.org/protobuf/proto"

	"rs_helper/internal/decode"
	"rs_helper/pb"
)

func main() {
	sockPath := flag.String("sock", "/tmp/uniflow_rs_helper.sock", "Unix socket path to listen on")
	flag.Parse()

	logger := log.New(os.Stdout, "", log.LstdFlags)

	if err := os.Remove(*sockPath); err != nil && !os.IsNotExist(err) {
		logger.Fatalf("rs_helper: failed to remove old socket file: %v", err)
	}

	listener, err := net.Listen("unix", *sockPath)
	if err != nil {
		logger.Fatalf("rs_helper: failed to listen on %s: %v", *sockPath, err)
	}
	defer listener.Close()
	defer os.Remove(*sockPath)

	logger.Printf("rs_helper: listening on %s", *sockPath)
	serve(listener, logger)
}

// serve accepts connections until the listener is closed.
func serve(listener net.Listener, logger *log.Logger) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			return // listener closed — normal shutdown, nothing left to log
		}
		go handleConnection(conn, logger)
	}
}

// handleConnection serves one Session Manager connection until it
// disconnects or a framing error occurs. Requests are handled strictly
// in the order they arrive on this connection — there's no request ID
// to multiplex on, so the Python side must wait for each response
// before sending the next request on the same connection.
func handleConnection(conn net.Conn, logger *log.Logger) {
	defer conn.Close()
	logger.Printf("rs_helper: session manager connected")

	for {
		req, err := readRequest(conn)
		if err != nil {
			if err != io.EOF {
				logger.Printf("rs_helper: read error: %v", err)
			}
			return
		}

		resp := decode.FromRequest(req)
		if !resp.GetOk() {
			logger.Printf("rs_helper: reconstruction failed for file_hash=%d block_id=%d: %s",
				req.GetFileHash(), req.GetBlockId(), resp.GetError())
		}

		if err := writeResponse(conn, resp); err != nil {
			logger.Printf("rs_helper: write error: %v", err)
			return
		}
	}
}

func readRequest(conn net.Conn) (*pb.ReconstructRequest, error) {
	header := make([]byte, 4)
	if _, err := io.ReadFull(conn, header); err != nil {
		return nil, err
	}
	payload := make([]byte, binary.BigEndian.Uint32(header))
	if _, err := io.ReadFull(conn, payload); err != nil {
		return nil, err
	}

	req := &pb.ReconstructRequest{}
	if err := proto.Unmarshal(payload, req); err != nil {
		return nil, err
	}
	return req, nil
}

func writeResponse(conn net.Conn, resp *pb.ReconstructResponse) error {
	payload, err := proto.Marshal(resp)
	if err != nil {
		return err
	}
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(payload)))
	buffers := net.Buffers{header, payload}
	_, err = buffers.WriteTo(conn)
	return err
}