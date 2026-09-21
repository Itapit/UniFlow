package main

import (
	"encoding/binary"
	"flag"
	"io"
	"log/slog"
	"net"
	"os"
	"time"

	"google.golang.org/protobuf/proto"

	"rs_helper/internal/decode"
	"rs_helper/internal/logger"
	"rs_helper/pb"
)

func main() {
	sockPath := flag.String("sock", "/tmp/uniflow_rs_helper.sock", "Unix socket path to listen on")
	flag.Parse()

	log := logger.New("rs_helper").With("socket_path", *sockPath)

	if err := os.Remove(*sockPath); err != nil && !os.IsNotExist(err) {
		log.Error("stale socket removal failed", "event", "socket_remove_failed", "err", err)
		os.Exit(1)
	} else if err == nil {
		log.Warn("stale socket removed", "event", "socket_removed")
	}

	listener, err := net.Listen("unix", *sockPath)
	if err != nil {
		log.Error("listen failed", "event", "listen_failed", "err", err)
		os.Exit(1)
	}
	defer listener.Close()
	defer func() {
		if err := os.Remove(*sockPath); err != nil && !os.IsNotExist(err) {
			log.Warn("socket cleanup failed", "event", "socket_remove_failed", "err", err)
		} else {
			log.Info("socket removed", "event", "socket_removed")
		}
	}()

	log.Info("rs_helper listening", "event", "listening")
	serve(listener, log)
}

// serve accepts connections until the listener is closed.
func serve(listener net.Listener, logger *slog.Logger) {
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
func handleConnection(conn net.Conn, logger *slog.Logger) {
	defer conn.Close()
	logger.Debug("session manager connected", "event", "peer_connected")

	for {
		req, err := readRequest(conn)
		if err != nil {
			if err != io.EOF {
				logger.Warn("read failed", "event", "read_error", "err", err)
			}
			return
		}

		start := time.Now()
		resp := decode.FromRequest(req)
		durationMs := time.Since(start).Milliseconds()
		if !resp.GetOk() {
			logger.Error("reconstruction failed",
				"event", "reconstruction_failed",
				"file_hash", req.GetFileHash(),
				"block_id", req.GetBlockId(),
				"k_symbols", req.GetKSymbols(),
				"n_symbols", req.GetNSymbols(),
				"shards_rx", len(req.GetShards()),
				"duration_ms", durationMs,
				"err", resp.GetError())
		} else {
			logger.Info("reconstruction ok",
				"event", "reconstruction_ok",
				"file_hash", req.GetFileHash(),
				"block_id", req.GetBlockId(),
				"k_symbols", req.GetKSymbols(),
				"n_symbols", req.GetNSymbols(),
				"shards_rx", len(req.GetShards()),
				"data_shards", len(resp.GetDataShards()),
				"duration_ms", durationMs)
		}

		if err := writeResponse(conn, resp); err != nil {
			logger.Warn("write failed", "event", "write_error", "err", err)
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
