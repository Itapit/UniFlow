package ipc

import (
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"

	"senders/internal/pb"

	"google.golang.org/protobuf/proto"
)

func StartUDSServer(socketPath string, logger *slog.Logger) (net.Listener, error) {
	if err := os.Remove(socketPath); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("failed to remove old socket file %s: %w", socketPath, err)
	} else if err == nil {
		logger.Warn("stale socket removed", "socket_path", socketPath, "event", "socket_removed")
	}

	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		return nil, fmt.Errorf("failed to start UDS server on %s: %w", socketPath, err)
	}
	logger.Info("socket listening", "event", "socket_listen", "socket_path", socketPath)

	return listener, nil
}

func HandleConn(listener net.Listener, taskChan chan<- *pb.TaskAssignment, getState func() pb.SenderState, logger *slog.Logger) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			logger.Warn("accept failed", "event", "accept_error", "err", err)
			continue
		}

		go func(c net.Conn) {
			defer c.Close()

			for {
				payload, err := readFramed(c)
				if err != nil {
					if err != io.EOF {
						logger.Warn("socket read failed", "event", "socket_read_error", "err", err)
					}
					return
				}

				task := &pb.TaskAssignment{}
				if err := proto.Unmarshal(payload, task); err == nil && task.GetFilePath() != "" {
					logger.Info("task received",
						"event", "task_received",
						"file_path", task.GetFilePath(),
						"file_hash", task.GetFileHash(),
						"payload_bytes", len(payload))
					taskChan <- task
					continue
				}

				ping := &pb.Ping{}
				if err := proto.Unmarshal(payload, ping); err == nil {
					hb := &pb.Heartbeat{State: getState()}
					if err := writeFramed(c, hb); err != nil {
						logger.Warn("heartbeat reply failed", "event", "heartbeat_error", "err", err)
						return
					}
					continue
				}

				logger.Warn("unrecognized payload",
					"event", "unrecognized_payload",
					"payload_bytes", len(payload))
			}
		}(conn)
	}
}

func readFramed(c net.Conn) ([]byte, error) {
	header := make([]byte, 4)
	if _, err := io.ReadFull(c, header); err != nil {
		return nil, err
	}
	payloadLen := binary.BigEndian.Uint32(header)
	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(c, payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func writeFramed(c net.Conn, msg proto.Message) error {
	payload, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(payload)))

	buffers := net.Buffers{header, payload}
	_, err = buffers.WriteTo(c)
	return err
}
