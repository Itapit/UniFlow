package ipc

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"os"

	"senders/internal/pb"

	"google.golang.org/protobuf/proto"
)

func StartUDSServer(socketPath string) (net.Listener, error) {
	if err := os.Remove(socketPath); err != nil && !os.IsNotExist(err) {
		log.Fatalf("Failed to remove old socket file: %v", err)
	}

	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		return nil, fmt.Errorf("failed to start UDS server: %w", err)
	}

	return listener, nil
}

func HandleConn(listener net.Listener, taskChan chan<- *pb.TaskAssignment, getState func() pb.SenderState) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Accept error: %v", err)
			continue
		}

		go func(c net.Conn) {
			defer c.Close()

			for {
				payload, err := readFramed(c)
				if err != nil {
					if err != io.EOF {
						log.Printf("Socket read error: %v", err)
					}
					return
				}

				task := &pb.TaskAssignment{}
				if err := proto.Unmarshal(payload, task); err == nil && task.GetFilePath() != "" {
					taskChan <- task
					continue
				}

				ping := &pb.Ping{}
				if err := proto.Unmarshal(payload, ping); err == nil {
					hb := &pb.Heartbeat{State: getState()}
					if err := writeFramed(c, hb); err != nil {
						log.Printf("Failed to send heartbeat reply: %v", err)
						return
					}
					continue
				}

				log.Printf("Received unrecognized protobuf payload (%d bytes)", len(payload))
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