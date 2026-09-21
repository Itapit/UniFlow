package ipc

import (
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

			buf := make([]byte, 4096)
			for {
				n, err := c.Read(buf)
				if err != nil {
					if err != io.EOF {
						log.Printf("Socket read error: %v", err)
					}
					break
				}

				payload := buf[:n]

				task := &pb.TaskAssignment{}
				if err := proto.Unmarshal(payload, task); err == nil && task.GetFilePath() != "" {
					taskChan <- task
					continue
				}

				ping := &pb.Ping{}
				if err := proto.Unmarshal(payload, ping); err == nil {
					currentState := getState()
					hb := &pb.Heartbeat{
						State: currentState,
					}
					replyBytes, err := proto.Marshal(hb)
					if err != nil {
						log.Printf("Failed to marshal heartbeat: %v", err)
						continue
					}
					if _, err := c.Write(replyBytes); err != nil {
						log.Printf("Failed to send heartbeat reply: %v", err)
						break
					}
					continue
				}

				log.Printf("Received unrecognized protobuf payload (%d bytes)", n)
			}
		}(conn)
	}
}