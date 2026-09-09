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

func HandleConn(listener net.Listener, taskChan chan<- *pb.TaskAssignment) {
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

				task := &pb.TaskAssignment{}
				if err := proto.Unmarshal(buf[:n], task); err != nil {
					log.Printf("Failed to unmarshal TaskAssignment: %v", err)
					continue
				}

				if task.GetFilePath() != "" {
					taskChan <- task
				}
			}
		}(conn)
	}
}