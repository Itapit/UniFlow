package ipc

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
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

func HandleConn(listener net.Listener, fileChan chan<- string) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Accept error: %v", err)
			continue
		}

		go func(c net.Conn) {
			defer c.Close()
			scanner := bufio.NewScanner(c)
			for scanner.Scan() {
				path := strings.TrimSpace(scanner.Text())
				if path != "" {
					fileChan <- path 
				}
			}
			if err := scanner.Err(); err != nil && err != io.EOF {
				log.Printf("Socket read error: %v", err)
			}
		}(conn)
	}
}