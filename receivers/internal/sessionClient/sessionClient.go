// Package sessionClient owns the persistent Unix Domain Socket
// connection to the session-manager and sends length-prefixed
// protobuf batches over it.
//
// Framing matches the TX house standard: 4-byte big-endian length
// prefix followed by the marshalled payload. The connection is
// re-established with exponential backoff; batches offered while
// disconnected are dropped (never buffered unboundedly) so a slow
// session-manager can never stall packet intake.
package sessionClient

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"
)

// Backoff bounds for reconnect attempts.
const (
	initialBackoff = 100 * time.Millisecond
	maxBackoff     = 5 * time.Second
)

// Client maintains the session socket connection. It is safe for use
// from a single sender goroutine (the forwarder).
type Client struct {
	sessionSocketPath string
	connection        net.Conn
	currentBackoff    time.Duration
	logger            *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
}

// NewClient builds a disconnected client for the given socket path.
// The first SendBatch dials lazily. A nil logger maps to a discard logger.
func NewClient(sessionSocketPath string, logger *slog.Logger) *Client {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Client{
		sessionSocketPath: sessionSocketPath,
		currentBackoff:    initialBackoff,
		logger:            logger.With("session_socket", sessionSocketPath),
		ctx:               ctx,
		cancel:            cancel,
	}
}

// SendBatch writes one framed payload to the session-manager. It
// returns nil on success. If the session-manager is unreachable it
// drops the payload and returns an error so the caller can account
// the loss. After Close no further sends are attempted.
func (client *Client) SendBatch(payload []byte) error {
	client.mu.Lock()
	defer client.mu.Unlock()

	select {
	case <-client.ctx.Done():
		return fmt.Errorf("sessionclient: closed")
	default:
	}

	if client.connection == nil {
		if err := client.reconnectLocked(); err != nil {
			return err
		}
	}

	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(payload)))

	// Vector write avoids allocating a single combined frame buffer
	buffers := net.Buffers{header[:], payload}
	if _, err := buffers.WriteTo(client.connection); err != nil {
		client.logger.Warn("session write failed, dropping batch",
			"event", "session_write_failed",
			"payload_bytes", len(payload),
			"err", err)
		client.closeConnectionLocked()
		return fmt.Errorf("sessionclient: write to %s: %w", client.sessionSocketPath, err)
	}
	return nil
}
func (client *Client) reconnectLocked() error {
	var dialer net.Dialer
	attempt := 0
	for {
		select {
		case <-client.ctx.Done():
			return fmt.Errorf("sessionclient: closed while reconnecting to %s", client.sessionSocketPath)
		default:
		}

		conn, err := dialer.DialContext(client.ctx, "unix", client.sessionSocketPath)
		if err == nil {
			client.connection = conn
			client.currentBackoff = initialBackoff
			if attempt > 0 {
				client.logger.Info("session reconnected",
					"event", "session_reconnected",
					"attempts", attempt+1)
			} else {
				client.logger.Info("session connected", "event", "session_connected")
			}
			return nil
		}

		attempt++
		client.logger.Warn("session reconnect pending",
			"event", "session_reconnect_attempt",
			"attempt", attempt,
			"backoff_ms", client.currentBackoff.Milliseconds(),
			"err", err)

		timer := time.NewTimer(client.currentBackoff)
		select {
		case <-client.ctx.Done():
			timer.Stop()
			return fmt.Errorf("sessionclient: closed while reconnecting to %s", client.sessionSocketPath)
		case <-timer.C:
		}

		client.currentBackoff *= 2
		if client.currentBackoff > maxBackoff {
			client.currentBackoff = maxBackoff
		}
	}
}

func (client *Client) Close() error {
	client.cancel()
	client.mu.Lock()
	defer client.mu.Unlock()

	return client.closeConnectionLocked()
}

func (client *Client) closeConnectionLocked() error {
	if client.connection == nil {
		return nil
	}
	err := client.connection.Close()
	client.connection = nil
	client.logger.Info("session connection closed", "event", "session_closed")
	return err
}
