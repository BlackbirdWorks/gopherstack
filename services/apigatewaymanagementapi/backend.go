package apigatewaymanagementapi

import (
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const maxPayloadBytes = 128 * 1024

// InMemoryBackend implements the StorageBackend for API Gateway Management API.
type InMemoryBackend struct {
	connections map[string]*Connection
	messages    map[string][]PostedMessage
	mu          *lockmetrics.RWMutex
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		connections: make(map[string]*Connection),
		messages:    make(map[string][]PostedMessage),
		mu:          lockmetrics.New("apigatewaymanagementapi"),
	}
}

// CreateConnection creates a new simulated WebSocket connection.
func (b *InMemoryBackend) CreateConnection(connectionID, sourceIP, userAgent string) (*Connection, error) {
	b.mu.Lock("CreateConnection")
	defer b.mu.Unlock()

	now := time.Now()
	conn := &Connection{
		ConnectionID: connectionID,
		SourceIP:     sourceIP,
		UserAgent:    userAgent,
		ConnectedAt:  now,
		LastActiveAt: now,
	}
	b.connections[connectionID] = conn

	cp := *conn

	return &cp, nil
}

// PostToConnection sends data to an existing connection and records the message.
func (b *InMemoryBackend) PostToConnection(connectionID string, data []byte) error {
	if len(data) > maxPayloadBytes {
		return fmt.Errorf("%w: payload size %d exceeds maximum %d", ErrPayloadTooLarge, len(data), maxPayloadBytes)
	}

	b.mu.Lock("PostToConnection")
	defer b.mu.Unlock()

	conn, ok := b.connections[connectionID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrConnectionNotFound, connectionID)
	}

	conn.LastActiveAt = time.Now()
	conn.PostedMessages++

	b.messages[connectionID] = append(b.messages[connectionID], PostedMessage{
		ReceivedAt:   conn.LastActiveAt,
		ConnectionID: connectionID,
		Data:         data,
	})

	return nil
}

// GetConnection returns the connection metadata for the given connection ID.
func (b *InMemoryBackend) GetConnection(connectionID string) (*Connection, error) {
	b.mu.RLock("GetConnection")
	defer b.mu.RUnlock()

	conn, ok := b.connections[connectionID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrConnectionNotFound, connectionID)
	}

	cp := *conn

	return &cp, nil
}

// DeleteConnection removes the connection with the given ID.
func (b *InMemoryBackend) DeleteConnection(connectionID string) error {
	b.mu.Lock("DeleteConnection")
	defer b.mu.Unlock()

	if _, ok := b.connections[connectionID]; !ok {
		return fmt.Errorf("%w: %s", ErrConnectionNotFound, connectionID)
	}

	delete(b.connections, connectionID)
	delete(b.messages, connectionID)

	return nil
}

// ListConnections returns all active connections.
func (b *InMemoryBackend) ListConnections() []Connection {
	b.mu.RLock("ListConnections")
	defer b.mu.RUnlock()

	out := make([]Connection, 0, len(b.connections))
	for _, c := range b.connections {
		out = append(out, *c)
	}

	return out
}

// GetMessages returns all messages posted to the given connection.
func (b *InMemoryBackend) GetMessages(connectionID string) []PostedMessage {
	b.mu.RLock("GetMessages")
	defer b.mu.RUnlock()

	msgs := b.messages[connectionID]
	out := make([]PostedMessage, len(msgs))
	copy(out, msgs)

	return out
}
