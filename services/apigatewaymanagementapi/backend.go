package apigatewaymanagementapi

import (
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	maxPayloadBytes          = 128 * 1024
	maxMessagesPerConnection = 1000
)

// messageBuffer is a fixed-capacity circular buffer of PostedMessage values.
// PostToConnection runs at write rates that previously triggered a slice
// copy on every overflow; ring storage reduces that to O(1).
type messageBuffer struct {
	items []PostedMessage
	head  int
	size  int
}

func newMessageBuffer(capacity int) *messageBuffer {
	return &messageBuffer{items: make([]PostedMessage, capacity)}
}

func (rb *messageBuffer) push(m PostedMessage) {
	rb.items[rb.head] = m
	rb.head = (rb.head + 1) % len(rb.items)

	if rb.size < len(rb.items) {
		rb.size++
	}
}

// snapshot returns the buffered messages in chronological order (oldest first).
// The returned slice does not alias the internal storage.
func (rb *messageBuffer) snapshot() []PostedMessage {
	if rb == nil || rb.size == 0 {
		return []PostedMessage{}
	}

	out := make([]PostedMessage, rb.size)
	capacity := len(rb.items)
	start := (rb.head - rb.size + capacity) % capacity

	if start+rb.size <= capacity {
		copy(out, rb.items[start:start+rb.size])

		return out
	}

	n := capacity - start
	copy(out, rb.items[start:])
	copy(out[n:], rb.items[:rb.size-n])

	return out
}

// loadOrdered seeds the buffer from a chronological slice (oldest first).
// Excess items past capacity are dropped from the head.
func (rb *messageBuffer) loadOrdered(msgs []PostedMessage) {
	capacity := len(rb.items)
	rb.head = 0
	rb.size = 0

	start := 0
	if len(msgs) > capacity {
		start = len(msgs) - capacity
	}

	for _, m := range msgs[start:] {
		rb.push(m)
	}
}

// InMemoryBackend implements the StorageBackend for API Gateway Management API.
type InMemoryBackend struct {
	connections map[string]*Connection
	messages    map[string]*messageBuffer
	mu          *lockmetrics.RWMutex
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		connections: make(map[string]*Connection),
		messages:    make(map[string]*messageBuffer),
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

	buf, ok := b.messages[connectionID]
	if !ok {
		buf = newMessageBuffer(maxMessagesPerConnection)
		b.messages[connectionID] = buf
	}

	buf.push(PostedMessage{
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

// GetMessages returns all messages posted to the given connection in
// chronological order (oldest first).
func (b *InMemoryBackend) GetMessages(connectionID string) []PostedMessage {
	b.mu.RLock("GetMessages")
	defer b.mu.RUnlock()

	return b.messages[connectionID].snapshot()
}
