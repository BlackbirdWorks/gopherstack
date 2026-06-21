package apigatewaymanagementapi

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	maxPayloadBytes          = 128 * 1024
	maxMessagesPerConnection = 1000
	maxLifecycleEvents       = 200
)

// connState is the in-memory representation of a single WebSocket connection.
// Messages are stored in a ring buffer for O(1) push and bounded memory; lifecycle
// events are stored in a separate ring so the message and timeline caps don't
// fight each other.
type connState struct {
	conn       *Connection
	msgs       *messageRing
	events     []LifecycleEvent
	downstream chan []byte // Channel to send data to the real WebSocket connection (if any)
}

// InMemoryBackend implements the StorageBackend for API Gateway Management API.
type InMemoryBackend struct {
	mu          *lockmetrics.RWMutex
	connections map[string]*connState
	stats       Stats
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		connections: make(map[string]*connState),
		mu:          lockmetrics.New("apigatewaymanagementapi"),
	}
}

func (b *InMemoryBackend) appendEvent(s *connState, ev LifecycleEvent) {
	s.events = append(s.events, ev)

	if len(s.events) > maxLifecycleEvents {
		s.events = s.events[len(s.events)-maxLifecycleEvents:]
	}
}

// CreateConnection creates a new simulated WebSocket connection.
func (b *InMemoryBackend) CreateConnection(connectionID, sourceIP, userAgent string, downstream chan []byte) (*Connection, error) {
	if connectionID == "" {
		return nil, fmt.Errorf("%w: connectionID required", awserr.ErrInvalidParameter)
	}

	b.mu.Lock("CreateConnection")
	defer b.mu.Unlock()

	if _, ok := b.connections[connectionID]; ok {
		return nil, fmt.Errorf("%w: %s", ErrConnectionExists, connectionID)
	}

	now := time.Now()
	conn := &Connection{
		ConnectionID: connectionID,
		SourceIP:     sourceIP,
		UserAgent:    userAgent,
		ConnectedAt:  now,
		LastActiveAt: now,
	}
	state := &connState{conn: conn, msgs: newMessageRing(maxMessagesPerConnection), downstream: downstream}
	b.appendEvent(state, LifecycleEvent{At: now, Type: EventConnected, Detail: sourceIP})
	b.connections[connectionID] = state
	b.stats.TotalConnections++

	cp := *conn

	return &cp, nil
}

// PostToConnection sends data to an existing connection and records the message.
func (b *InMemoryBackend) PostToConnection(connectionID string, data []byte) error {
	if len(data) > maxPayloadBytes {
		b.mu.Lock("PostToConnection.reject")
		b.stats.TotalRejected++
		b.mu.Unlock()

		return fmt.Errorf("%w: payload size %d exceeds maximum %d", ErrPayloadTooLarge, len(data), maxPayloadBytes)
	}

	b.mu.Lock("PostToConnection")
	defer b.mu.Unlock()

	state, ok := b.connections[connectionID]
	if !ok {
		b.stats.TotalRejected++

		return fmt.Errorf("%w: %s", ErrConnectionNotFound, connectionID)
	}

	now := time.Now()
	state.conn.LastActiveAt = now
	state.conn.PostedMessages++
	state.conn.BytesSent += int64(len(data))

	stored := make([]byte, len(data))
	copy(stored, data)

	state.msgs.push(PostedMessage{
		ReceivedAt:   now,
		ConnectionID: connectionID,
		Data:         stored,
	})

	b.appendEvent(state, LifecycleEvent{At: now, Type: EventMessage, Bytes: len(data)})
	b.stats.TotalMessages++
	b.stats.TotalBytesSent += int64(len(data))

	// Write to real websocket if connected
	if state.downstream != nil {
		select {
		case state.downstream <- stored:
		default:
			// If channel is full, we log but don't block
			fmt.Printf("Warning: WebSocket downstream channel full for connection %s\n", connectionID)
		}
	}

	return nil
}

// GetConnection returns the connection metadata for the given connection ID.
func (b *InMemoryBackend) GetConnection(connectionID string) (*Connection, error) {
	b.mu.RLock("GetConnection")
	defer b.mu.RUnlock()

	state, ok := b.connections[connectionID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrConnectionNotFound, connectionID)
	}

	cp := *state.conn

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
	b.stats.TotalDisconnections++

	return nil
}

// ListConnections returns all active connections sorted by ConnectedAt ascending.
func (b *InMemoryBackend) ListConnections() []Connection {
	b.mu.RLock("ListConnections")
	defer b.mu.RUnlock()

	out := make([]Connection, 0, len(b.connections))
	for _, s := range b.connections {
		out = append(out, *s.conn)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].ConnectedAt.Before(out[j].ConnectedAt)
	})

	return out
}

// FilterConnections returns connections whose ID, IP, or user-agent contain query.
// query is matched case-insensitively. An empty query returns all connections.
func (b *InMemoryBackend) FilterConnections(query string) []Connection {
	all := b.ListConnections()

	if query == "" {
		return all
	}

	q := strings.ToLower(query)
	out := make([]Connection, 0, len(all))

	for _, c := range all {
		if strings.Contains(strings.ToLower(c.ConnectionID), q) ||
			strings.Contains(strings.ToLower(c.SourceIP), q) ||
			strings.Contains(strings.ToLower(c.UserAgent), q) {
			out = append(out, c)
		}
	}

	return out
}

// GetMessages returns all messages posted to the given connection in arrival order.
func (b *InMemoryBackend) GetMessages(connectionID string) []PostedMessage {
	b.mu.RLock("GetMessages")
	defer b.mu.RUnlock()

	state, ok := b.connections[connectionID]
	if !ok {
		return nil
	}

	return state.msgs.snapshot()
}

// ClearMessages drops all stored messages for connectionID without touching the
// connection itself. Returns ErrConnectionNotFound if the connection is gone.
func (b *InMemoryBackend) ClearMessages(connectionID string) error {
	b.mu.Lock("ClearMessages")
	defer b.mu.Unlock()

	state, ok := b.connections[connectionID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrConnectionNotFound, connectionID)
	}

	state.msgs.reset()

	return nil
}

// GetTimeline returns lifecycle events for the given connection in chronological order.
func (b *InMemoryBackend) GetTimeline(connectionID string) []LifecycleEvent {
	b.mu.RLock("GetTimeline")
	defer b.mu.RUnlock()

	state, ok := b.connections[connectionID]
	if !ok {
		return nil
	}

	out := make([]LifecycleEvent, len(state.events))
	copy(out, state.events)

	return out
}

// PingConnection refreshes LastActiveAt and records a ping event without storing
// payload data. Useful for the UI's "mark active" button.
func (b *InMemoryBackend) PingConnection(connectionID string) error {
	b.mu.Lock("PingConnection")
	defer b.mu.Unlock()

	state, ok := b.connections[connectionID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrConnectionNotFound, connectionID)
	}

	now := time.Now()
	state.conn.LastActiveAt = now
	b.appendEvent(state, LifecycleEvent{At: now, Type: EventPing})

	return nil
}

// Broadcast posts data to every active connection. It returns the number of
// connections that successfully received the message; oversized payloads return
// ErrPayloadTooLarge before any send.
func (b *InMemoryBackend) Broadcast(data []byte) (int, error) {
	if len(data) > maxPayloadBytes {
		return 0, fmt.Errorf("%w: payload size %d exceeds maximum %d", ErrPayloadTooLarge, len(data), maxPayloadBytes)
	}

	b.mu.Lock("Broadcast")
	defer b.mu.Unlock()

	now := time.Now()
	delivered := 0

	for _, state := range b.connections {
		stored := make([]byte, len(data))
		copy(stored, data)

		state.msgs.push(PostedMessage{
			ReceivedAt:   now,
			ConnectionID: state.conn.ConnectionID,
			Data:         stored,
		})
		state.conn.LastActiveAt = now
		state.conn.PostedMessages++
		state.conn.BytesSent += int64(len(data))

		b.appendEvent(state, LifecycleEvent{At: now, Type: EventBroadcast, Bytes: len(data)})
		delivered++
	}

	b.stats.TotalBroadcasts++
	b.stats.TotalMessages += int64(delivered)
	b.stats.TotalBytesSent += int64(delivered) * int64(len(data))

	return delivered, nil
}

// PruneIdle deletes every connection whose LastActiveAt is older than threshold.
// Returns the list of disconnected connection IDs.
func (b *InMemoryBackend) PruneIdle(threshold time.Duration) []string {
	b.mu.Lock("PruneIdle")
	defer b.mu.Unlock()

	if threshold <= 0 {
		return []string{}
	}

	cutoff := time.Now().Add(-threshold)
	pruned := make([]string, 0)

	for id, state := range b.connections {
		if state.conn.LastActiveAt.Before(cutoff) {
			pruned = append(pruned, id)
			delete(b.connections, id)
			b.stats.TotalDisconnections++
		}
	}

	sort.Strings(pruned)

	return pruned
}

// Stats returns a snapshot of cumulative backend counters.
func (b *InMemoryBackend) Stats() Stats {
	b.mu.RLock("Stats")
	defer b.mu.RUnlock()

	s := b.stats
	s.ActiveConnections = len(b.connections)

	for _, state := range b.connections {
		s.BufferedMessages += state.msgs.len()
	}

	return s
}
