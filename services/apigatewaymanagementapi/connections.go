package apigatewaymanagementapi

import (
	"fmt"
	"time"
)

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

	stored := make([]byte, len(data))
	copy(stored, data)

	// Attempt delivery to the real WebSocket transport, if one is wired,
	// before recording the message as posted. A full client-side buffer means
	// the frame was never queued for delivery, so nothing below should treat
	// it as a successful post -- real AWS reports this exact condition as
	// LimitExceededException rather than silently accepting the message.
	if state.downstream != nil {
		select {
		case state.downstream <- stored:
		default:
			b.stats.TotalRejected++

			return fmt.Errorf("%w: connection %s", ErrLimitExceeded, connectionID)
		}
	}

	now := time.Now()
	state.conn.LastActiveAt = now
	state.conn.PostedMessages++
	state.conn.BytesSent += int64(len(data))

	state.msgs.push(PostedMessage{
		ReceivedAt:   now,
		ConnectionID: connectionID,
		Data:         stored,
	})

	b.appendEvent(state, LifecycleEvent{At: now, Type: EventMessage, Bytes: len(data)})
	b.stats.TotalMessages++
	b.stats.TotalBytesSent += int64(len(data))

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

// DeleteConnection forcibly disconnects the connection with the given ID,
// matching real AWS semantics: the connection is torn down, not merely
// forgotten. If a real transport is wired (connState.downstream is non-nil),
// closing it here signals the owning transport goroutine to close the
// underlying socket.
func (b *InMemoryBackend) DeleteConnection(connectionID string) error {
	b.mu.Lock("DeleteConnection")
	defer b.mu.Unlock()

	state, ok := b.connections[connectionID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrConnectionNotFound, connectionID)
	}

	delete(b.connections, connectionID)
	closeDownstream(state)
	b.stats.TotalDisconnections++

	return nil
}
