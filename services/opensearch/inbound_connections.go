package opensearch

import (
	"fmt"
)

// AcceptInboundConnection accepts an inbound cross-cluster connection by ID,
// transitioning it (and, if present, its mirrored outbound counterpart) to
// ACTIVE.
func (b *InMemoryBackend) AcceptInboundConnection(connectionID string) (*InboundConnection, error) {
	if connectionID == "" {
		return nil, fmt.Errorf("%w: ConnectionId is required", ErrInvalidParameter)
	}

	b.mu.Lock("AcceptInboundConnection")
	defer b.mu.Unlock()

	conn, exists := b.inboundConnections.Get(connectionID)
	if !exists {
		return nil, fmt.Errorf("%w: connection %s not found", ErrConnectionNotFound, connectionID)
	}

	conn.Status = connectionStatusActive
	conn.StatusMessage = ""

	if out, ok := b.outboundConnections.Get(connectionID); ok {
		out.Status = connectionStatusActive
		out.StatusMessage = ""
	}

	cp := *conn

	return &cp, nil
}

// RejectInboundConnection sets an inbound connection status to REJECTED,
// propagating the same status to its mirrored outbound counterpart when one
// exists.
func (b *InMemoryBackend) RejectInboundConnection(connectionID string) (*InboundConnection, error) {
	if connectionID == "" {
		return nil, fmt.Errorf("%w: ConnectionId is required", ErrInvalidParameter)
	}

	b.mu.Lock("RejectInboundConnection")
	defer b.mu.Unlock()

	conn, exists := b.inboundConnections.Get(connectionID)
	if !exists {
		return nil, fmt.Errorf(
			"%w: inbound connection %s not found",
			ErrConnectionNotFound,
			connectionID,
		)
	}

	conn.Status = connStatusRejected

	if out, ok := b.outboundConnections.Get(connectionID); ok {
		out.Status = connStatusRejected
	}

	cp := *conn

	return &cp, nil
}

// DeleteInboundConnection removes an inbound connection by ID. With a
// processing delay configured the connection first enters an observable
// DELETING window. Deleting an unknown connection ID is a
// ResourceNotFoundException, matching DeleteOutboundConnection.
func (b *InMemoryBackend) DeleteInboundConnection(connectionID string) (*InboundConnection, error) {
	b.mu.Lock("DeleteInboundConnection")
	defer b.mu.Unlock()

	b.purgeExpiredInboundLocked()

	conn, exists := b.inboundConnections.Get(connectionID)
	if !exists {
		return nil, fmt.Errorf(
			"%w: inbound connection %s not found",
			ErrConnectionNotFound,
			connectionID,
		)
	}

	if b.processingDelay == 0 {
		cp := *conn
		cp.Status = statusDeleting
		b.inboundConnections.Delete(connectionID)

		return &cp, nil
	}

	conn.Status = statusDeleting
	conn.StatusUntil = b.clock().Add(b.processingDelay)
	cp := *conn

	return &cp, nil
}

// purgeExpiredInboundLocked removes inbound connections past their deleting
// window. The caller must hold the write lock.
func (b *InMemoryBackend) purgeExpiredInboundLocked() {
	now := b.clock()
	for _, c := range b.inboundConnections.All() {
		if statusWindowElapsed(c.Status, c.StatusUntil, now) {
			b.inboundConnections.Delete(c.ConnectionID)
		}
	}
}

// DescribeInboundConnections returns all inbound connections, excluding any
// whose deleting window has elapsed.
func (b *InMemoryBackend) DescribeInboundConnections() []*InboundConnection {
	b.mu.RLock("DescribeInboundConnections")
	defer b.mu.RUnlock()

	now := b.clock()
	out := make([]*InboundConnection, 0, b.inboundConnections.Len())

	for _, c := range b.inboundConnections.All() {
		if statusWindowElapsed(c.Status, c.StatusUntil, now) {
			continue
		}

		cp := *c
		out = append(out, &cp)
	}

	return out
}
