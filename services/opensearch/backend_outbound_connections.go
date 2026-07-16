package opensearch

import (
	"fmt"
)

// CreateOutboundConnection creates a new outbound cross-cluster connection.
func (b *InMemoryBackend) CreateOutboundConnection(
	connectionAlias string,
	localDomainInfo, remoteDomainInfo map[string]any,
) (*OutboundConnection, error) {
	b.mu.Lock("CreateOutboundConnection")
	defer b.mu.Unlock()

	b.connCounter++
	id := fmt.Sprintf("co-%d", b.connCounter)

	conn := &OutboundConnection{
		ConnectionID:     id,
		ConnectionAlias:  connectionAlias,
		LocalDomainInfo:  localDomainInfo,
		RemoteDomainInfo: remoteDomainInfo,
		Status:           connectionStatusActive,
	}
	b.outboundConnections.Put(conn)

	cp := *conn

	return &cp, nil
}

// DescribeOutboundConnections returns all outbound connections, excluding any
// whose deleting window has elapsed.
func (b *InMemoryBackend) DescribeOutboundConnections() []*OutboundConnection {
	b.mu.RLock("DescribeOutboundConnections")
	defer b.mu.RUnlock()

	now := b.clock()
	out := make([]*OutboundConnection, 0, b.outboundConnections.Len())

	for _, c := range b.outboundConnections.All() {
		if statusWindowElapsed(c.Status, c.StatusUntil, now) {
			continue
		}

		cp := *c
		out = append(out, &cp)
	}

	return out
}

// DeleteOutboundConnection removes an outbound connection by ID. With a
// processing delay configured the connection first enters an observable DELETING
// window before it is finally removed.
func (b *InMemoryBackend) DeleteOutboundConnection(
	connectionID string,
) (*OutboundConnection, error) {
	b.mu.Lock("DeleteOutboundConnection")
	defer b.mu.Unlock()

	b.purgeExpiredOutboundLocked()

	conn, exists := b.outboundConnections.Get(connectionID)
	if !exists {
		return nil, fmt.Errorf(
			"%w: outbound connection %s not found",
			ErrConnectionNotFound,
			connectionID,
		)
	}

	if b.processingDelay == 0 {
		cp := *conn
		cp.Status = statusDeleting
		b.outboundConnections.Delete(connectionID)

		return &cp, nil
	}

	conn.Status = statusDeleting
	conn.StatusUntil = b.clock().Add(b.processingDelay)
	cp := *conn

	return &cp, nil
}

// purgeExpiredOutboundLocked removes outbound connections past their deleting
// window. The caller must hold the write lock.
func (b *InMemoryBackend) purgeExpiredOutboundLocked() {
	now := b.clock()
	// Table.All returns a fresh slice, so deleting from the table while
	// ranging over it here is safe.
	for _, c := range b.outboundConnections.All() {
		if statusWindowElapsed(c.Status, c.StatusUntil, now) {
			b.outboundConnections.Delete(c.ConnectionID)
		}
	}
}
