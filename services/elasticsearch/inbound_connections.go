package elasticsearch

import (
	"context"
	"fmt"
)

// AcceptInboundCrossClusterSearchConnection accepts a pending inbound cross-cluster
// search connection.
func (b *InMemoryBackend) AcceptInboundCrossClusterSearchConnection(
	ctx context.Context, connectionID string,
) (*InboundConnection, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("AcceptInboundCrossClusterSearchConnection")
	defer b.mu.Unlock()

	conn, exists := b.inboundConnectionGet(region, connectionID)
	if !exists {
		return nil, fmt.Errorf("%w: inbound connection %s not found", ErrConnectionNotFound, connectionID)
	}

	conn.ConnectionStatus = statusActive
	cp := *conn

	return &cp, nil
}

// AddInboundConnectionInternal seeds an inbound connection for testing.
func (b *InMemoryBackend) AddInboundConnectionInternal(ctx context.Context, conn InboundConnection) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("AddInboundConnectionInternal")
	defer b.mu.Unlock()

	cp := conn
	cp.region = region
	b.inboundConnectionPut(&cp)
}

// DeleteInboundCrossClusterSearchConnection removes an inbound cross-cluster connection.
func (b *InMemoryBackend) DeleteInboundCrossClusterSearchConnection(
	ctx context.Context, connectionID string,
) (*InboundConnection, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("DeleteInboundCrossClusterSearchConnection")
	defer b.mu.Unlock()

	conn, exists := b.inboundConnectionGet(region, connectionID)
	if !exists {
		return nil, fmt.Errorf("%w: inbound connection %s not found", ErrConnectionNotFound, connectionID)
	}

	cp := *conn
	b.inboundConnectionDelete(region, connectionID)

	return &cp, nil
}

// RejectInboundCrossClusterSearchConnection rejects a pending inbound connection.
func (b *InMemoryBackend) RejectInboundCrossClusterSearchConnection(
	ctx context.Context, connectionID string,
) (*InboundConnection, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("RejectInboundCrossClusterSearchConnection")
	defer b.mu.Unlock()

	conn, exists := b.inboundConnectionGet(region, connectionID)
	if !exists {
		return nil, fmt.Errorf("%w: inbound connection %s not found", ErrConnectionNotFound, connectionID)
	}

	conn.ConnectionStatus = "REJECTED"
	cp := *conn

	return &cp, nil
}

// DescribeInboundCrossClusterSearchConnections returns all inbound cross-cluster connections.
func (b *InMemoryBackend) DescribeInboundCrossClusterSearchConnections(ctx context.Context) []*InboundConnection {
	region := getRegion(ctx, b.region)
	b.mu.RLock("DescribeInboundCrossClusterSearchConnections")
	defer b.mu.RUnlock()

	conns := b.inboundConnectionsInRegion(region)
	result := make([]*InboundConnection, 0, len(conns))
	for _, conn := range conns {
		cp := *conn
		result = append(result, &cp)
	}

	return result
}
