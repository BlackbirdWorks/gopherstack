package elasticsearch

import (
	"context"
	"fmt"
)

// CreateOutboundCrossClusterSearchConnection creates a new outbound cross-cluster
// search connection request.
func (b *InMemoryBackend) CreateOutboundCrossClusterSearchConnection(
	ctx context.Context,
	localDomain, remoteDomain CrossClusterDomainInfo,
	alias string,
) (*OutboundConnection, error) {
	if alias == "" {
		return nil, fmt.Errorf("%w: ConnectionAlias is required", ErrValidation)
	}

	region := getRegion(ctx, b.region)
	b.mu.Lock("CreateOutboundCrossClusterSearchConnection")
	defer b.mu.Unlock()

	id := fmt.Sprintf("out-%010d", b.nextIDLocked())
	conn := &OutboundConnection{
		ConnectionID:     id,
		ConnectionAlias:  alias,
		ConnectionStatus: "VALIDATING",
		LocalDomainInfo:  localDomain,
		RemoteDomainInfo: remoteDomain,
		region:           region,
	}
	b.outboundConnectionPut(conn)
	cp := *conn

	return &cp, nil
}

// DeleteOutboundCrossClusterSearchConnection removes an outbound cross-cluster connection.
func (b *InMemoryBackend) DeleteOutboundCrossClusterSearchConnection(
	ctx context.Context, connectionID string,
) (*OutboundConnection, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("DeleteOutboundCrossClusterSearchConnection")
	defer b.mu.Unlock()

	conn, exists := b.outboundConnectionGet(region, connectionID)
	if !exists {
		return nil, fmt.Errorf("%w: outbound connection %s not found", ErrConnectionNotFound, connectionID)
	}

	cp := *conn
	b.outboundConnectionDelete(region, connectionID)

	return &cp, nil
}

// DescribeOutboundCrossClusterSearchConnections returns all outbound cross-cluster connections.
func (b *InMemoryBackend) DescribeOutboundCrossClusterSearchConnections(ctx context.Context) []*OutboundConnection {
	region := getRegion(ctx, b.region)
	b.mu.RLock("DescribeOutboundCrossClusterSearchConnections")
	defer b.mu.RUnlock()

	conns := b.outboundConnectionsInRegion(region)
	result := make([]*OutboundConnection, 0, len(conns))
	for _, conn := range conns {
		cp := *conn
		result = append(result, &cp)
	}

	return result
}
