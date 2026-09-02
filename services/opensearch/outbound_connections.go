package opensearch

import (
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// CreateOutboundConnection creates a new outbound cross-cluster connection.
// It starts in PENDING_ACCEPTANCE, matching real AWS behavior where the
// remote domain owner must accept the connection before it becomes ACTIVE.
// gopherstack emulates a single account/region, so it also mirrors a pending
// InboundConnection sharing the same ConnectionId (Local/Remote swapped) so
// the connection can be discovered and accepted/rejected via the inbound
// cross-cluster connection APIs, exactly like a real remote domain owner
// would see it.
func (b *InMemoryBackend) CreateOutboundConnection(
	connectionAlias, connectionMode string,
	localDomainInfo, remoteDomainInfo DomainInformation,
	skipUnavailable, endpoint string,
) (*OutboundConnection, error) {
	if connectionAlias == "" {
		return nil, fmt.Errorf("%w: ConnectionAlias is required", ErrInvalidParameter)
	}

	if localDomainInfo.DomainName == "" {
		return nil, fmt.Errorf("%w: LocalDomainInfo.AWSDomainInformation.DomainName is required", ErrInvalidParameter)
	}

	if remoteDomainInfo.DomainName == "" {
		return nil, fmt.Errorf("%w: RemoteDomainInfo.AWSDomainInformation.DomainName is required", ErrInvalidParameter)
	}

	if connectionMode == "" {
		connectionMode = connectionModeDirect
	}

	b.mu.Lock("CreateOutboundConnection")
	defer b.mu.Unlock()

	b.connCounter++
	id := fmt.Sprintf("co-%d", b.connCounter)

	conn := &OutboundConnection{
		ConnectionID:     id,
		ConnectionAlias:  connectionAlias,
		ConnectionMode:   connectionMode,
		Status:           connStatusPendingAcceptance,
		SkipUnavailable:  skipUnavailable,
		Endpoint:         endpoint,
		LocalDomainInfo:  localDomainInfo,
		RemoteDomainInfo: remoteDomainInfo,
	}
	b.outboundConnections.Put(conn)

	b.inboundConnections.Put(&InboundConnection{
		ConnectionID:     id,
		ConnectionMode:   connectionMode,
		Status:           connStatusPendingAcceptance,
		LocalDomainInfo:  remoteDomainInfo,
		RemoteDomainInfo: localDomainInfo,
	})

	cp := *conn

	return &cp, nil
}

// DescribeOutboundConnections returns outbound connections excluding any
// whose deleting window has elapsed, filtered and paginated per the request.
// connectionIDs comes from Filter entries named "connection-id" -- the only
// Filter Name documented anywhere in api_op_DescribeOutboundConnections.go or
// API_Filter.html for this operation (neither enumerates a Name value set);
// an empty slice matches everything.
func (b *InMemoryBackend) DescribeOutboundConnections(
	connectionIDs []string, nextToken string, maxResults int,
) page.Page[*OutboundConnection] {
	b.mu.RLock("DescribeOutboundConnections")
	defer b.mu.RUnlock()

	now := b.clock()
	all := make([]*OutboundConnection, 0, b.outboundConnections.Len())

	for _, c := range b.outboundConnections.All() {
		if statusWindowElapsed(c.Status, c.StatusUntil, now) {
			continue
		}

		cp := *c
		all = append(all, &cp)
	}

	return filterAndPageConnections(
		all, func(c *OutboundConnection) string { return c.ConnectionID }, connectionIDs, nextToken, maxResults,
	)
}

// DeleteOutboundConnection removes an outbound connection by ID. With a
// processing delay configured the connection first enters an observable
// DELETING window before it is finally removed.
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
