package dms

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/dmsengine"
)

// TestConnection tests a DMS connection and records the result. With the engine enabled the endpoint is really
// contacted and a failure carries the driver's own message; engines the emulator cannot reach report success.
func (b *InMemoryBackend) TestConnection(
	ctx context.Context,
	replicationInstanceArn, endpointArn string,
) (*Connection, error) {
	b.mu.Lock("TestConnection")
	region := getRegion(ctx, b.region)

	ri, ok := lookupUnique(b.replicationInstancesByARN, regionKey(region, replicationInstanceArn))
	if !ok {
		b.mu.Unlock()

		return nil, fmt.Errorf(
			"%w: replication instance %s not found",
			ErrNotFound,
			replicationInstanceArn,
		)
	}

	ep, ok := lookupUnique(b.endpointsByARN, regionKey(region, endpointArn))
	if !ok {
		b.mu.Unlock()

		return nil, fmt.Errorf("%w: endpoint %s not found", ErrNotFound, endpointArn)
	}

	conn := &Connection{
		ReplicationInstanceArn:        replicationInstanceArn,
		ReplicationInstanceIdentifier: ri.ReplicationInstanceIdentifier,
		EndpointArn:                   endpointArn,
		EndpointIdentifier:            ep.EndpointIdentifier,
		Status:                        statusSuccessful,
		Region:                        region,
	}

	var (
		probe  dmsengine.EndpointConfig
		opener dmsengine.Opener
		source bool
	)

	if b.eng != nil && b.eng.opener != nil && dmsengine.SupportedTarget(ep.EngineName) &&
		(ep.EndpointType != endpointTypeSource || dmsengine.SupportedSource(ep.EngineName)) {
		probe, opener, source = endpointConfig(ep), b.eng.opener, ep.EndpointType == endpointTypeSource
	}
	b.mu.Unlock()

	if opener != nil {
		tctx, cancel := context.WithTimeout(ctx, connectionTestLimit)
		defer cancel()

		if err := dmsengine.TestEndpoint(tctx, opener, probe, source); err != nil {
			conn.Status, conn.LastFailureMessage = statusFailed, "Failed to connect to database. "+err.Error()
		}
	}

	b.mu.Lock("TestConnectionStore")
	defer b.mu.Unlock()

	b.connections.Put(conn)
	cp := *conn

	return &cp, nil
}

// DescribeConnections returns stored connections matching filters (valid
// filter names per api_op_DescribeConnections.go: endpoint-arn |
// replication-instance-arn).
func (b *InMemoryBackend) DescribeConnections(
	ctx context.Context,
	filters DescribeFilters,
) ([]*Connection, error) {
	b.mu.RLock("DescribeConnections")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	items := b.connectionsByRegion.Get(region)
	list := make([]*Connection, 0, len(items))

	for _, conn := range items {
		if !filters.Matches("replication-instance-arn", conn.ReplicationInstanceArn) {
			continue
		}

		if !filters.Matches("endpoint-arn", conn.EndpointArn) {
			continue
		}

		cp := *conn
		list = append(list, &cp)
	}

	return list, nil
}

// DeleteConnection removes a connection record created by TestConnection.
func (b *InMemoryBackend) DeleteConnection(
	ctx context.Context,
	replicationInstanceArn, endpointArn string,
) (*Connection, error) {
	b.mu.Lock("DeleteConnection")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	key := regionKey(region, replicationInstanceArn+":"+endpointArn)

	conn, ok := b.connections.Get(key)
	if !ok {
		return nil, fmt.Errorf("%w: connection not found", ErrNotFound)
	}

	cp := *conn
	b.connections.Delete(key)

	return &cp, nil
}
