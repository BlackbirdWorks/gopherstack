package dms

import (
	"context"
	"fmt"
)

// TestConnection tests a DMS connection and records the result.
func (b *InMemoryBackend) TestConnection(
	ctx context.Context,
	replicationInstanceArn, endpointArn string,
) (*Connection, error) {
	b.mu.Lock("TestConnection")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	ri, ok := lookupUnique(b.replicationInstancesByARN, regionKey(region, replicationInstanceArn))
	if !ok {
		return nil, fmt.Errorf(
			"%w: replication instance %s not found",
			ErrNotFound,
			replicationInstanceArn,
		)
	}

	ep, ok := lookupUnique(b.endpointsByARN, regionKey(region, endpointArn))
	if !ok {
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
	b.connections.Put(conn)
	cp := *conn

	return &cp, nil
}

// DescribeConnections returns stored connections, optionally filtered by replication instance ARN or endpoint ARN.
func (b *InMemoryBackend) DescribeConnections(
	ctx context.Context,
	replicationInstanceArn, endpointArn string,
) ([]*Connection, error) {
	b.mu.RLock("DescribeConnections")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	items := b.connectionsByRegion.Get(region)
	list := make([]*Connection, 0, len(items))
	for _, conn := range items {
		if replicationInstanceArn != "" && conn.ReplicationInstanceArn != replicationInstanceArn {
			continue
		}
		if endpointArn != "" && conn.EndpointArn != endpointArn {
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
