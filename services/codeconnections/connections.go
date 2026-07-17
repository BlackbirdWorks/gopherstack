package codeconnections

import (
	"context"
	"fmt"
	"maps"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// CreateConnection creates a new connection.
func (b *InMemoryBackend) CreateConnection(
	ctx context.Context,
	name, providerType, hostArn string,
	tags map[string]string,
) (*Connection, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: ConnectionName is required", ErrValidation)
	}

	if providerType == "" || !validProviderTypes()[providerType] {
		return nil, fmt.Errorf("%w: invalid ProviderType %q", ErrValidation, providerType)
	}

	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("CreateConnection")
	defer b.mu.Unlock()

	if len(b.connectionsByName.Get(regionKey(region, name))) > 0 {
		return nil, fmt.Errorf("%w: connection %q already exists", ErrAlreadyExists, name)
	}

	id := uuid.NewString()
	connectionArn := arn.Build("codeconnections", region, b.accountID, "connection/"+id)

	tagsCopy := make(map[string]string, len(tags))
	maps.Copy(tagsCopy, tags)

	conn := &Connection{
		ConnectionName: name,
		ConnectionArn:  connectionArn,
		ProviderType:   providerType,
		HostArn:        hostArn,
		Status:         "AVAILABLE",
		OwnerAccountID: b.accountID,
		Tags:           tagsCopy,
		CreatedAt:      time.Now().UTC(),
	}

	b.connections.Put(conn)

	cp := *conn
	cp.Tags = make(map[string]string, len(conn.Tags))
	maps.Copy(cp.Tags, conn.Tags)

	return &cp, nil
}

// GetConnection retrieves a connection by ARN. The lookup is scoped to the
// caller's request region -- an ARN created in one region is not visible from
// another, matching the old per-region map's isolation.
func (b *InMemoryBackend) GetConnection(
	ctx context.Context,
	connectionArn string,
) (*Connection, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("GetConnection")
	defer b.mu.RUnlock()

	conn, ok := b.connections.Get(connectionArn)
	if !ok || regionFromARN(connectionArn) != region {
		return nil, ErrNotFound
	}

	cp := *conn
	cp.Tags = make(map[string]string, len(conn.Tags))
	maps.Copy(cp.Tags, conn.Tags)

	return &cp, nil
}

// ListConnections returns all connections, optionally filtered by provider type or host ARN.
func (b *InMemoryBackend) ListConnections(
	ctx context.Context,
	providerTypeFilter, hostArnFilter string,
) []*Connection {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("ListConnections")
	defer b.mu.RUnlock()

	conns := b.connectionsByRegion.Get(region)
	result := make([]*Connection, 0, len(conns))

	for _, conn := range conns {
		if providerTypeFilter != "" && conn.ProviderType != providerTypeFilter {
			continue
		}

		if hostArnFilter != "" && conn.HostArn != hostArnFilter {
			continue
		}

		cp := *conn
		cp.Tags = make(map[string]string, len(conn.Tags))
		maps.Copy(cp.Tags, conn.Tags)
		result = append(result, &cp)
	}

	return result
}

// DeleteConnection removes a connection by ARN.
func (b *InMemoryBackend) DeleteConnection(ctx context.Context, connectionArn string) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("DeleteConnection")
	defer b.mu.Unlock()

	if !b.connections.Has(connectionArn) || regionFromARN(connectionArn) != region {
		return ErrNotFound
	}

	b.connections.Delete(connectionArn)

	return nil
}

// AddConnectionInternal seeds a connection directly for testing.
func (b *InMemoryBackend) AddConnectionInternal(_ context.Context, conn *Connection) {
	b.mu.Lock("AddConnectionInternal")
	defer b.mu.Unlock()

	b.connections.Put(conn)
}
