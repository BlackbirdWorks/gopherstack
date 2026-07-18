package glue

import (
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// cloneConnection returns a deep copy of a Connection.
func cloneConnection(c *Connection) *Connection {
	cp := *c
	cp.ConnectionProperties = maps.Clone(c.ConnectionProperties)
	cp.Tags = maps.Clone(c.Tags)

	return &cp
}

// BatchDeleteConnection deletes multiple connections.
func (b *InMemoryBackend) BatchDeleteConnection(names []string) ([]string, []ErrorDetail) {
	b.mu.Lock("BatchDeleteConnection")
	defer b.mu.Unlock()

	succeeded := make([]string, 0, len(names))
	errs := make([]ErrorDetail, 0, len(names))

	for _, name := range names {
		if !b.connections.Has(name) {
			errs = append(errs, ErrorDetail{
				ErrorCode:    errEntityNotFoundCode,
				ErrorMessage: "connection not found: " + name,
			})

			continue
		}

		b.connections.Delete(name)
		succeeded = append(succeeded, name)
	}

	return succeeded, errs
}

// AddConnectionInternal adds a connection directly to the backend without validation.
func (b *InMemoryBackend) AddConnectionInternal(conn *Connection) {
	b.mu.Lock("AddConnectionInternal")
	defer b.mu.Unlock()

	b.connections.Put(cloneConnection(conn))
}

// connectionARN returns the ARN for a Glue connection.
func (b *InMemoryBackend) connectionARN(name string) string {
	return arn.Build("glue", b.region, b.accountID, "connection/"+name)
}

// CreateConnection creates a new Glue connection.
func (b *InMemoryBackend) CreateConnection(
	name, connType string, props map[string]string, tags map[string]string,
) (*Connection, error) {
	b.mu.Lock("CreateConnection")
	defer b.mu.Unlock()

	if name == "" {
		return nil, ErrValidation
	}

	if b.connections.Has(name) {
		return nil, ErrAlreadyExists
	}

	now := float64(time.Now().Unix())
	c := &Connection{
		Name:                 name,
		ConnectionType:       connType,
		ConnectionProperties: maps.Clone(props),
		Tags:                 maps.Clone(tags),
		ARN:                  b.connectionARN(name),
		CreationTime:         now,
		LastUpdatedTime:      now,
	}
	b.connections.Put(c)

	return cloneConnection(c), nil
}

// GetConnection retrieves a single Glue connection by name.
func (b *InMemoryBackend) GetConnection(name string) (*Connection, error) {
	b.mu.RLock("GetConnection")
	defer b.mu.RUnlock()

	c, ok := b.connections.Get(name)
	if !ok {
		return nil, ErrNotFound
	}

	return cloneConnection(c), nil
}

// GetConnections returns all Glue connections sorted by name.
func (b *InMemoryBackend) GetConnections() []*Connection {
	b.mu.RLock("GetConnections")
	defer b.mu.RUnlock()

	src := b.connections.Snapshot()
	out := make([]*Connection, 0, len(src))
	for _, c := range src {
		out = append(out, cloneConnection(c))
	}

	return out
}

// DeleteConnection deletes a single Glue connection by name.
func (b *InMemoryBackend) DeleteConnection(name string) error {
	b.mu.Lock("DeleteConnection")
	defer b.mu.Unlock()

	if !b.connections.Has(name) {
		return ErrNotFound
	}

	b.connections.Delete(name)

	return nil
}

// UpdateConnection updates an existing connection's type and properties.
func (b *InMemoryBackend) UpdateConnection(name string, connType string, props map[string]string) error {
	b.mu.Lock("UpdateConnection")
	defer b.mu.Unlock()

	c, ok := b.connections.Get(name)
	if !ok {
		return ErrNotFound
	}

	c.ConnectionType = connType
	c.ConnectionProperties = maps.Clone(props)
	c.LastUpdatedTime = float64(time.Now().Unix())

	return nil
}
