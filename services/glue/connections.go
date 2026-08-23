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
	cp.MatchCriteria = append([]string(nil), c.MatchCriteria...)

	if c.PhysicalConnectionRequirements != nil {
		pcr := *c.PhysicalConnectionRequirements
		pcr.SecurityGroupIDList = append([]string(nil), c.PhysicalConnectionRequirements.SecurityGroupIDList...)
		cp.PhysicalConnectionRequirements = &pcr
	}

	return &cp
}

// ConnectionOptions carries the optional ConnectionInput fields beyond
// Name/ConnectionType/ConnectionProperties that CreateConnection/
// UpdateConnection predate: Description, MatchCriteria, and
// PhysicalConnectionRequirements (VPC/subnet/security-group settings, used
// e.g. by NETWORK-type connections in place of ConnectionProperties).
type ConnectionOptions struct {
	PhysicalConnectionRequirements *PhysicalConnectionRequirements
	Description                    string
	MatchCriteria                  []string
}

// BatchDeleteConnection deletes multiple connections. The real
// BatchDeleteConnectionOutput.Errors is a map keyed by connection name
// (glue@v1.152.0 deserializers.go: awsAwsjson11_deserializeDocumentErrorByName
// decodes a JSON object, not an array) -- a real client fails to decode a
// JSON array in that field.
func (b *InMemoryBackend) BatchDeleteConnection(names []string) ([]string, map[string]ErrorDetail) {
	b.mu.Lock("BatchDeleteConnection")
	defer b.mu.Unlock()

	succeeded := make([]string, 0, len(names))
	errs := make(map[string]ErrorDetail, len(names))

	for _, name := range names {
		if !b.connections.Has(name) {
			errs[name] = ErrorDetail{
				ErrorCode:    errEntityNotFoundCode,
				ErrorMessage: "connection not found: " + name,
			}

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
	return b.CreateConnectionWithOptions(name, connType, props, tags, ConnectionOptions{})
}

// CreateConnectionWithOptions is CreateConnection plus the optional
// creation-time settings ConnectionInput also supports (Description/
// MatchCriteria/PhysicalConnectionRequirements).
func (b *InMemoryBackend) CreateConnectionWithOptions(
	name, connType string, props map[string]string, tags map[string]string, opts ConnectionOptions,
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
		Name:                           name,
		ConnectionType:                 connType,
		ConnectionProperties:           maps.Clone(props),
		Tags:                           maps.Clone(tags),
		ARN:                            b.connectionARN(name),
		CreationTime:                   now,
		LastUpdatedTime:                now,
		Description:                    opts.Description,
		MatchCriteria:                  append([]string(nil), opts.MatchCriteria...),
		PhysicalConnectionRequirements: opts.PhysicalConnectionRequirements,
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
	return b.UpdateConnectionWithOptions(name, connType, props, ConnectionOptions{})
}

// UpdateConnectionWithOptions is UpdateConnection plus the optional settings
// ConnectionInput also supports (Description/MatchCriteria/
// PhysicalConnectionRequirements).
func (b *InMemoryBackend) UpdateConnectionWithOptions(
	name, connType string, props map[string]string, opts ConnectionOptions,
) error {
	b.mu.Lock("UpdateConnection")
	defer b.mu.Unlock()

	c, ok := b.connections.Get(name)
	if !ok {
		return ErrNotFound
	}

	c.ConnectionType = connType
	c.ConnectionProperties = maps.Clone(props)
	c.Description = opts.Description
	c.MatchCriteria = append([]string(nil), opts.MatchCriteria...)
	c.PhysicalConnectionRequirements = opts.PhysicalConnectionRequirements
	c.LastUpdatedTime = float64(time.Now().Unix())

	return nil
}
