package codeconnections

import (
	"fmt"
	"maps"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

var (
	// ErrNotFound is returned when a requested connection does not exist.
	ErrNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a connection already exists.
	ErrAlreadyExists = awserr.New("ResourceAlreadyExistsException", awserr.ErrConflict)
)

// Connection represents an AWS CodeConnections connection.
type Connection struct {
	CreatedAt      time.Time
	Tags           map[string]string
	ConnectionName string
	ConnectionArn  string
	ProviderType   string
	Status         string
	OwnerAccountID string
}

// InMemoryBackend is the in-memory store for AWS CodeConnections resources.
type InMemoryBackend struct {
	connections map[string]*Connection // keyed by ARN
	mu          *lockmetrics.RWMutex
	accountID   string
	region      string
}

// NewInMemoryBackend creates a new in-memory CodeConnections backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		connections: make(map[string]*Connection),
		accountID:   accountID,
		region:      region,
		mu:          lockmetrics.New("codeconnections"),
	}
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// CreateConnection creates a new connection.
func (b *InMemoryBackend) CreateConnection(name, providerType string, tags map[string]string) (*Connection, error) {
	b.mu.Lock("CreateConnection")
	defer b.mu.Unlock()

	id := uuid.NewString()
	connectionArn := arn.Build("codeconnections", b.region, b.accountID, fmt.Sprintf("connection/%s", id))

	tagsCopy := make(map[string]string, len(tags))
	maps.Copy(tagsCopy, tags)

	conn := &Connection{
		ConnectionName: name,
		ConnectionArn:  connectionArn,
		ProviderType:   providerType,
		Status:         "AVAILABLE",
		OwnerAccountID: b.accountID,
		Tags:           tagsCopy,
		CreatedAt:      time.Now().UTC(),
	}

	b.connections[connectionArn] = conn

	return conn, nil
}

// GetConnection retrieves a connection by ARN.
func (b *InMemoryBackend) GetConnection(connectionArn string) (*Connection, error) {
	b.mu.RLock("GetConnection")
	defer b.mu.RUnlock()

	conn, ok := b.connections[connectionArn]
	if !ok {
		return nil, ErrNotFound
	}

	return conn, nil
}

// ListConnections returns all connections, optionally filtered by provider type.
func (b *InMemoryBackend) ListConnections(providerTypeFilter string) []*Connection {
	b.mu.RLock("ListConnections")
	defer b.mu.RUnlock()

	conns := make([]*Connection, 0, len(b.connections))

	for _, conn := range b.connections {
		if providerTypeFilter == "" || conn.ProviderType == providerTypeFilter {
			conns = append(conns, conn)
		}
	}

	return conns
}

// DeleteConnection removes a connection by ARN.
func (b *InMemoryBackend) DeleteConnection(connectionArn string) error {
	b.mu.Lock("DeleteConnection")
	defer b.mu.Unlock()

	if _, ok := b.connections[connectionArn]; !ok {
		return ErrNotFound
	}

	delete(b.connections, connectionArn)

	return nil
}

// TagResource adds or updates tags on a connection.
func (b *InMemoryBackend) TagResource(resourceArn string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	conn, ok := b.connections[resourceArn]
	if !ok {
		return ErrNotFound
	}

	if conn.Tags == nil {
		conn.Tags = make(map[string]string)
	}

	maps.Copy(conn.Tags, tags)

	return nil
}

// UntagResource removes tags from a connection.
func (b *InMemoryBackend) UntagResource(resourceArn string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	conn, ok := b.connections[resourceArn]
	if !ok {
		return ErrNotFound
	}

	for _, k := range tagKeys {
		delete(conn.Tags, k)
	}

	return nil
}

// ListTagsForResource returns the tags for a connection.
func (b *InMemoryBackend) ListTagsForResource(resourceArn string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	conn, ok := b.connections[resourceArn]
	if !ok {
		return nil, ErrNotFound
	}

	result := make(map[string]string, len(conn.Tags))
	maps.Copy(result, conn.Tags)

	return result, nil
}
