// Package codestarconnections provides an in-memory implementation of the AWS CodeStar Connections service.
package codestarconnections

import (
	"maps"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// Connection status values.
const (
	ConnectionStatusAvailable = "AVAILABLE"
	ConnectionStatusPending   = "PENDING"
	ConnectionStatusError     = "ERROR"
)

// Host status values.
const (
	HostStatusAvailable = "AVAILABLE"
	HostStatusPending   = "PENDING"
)

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource with the same name already exists.
	ErrAlreadyExists = awserr.New("InvalidInputException", awserr.ErrAlreadyExists)
)

// Connection represents an in-memory AWS CodeStar connection.
type Connection struct {
	Tags             map[string]string `json:"tags,omitempty"`
	ConnectionName   string            `json:"connectionName"`
	ConnectionArn    string            `json:"connectionArn"`
	ConnectionStatus string            `json:"connectionStatus"`
	OwnerAccountID   string            `json:"ownerAccountId"`
	ProviderType     string            `json:"providerType"`
	HostArn          string            `json:"hostArn,omitempty"`
}

// Host represents an in-memory AWS CodeStar host.
type Host struct {
	Tags             map[string]string `json:"tags,omitempty"`
	Name             string            `json:"name"`
	HostArn          string            `json:"hostArn"`
	ProviderType     string            `json:"providerType"`
	ProviderEndpoint string            `json:"providerEndpoint"`
	Status           string            `json:"status"`
	StatusMessage    string            `json:"statusMessage,omitempty"`
}

// InMemoryBackend is a thread-safe in-memory store for CodeStar Connections resources.
type InMemoryBackend struct {
	connections map[string]*Connection
	hosts       map[string]*Host
	mu          *lockmetrics.RWMutex
	accountID   string
	region      string
}

// NewInMemoryBackend creates a new backend for the given account and region.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		connections: make(map[string]*Connection),
		hosts:       make(map[string]*Host),
		accountID:   accountID,
		region:      region,
		mu:          lockmetrics.New("codestarconnections"),
	}
}

// Region returns the region for this backend instance.
func (b *InMemoryBackend) Region() string { return b.region }

// AccountID returns the account ID for this backend instance.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// CreateConnection creates a new CodeStar connection.
func (b *InMemoryBackend) CreateConnection(
	name, providerType, hostArn string,
	tags map[string]string,
) (*Connection, error) {
	b.mu.Lock("CreateConnection")
	defer b.mu.Unlock()

	for _, c := range b.connections {
		if c.ConnectionName == name {
			return nil, ErrAlreadyExists
		}
	}

	id := uuid.NewString()
	connArn := arn.Build("codestar-connections", b.region, b.accountID, "connection/"+id)

	conn := &Connection{
		ConnectionName:   name,
		ConnectionArn:    connArn,
		ConnectionStatus: ConnectionStatusAvailable,
		OwnerAccountID:   b.accountID,
		ProviderType:     providerType,
		HostArn:          hostArn,
		Tags:             maps.Clone(tags),
	}
	b.connections[connArn] = conn

	out := *conn

	return &out, nil
}

// GetConnection returns a connection by ARN.
func (b *InMemoryBackend) GetConnection(connectionArn string) (*Connection, error) {
	b.mu.RLock("GetConnection")
	defer b.mu.RUnlock()

	conn, ok := b.connections[connectionArn]
	if !ok {
		return nil, ErrNotFound
	}

	out := *conn

	return &out, nil
}

// ListConnections returns all connections, optionally filtered by provider type or host ARN.
func (b *InMemoryBackend) ListConnections(providerTypeFilter, hostArnFilter string) []*Connection {
	b.mu.RLock("ListConnections")
	defer b.mu.RUnlock()

	result := make([]*Connection, 0, len(b.connections))

	for _, conn := range b.connections {
		if providerTypeFilter != "" && conn.ProviderType != providerTypeFilter {
			continue
		}

		if hostArnFilter != "" && conn.HostArn != hostArnFilter {
			continue
		}

		out := *conn
		result = append(result, &out)
	}

	return result
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

// CreateHost creates a new CodeStar host.
func (b *InMemoryBackend) CreateHost(
	name, providerType, providerEndpoint string,
	tags map[string]string,
) (*Host, error) {
	b.mu.Lock("CreateHost")
	defer b.mu.Unlock()

	for _, h := range b.hosts {
		if h.Name == name {
			return nil, ErrAlreadyExists
		}
	}

	id := uuid.NewString()
	hostArn := arn.Build("codestar-connections", b.region, b.accountID, "host/"+name+"/"+id[:8])

	host := &Host{
		Name:             name,
		HostArn:          hostArn,
		ProviderType:     providerType,
		ProviderEndpoint: providerEndpoint,
		Status:           HostStatusAvailable,
		Tags:             maps.Clone(tags),
	}
	b.hosts[hostArn] = host

	out := *host

	return &out, nil
}

// GetHost returns a host by ARN.
func (b *InMemoryBackend) GetHost(hostArn string) (*Host, error) {
	b.mu.RLock("GetHost")
	defer b.mu.RUnlock()

	host, ok := b.hosts[hostArn]
	if !ok {
		return nil, ErrNotFound
	}

	out := *host

	return &out, nil
}

// ListHosts returns all hosts.
func (b *InMemoryBackend) ListHosts() []*Host {
	b.mu.RLock("ListHosts")
	defer b.mu.RUnlock()

	result := make([]*Host, 0, len(b.hosts))

	for _, host := range b.hosts {
		out := *host
		result = append(result, &out)
	}

	return result
}

// DeleteHost removes a host by ARN.
func (b *InMemoryBackend) DeleteHost(hostArn string) error {
	b.mu.Lock("DeleteHost")
	defer b.mu.Unlock()

	if _, ok := b.hosts[hostArn]; !ok {
		return ErrNotFound
	}

	delete(b.hosts, hostArn)

	return nil
}

// UpdateHost updates the provider endpoint for a host.
func (b *InMemoryBackend) UpdateHost(hostArn, providerEndpoint string) error {
	b.mu.Lock("UpdateHost")
	defer b.mu.Unlock()

	host, ok := b.hosts[hostArn]
	if !ok {
		return ErrNotFound
	}

	host.ProviderEndpoint = providerEndpoint

	return nil
}

// ListTagsForResource returns the tags for a resource by ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceArn string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if conn, ok := b.connections[resourceArn]; ok {
		return maps.Clone(conn.Tags), nil
	}

	for _, host := range b.hosts {
		if host.HostArn == resourceArn {
			return maps.Clone(host.Tags), nil
		}
	}

	return nil, ErrNotFound
}

// TagResource adds or updates tags on a resource.
func (b *InMemoryBackend) TagResource(resourceArn string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if conn, ok := b.connections[resourceArn]; ok {
		if conn.Tags == nil {
			conn.Tags = make(map[string]string)
		}

		maps.Copy(conn.Tags, tags)

		return nil
	}

	for _, host := range b.hosts {
		if host.HostArn == resourceArn {
			if host.Tags == nil {
				host.Tags = make(map[string]string)
			}

			maps.Copy(host.Tags, tags)

			return nil
		}
	}

	return ErrNotFound
}

// UntagResource removes tags from a resource.
func (b *InMemoryBackend) UntagResource(resourceArn string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if conn, ok := b.connections[resourceArn]; ok {
		for _, k := range tagKeys {
			delete(conn.Tags, k)
		}

		return nil
	}

	for _, host := range b.hosts {
		if host.HostArn == resourceArn {
			for _, k := range tagKeys {
				delete(host.Tags, k)
			}

			return nil
		}
	}

	return ErrNotFound
}
