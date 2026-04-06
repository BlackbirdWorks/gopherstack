// Package codestarconnections provides an in-memory implementation of the AWS CodeStar Connections service.
package codestarconnections

import (
	"fmt"
	"maps"
	"time"

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
	// ErrValidation is returned when input validation fails.
	ErrValidation = awserr.New("ValidationException", awserr.ErrInvalidParameter)
)

// validSyncTypes returns the set of sync configuration types accepted by AWS CodeStar Connections.
func validSyncTypes() map[string]bool {
	return map[string]bool{
		"CFN_STACK_SYNC": true,
	}
}

// syncConfigKey returns the composite map key for a sync configuration.
func syncConfigKey(resourceName, syncType string) string {
	return resourceName + "/" + syncType
}

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
	connections        map[string]*Connection
	hosts              map[string]*Host
	repositoryLinks    map[string]*RepositoryLink    // keyed by RepositoryLinkID
	syncConfigurations map[string]*SyncConfiguration // keyed by ResourceName+SyncType
	mu                 *lockmetrics.RWMutex
	accountID          string
	region             string
}

// NewInMemoryBackend creates a new backend for the given account and region.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		connections:        make(map[string]*Connection),
		hosts:              make(map[string]*Host),
		repositoryLinks:    make(map[string]*RepositoryLink),
		syncConfigurations: make(map[string]*SyncConfiguration),
		accountID:          accountID,
		region:             region,
		mu:                 lockmetrics.New("codestarconnections"),
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

// RepositoryLink represents an in-memory AWS CodeStar Connections repository link.
type RepositoryLink struct {
	CreatedAt         time.Time `json:"createdAt"`
	ConnectionArn     string    `json:"connectionArn"`
	OwnerID           string    `json:"ownerID"`
	RepositoryName    string    `json:"repositoryName"`
	RepositoryLinkID  string    `json:"repositoryLinkID"`
	RepositoryLinkArn string    `json:"repositoryLinkArn"`
	ProviderType      string    `json:"providerType"`
	EncryptionKeyArn  string    `json:"encryptionKeyArn,omitempty"`
}

// CreateRepositoryLink creates a new repository link.
func (b *InMemoryBackend) CreateRepositoryLink(
	connectionArn, ownerID, repoName, encryptionKeyArn string,
) (*RepositoryLink, error) {
	b.mu.Lock("CreateRepositoryLink")
	defer b.mu.Unlock()

	id := uuid.NewString()
	linkArn := arn.Build("codestar-connections", b.region, b.accountID, fmt.Sprintf("repository-link/%s", id))

	providerType := ""
	if conn, ok := b.connections[connectionArn]; ok {
		providerType = conn.ProviderType
	}

	link := &RepositoryLink{
		ConnectionArn:     connectionArn,
		OwnerID:           ownerID,
		RepositoryName:    repoName,
		RepositoryLinkID:  id,
		RepositoryLinkArn: linkArn,
		ProviderType:      providerType,
		EncryptionKeyArn:  encryptionKeyArn,
		CreatedAt:         time.Now().UTC(),
	}

	b.repositoryLinks[id] = link

	cp := *link

	return &cp, nil
}

// GetRepositoryLink retrieves a repository link by ID.
func (b *InMemoryBackend) GetRepositoryLink(repositoryLinkID string) (*RepositoryLink, error) {
	b.mu.RLock("GetRepositoryLink")
	defer b.mu.RUnlock()

	link, ok := b.repositoryLinks[repositoryLinkID]
	if !ok {
		return nil, ErrNotFound
	}

	cp := *link

	return &cp, nil
}

// DeleteRepositoryLink removes a repository link by ID.
func (b *InMemoryBackend) DeleteRepositoryLink(repositoryLinkID string) error {
	b.mu.Lock("DeleteRepositoryLink")
	defer b.mu.Unlock()

	if _, ok := b.repositoryLinks[repositoryLinkID]; !ok {
		return ErrNotFound
	}

	delete(b.repositoryLinks, repositoryLinkID)

	return nil
}

// ListRepositoryLinks returns all repository links.
func (b *InMemoryBackend) ListRepositoryLinks() []*RepositoryLink {
	b.mu.RLock("ListRepositoryLinks")
	defer b.mu.RUnlock()

	result := make([]*RepositoryLink, 0, len(b.repositoryLinks))

	for _, link := range b.repositoryLinks {
		cp := *link
		result = append(result, &cp)
	}

	return result
}

// SyncConfiguration represents an in-memory AWS CodeStar Connections sync configuration.
type SyncConfiguration struct {
	CreatedAt        time.Time `json:"createdAt"`
	Branch           string    `json:"branch"`
	ConfigFile       string    `json:"configFile"`
	RepositoryLinkID string    `json:"repositoryLinkID"`
	ResourceName     string    `json:"resourceName"`
	RoleArn          string    `json:"roleArn"`
	SyncType         string    `json:"syncType"`
	OwnerID          string    `json:"ownerID"`
	ProviderType     string    `json:"providerType"`
	RepositoryName   string    `json:"repositoryName"`
}

// CreateSyncConfiguration creates a new sync configuration.
func (b *InMemoryBackend) CreateSyncConfiguration(
	branch, configFile, repositoryLinkID, resourceName, roleArn, syncType string,
) (*SyncConfiguration, error) {
	if !validSyncTypes()[syncType] {
		return nil, fmt.Errorf("%w: invalid SyncType %q", ErrValidation, syncType)
	}

	b.mu.Lock("CreateSyncConfiguration")
	defer b.mu.Unlock()

	ownerID := ""
	providerType := ""
	repoName := ""

	if link, ok := b.repositoryLinks[repositoryLinkID]; ok {
		ownerID = link.OwnerID
		providerType = link.ProviderType
		repoName = link.RepositoryName
	}

	cfg := &SyncConfiguration{
		Branch:           branch,
		ConfigFile:       configFile,
		RepositoryLinkID: repositoryLinkID,
		ResourceName:     resourceName,
		RoleArn:          roleArn,
		SyncType:         syncType,
		OwnerID:          ownerID,
		ProviderType:     providerType,
		RepositoryName:   repoName,
		CreatedAt:        time.Now().UTC(),
	}

	b.syncConfigurations[syncConfigKey(resourceName, syncType)] = cfg

	cp := *cfg

	return &cp, nil
}

// GetSyncConfiguration retrieves a sync configuration by resource name and sync type.
func (b *InMemoryBackend) GetSyncConfiguration(resourceName, syncType string) (*SyncConfiguration, error) {
	b.mu.RLock("GetSyncConfiguration")
	defer b.mu.RUnlock()

	cfg, ok := b.syncConfigurations[syncConfigKey(resourceName, syncType)]
	if !ok {
		return nil, ErrNotFound
	}

	cp := *cfg

	return &cp, nil
}

// DeleteSyncConfiguration removes a sync configuration.
func (b *InMemoryBackend) DeleteSyncConfiguration(resourceName, syncType string) error {
	if !validSyncTypes()[syncType] {
		return fmt.Errorf("%w: invalid SyncType %q", ErrValidation, syncType)
	}

	b.mu.Lock("DeleteSyncConfiguration")
	defer b.mu.Unlock()

	key := syncConfigKey(resourceName, syncType)
	if _, ok := b.syncConfigurations[key]; !ok {
		return ErrNotFound
	}

	delete(b.syncConfigurations, key)

	return nil
}

// SyncEvent is a single event in a sync attempt.
type SyncEvent struct {
	Time       time.Time
	Event      string
	Type       string
	ExternalID string
}

// RepositorySyncStatus holds the latest sync attempt information for a repository link.
type RepositorySyncStatus struct {
	StartedAt time.Time
	Status    string
	Events    []SyncEvent
}

// GetRepositorySyncStatus returns a stub latest sync status for a repository link and branch.
func (b *InMemoryBackend) GetRepositorySyncStatus(
	repositoryLinkID, _ /*branch*/, _ /*syncType*/ string,
) (*RepositorySyncStatus, error) {
	b.mu.RLock("GetRepositorySyncStatus")
	defer b.mu.RUnlock()

	if _, ok := b.repositoryLinks[repositoryLinkID]; !ok {
		return nil, ErrNotFound
	}

	return &RepositorySyncStatus{
		StartedAt: time.Now().UTC(),
		Status:    "SUCCEEDED",
		Events:    []SyncEvent{},
	}, nil
}

// ResourceSyncStatus holds the latest sync attempt for an AWS resource.
type ResourceSyncStatus struct {
	StartedAt time.Time
	Status    string
	Events    []SyncEvent
}

// GetResourceSyncStatus returns a stub latest sync status for a resource.
func (b *InMemoryBackend) GetResourceSyncStatus(resourceName, syncType string) (*ResourceSyncStatus, error) {
	b.mu.RLock("GetResourceSyncStatus")
	defer b.mu.RUnlock()

	key := syncConfigKey(resourceName, syncType)
	if _, ok := b.syncConfigurations[key]; !ok {
		return nil, ErrNotFound
	}

	return &ResourceSyncStatus{
		StartedAt: time.Now().UTC(),
		Status:    "SUCCEEDED",
		Events:    []SyncEvent{},
	}, nil
}

// SyncBlockerSummary is a stub summary of sync blockers for a resource.
type SyncBlockerSummary struct {
	ResourceName       string
	ParentResourceName string
	LatestBlockers     []SyncBlocker
}

// SyncBlocker represents a single sync blocker entry.
type SyncBlocker struct {
	ID            string
	Type          string
	Status        string
	CreatedAt     time.Time
	CreatedReason string
}

// GetSyncBlockerSummary returns a stub sync blocker summary for a resource.
func (b *InMemoryBackend) GetSyncBlockerSummary(
	resourceName, syncType string,
) (*SyncBlockerSummary, error) {
	b.mu.RLock("GetSyncBlockerSummary")
	defer b.mu.RUnlock()

	key := syncConfigKey(resourceName, syncType)
	if _, ok := b.syncConfigurations[key]; !ok {
		return nil, ErrNotFound
	}

	return &SyncBlockerSummary{
		ResourceName:   resourceName,
		LatestBlockers: []SyncBlocker{},
	}, nil
}
