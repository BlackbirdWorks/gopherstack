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
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource already exists.
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
	connections        map[string]*Connection        // keyed by ARN
	hosts              map[string]*Host              // keyed by ARN
	repositoryLinks    map[string]*RepositoryLink    // keyed by RepositoryLinkId
	syncConfigurations map[string]*SyncConfiguration // keyed by ResourceName+SyncType
	mu                 *lockmetrics.RWMutex
	accountID          string
	region             string
}

// NewInMemoryBackend creates a new in-memory CodeConnections backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		connections:        make(map[string]*Connection),
		hosts:              make(map[string]*Host),
		repositoryLinks:    make(map[string]*RepositoryLink),
		syncConfigurations: make(map[string]*SyncConfiguration),
		accountID:          accountID,
		region:             region,
		mu:                 lockmetrics.New("codeconnections"),
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

	cp := *conn

	return &cp, nil
}

// GetConnection retrieves a connection by ARN.
func (b *InMemoryBackend) GetConnection(connectionArn string) (*Connection, error) {
	b.mu.RLock("GetConnection")
	defer b.mu.RUnlock()

	conn, ok := b.connections[connectionArn]
	if !ok {
		return nil, ErrNotFound
	}

	cp := *conn
	cp.Tags = make(map[string]string, len(conn.Tags))
	maps.Copy(cp.Tags, conn.Tags)

	return &cp, nil
}

// ListConnections returns all connections, optionally filtered by provider type.
func (b *InMemoryBackend) ListConnections(providerTypeFilter string) []*Connection {
	b.mu.RLock("ListConnections")
	defer b.mu.RUnlock()

	conns := make([]*Connection, 0, len(b.connections))

	for _, conn := range b.connections {
		if providerTypeFilter == "" || conn.ProviderType == providerTypeFilter {
			cp := *conn
			cp.Tags = make(map[string]string, len(conn.Tags))
			maps.Copy(cp.Tags, conn.Tags)
			conns = append(conns, &cp)
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

// Host represents an AWS CodeConnections host (infrastructure endpoint).
type Host struct {
	CreatedAt        time.Time
	Name             string
	HostArn          string
	ProviderType     string
	ProviderEndpoint string
	Status           string
}

// CreateHost creates a new host.
func (b *InMemoryBackend) CreateHost(name, providerType, providerEndpoint string) (*Host, error) {
	b.mu.Lock("CreateHost")
	defer b.mu.Unlock()

	id := uuid.NewString()
	hostArn := arn.Build("codeconnections", b.region, b.accountID, fmt.Sprintf("host/%s", id))

	host := &Host{
		Name:             name,
		HostArn:          hostArn,
		ProviderType:     providerType,
		ProviderEndpoint: providerEndpoint,
		Status:           "AVAILABLE",
		CreatedAt:        time.Now().UTC(),
	}

	b.hosts[hostArn] = host

	cp := *host

	return &cp, nil
}

// GetHost retrieves a host by ARN.
func (b *InMemoryBackend) GetHost(hostArn string) (*Host, error) {
	b.mu.RLock("GetHost")
	defer b.mu.RUnlock()

	host, ok := b.hosts[hostArn]
	if !ok {
		return nil, ErrNotFound
	}

	cp := *host

	return &cp, nil
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

// RepositoryLink represents an AWS CodeConnections repository link.
type RepositoryLink struct {
	CreatedAt         time.Time
	ConnectionArn     string
	OwnerID           string
	RepositoryName    string
	RepositoryLinkID  string
	RepositoryLinkArn string
	ProviderType      string
	EncryptionKeyArn  string
}

// CreateRepositoryLink creates a new repository link.
func (b *InMemoryBackend) CreateRepositoryLink(
	connectionArn, ownerID, repoName, encryptionKeyArn string,
) (*RepositoryLink, error) {
	b.mu.Lock("CreateRepositoryLink")
	defer b.mu.Unlock()

	id := uuid.NewString()
	linkArn := arn.Build("codeconnections", b.region, b.accountID, fmt.Sprintf("repository-link/%s", id))

	// Derive provider type from connection if present.
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

// SyncConfiguration represents an AWS CodeConnections sync configuration.
type SyncConfiguration struct {
	CreatedAt        time.Time
	Branch           string
	ConfigFile       string
	RepositoryLinkID string
	ResourceName     string
	RoleArn          string
	SyncType         string
	OwnerID          string
	ProviderType     string
	RepositoryName   string
}

// syncConfigKey returns the composite map key for a sync configuration.
func syncConfigKey(resourceName, syncType string) string {
	return resourceName + "/" + syncType
}

// CreateSyncConfiguration creates a new sync configuration.
func (b *InMemoryBackend) CreateSyncConfiguration(
	branch, configFile, repositoryLinkID, resourceName, roleArn, syncType string,
) (*SyncConfiguration, error) {
	b.mu.Lock("CreateSyncConfiguration")
	defer b.mu.Unlock()

	// Derive owner/provider/repo from repository link if present.
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

// DeleteSyncConfiguration removes a sync configuration.
func (b *InMemoryBackend) DeleteSyncConfiguration(resourceName, syncType string) error {
	b.mu.Lock("DeleteSyncConfiguration")
	defer b.mu.Unlock()

	key := syncConfigKey(resourceName, syncType)
	if _, ok := b.syncConfigurations[key]; !ok {
		return ErrNotFound
	}

	delete(b.syncConfigurations, key)

	return nil
}

// RepositorySyncStatus holds the latest sync attempt information for a repository link.
type RepositorySyncStatus struct {
	StartedAt time.Time
	Status    string
	Events    []SyncEvent
}

// SyncEvent is a single event in a sync attempt.
type SyncEvent struct {
	Time       time.Time
	Event      string
	Type       string
	ExternalID string
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
