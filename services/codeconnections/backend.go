package codeconnections

import (
	"fmt"
	"maps"
	"sort"
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
	// ErrValidation is returned when input validation fails.
	ErrValidation = awserr.New("ValidationException", awserr.ErrInvalidParameter)
)

// validProviderTypes is the set of provider types accepted by AWS CodeConnections.
func validProviderTypes() map[string]bool {
	return map[string]bool{
		"Bitbucket":              true,
		"GitHub":                 true,
		"GitHubEnterpriseServer": true,
		"GitLab":                 true,
		"GitLabSelfManaged":      true,
	}
}

// validSyncTypes is the set of sync configuration types accepted by AWS CodeConnections.
func validSyncTypes() map[string]bool {
	return map[string]bool{
		"CFN_STACK_SYNC": true,
	}
}

// Connection represents an AWS CodeConnections connection.
type Connection struct {
	Tags           map[string]string `json:"tags,omitempty"`
	CreatedAt      time.Time         `json:"createdAt"`
	ConnectionName string            `json:"connectionName"`
	ConnectionArn  string            `json:"connectionArn"`
	HostArn        string            `json:"hostArn,omitempty"`
	OwnerAccountID string            `json:"ownerAccountID"`
	ProviderType   string            `json:"providerType"`
	Status         string            `json:"status"`
}

// InMemoryBackend is the in-memory store for AWS CodeConnections resources.
type InMemoryBackend struct {
	connections        map[string]*Connection        // keyed by ARN
	connectionsByName  map[string]string             // name → ARN
	hosts              map[string]*Host              // keyed by ARN
	repositoryLinks    map[string]*RepositoryLink    // keyed by RepositoryLinkID
	syncConfigurations map[string]*SyncConfiguration // keyed by ResourceName+SyncType
	mu                 *lockmetrics.RWMutex
	accountID          string
	region             string
}

// NewInMemoryBackend creates a new in-memory CodeConnections backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		connections:        make(map[string]*Connection),
		connectionsByName:  make(map[string]string),
		hosts:              make(map[string]*Host),
		repositoryLinks:    make(map[string]*RepositoryLink),
		syncConfigurations: make(map[string]*SyncConfiguration),
		accountID:          accountID,
		region:             region,
		mu:                 lockmetrics.New("codeconnections"),
	}
}

// Reset clears all state in the backend.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.connections = make(map[string]*Connection)
	b.connectionsByName = make(map[string]string)
	b.hosts = make(map[string]*Host)
	b.repositoryLinks = make(map[string]*RepositoryLink)
	b.syncConfigurations = make(map[string]*SyncConfiguration)
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// CreateConnection creates a new connection.
func (b *InMemoryBackend) CreateConnection(name, providerType string, tags map[string]string) (*Connection, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: ConnectionName is required", ErrValidation)
	}

	if providerType != "" {
		if !validProviderTypes()[providerType] {
			return nil, fmt.Errorf("%w: invalid ProviderType %q", ErrValidation, providerType)
		}
	}

	b.mu.Lock("CreateConnection")
	defer b.mu.Unlock()

	if _, exists := b.connectionsByName[name]; exists {
		return nil, fmt.Errorf("%w: connection %q already exists", ErrAlreadyExists, name)
	}

	id := uuid.NewString()
	connectionArn := arn.Build("codeconnections", b.region, b.accountID, "connection/"+id)

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
	b.connectionsByName[name] = connectionArn

	cp := *conn
	cp.Tags = make(map[string]string, len(conn.Tags))
	maps.Copy(cp.Tags, conn.Tags)

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

// ListConnections returns all connections, optionally filtered by provider type or host ARN.
func (b *InMemoryBackend) ListConnections(providerTypeFilter, hostArnFilter string) []*Connection {
	b.mu.RLock("ListConnections")
	defer b.mu.RUnlock()

	conns := make([]*Connection, 0, len(b.connections))

	for _, conn := range b.connections {
		if providerTypeFilter != "" && conn.ProviderType != providerTypeFilter {
			continue
		}

		if hostArnFilter != "" && conn.HostArn != hostArnFilter {
			continue
		}

		cp := *conn
		cp.Tags = make(map[string]string, len(conn.Tags))
		maps.Copy(cp.Tags, conn.Tags)
		conns = append(conns, &cp)
	}

	return conns
}

// DeleteConnection removes a connection by ARN.
func (b *InMemoryBackend) DeleteConnection(connectionArn string) error {
	b.mu.Lock("DeleteConnection")
	defer b.mu.Unlock()

	conn, ok := b.connections[connectionArn]
	if !ok {
		return ErrNotFound
	}

	delete(b.connectionsByName, conn.ConnectionName)
	delete(b.connections, connectionArn)

	return nil
}

// findResourceTagsLocked returns the tag map for a resource ARN.
// Must be called with the appropriate lock held.
func (b *InMemoryBackend) findResourceTagsLocked(resourceArn string) (map[string]string, bool) {
	if conn, ok := b.connections[resourceArn]; ok {
		return conn.Tags, true
	}

	if host, ok := b.hosts[resourceArn]; ok {
		return host.Tags, true
	}

	return nil, false
}

// TagResource adds or updates tags on a connection or host.
func (b *InMemoryBackend) TagResource(resourceArn string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	existing, ok := b.findResourceTagsLocked(resourceArn)
	if !ok {
		return ErrNotFound
	}

	maps.Copy(existing, tags)

	return nil
}

// UntagResource removes tags from a connection or host.
func (b *InMemoryBackend) UntagResource(resourceArn string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	existing, ok := b.findResourceTagsLocked(resourceArn)
	if !ok {
		return ErrNotFound
	}

	for _, k := range tagKeys {
		delete(existing, k)
	}

	return nil
}

// ListTagsForResource returns the tags for a connection or host.
func (b *InMemoryBackend) ListTagsForResource(resourceArn string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	existing, ok := b.findResourceTagsLocked(resourceArn)
	if !ok {
		return nil, ErrNotFound
	}

	result := make(map[string]string, len(existing))
	maps.Copy(result, existing)

	return result, nil
}

// AddConnectionInternal seeds a connection directly for testing.
func (b *InMemoryBackend) AddConnectionInternal(conn *Connection) {
	b.mu.Lock("AddConnectionInternal")
	defer b.mu.Unlock()

	b.connections[conn.ConnectionArn] = conn
	b.connectionsByName[conn.ConnectionName] = conn.ConnectionArn
}

// Host represents an AWS CodeConnections host (infrastructure endpoint).
type Host struct {
	Tags             map[string]string `json:"tags,omitempty"`
	CreatedAt        time.Time         `json:"createdAt"`
	Name             string            `json:"name"`
	HostArn          string            `json:"hostArn"`
	ProviderType     string            `json:"providerType"`
	ProviderEndpoint string            `json:"providerEndpoint"`
	Status           string            `json:"status"`
	StatusMessage    string            `json:"statusMessage,omitempty"`
}

// CreateHost creates a new host.
func (b *InMemoryBackend) CreateHost(
	name, providerType, providerEndpoint string,
	tags map[string]string,
) (*Host, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	if providerEndpoint == "" {
		return nil, fmt.Errorf("%w: ProviderEndpoint is required", ErrValidation)
	}

	if providerType != "" {
		if !validProviderTypes()[providerType] {
			return nil, fmt.Errorf("%w: invalid ProviderType %q", ErrValidation, providerType)
		}
	}

	b.mu.Lock("CreateHost")
	defer b.mu.Unlock()

	id := uuid.NewString()
	hostArn := arn.Build("codeconnections", b.region, b.accountID, "host/"+id)

	tagsCopy := make(map[string]string, len(tags))
	maps.Copy(tagsCopy, tags)

	host := &Host{
		Name:             name,
		HostArn:          hostArn,
		ProviderType:     providerType,
		ProviderEndpoint: providerEndpoint,
		Status:           "AVAILABLE",
		Tags:             tagsCopy,
		CreatedAt:        time.Now().UTC(),
	}

	b.hosts[hostArn] = host

	cp := *host
	cp.Tags = make(map[string]string, len(host.Tags))
	maps.Copy(cp.Tags, host.Tags)

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
	cp.Tags = make(map[string]string, len(host.Tags))
	maps.Copy(cp.Tags, host.Tags)

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

// AddHostInternal seeds a host directly for testing.
func (b *InMemoryBackend) AddHostInternal(host *Host) {
	b.mu.Lock("AddHostInternal")
	defer b.mu.Unlock()

	b.hosts[host.HostArn] = host
}

// RepositoryLink represents an AWS CodeConnections repository link.
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
	linkArn := arn.Build("codeconnections", b.region, b.accountID, "repository-link/"+id)

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

// AddRepositoryLinkInternal seeds a repository link directly for testing.
func (b *InMemoryBackend) AddRepositoryLinkInternal(link *RepositoryLink) {
	b.mu.Lock("AddRepositoryLinkInternal")
	defer b.mu.Unlock()

	b.repositoryLinks[link.RepositoryLinkID] = link
}

// SyncConfiguration represents an AWS CodeConnections sync configuration.
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

// syncConfigKey returns the composite map key for a sync configuration.
// ResourceName values must not contain "/" to avoid key collisions with SyncType.
func syncConfigKey(resourceName, syncType string) string {
	return resourceName + "/" + syncType
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
	if syncType != "" && !validSyncTypes()[syncType] {
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

// ListHosts returns all hosts sorted by name.
func (b *InMemoryBackend) ListHosts() []*Host {
	b.mu.RLock("ListHosts")
	defer b.mu.RUnlock()

	result := make([]*Host, 0, len(b.hosts))

	for _, host := range b.hosts {
		cp := *host
		cp.Tags = make(map[string]string, len(host.Tags))
		maps.Copy(cp.Tags, host.Tags)
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return result
}

// UpdateHost updates the provider endpoint for a host.
func (b *InMemoryBackend) UpdateHost(hostArn, providerEndpoint string) error {
	b.mu.Lock("UpdateHost")
	defer b.mu.Unlock()

	host, ok := b.hosts[hostArn]
	if !ok {
		return ErrNotFound
	}

	if providerEndpoint != "" {
		host.ProviderEndpoint = providerEndpoint
	}

	return nil
}

// ListRepositoryLinks returns all repository links sorted by ID.
func (b *InMemoryBackend) ListRepositoryLinks() []*RepositoryLink {
	b.mu.RLock("ListRepositoryLinks")
	defer b.mu.RUnlock()

	result := make([]*RepositoryLink, 0, len(b.repositoryLinks))

	for _, link := range b.repositoryLinks {
		cp := *link
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].RepositoryLinkID < result[j].RepositoryLinkID
	})

	return result
}

// UpdateRepositoryLink updates the connection ARN or encryption key for a repository link.
func (b *InMemoryBackend) UpdateRepositoryLink(
	repositoryLinkID, connectionArn, encryptionKeyArn string,
) (*RepositoryLink, error) {
	b.mu.Lock("UpdateRepositoryLink")
	defer b.mu.Unlock()

	link, ok := b.repositoryLinks[repositoryLinkID]
	if !ok {
		return nil, ErrNotFound
	}

	if connectionArn != "" {
		link.ConnectionArn = connectionArn
	}

	if encryptionKeyArn != "" {
		link.EncryptionKeyArn = encryptionKeyArn
	}

	cp := *link

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

// ListSyncConfigurations returns all sync configurations for a repository link and sync type.
func (b *InMemoryBackend) ListSyncConfigurations(repositoryLinkID, syncType string) []*SyncConfiguration {
	b.mu.RLock("ListSyncConfigurations")
	defer b.mu.RUnlock()

	result := make([]*SyncConfiguration, 0, len(b.syncConfigurations))

	for _, cfg := range b.syncConfigurations {
		if cfg.RepositoryLinkID != repositoryLinkID {
			continue
		}

		if syncType != "" && cfg.SyncType != syncType {
			continue
		}

		cp := *cfg
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ResourceName < result[j].ResourceName
	})

	return result
}

// UpdateSyncConfiguration updates fields on an existing sync configuration.
func (b *InMemoryBackend) UpdateSyncConfiguration(
	resourceName, syncType, branch, configFile, repositoryLinkID, roleArn string,
) (*SyncConfiguration, error) {
	if syncType != "" && !validSyncTypes()[syncType] {
		return nil, fmt.Errorf("%w: invalid SyncType %q", ErrValidation, syncType)
	}

	b.mu.Lock("UpdateSyncConfiguration")
	defer b.mu.Unlock()

	key := syncConfigKey(resourceName, syncType)
	cfg, ok := b.syncConfigurations[key]

	if !ok {
		return nil, ErrNotFound
	}

	if branch != "" {
		cfg.Branch = branch
	}

	if configFile != "" {
		cfg.ConfigFile = configFile
	}

	if repositoryLinkID != "" {
		cfg.RepositoryLinkID = repositoryLinkID
	}

	if roleArn != "" {
		cfg.RoleArn = roleArn
	}

	cp := *cfg

	return &cp, nil
}

// RepositorySyncDefinition is a stub definition for a repository sync.
type RepositorySyncDefinition struct {
	Branch    string
	Directory string
	Parent    string
	Target    string
}

// ListRepositorySyncDefinitions returns stub sync definitions for a repository link and sync type.
func (b *InMemoryBackend) ListRepositorySyncDefinitions(
	repositoryLinkID, syncType string,
) ([]RepositorySyncDefinition, error) {
	b.mu.RLock("ListRepositorySyncDefinitions")
	defer b.mu.RUnlock()

	if _, ok := b.repositoryLinks[repositoryLinkID]; !ok {
		return nil, ErrNotFound
	}

	_ = syncType

	return []RepositorySyncDefinition{}, nil
}

// SyncBlockerSummary is a stub summary of sync blockers for a resource.
type SyncBlockerSummary struct {
	ResourceName       string
	ParentResourceName string
	LatestBlockers     []SyncBlocker
}

// SyncBlocker represents a single sync blocker.
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

// UpdateSyncBlocker is a stub that accepts blocker resolution.
func (b *InMemoryBackend) UpdateSyncBlocker(
	id, resolvedReason string,
) (*SyncBlockerSummary, error) {
	b.mu.RLock("UpdateSyncBlocker")
	defer b.mu.RUnlock()

	_ = id
	_ = resolvedReason

	return &SyncBlockerSummary{
		LatestBlockers: []SyncBlocker{},
	}, nil
}

// sortedTagKeys returns the keys of the tags map in sorted order.
func sortedTagKeys(tags map[string]string) []string {
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	return keys
}
