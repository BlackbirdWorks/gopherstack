package codeconnections

import (
	"context"
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// regionContextKey is the context key under which the per-request AWS region is stored.
type regionContextKey struct{}

// getRegion extracts the region from ctx, falling back to defaultRegion when unset.
// CodeConnections resources are isolated per region: every backend operation resolves
// the caller's region from the request context and operates only on that region's
// nested store.
func getRegion(ctx context.Context, defaultRegion string) string {
	if r, ok := ctx.Value(regionContextKey{}).(string); ok && r != "" {
		return r
	}

	return defaultRegion
}

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
//
// All resource maps are nested by region (outer key = region) so that
// same-named resources are isolated across regions. The per-region inner maps
// are created lazily via the *Store helpers. Callers must hold b.mu while
// accessing the inner maps.
type InMemoryBackend struct {
	connections        map[string]map[string]*Connection        // region → arn → Connection
	connectionsByName  map[string]map[string]string             // region → name → ARN
	hosts              map[string]map[string]*Host              // region → arn → Host
	hostsByName        map[string]map[string]string             // region → name → ARN
	repositoryLinks    map[string]map[string]*RepositoryLink    // region → id → RepositoryLink
	syncConfigurations map[string]map[string]*SyncConfiguration // region → key → SyncConfiguration
	mu                 *lockmetrics.RWMutex
	accountID          string
	defaultRegion      string
}

// NewInMemoryBackend creates a new in-memory CodeConnections backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		connections:        make(map[string]map[string]*Connection),
		connectionsByName:  make(map[string]map[string]string),
		hosts:              make(map[string]map[string]*Host),
		hostsByName:        make(map[string]map[string]string),
		repositoryLinks:    make(map[string]map[string]*RepositoryLink),
		syncConfigurations: make(map[string]map[string]*SyncConfiguration),
		accountID:          accountID,
		defaultRegion:      region,
		mu:                 lockmetrics.New("codeconnections"),
	}
}

// The *Store helpers return the per-region inner map, lazily creating it.
// Callers must hold b.mu.

func (b *InMemoryBackend) connectionsStore(region string) map[string]*Connection {
	if b.connections[region] == nil {
		b.connections[region] = make(map[string]*Connection)
	}

	return b.connections[region]
}

func (b *InMemoryBackend) connectionsByNameStore(region string) map[string]string {
	if b.connectionsByName[region] == nil {
		b.connectionsByName[region] = make(map[string]string)
	}

	return b.connectionsByName[region]
}

func (b *InMemoryBackend) hostsStore(region string) map[string]*Host {
	if b.hosts[region] == nil {
		b.hosts[region] = make(map[string]*Host)
	}

	return b.hosts[region]
}

func (b *InMemoryBackend) hostsByNameStore(region string) map[string]string {
	if b.hostsByName[region] == nil {
		b.hostsByName[region] = make(map[string]string)
	}

	return b.hostsByName[region]
}

func (b *InMemoryBackend) repositoryLinksStore(region string) map[string]*RepositoryLink {
	if b.repositoryLinks[region] == nil {
		b.repositoryLinks[region] = make(map[string]*RepositoryLink)
	}

	return b.repositoryLinks[region]
}

func (b *InMemoryBackend) syncConfigurationsStore(region string) map[string]*SyncConfiguration {
	if b.syncConfigurations[region] == nil {
		b.syncConfigurations[region] = make(map[string]*SyncConfiguration)
	}

	return b.syncConfigurations[region]
}

// Reset clears all state in the backend.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.connections = make(map[string]map[string]*Connection)
	b.connectionsByName = make(map[string]map[string]string)
	b.hosts = make(map[string]map[string]*Host)
	b.hostsByName = make(map[string]map[string]string)
	b.repositoryLinks = make(map[string]map[string]*RepositoryLink)
	b.syncConfigurations = make(map[string]map[string]*SyncConfiguration)
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.defaultRegion }

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

	if _, exists := b.connectionsByNameStore(region)[name]; exists {
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

	b.connectionsStore(region)[connectionArn] = conn
	b.connectionsByNameStore(region)[name] = connectionArn

	cp := *conn
	cp.Tags = make(map[string]string, len(conn.Tags))
	maps.Copy(cp.Tags, conn.Tags)

	return &cp, nil
}

// GetConnection retrieves a connection by ARN.
func (b *InMemoryBackend) GetConnection(
	ctx context.Context,
	connectionArn string,
) (*Connection, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("GetConnection")
	defer b.mu.RUnlock()

	conn, ok := b.connectionsStore(region)[connectionArn]
	if !ok {
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

	conns := make([]*Connection, 0, len(b.connectionsStore(region)))

	for _, conn := range b.connectionsStore(region) {
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
func (b *InMemoryBackend) DeleteConnection(ctx context.Context, connectionArn string) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("DeleteConnection")
	defer b.mu.Unlock()

	conn, ok := b.connectionsStore(region)[connectionArn]
	if !ok {
		return ErrNotFound
	}

	delete(b.connectionsByNameStore(region), conn.ConnectionName)
	delete(b.connectionsStore(region), connectionArn)

	return nil
}

// findResourceTagsLocked returns the tag map for a resource ARN within the given region.
// Must be called with the appropriate lock held.
func (b *InMemoryBackend) findResourceTagsLocked(
	region, resourceArn string,
) (map[string]string, bool) {
	if conn, ok := b.connectionsStore(region)[resourceArn]; ok {
		return conn.Tags, true
	}

	if host, ok := b.hostsStore(region)[resourceArn]; ok {
		return host.Tags, true
	}

	return nil, false
}

// TagResource adds or updates tags on a connection or host.
func (b *InMemoryBackend) TagResource(
	ctx context.Context,
	resourceArn string,
	tags map[string]string,
) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	existing, ok := b.findResourceTagsLocked(region, resourceArn)
	if !ok {
		return ErrNotFound
	}

	maps.Copy(existing, tags)

	return nil
}

// UntagResource removes tags from a connection or host.
func (b *InMemoryBackend) UntagResource(
	ctx context.Context,
	resourceArn string,
	tagKeys []string,
) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	existing, ok := b.findResourceTagsLocked(region, resourceArn)
	if !ok {
		return ErrNotFound
	}

	for _, k := range tagKeys {
		delete(existing, k)
	}

	return nil
}

// ListTagsForResource returns the tags for a connection or host.
func (b *InMemoryBackend) ListTagsForResource(
	ctx context.Context,
	resourceArn string,
) (map[string]string, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	existing, ok := b.findResourceTagsLocked(region, resourceArn)
	if !ok {
		return nil, ErrNotFound
	}

	result := make(map[string]string, len(existing))
	maps.Copy(result, existing)

	return result, nil
}

// AddConnectionInternal seeds a connection directly for testing.
func (b *InMemoryBackend) AddConnectionInternal(ctx context.Context, conn *Connection) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("AddConnectionInternal")
	defer b.mu.Unlock()

	b.connectionsStore(region)[conn.ConnectionArn] = conn
	b.connectionsByNameStore(region)[conn.ConnectionName] = conn.ConnectionArn
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
	ctx context.Context,
	name, providerType, providerEndpoint string,
	tags map[string]string,
) (*Host, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	if providerEndpoint == "" {
		return nil, fmt.Errorf("%w: ProviderEndpoint is required", ErrValidation)
	}

	if providerType == "" || !validProviderTypes()[providerType] {
		return nil, fmt.Errorf("%w: invalid ProviderType %q", ErrValidation, providerType)
	}

	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("CreateHost")
	defer b.mu.Unlock()

	if _, exists := b.hostsByNameStore(region)[name]; exists {
		return nil, fmt.Errorf("%w: host %q already exists", ErrAlreadyExists, name)
	}

	id := uuid.NewString()
	hostArn := arn.Build("codeconnections", region, b.accountID, "host/"+id)

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

	b.hostsStore(region)[hostArn] = host
	b.hostsByNameStore(region)[name] = hostArn

	cp := *host
	cp.Tags = make(map[string]string, len(host.Tags))
	maps.Copy(cp.Tags, host.Tags)

	return &cp, nil
}

// GetHost retrieves a host by ARN.
func (b *InMemoryBackend) GetHost(ctx context.Context, hostArn string) (*Host, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("GetHost")
	defer b.mu.RUnlock()

	host, ok := b.hostsStore(region)[hostArn]
	if !ok {
		return nil, ErrNotFound
	}

	cp := *host
	cp.Tags = make(map[string]string, len(host.Tags))
	maps.Copy(cp.Tags, host.Tags)

	return &cp, nil
}

// DeleteHost removes a host by ARN.
func (b *InMemoryBackend) DeleteHost(ctx context.Context, hostArn string) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("DeleteHost")
	defer b.mu.Unlock()

	host, ok := b.hostsStore(region)[hostArn]
	if !ok {
		return ErrNotFound
	}

	delete(b.hostsByNameStore(region), host.Name)
	delete(b.hostsStore(region), hostArn)

	return nil
}

// AddHostInternal seeds a host directly for testing.
func (b *InMemoryBackend) AddHostInternal(ctx context.Context, host *Host) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("AddHostInternal")
	defer b.mu.Unlock()

	b.hostsStore(region)[host.HostArn] = host
	b.hostsByNameStore(region)[host.Name] = host.HostArn
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
	ctx context.Context,
	connectionArn, ownerID, repoName, encryptionKeyArn string,
) (*RepositoryLink, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("CreateRepositoryLink")
	defer b.mu.Unlock()

	id := uuid.NewString()
	linkArn := arn.Build("codeconnections", region, b.accountID, "repository-link/"+id)

	// Derive provider type from connection if present.
	providerType := ""
	if conn, ok := b.connectionsStore(region)[connectionArn]; ok {
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

	b.repositoryLinksStore(region)[id] = link

	cp := *link

	return &cp, nil
}

// GetRepositoryLink retrieves a repository link by ID.
func (b *InMemoryBackend) GetRepositoryLink(
	ctx context.Context,
	repositoryLinkID string,
) (*RepositoryLink, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("GetRepositoryLink")
	defer b.mu.RUnlock()

	link, ok := b.repositoryLinksStore(region)[repositoryLinkID]
	if !ok {
		return nil, ErrNotFound
	}

	cp := *link

	return &cp, nil
}

// DeleteRepositoryLink removes a repository link by ID.
func (b *InMemoryBackend) DeleteRepositoryLink(ctx context.Context, repositoryLinkID string) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("DeleteRepositoryLink")
	defer b.mu.Unlock()

	if _, ok := b.repositoryLinksStore(region)[repositoryLinkID]; !ok {
		return ErrNotFound
	}

	delete(b.repositoryLinksStore(region), repositoryLinkID)

	return nil
}

// AddRepositoryLinkInternal seeds a repository link directly for testing.
func (b *InMemoryBackend) AddRepositoryLinkInternal(ctx context.Context, link *RepositoryLink) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("AddRepositoryLinkInternal")
	defer b.mu.Unlock()

	b.repositoryLinksStore(region)[link.RepositoryLinkID] = link
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
	ctx context.Context,
	branch, configFile, repositoryLinkID, resourceName, roleArn, syncType string,
) (*SyncConfiguration, error) {
	if !validSyncTypes()[syncType] {
		return nil, fmt.Errorf("%w: invalid SyncType %q", ErrValidation, syncType)
	}

	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("CreateSyncConfiguration")
	defer b.mu.Unlock()

	// Derive owner/provider/repo from repository link if present.
	ownerID := ""
	providerType := ""
	repoName := ""

	if link, ok := b.repositoryLinksStore(region)[repositoryLinkID]; ok {
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

	b.syncConfigurationsStore(region)[syncConfigKey(resourceName, syncType)] = cfg

	cp := *cfg

	return &cp, nil
}

// DeleteSyncConfiguration removes a sync configuration.
func (b *InMemoryBackend) DeleteSyncConfiguration(
	ctx context.Context,
	resourceName, syncType string,
) error {
	if syncType != "" && !validSyncTypes()[syncType] {
		return fmt.Errorf("%w: invalid SyncType %q", ErrValidation, syncType)
	}

	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("DeleteSyncConfiguration")
	defer b.mu.Unlock()

	key := syncConfigKey(resourceName, syncType)
	if _, ok := b.syncConfigurationsStore(region)[key]; !ok {
		return ErrNotFound
	}

	delete(b.syncConfigurationsStore(region), key)

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
	ctx context.Context,
	repositoryLinkID, _ /*branch*/, _ /*syncType*/ string,
) (*RepositorySyncStatus, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("GetRepositorySyncStatus")
	defer b.mu.RUnlock()

	if _, ok := b.repositoryLinksStore(region)[repositoryLinkID]; !ok {
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
func (b *InMemoryBackend) GetResourceSyncStatus(
	ctx context.Context,
	resourceName, syncType string,
) (*ResourceSyncStatus, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("GetResourceSyncStatus")
	defer b.mu.RUnlock()

	key := syncConfigKey(resourceName, syncType)
	if _, ok := b.syncConfigurationsStore(region)[key]; !ok {
		return nil, ErrNotFound
	}

	return &ResourceSyncStatus{
		StartedAt: time.Now().UTC(),
		Status:    "SUCCEEDED",
		Events:    []SyncEvent{},
	}, nil
}

// ListHosts returns all hosts sorted by name.
func (b *InMemoryBackend) ListHosts(ctx context.Context) []*Host {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("ListHosts")
	defer b.mu.RUnlock()

	result := make([]*Host, 0, len(b.hostsStore(region)))

	for _, host := range b.hostsStore(region) {
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
func (b *InMemoryBackend) UpdateHost(ctx context.Context, hostArn, providerEndpoint string) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("UpdateHost")
	defer b.mu.Unlock()

	host, ok := b.hostsStore(region)[hostArn]
	if !ok {
		return ErrNotFound
	}

	if providerEndpoint != "" {
		host.ProviderEndpoint = providerEndpoint
	}

	return nil
}

// ListRepositoryLinks returns all repository links sorted by ID.
func (b *InMemoryBackend) ListRepositoryLinks(ctx context.Context) []*RepositoryLink {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("ListRepositoryLinks")
	defer b.mu.RUnlock()

	result := make([]*RepositoryLink, 0, len(b.repositoryLinksStore(region)))

	for _, link := range b.repositoryLinksStore(region) {
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
	ctx context.Context,
	repositoryLinkID, connectionArn, encryptionKeyArn string,
) (*RepositoryLink, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("UpdateRepositoryLink")
	defer b.mu.Unlock()

	link, ok := b.repositoryLinksStore(region)[repositoryLinkID]
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
func (b *InMemoryBackend) GetSyncConfiguration(
	ctx context.Context,
	resourceName, syncType string,
) (*SyncConfiguration, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("GetSyncConfiguration")
	defer b.mu.RUnlock()

	cfg, ok := b.syncConfigurationsStore(region)[syncConfigKey(resourceName, syncType)]
	if !ok {
		return nil, ErrNotFound
	}

	cp := *cfg

	return &cp, nil
}

// ListSyncConfigurations returns all sync configurations for a repository link and sync type.
func (b *InMemoryBackend) ListSyncConfigurations(
	ctx context.Context,
	repositoryLinkID, syncType string,
) []*SyncConfiguration {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("ListSyncConfigurations")
	defer b.mu.RUnlock()

	result := make([]*SyncConfiguration, 0, len(b.syncConfigurationsStore(region)))

	for _, cfg := range b.syncConfigurationsStore(region) {
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
	ctx context.Context,
	resourceName, syncType, branch, configFile, repositoryLinkID, roleArn string,
) (*SyncConfiguration, error) {
	if syncType != "" && !validSyncTypes()[syncType] {
		return nil, fmt.Errorf("%w: invalid SyncType %q", ErrValidation, syncType)
	}

	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("UpdateSyncConfiguration")
	defer b.mu.Unlock()

	key := syncConfigKey(resourceName, syncType)
	cfg, ok := b.syncConfigurationsStore(region)[key]

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
	ctx context.Context,
	repositoryLinkID, syncType string,
) ([]RepositorySyncDefinition, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("ListRepositorySyncDefinitions")
	defer b.mu.RUnlock()

	if _, ok := b.repositoryLinksStore(region)[repositoryLinkID]; !ok {
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
	ctx context.Context,
	resourceName, syncType string,
) (*SyncBlockerSummary, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("GetSyncBlockerSummary")
	defer b.mu.RUnlock()

	key := syncConfigKey(resourceName, syncType)
	if _, ok := b.syncConfigurationsStore(region)[key]; !ok {
		return nil, ErrNotFound
	}

	return &SyncBlockerSummary{
		ResourceName:   resourceName,
		LatestBlockers: []SyncBlocker{},
	}, nil
}

// UpdateSyncBlocker is a stub that accepts blocker resolution.
func (b *InMemoryBackend) UpdateSyncBlocker(
	_ context.Context,
	id, resolvedReason string,
) (*SyncBlockerSummary, error) {
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
