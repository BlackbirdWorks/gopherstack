package codeconnections

import (
	"context"
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/collections"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// regionContextKey is the context key under which the per-request AWS region is stored.
type regionContextKey struct{}

// getRegion extracts the region from ctx, falling back to defaultRegion when unset.
// CodeConnections resources are isolated per region: every backend operation resolves
// the caller's region from the request context and operates only on that region's
// resources.
func getRegion(ctx context.Context, defaultRegion string) string {
	if r, ok := ctx.Value(regionContextKey{}).(string); ok && r != "" {
		return r
	}

	return defaultRegion
}

// regionFromARN extracts the region component (index 3) from an AWS ARN
// (arn:partition:service:region:account:resource), returning "" if the ARN is
// malformed (which then simply fails to match any real region in every
// caller below -- see e.g. GetConnection). Connection.ConnectionArn and
// Host.HostArn are always built via arn.Build with the same region the
// resource was created in (see CreateConnection/CreateHost), so this is
// equivalent to -- and replaces -- the old outer "region" map key those two
// resource families used to be nested under.
func regionFromARN(resourceARN string) string {
	parts := strings.Split(resourceARN, ":")
	const regionIndex = 3
	if len(parts) > regionIndex {
		return parts[regionIndex]
	}

	return ""
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
//
// ConnectionArn already embeds its own region (arn:partition:service:region:
// account:resource, see regionFromARN), so Connection needs no hidden region
// field: store_setup.go's connections table is keyed directly by
// ConnectionArn and its byRegion/byName indexes derive region from the ARN.
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
// connections and hosts are "clean" store.Table collections (see
// store_setup.go): each is keyed directly by its own ARN, which already
// embeds its region, so region isolation for Get/Delete/List/duplicate-name
// checks falls out of the byRegion/byName secondary indexes below, which
// derive their group key from the ARN. Both are registered directly on
// registry. repositoryLinks and syncConfigurations are "dirty": their own
// identity (RepositoryLinkID; ResourceName+SyncType) carries no region of its
// own, and lookups are scoped by the caller's context region rather than by
// any ARN, so each carries an unexported region-qualifying field and is
// registered with a composite "region|id" key (see regionKey). They are built
// with store.New only -- deliberately NOT store.Register-ed onto registry --
// so registry.ResetAll()/SnapshotAll()/RestoreAll() never touch them
// directly; see Reset below and persistence.go's mixed clean/dirty
// Snapshot/Restore.
type InMemoryBackend struct {
	mu       *lockmetrics.RWMutex
	registry *store.Registry

	connections         *store.Table[Connection]
	connectionsByRegion *store.Index[Connection]
	connectionsByName   *store.Index[Connection]

	hosts         *store.Table[Host]
	hostsByRegion *store.Index[Host]
	hostsByName   *store.Index[Host]

	repositoryLinks         *store.Table[RepositoryLink]
	repositoryLinksByRegion *store.Index[RepositoryLink]

	syncConfigurations         *store.Table[SyncConfiguration]
	syncConfigurationsByRegion *store.Index[SyncConfiguration]

	accountID     string
	defaultRegion string
}

// NewInMemoryBackend creates a new in-memory CodeConnections backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		accountID:     accountID,
		defaultRegion: region,
		mu:            lockmetrics.New("codeconnections"),
		registry:      store.NewRegistry(),
	}

	registerAllTables(b)

	return b
}

// Reset clears all state in the backend.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	// repositoryLinks/syncConfigurations (see store_setup.go's registerAllTables
	// doc) are deliberately NOT on b.registry, so each needs its own Reset() call.
	b.repositoryLinks.Reset()
	b.syncConfigurations.Reset()
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

// findResourceTagsLocked returns the tag map for a resource ARN within the given region.
// Must be called with the appropriate lock held.
func (b *InMemoryBackend) findResourceTagsLocked(
	region, resourceArn string,
) (map[string]string, bool) {
	if conn, ok := b.connections.Get(resourceArn); ok && regionFromARN(resourceArn) == region {
		return conn.Tags, true
	}

	if host, ok := b.hosts.Get(resourceArn); ok && regionFromARN(resourceArn) == region {
		return host.Tags, true
	}

	// Repository links are keyed by ID, not ARN; scan by ARN within the region.
	for _, link := range b.repositoryLinksByRegion.Get(region) {
		if link.RepositoryLinkArn == resourceArn {
			return link.Tags, true
		}
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
func (b *InMemoryBackend) AddConnectionInternal(_ context.Context, conn *Connection) {
	b.mu.Lock("AddConnectionInternal")
	defer b.mu.Unlock()

	b.connections.Put(conn)
}

// Host represents an AWS CodeConnections host (infrastructure endpoint).
//
// Like Connection, HostArn already embeds its own region, so Host needs no
// hidden region field either.
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

	if len(b.hostsByName.Get(regionKey(region, name))) > 0 {
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

	b.hosts.Put(host)

	cp := *host
	cp.Tags = make(map[string]string, len(host.Tags))
	maps.Copy(cp.Tags, host.Tags)

	return &cp, nil
}

// GetHost retrieves a host by ARN, scoped to the caller's request region (see GetConnection).
func (b *InMemoryBackend) GetHost(ctx context.Context, hostArn string) (*Host, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("GetHost")
	defer b.mu.RUnlock()

	host, ok := b.hosts.Get(hostArn)
	if !ok || regionFromARN(hostArn) != region {
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

	if !b.hosts.Has(hostArn) || regionFromARN(hostArn) != region {
		return ErrNotFound
	}

	b.hosts.Delete(hostArn)

	return nil
}

// AddHostInternal seeds a host directly for testing.
func (b *InMemoryBackend) AddHostInternal(_ context.Context, host *Host) {
	b.mu.Lock("AddHostInternal")
	defer b.mu.Unlock()

	b.hosts.Put(host)
}

// RepositoryLink represents an AWS CodeConnections repository link.
type RepositoryLink struct {
	Tags              map[string]string `json:"tags,omitempty"`
	CreatedAt         time.Time         `json:"createdAt"`
	ConnectionArn     string            `json:"connectionArn"`
	OwnerID           string            `json:"ownerID"`
	RepositoryName    string            `json:"repositoryName"`
	RepositoryLinkID  string            `json:"repositoryLinkID"`
	RepositoryLinkArn string            `json:"repositoryLinkArn"`
	ProviderType      string            `json:"providerType"`
	EncryptionKeyArn  string            `json:"encryptionKeyArn,omitempty"`
	// region is the store.Table composite-key qualifier (see regionKey and
	// store_setup.go); it is unexported so a plain json.Marshal(RepositoryLink)
	// never sees it and is instead carried through persistence via a
	// regionalDTO (see persistence.go). GetRepositoryLink/DeleteRepositoryLink/
	// UpdateRepositoryLink resolve their region from the caller's context
	// rather than from any ARN (a bare RepositoryLinkID carries no region of
	// its own), so region must be captured explicitly at creation time and
	// used for every subsequent lookup by ID.
	region string
}

// CreateRepositoryLink creates a new repository link.
func (b *InMemoryBackend) CreateRepositoryLink(
	ctx context.Context,
	connectionArn, ownerID, repoName, encryptionKeyArn string,
	tags map[string]string,
) (*RepositoryLink, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("CreateRepositoryLink")
	defer b.mu.Unlock()

	// Derive provider type from the connection if present.
	providerType := ""
	if conn, ok := b.connections.Get(connectionArn); ok {
		providerType = conn.ProviderType
	}

	tagsCopy := make(map[string]string, len(tags))
	maps.Copy(tagsCopy, tags)

	id := uuid.NewString()
	linkArn := arn.Build("codeconnections", region, b.accountID, "repository-link/"+id)

	link := &RepositoryLink{
		ConnectionArn:     connectionArn,
		OwnerID:           ownerID,
		RepositoryName:    repoName,
		RepositoryLinkID:  id,
		RepositoryLinkArn: linkArn,
		ProviderType:      providerType,
		EncryptionKeyArn:  encryptionKeyArn,
		Tags:              tagsCopy,
		CreatedAt:         time.Now().UTC(),
		region:            region,
	}

	b.repositoryLinks.Put(link)

	cp := *link
	cp.Tags = make(map[string]string, len(link.Tags))
	maps.Copy(cp.Tags, link.Tags)

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

	link, ok := b.repositoryLinks.Get(regionKey(region, repositoryLinkID))
	if !ok {
		return nil, ErrNotFound
	}

	cp := *link
	cp.Tags = make(map[string]string, len(link.Tags))
	maps.Copy(cp.Tags, link.Tags)

	return &cp, nil
}

// DeleteRepositoryLink removes a repository link by ID.
func (b *InMemoryBackend) DeleteRepositoryLink(ctx context.Context, repositoryLinkID string) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("DeleteRepositoryLink")
	defer b.mu.Unlock()

	key := regionKey(region, repositoryLinkID)
	if !b.repositoryLinks.Has(key) {
		return ErrNotFound
	}

	b.repositoryLinks.Delete(key)

	return nil
}

// AddRepositoryLinkInternal seeds a repository link directly for testing.
func (b *InMemoryBackend) AddRepositoryLinkInternal(ctx context.Context, link *RepositoryLink) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("AddRepositoryLinkInternal")
	defer b.mu.Unlock()

	link.region = region
	b.repositoryLinks.Put(link)
}

// SyncConfiguration represents an AWS CodeConnections sync configuration.
type SyncConfiguration struct {
	CreatedAt               time.Time `json:"createdAt"`
	Branch                  string    `json:"branch"`
	ConfigFile              string    `json:"configFile"`
	RepositoryLinkID        string    `json:"repositoryLinkID"`
	ResourceName            string    `json:"resourceName"`
	RoleArn                 string    `json:"roleArn"`
	SyncType                string    `json:"syncType"`
	OwnerID                 string    `json:"ownerID"`
	ProviderType            string    `json:"providerType"`
	RepositoryName          string    `json:"repositoryName"`
	PublishDeploymentStatus string    `json:"publishDeploymentStatus,omitempty"`
	TriggerResourceUpdateOn string    `json:"triggerResourceUpdateOn,omitempty"`
	// region is the store.Table composite-key qualifier: ResourceName+SyncType
	// carries no region of its own and every lookup is scoped by the caller's
	// context region, exactly like RepositoryLink.region above. See
	// persistence.go for how it survives Snapshot/Restore.
	region string
}

// syncConfigKey returns the composite lookup key for a sync configuration.
// ResourceName values must not contain "/" to avoid key collisions with SyncType.
func syncConfigKey(resourceName, syncType string) string {
	return resourceName + "/" + syncType
}

// regionKey returns the composite store.Table primary key ("region|id") used
// by repositoryLinks/syncConfigurations (see store_setup.go). Neither has an
// ARN of its own to derive a region from (unlike Connection/Host), so each
// carries an unexported region field set at creation time and combined with
// its own identity via this helper.
func regionKey(region, id string) string {
	return region + "|" + id
}

// CreateSyncConfiguration creates a new sync configuration.
func (b *InMemoryBackend) CreateSyncConfiguration(
	ctx context.Context,
	branch, configFile, repositoryLinkID, resourceName, roleArn, syncType string,
	publishDeploymentStatus, triggerResourceUpdateOn string,
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

	if link, ok := b.repositoryLinks.Get(regionKey(region, repositoryLinkID)); ok {
		ownerID = link.OwnerID
		providerType = link.ProviderType
		repoName = link.RepositoryName
	}

	cfg := &SyncConfiguration{
		Branch:                  branch,
		ConfigFile:              configFile,
		RepositoryLinkID:        repositoryLinkID,
		ResourceName:            resourceName,
		RoleArn:                 roleArn,
		SyncType:                syncType,
		OwnerID:                 ownerID,
		ProviderType:            providerType,
		RepositoryName:          repoName,
		PublishDeploymentStatus: publishDeploymentStatus,
		TriggerResourceUpdateOn: triggerResourceUpdateOn,
		CreatedAt:               time.Now().UTC(),
		region:                  region,
	}

	b.syncConfigurations.Put(cfg)

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

	key := regionKey(region, syncConfigKey(resourceName, syncType))
	if !b.syncConfigurations.Has(key) {
		return ErrNotFound
	}

	b.syncConfigurations.Delete(key)

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

	if !b.repositoryLinks.Has(regionKey(region, repositoryLinkID)) {
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

	key := regionKey(region, syncConfigKey(resourceName, syncType))
	if !b.syncConfigurations.Has(key) {
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

	hs := b.hostsByRegion.Get(region)
	result := make([]*Host, 0, len(hs))

	for _, host := range hs {
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

	host, ok := b.hosts.Get(hostArn)
	if !ok || regionFromARN(hostArn) != region {
		return ErrNotFound
	}

	// ProviderEndpoint is not part of any index key (hosts is keyed by
	// HostArn; byRegion/byName derive from HostArn/Name), so mutating the
	// stored *Host in place is safe -- no Delete+Put needed.
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

	links := b.repositoryLinksByRegion.Get(region)
	result := make([]*RepositoryLink, 0, len(links))

	for _, link := range links {
		cp := *link
		cp.Tags = make(map[string]string, len(link.Tags))
		maps.Copy(cp.Tags, link.Tags)
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

	link, ok := b.repositoryLinks.Get(regionKey(region, repositoryLinkID))
	if !ok {
		return nil, ErrNotFound
	}

	// ConnectionArn/EncryptionKeyArn are not part of the repositoryLinks key
	// (region|RepositoryLinkID) or the byRegion index, so mutating the stored
	// *RepositoryLink in place is safe -- no Delete+Put needed.
	if connectionArn != "" {
		link.ConnectionArn = connectionArn
	}

	if encryptionKeyArn != "" {
		link.EncryptionKeyArn = encryptionKeyArn
	}

	cp := *link
	cp.Tags = make(map[string]string, len(link.Tags))
	maps.Copy(cp.Tags, link.Tags)

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

	cfg, ok := b.syncConfigurations.Get(regionKey(region, syncConfigKey(resourceName, syncType)))
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

	cfgs := b.syncConfigurationsByRegion.Get(region)
	result := make([]*SyncConfiguration, 0, len(cfgs))

	for _, cfg := range cfgs {
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
	publishDeploymentStatus, triggerResourceUpdateOn string,
) (*SyncConfiguration, error) {
	if syncType != "" && !validSyncTypes()[syncType] {
		return nil, fmt.Errorf("%w: invalid SyncType %q", ErrValidation, syncType)
	}

	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("UpdateSyncConfiguration")
	defer b.mu.Unlock()

	key := regionKey(region, syncConfigKey(resourceName, syncType))
	cfg, ok := b.syncConfigurations.Get(key)

	if !ok {
		return nil, ErrNotFound
	}

	// None of the fields below are part of the syncConfigurations key
	// (region|ResourceName/SyncType) or the byRegion index, so mutating the
	// stored *SyncConfiguration in place is safe -- no Delete+Put needed.
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

	if publishDeploymentStatus != "" {
		cfg.PublishDeploymentStatus = publishDeploymentStatus
	}

	if triggerResourceUpdateOn != "" {
		cfg.TriggerResourceUpdateOn = triggerResourceUpdateOn
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

	if !b.repositoryLinks.Has(regionKey(region, repositoryLinkID)) {
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

	key := regionKey(region, syncConfigKey(resourceName, syncType))
	if !b.syncConfigurations.Has(key) {
		return nil, ErrNotFound
	}

	return &SyncBlockerSummary{
		ResourceName:   resourceName,
		LatestBlockers: []SyncBlocker{},
	}, nil
}

// UpdateSyncBlocker is a stub that accepts blocker resolution and returns the resource summary.
func (b *InMemoryBackend) UpdateSyncBlocker(
	_ context.Context,
	id, resolvedReason, resourceName, syncType string,
) (*SyncBlockerSummary, error) {
	_ = id
	_ = resolvedReason
	_ = syncType

	return &SyncBlockerSummary{
		ResourceName:   resourceName,
		LatestBlockers: []SyncBlocker{},
	}, nil
}

// sortedTagKeys returns the keys of the tags map in sorted order.
func sortedTagKeys(tags map[string]string) []string {
	keys := collections.SortedKeys(tags)

	return keys
}
