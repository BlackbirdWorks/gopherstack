// Package codestarconnections provides an in-memory implementation of the AWS CodeStar Connections service.
package codestarconnections

import (
	"context"
	"fmt"
	"maps"
	"regexp"
	"slices"
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
func getRegion(ctx context.Context, defaultRegion string) string {
	if r, ok := ctx.Value(regionContextKey{}).(string); ok && r != "" {
		return r
	}

	return defaultRegion
}

// regionFromARN extracts the region component (index 3) from an AWS ARN
// (arn:partition:service:region:account:resource), falling back to defaultRegion.
func regionFromARN(resourceARN, defaultRegion string) string {
	parts := strings.Split(resourceARN, ":")
	const regionIndex = 3
	if len(parts) > regionIndex && parts[regionIndex] != "" {
		return parts[regionIndex]
	}

	return defaultRegion
}

// Connection status values.
const (
	ConnectionStatusAvailable = "AVAILABLE"
	ConnectionStatusPending   = "PENDING"
	ConnectionStatusError     = "ERROR"
)

// Host status values.
const (
	HostStatusAvailable           = "AVAILABLE"
	HostStatusPending             = "PENDING"
	HostStatusVPCConfigDeleting   = "VPC_CONFIG_DELETING"
	HostStatusVPCConfigFailed     = "VPC_CONFIG_FAILED"
	HostStatusVPCConfigInProgress = "VPC_CONFIG_IN_PROGRESS"
)

// VpcConfiguration holds the VPC connectivity settings for a host.
type VpcConfiguration struct {
	VpcID            string   `json:"VpcId"`
	TLSCertificate   string   `json:"TlsCertificate,omitempty"`
	SubnetIDs        []string `json:"SubnetIds"`
	SecurityGroupIDs []string `json:"SecurityGroupIds"`
}

// Sync status values.
const (
	SyncStatusSucceeded  = "SUCCEEDED"
	SyncStatusFailed     = "FAILED"
	SyncStatusInProgress = "IN_PROGRESS"
	SyncStatusQueued     = "QUEUED"
)

// SyncBlocker status values.
const (
	SyncBlockerStatusActive   = "ACTIVE"
	SyncBlockerStatusResolved = "RESOLVED"
)

// SyncBlocker type values.
const (
	SyncBlockerTypeAutomated = "AUTOMATED"
	SyncBlockerTypeManual    = "MANUAL"
)

// Validation limits.
const (
	maxConnectionNameLen   = 32
	maxTagKeyLen           = 128
	maxTagValueLen         = 256
	maxTagsPerResource     = 200
	maxProviderEndpointLen = 512
)

// connectionNameRE matches valid connection and host names: 1-32 alphanumeric, hyphen, underscore, dot.
var connectionNameRE = regexp.MustCompile(`^[a-zA-Z0-9_.\-]+$`)

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a connection or host with the same name
	// already exists. The real CreateConnection/CreateHost operations do not
	// document a dedicated typed exception for this, so it maps to the generic
	// InvalidInputException (see handler.go's error switch).
	ErrAlreadyExists = awserr.New("InvalidInputException", awserr.ErrAlreadyExists)
	// ErrResourceAlreadyExists is returned when a repository link or sync
	// configuration with the same identity already exists. Unlike
	// ErrAlreadyExists above, the real CreateRepositoryLink/CreateSyncConfiguration
	// operations both register a dedicated ResourceAlreadyExistsException for
	// this case (confirmed against aws-sdk-go-v2's per-op error deserializers).
	ErrResourceAlreadyExists = awserr.New("ResourceAlreadyExistsException", awserr.ErrAlreadyExists)
	// ErrValidation is returned when input validation fails.
	ErrValidation = awserr.New("ValidationException", awserr.ErrInvalidParameter)
	// ErrResourceInUse is returned when a host cannot be deleted because a
	// connection still references it. The real DeleteHost operation does not
	// document a dedicated typed exception for this either, so it maps to the
	// generic ConflictException ("two conflicting operations... on the same
	// resource"), which at least exists in the real service's error catalog
	// (unlike a fabricated "ResourceInUseException", which does not).
	ErrResourceInUse = awserr.New("ConflictException", awserr.ErrConflict)
	// ErrSyncConfigStillExists is returned when a repository link cannot be
	// deleted because a sync configuration still references it. The real
	// DeleteRepositoryLink operation documents SyncConfigurationStillExistsException
	// for exactly this case.
	ErrSyncConfigStillExists = awserr.New("SyncConfigurationStillExistsException", awserr.ErrConflict)
	// ErrSyncBlockerNotFound is returned by UpdateSyncBlocker when the blocker ID
	// does not exist (or was created in a different region). The real operation
	// documents SyncBlockerDoesNotExistException for this case; it does NOT
	// resolve unknown IDs gracefully.
	ErrSyncBlockerNotFound = awserr.New("SyncBlockerDoesNotExistException", awserr.ErrNotFound)
)

// validProviderTypes returns the set of valid provider types for connections and hosts.
func validProviderTypes() map[string]bool {
	return map[string]bool{
		"Bitbucket":              true,
		"GitHub":                 true,
		"GitHubEnterpriseServer": true,
		"GitLab":                 true,
		"GitLabSelfManaged":      true,
	}
}

// validSyncTypes returns the set of sync configuration types accepted by AWS CodeStar Connections.
func validSyncTypes() map[string]bool {
	return map[string]bool{
		"CFN_STACK_SYNC": true,
	}
}

// validPublishDeploymentStatus is the set of accepted values.
func validPublishDeploymentStatus() map[string]bool {
	return map[string]bool{
		"ENABLED":  true,
		"DISABLED": true,
	}
}

// validTriggerResourceUpdateOn is the set of accepted values.
func validTriggerResourceUpdateOn() map[string]bool {
	return map[string]bool{
		"ANY_CHANGE":  true,
		"FILE_CHANGE": true,
	}
}

// syncConfigKey returns the composite lookup key for a sync configuration.
func syncConfigKey(resourceName, syncType string) string {
	return resourceName + "/" + syncType
}

// regionKey returns the composite store.Table primary key ("region|id") used
// by every region-nested resource collection below (see store_setup.go).
// RepositoryLink/SyncConfiguration/RepositorySyncStatus/ResourceSyncStatus
// have no ARN of their own to derive a region from (unlike Connection/Host),
// so each carries an unexported region field set at creation time and
// combined with its own identity via this helper.
func regionKey(region, id string) string {
	return region + "|" + id
}

// sortedTagKeys returns the keys of the tags map in sorted order for deterministic output.
func sortedTagKeys(tags map[string]string) []string {
	keys := collections.SortedKeys(tags)

	return keys
}

// validateConnectionName validates the connection/host name rules.
func validateConnectionName(name string) error {
	if name == "" {
		return fmt.Errorf("%w: name is required", ErrValidation)
	}

	if len(name) > maxConnectionNameLen {
		return fmt.Errorf("%w: name must not exceed %d characters", ErrValidation, maxConnectionNameLen)
	}

	if !connectionNameRE.MatchString(name) {
		return fmt.Errorf("%w: name must match [a-zA-Z0-9_.\\-]+", ErrValidation)
	}

	return nil
}

// validateTags validates tag key/value lengths and total count.
func validateTags(tags map[string]string) error {
	if len(tags) > maxTagsPerResource {
		return fmt.Errorf("%w: cannot have more than %d tags", ErrValidation, maxTagsPerResource)
	}

	for k, v := range tags {
		if k == "" {
			return fmt.Errorf("%w: tag key must not be empty", ErrValidation)
		}

		if len(k) > maxTagKeyLen {
			return fmt.Errorf("%w: tag key %q exceeds %d characters", ErrValidation, k, maxTagKeyLen)
		}

		if len(v) > maxTagValueLen {
			return fmt.Errorf("%w: tag value for key %q exceeds %d characters", ErrValidation, k, maxTagValueLen)
		}
	}

	return nil
}

// Connection represents an in-memory AWS CodeStar connection.
//
// ConnectionArn already embeds its own region (arn:partition:service:region:
// account:resource, see regionFromARN), so Connection needs no hidden region
// field: store_setup.go's connections table is keyed directly by
// ConnectionArn and its byRegion/byName indexes derive region from the ARN.
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
//
// Like Connection, HostArn already embeds its own region, so Host needs no
// hidden region field either.
type Host struct {
	Tags             map[string]string `json:"tags,omitempty"`
	VpcConfiguration *VpcConfiguration `json:"vpcConfiguration,omitempty"`
	Name             string            `json:"name"`
	HostArn          string            `json:"hostArn"`
	ProviderType     string            `json:"providerType"`
	ProviderEndpoint string            `json:"providerEndpoint"`
	Status           string            `json:"status"`
	StatusMessage    string            `json:"statusMessage,omitempty"`
}

// repositorySyncStatusKey is the composite lookup key for per-branch/syncType sync status.
func repositorySyncStatusKey(repositoryLinkID, branch, syncType string) string {
	return repositoryLinkID + "/" + branch + "/" + syncType
}

// InMemoryBackend is a thread-safe in-memory store for CodeStar Connections resources.
//
// connections and hosts are "clean" store.Table collections (see
// store_setup.go): each is keyed directly by its own ARN, which already
// embeds its region, so region isolation falls out of the byRegion/byName
// indexes with no hidden fields needed. repositoryLinks, syncConfigurations,
// repositorySyncStatuses, resourceSyncStatuses, and syncBlockers are "dirty":
// their identity (RepositoryLinkID; ResourceName+SyncType;
// RepositoryLinkID+Branch+SyncType; ResourceName+SyncType; ID) carries no
// region of its own, and lookups for the first four are scoped by the
// caller's context region (not by any embedded ARN region), so each type
// carries an unexported region-qualifying field and is registered with a
// composite "region|id" key. connections/hosts are registered on registry;
// the five dirty tables are NOT (store.New only), so they are excluded from
// registry.ResetAll() and must be reset explicitly -- see Reset below and
// persistence.go's mixed clean/dirty Snapshot/Restore.
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

	repositorySyncStatuses *store.Table[RepositorySyncStatus]

	resourceSyncStatuses *store.Table[ResourceSyncStatus]

	syncBlockers           *store.Table[SyncBlocker]
	syncBlockersByResource *store.Index[SyncBlocker]

	accountID     string
	defaultRegion string
}

// NewInMemoryBackend creates a new backend for the given account and region.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		accountID:     accountID,
		defaultRegion: region,
		mu:            lockmetrics.New("codestarconnections"),
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
	// The five dirty tables (see store_setup.go's registerAllTables doc) are
	// deliberately NOT on b.registry, so each needs its own Reset() call here.
	b.repositoryLinks.Reset()
	b.syncConfigurations.Reset()
	b.repositorySyncStatuses.Reset()
	b.resourceSyncStatuses.Reset()
	b.syncBlockers.Reset()
}

// Region returns the default region for this backend instance.
func (b *InMemoryBackend) Region() string { return b.defaultRegion }

// AccountID returns the account ID for this backend instance.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// findResourceTagsLocked returns the tag map for a resource ARN.
// Must be called with at least an RLock held.
func (b *InMemoryBackend) findResourceTagsLocked(resourceArn string) (map[string]string, bool) {
	if conn, ok := b.connections.Get(resourceArn); ok {
		return conn.Tags, true
	}

	if host, ok := b.hosts.Get(resourceArn); ok {
		return host.Tags, true
	}

	return nil, false
}

// ensureTagsLocked returns a non-nil tag map for the resource, initialising it when nil.
// Must be called with a write lock held.
func (b *InMemoryBackend) ensureTagsLocked(resourceArn string) (map[string]string, bool) {
	if conn, ok := b.connections.Get(resourceArn); ok {
		if conn.Tags == nil {
			conn.Tags = make(map[string]string)
		}

		return conn.Tags, true
	}

	if host, ok := b.hosts.Get(resourceArn); ok {
		if host.Tags == nil {
			host.Tags = make(map[string]string)
		}

		return host.Tags, true
	}

	return nil, false
}

// connectionHasReferenceToHostLocked returns true if any connection in the region references hostArn.
// Must be called with at least an RLock held.
func (b *InMemoryBackend) connectionHasReferenceToHostLocked(region, hostArn string) bool {
	for _, conn := range b.connectionsByRegion.Get(region) {
		if conn.HostArn == hostArn {
			return true
		}
	}

	return false
}

// syncConfigHasReferenceToLinkLocked returns true if any sync config references the given repositoryLinkID.
// Must be called with at least an RLock held.
func (b *InMemoryBackend) syncConfigHasReferenceToLinkLocked(region, repositoryLinkID string) bool {
	for _, cfg := range b.syncConfigurationsByRegion.Get(region) {
		if cfg.RepositoryLinkID == repositoryLinkID {
			return true
		}
	}

	return false
}

// CreateConnection creates a new CodeStar connection.
func (b *InMemoryBackend) CreateConnection(
	ctx context.Context,
	name, providerType, hostArn string,
	tags map[string]string,
) (*Connection, error) {
	if err := validateConnectionName(name); err != nil {
		return nil, err
	}

	if providerType != "" && !validProviderTypes()[providerType] {
		return nil, fmt.Errorf("%w: invalid ProviderType %q", ErrValidation, providerType)
	}

	if err := validateTags(tags); err != nil {
		return nil, err
	}

	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("CreateConnection")
	defer b.mu.Unlock()

	if len(b.connectionsByName.Get(regionKey(region, name))) > 0 {
		return nil, fmt.Errorf("%w: connection %q already exists", ErrAlreadyExists, name)
	}

	id := uuid.NewString()
	connArn := arn.Build("codestar-connections", region, b.accountID, "connection/"+id)

	tagsCopy := make(map[string]string, len(tags))
	maps.Copy(tagsCopy, tags)

	conn := &Connection{
		ConnectionName:   name,
		ConnectionArn:    connArn,
		ConnectionStatus: ConnectionStatusAvailable,
		OwnerAccountID:   b.accountID,
		ProviderType:     providerType,
		HostArn:          hostArn,
		Tags:             tagsCopy,
	}
	b.connections.Put(conn)

	cp := *conn
	cp.Tags = make(map[string]string, len(conn.Tags))
	maps.Copy(cp.Tags, conn.Tags)

	return &cp, nil
}

// GetConnection returns a connection by ARN.
func (b *InMemoryBackend) GetConnection(_ context.Context, connectionArn string) (*Connection, error) {
	b.mu.RLock("GetConnection")
	defer b.mu.RUnlock()

	conn, ok := b.connections.Get(connectionArn)
	if !ok {
		return nil, fmt.Errorf("%w: connection not found: %s", ErrNotFound, connectionArn)
	}

	cp := *conn
	cp.Tags = make(map[string]string, len(conn.Tags))
	maps.Copy(cp.Tags, conn.Tags)

	return &cp, nil
}

// ListConnections returns all connections sorted by name, optionally filtered by provider type or host ARN.
func (b *InMemoryBackend) ListConnections(ctx context.Context, providerTypeFilter, hostArnFilter string) []*Connection {
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

	sort.Slice(result, func(i, j int) bool {
		return result[i].ConnectionName < result[j].ConnectionName
	})

	return result
}

// DeleteConnection removes a connection by ARN.
func (b *InMemoryBackend) DeleteConnection(_ context.Context, connectionArn string) error {
	b.mu.Lock("DeleteConnection")
	defer b.mu.Unlock()

	if !b.connections.Has(connectionArn) {
		return fmt.Errorf("%w: connection not found: %s", ErrNotFound, connectionArn)
	}

	b.connections.Delete(connectionArn)

	return nil
}

// CreateHost creates a new CodeStar host.
func (b *InMemoryBackend) CreateHost(
	ctx context.Context,
	name, providerType, providerEndpoint string,
	vpcConfig *VpcConfiguration,
	tags map[string]string,
) (*Host, error) {
	if err := validateConnectionName(name); err != nil {
		return nil, err
	}

	if providerEndpoint == "" {
		return nil, fmt.Errorf("%w: ProviderEndpoint is required", ErrValidation)
	}

	if len(providerEndpoint) > maxProviderEndpointLen {
		return nil, fmt.Errorf("%w: ProviderEndpoint must not exceed %d characters",
			ErrValidation, maxProviderEndpointLen)
	}

	if providerType != "" && !validProviderTypes()[providerType] {
		return nil, fmt.Errorf("%w: invalid ProviderType %q", ErrValidation, providerType)
	}

	if err := validateTags(tags); err != nil {
		return nil, err
	}

	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("CreateHost")
	defer b.mu.Unlock()

	if len(b.hostsByName.Get(regionKey(region, name))) > 0 {
		return nil, fmt.Errorf("%w: host %q already exists", ErrAlreadyExists, name)
	}

	id := uuid.NewString()
	hostArn := arn.Build("codestar-connections", region, b.accountID, "host/"+name+"/"+id[:8])

	tagsCopy := make(map[string]string, len(tags))
	maps.Copy(tagsCopy, tags)

	host := &Host{
		Name:             name,
		HostArn:          hostArn,
		ProviderType:     providerType,
		ProviderEndpoint: providerEndpoint,
		Status:           HostStatusPending,
		VpcConfiguration: vpcConfig,
		Tags:             tagsCopy,
	}
	b.hosts.Put(host)

	cp := *host
	cp.Tags = make(map[string]string, len(host.Tags))
	maps.Copy(cp.Tags, host.Tags)

	return &cp, nil
}

// GetHost returns a host by ARN.
func (b *InMemoryBackend) GetHost(_ context.Context, hostArn string) (*Host, error) {
	b.mu.RLock("GetHost")
	defer b.mu.RUnlock()

	host, ok := b.hosts.Get(hostArn)
	if !ok {
		return nil, fmt.Errorf("%w: host not found: %s", ErrNotFound, hostArn)
	}

	cp := *host
	cp.Tags = make(map[string]string, len(host.Tags))
	maps.Copy(cp.Tags, host.Tags)

	return &cp, nil
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

// DeleteHost removes a host by ARN. Returns ErrResourceInUse if any connection references the host.
func (b *InMemoryBackend) DeleteHost(ctx context.Context, hostArn string) error {
	region := regionFromARN(hostArn, getRegion(ctx, b.defaultRegion))

	b.mu.Lock("DeleteHost")
	defer b.mu.Unlock()

	host, ok := b.hosts.Get(hostArn)
	if !ok {
		return fmt.Errorf("%w: host not found: %s", ErrNotFound, hostArn)
	}

	if b.connectionHasReferenceToHostLocked(region, hostArn) {
		return fmt.Errorf("%w: host %q has active connections; delete them first", ErrResourceInUse, host.Name)
	}

	b.hosts.Delete(hostArn)

	return nil
}

// UpdateHost updates the provider endpoint and optional VPC configuration for a host.
func (b *InMemoryBackend) UpdateHost(
	_ context.Context,
	hostArn, providerEndpoint string,
	vpcConfig *VpcConfiguration,
) error {
	if providerEndpoint != "" && len(providerEndpoint) > maxProviderEndpointLen {
		return fmt.Errorf("%w: ProviderEndpoint must not exceed %d characters", ErrValidation, maxProviderEndpointLen)
	}

	b.mu.Lock("UpdateHost")
	defer b.mu.Unlock()

	host, ok := b.hosts.Get(hostArn)
	if !ok {
		return fmt.Errorf("%w: host not found: %s", ErrNotFound, hostArn)
	}

	// ProviderEndpoint/VpcConfiguration are not part of any index key
	// (hosts is keyed by HostArn; byRegion/byName derive from HostArn/Name),
	// so mutating the stored *Host in place is safe -- no Delete+Put needed.
	if providerEndpoint != "" {
		host.ProviderEndpoint = providerEndpoint
	}

	if vpcConfig != nil {
		host.VpcConfiguration = vpcConfig
	}

	return nil
}

// ListTagsForResource returns the tags for a resource by ARN.
func (b *InMemoryBackend) ListTagsForResource(_ context.Context, resourceArn string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	existing, ok := b.findResourceTagsLocked(resourceArn)
	if !ok {
		return nil, fmt.Errorf("%w: resource not found: %s", ErrNotFound, resourceArn)
	}

	result := make(map[string]string, len(existing))
	maps.Copy(result, existing)

	return result, nil
}

// TagResource adds or updates tags on a resource.
func (b *InMemoryBackend) TagResource(_ context.Context, resourceArn string, tags map[string]string) error {
	if err := validateTags(tags); err != nil {
		return err
	}

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	existing, ok := b.ensureTagsLocked(resourceArn)
	if !ok {
		return fmt.Errorf("%w: resource not found: %s", ErrNotFound, resourceArn)
	}

	// Check total count after applying new tags.
	merged := make(map[string]string, len(existing)+len(tags))
	maps.Copy(merged, existing)
	maps.Copy(merged, tags)

	if len(merged) > maxTagsPerResource {
		return fmt.Errorf("%w: cannot have more than %d tags on a resource", ErrValidation, maxTagsPerResource)
	}

	maps.Copy(existing, tags)

	return nil
}

// UntagResource removes tags from a resource.
func (b *InMemoryBackend) UntagResource(_ context.Context, resourceArn string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	existing, ok := b.findResourceTagsLocked(resourceArn)
	if !ok {
		return fmt.Errorf("%w: resource not found: %s", ErrNotFound, resourceArn)
	}

	for _, k := range tagKeys {
		delete(existing, k)
	}

	return nil
}

// AddConnectionInternal seeds a connection directly for testing.
func (b *InMemoryBackend) AddConnectionInternal(conn *Connection) {
	b.mu.Lock("AddConnectionInternal")
	defer b.mu.Unlock()

	b.connections.Put(conn)
}

// AddHostInternal seeds a host directly for testing.
func (b *InMemoryBackend) AddHostInternal(host *Host) {
	b.mu.Lock("AddHostInternal")
	defer b.mu.Unlock()

	b.hosts.Put(host)
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
	// region is the store.Table composite-key qualifier (see regionKey and
	// store_setup.go); it is unexported so a plain json.Marshal(RepositoryLink)
	// never sees it and is instead carried through persistence via a
	// regionalDTO (see persistence.go). Unlike Connection/Host,
	// GetRepositoryLink/DeleteRepositoryLink/UpdateRepositoryLink resolve
	// their region from the caller's context rather than from any ARN (a
	// bare RepositoryLinkID carries no region of its own), so region must be
	// captured explicitly at creation time and used for every subsequent
	// lookup by ID.
	region string
}

// CreateRepositoryLink creates a new repository link.
func (b *InMemoryBackend) CreateRepositoryLink(
	ctx context.Context,
	connectionArn, ownerID, repoName, encryptionKeyArn string,
) (*RepositoryLink, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("CreateRepositoryLink")
	defer b.mu.Unlock()

	// Derive provider type from the connection if it exists (Connection is
	// keyed directly by its own ARN, which already embeds its region).
	providerType := ""
	if conn, ok := b.connections.Get(connectionArn); ok {
		providerType = conn.ProviderType
	}

	// Check for duplicate: same connection + owner + repo, within this region.
	for _, existing := range b.repositoryLinksByRegion.Get(region) {
		if existing.ConnectionArn == connectionArn &&
			existing.OwnerID == ownerID &&
			existing.RepositoryName == repoName {
			return nil, fmt.Errorf(
				"%w: repository link for %s/%s already exists", ErrResourceAlreadyExists, ownerID, repoName,
			)
		}
	}

	id := uuid.NewString()
	linkArn := arn.Build("codestar-connections", region, b.accountID, "repository-link/"+id)

	link := &RepositoryLink{
		ConnectionArn:     connectionArn,
		OwnerID:           ownerID,
		RepositoryName:    repoName,
		RepositoryLinkID:  id,
		RepositoryLinkArn: linkArn,
		ProviderType:      providerType,
		EncryptionKeyArn:  encryptionKeyArn,
		CreatedAt:         time.Now().UTC(),
		region:            region,
	}

	b.repositoryLinks.Put(link)

	cp := *link

	return &cp, nil
}

// GetRepositoryLink retrieves a repository link by ID.
func (b *InMemoryBackend) GetRepositoryLink(ctx context.Context, repositoryLinkID string) (*RepositoryLink, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("GetRepositoryLink")
	defer b.mu.RUnlock()

	link, ok := b.repositoryLinks.Get(regionKey(region, repositoryLinkID))
	if !ok {
		return nil, fmt.Errorf("%w: repository link not found: %s", ErrNotFound, repositoryLinkID)
	}

	cp := *link

	return &cp, nil
}

// DeleteRepositoryLink removes a repository link by ID. Returns ErrResourceInUse if sync configs reference it.
func (b *InMemoryBackend) DeleteRepositoryLink(ctx context.Context, repositoryLinkID string) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("DeleteRepositoryLink")
	defer b.mu.Unlock()

	key := regionKey(region, repositoryLinkID)
	if !b.repositoryLinks.Has(key) {
		return fmt.Errorf("%w: repository link not found: %s", ErrNotFound, repositoryLinkID)
	}

	if b.syncConfigHasReferenceToLinkLocked(region, repositoryLinkID) {
		return fmt.Errorf("%w: repository link %q has active sync configurations; delete them first",
			ErrSyncConfigStillExists, repositoryLinkID)
	}

	b.repositoryLinks.Delete(key)

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
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].RepositoryLinkID < result[j].RepositoryLinkID
	})

	return result
}

// AddRepositoryLinkInternal seeds a repository link directly for testing.
func (b *InMemoryBackend) AddRepositoryLinkInternal(ctx context.Context, link *RepositoryLink) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("AddRepositoryLinkInternal")
	defer b.mu.Unlock()

	link.region = region
	b.repositoryLinks.Put(link)
}

// SyncConfiguration represents an in-memory AWS CodeStar Connections sync configuration.
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
	// carries no region of its own and every lookup is scoped by the
	// caller's context region, exactly like RepositoryLink.region above. See
	// persistence.go for how it survives Snapshot/Restore.
	region string
}

// CreateSyncConfiguration creates a new sync configuration.
func (b *InMemoryBackend) CreateSyncConfiguration(
	ctx context.Context,
	branch, configFile, repositoryLinkID, resourceName, roleArn, syncType string,
) (*SyncConfiguration, error) {
	return b.CreateSyncConfigurationFull(
		ctx, branch, configFile, repositoryLinkID, resourceName, roleArn, syncType, "", "",
	)
}

// CreateSyncConfigurationFull creates a sync configuration with optional
// PublishDeploymentStatus and TriggerResourceUpdateOn.
func (b *InMemoryBackend) CreateSyncConfigurationFull(
	ctx context.Context,
	branch, configFile, repositoryLinkID, resourceName, roleArn, syncType,
	publishDeploymentStatus, triggerResourceUpdateOn string,
) (*SyncConfiguration, error) {
	if !validSyncTypes()[syncType] {
		return nil, fmt.Errorf("%w: invalid SyncType %q", ErrValidation, syncType)
	}

	if strings.Contains(resourceName, "/") {
		return nil, fmt.Errorf("%w: ResourceName must not contain \"/\"", ErrValidation)
	}

	if publishDeploymentStatus != "" && !validPublishDeploymentStatus()[publishDeploymentStatus] {
		return nil, fmt.Errorf("%w: invalid PublishDeploymentStatus %q", ErrValidation, publishDeploymentStatus)
	}

	if triggerResourceUpdateOn != "" && !validTriggerResourceUpdateOn()[triggerResourceUpdateOn] {
		return nil, fmt.Errorf("%w: invalid TriggerResourceUpdateOn %q", ErrValidation, triggerResourceUpdateOn)
	}

	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("CreateSyncConfiguration")
	defer b.mu.Unlock()

	// Derive owner/provider/repo from the link if it exists.
	ownerID := ""
	providerType := ""
	repoName := ""

	if link, ok := b.repositoryLinks.Get(regionKey(region, repositoryLinkID)); ok {
		ownerID = link.OwnerID
		providerType = link.ProviderType
		repoName = link.RepositoryName
	}

	// Check for duplicate.
	key := regionKey(region, syncConfigKey(resourceName, syncType))
	if b.syncConfigurations.Has(key) {
		return nil, fmt.Errorf("%w: sync configuration for %q/%q already exists",
			ErrResourceAlreadyExists, resourceName, syncType)
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

	// Seed an initial sync status for this resource. resourceSyncStatusKeyFn
	// (store_setup.go) derives the exact same composite key from
	// ResourceName/SyncType/region, so key is reused as-is.
	b.resourceSyncStatuses.Put(&ResourceSyncStatus{
		StartedAt:    time.Now().UTC(),
		Status:       SyncStatusSucceeded,
		Events:       []SyncEvent{},
		region:       region,
		resourceName: resourceName,
		syncType:     syncType,
	})

	cp := *cfg

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
		return nil, fmt.Errorf("%w: sync configuration not found: %s/%s", ErrNotFound, resourceName, syncType)
	}

	cp := *cfg

	return &cp, nil
}

// DeleteSyncConfiguration removes a sync configuration.
func (b *InMemoryBackend) DeleteSyncConfiguration(ctx context.Context, resourceName, syncType string) error {
	if resourceName == "" {
		return fmt.Errorf("%w: ResourceName is required", ErrValidation)
	}

	if !validSyncTypes()[syncType] {
		return fmt.Errorf("%w: invalid SyncType %q", ErrValidation, syncType)
	}

	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("DeleteSyncConfiguration")
	defer b.mu.Unlock()

	key := regionKey(region, syncConfigKey(resourceName, syncType))
	if !b.syncConfigurations.Has(key) {
		return fmt.Errorf("%w: sync configuration not found: %s/%s", ErrNotFound, resourceName, syncType)
	}

	b.syncConfigurations.Delete(key)

	// Remove associated sync status (same composite key shape as above).
	b.resourceSyncStatuses.Delete(key)

	// Remove associated sync blockers. The Index result slice mutates as
	// entries are deleted from the underlying table, so it must be cloned
	// before the delete loop runs.
	for _, blocker := range slices.Clone(b.syncBlockersByResource.Get(key)) {
		b.syncBlockers.Delete(blocker.ID)
	}

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
	// region/repositoryLinkID/branch/syncType are the store.Table composite-
	// key components: RepositorySyncStatus carries no identity field of its
	// own (it is looked up purely via repositorySyncStatusKey), so each is
	// captured explicitly at write time and carried through persistence via
	// a dedicated DTO (see persistence.go).
	region           string
	repositoryLinkID string
	branch           string
	syncType         string
	Events           []SyncEvent
}

// GetRepositorySyncStatus returns the latest sync status for a repository link and branch.
func (b *InMemoryBackend) GetRepositorySyncStatus(
	ctx context.Context,
	repositoryLinkID, branch, syncType string,
) (*RepositorySyncStatus, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("GetRepositorySyncStatus")
	defer b.mu.RUnlock()

	if !b.repositoryLinks.Has(regionKey(region, repositoryLinkID)) {
		return nil, fmt.Errorf("%w: repository link not found: %s", ErrNotFound, repositoryLinkID)
	}

	key := regionKey(region, repositorySyncStatusKey(repositoryLinkID, branch, syncType))
	if s, ok := b.repositorySyncStatuses.Get(key); ok {
		cp := *s
		cp.Events = append([]SyncEvent(nil), s.Events...)

		return &cp, nil
	}

	return &RepositorySyncStatus{
		StartedAt: time.Now().UTC(),
		Status:    SyncStatusSucceeded,
		Events:    []SyncEvent{},
	}, nil
}

// SetRepositorySyncStatus stores a sync status for a repository link/branch/syncType (test helper).
func (b *InMemoryBackend) SetRepositorySyncStatus(
	ctx context.Context,
	repositoryLinkID, branch, syncType, status string,
	events []SyncEvent,
) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("SetRepositorySyncStatus")
	defer b.mu.Unlock()

	b.repositorySyncStatuses.Put(&RepositorySyncStatus{
		StartedAt:        time.Now().UTC(),
		Status:           status,
		Events:           events,
		region:           region,
		repositoryLinkID: repositoryLinkID,
		branch:           branch,
		syncType:         syncType,
	})
}

// ResourceSyncStatus holds the latest sync attempt for an AWS resource.
type ResourceSyncStatus struct {
	StartedAt time.Time
	Status    string
	// region/resourceName/syncType are the store.Table composite-key
	// components: ResourceSyncStatus carries no identity field of its own,
	// exactly like RepositorySyncStatus above.
	region       string
	resourceName string
	syncType     string
	Events       []SyncEvent
}

// GetResourceSyncStatus returns the latest sync status for a resource.
func (b *InMemoryBackend) GetResourceSyncStatus(
	ctx context.Context,
	resourceName, syncType string,
) (*ResourceSyncStatus, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("GetResourceSyncStatus")
	defer b.mu.RUnlock()

	key := regionKey(region, syncConfigKey(resourceName, syncType))
	if !b.syncConfigurations.Has(key) {
		return nil, fmt.Errorf("%w: sync configuration not found: %s/%s", ErrNotFound, resourceName, syncType)
	}

	if s, ok := b.resourceSyncStatuses.Get(key); ok {
		cp := *s
		cp.Events = append([]SyncEvent(nil), s.Events...)

		return &cp, nil
	}

	return &ResourceSyncStatus{
		StartedAt: time.Now().UTC(),
		Status:    SyncStatusSucceeded,
		Events:    []SyncEvent{},
	}, nil
}

// SetResourceSyncStatus stores a sync status for a resource (test helper).
func (b *InMemoryBackend) SetResourceSyncStatus(
	ctx context.Context,
	resourceName, syncType, status string,
	events []SyncEvent,
) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("SetResourceSyncStatus")
	defer b.mu.Unlock()

	b.resourceSyncStatuses.Put(&ResourceSyncStatus{
		StartedAt:    time.Now().UTC(),
		Status:       status,
		Events:       events,
		region:       region,
		resourceName: resourceName,
		syncType:     syncType,
	})
}

// SyncBlockerSummary is a summary of sync blockers for a resource.
type SyncBlockerSummary struct {
	ResourceName       string
	ParentResourceName string
	LatestBlockers     []SyncBlocker
}

// SyncBlocker represents a single sync blocker entry.
type SyncBlocker struct {
	ID             string
	Type           string
	Status         string
	CreatedAt      time.Time
	CreatedReason  string
	ResolvedAt     *time.Time
	ResolvedReason string
	ResourceName   string
	SyncType       string
	// region qualifies the byResource secondary index (see
	// syncBlockerResourceIndexKeyFn in store_setup.go): ResourceName+SyncType
	// alone is not region-unique, and UpdateSyncBlocker's original map-based
	// lookup was itself scoped by the caller's context region, so region is
	// captured at creation time and re-checked on every ID-based lookup (see
	// UpdateSyncBlocker) to preserve that scoping even though ID itself is
	// already globally unique.
	region string
}

// GetSyncBlockerSummary returns the sync blocker summary for a resource.
func (b *InMemoryBackend) GetSyncBlockerSummary(
	ctx context.Context,
	resourceName, syncType string,
) (*SyncBlockerSummary, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("GetSyncBlockerSummary")
	defer b.mu.RUnlock()

	key := regionKey(region, syncConfigKey(resourceName, syncType))
	if !b.syncConfigurations.Has(key) {
		return nil, fmt.Errorf("%w: sync configuration not found: %s/%s", ErrNotFound, resourceName, syncType)
	}

	summary := &SyncBlockerSummary{
		ResourceName:   resourceName,
		LatestBlockers: []SyncBlocker{},
	}

	group := b.syncBlockersByResource.Get(key)
	blockers := make([]SyncBlocker, 0, len(group))

	for _, blocker := range group {
		blockers = append(blockers, *blocker)
	}

	// Sort by CreatedAt descending.
	sort.Slice(blockers, func(i, j int) bool {
		return blockers[i].CreatedAt.After(blockers[j].CreatedAt)
	})

	summary.LatestBlockers = blockers

	return summary, nil
}

// CreateSyncBlocker creates a new sync blocker for a resource (test helper + internal use).
func (b *InMemoryBackend) CreateSyncBlocker(
	ctx context.Context,
	resourceName, syncType, blockerType, createdReason string,
) (*SyncBlocker, error) {
	if !validSyncTypes()[syncType] {
		return nil, fmt.Errorf("%w: invalid SyncType %q", ErrValidation, syncType)
	}

	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("CreateSyncBlocker")
	defer b.mu.Unlock()

	key := regionKey(region, syncConfigKey(resourceName, syncType))
	if !b.syncConfigurations.Has(key) {
		return nil, fmt.Errorf("%w: sync configuration not found: %s/%s", ErrNotFound, resourceName, syncType)
	}

	id := uuid.NewString()
	blocker := &SyncBlocker{
		ID:            id,
		Type:          blockerType,
		Status:        SyncBlockerStatusActive,
		CreatedAt:     time.Now().UTC(),
		CreatedReason: createdReason,
		ResourceName:  resourceName,
		SyncType:      syncType,
		region:        region,
	}

	b.syncBlockers.Put(blocker)

	cp := *blocker

	return &cp, nil
}

// UpdateSyncBlocker resolves a sync blocker by ID. If the blocker ID is not found
// (or was created in a different region than the caller's context, matching the
// original map-based lookup's region scoping), returns ErrSyncBlockerNotFound --
// the real UpdateSyncBlocker operation documents SyncBlockerDoesNotExistException
// for exactly this case, it does not resolve unknown IDs gracefully.
func (b *InMemoryBackend) UpdateSyncBlocker(
	ctx context.Context,
	id, resolvedReason string,
) (*SyncBlockerSummary, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("UpdateSyncBlocker")
	defer b.mu.Unlock()

	blocker, ok := b.syncBlockers.Get(id)
	if !ok || blocker.region != region {
		return nil, fmt.Errorf("%w: sync blocker not found: %s", ErrSyncBlockerNotFound, id)
	}

	now := time.Now().UTC()
	// Status/ResolvedReason/ResolvedAt are not part of any index key
	// (syncBlockers is keyed by ID; byResource derives from
	// region/ResourceName/SyncType, none of which change here), so mutating
	// the stored *SyncBlocker in place is safe -- no Delete+Put needed.
	blocker.Status = SyncBlockerStatusResolved
	blocker.ResolvedReason = resolvedReason
	blocker.ResolvedAt = &now

	// Return summary for the resource that owns this blocker.
	key := regionKey(region, syncConfigKey(blocker.ResourceName, blocker.SyncType))
	summary := &SyncBlockerSummary{
		ResourceName:   blocker.ResourceName,
		LatestBlockers: []SyncBlocker{},
	}

	group := b.syncBlockersByResource.Get(key)
	for _, b2 := range group {
		summary.LatestBlockers = append(summary.LatestBlockers, *b2)
	}

	sort.Slice(summary.LatestBlockers, func(i, j int) bool {
		return summary.LatestBlockers[i].CreatedAt.After(summary.LatestBlockers[j].CreatedAt)
	})

	return summary, nil
}

// RepositorySyncDefinition is a mapping from a repository branch to the AWS
// resource(s) being synced from that branch (see AWS docs for
// RepositorySyncDefinition).
type RepositorySyncDefinition struct {
	Branch    string
	Directory string
	Parent    string
	Target    string
}

// ListRepositorySyncDefinitions returns the sync definitions derived from the
// sync configurations linked to repositoryLinkID, optionally filtered by
// syncType. Directory is sourced from each sync configuration's ConfigFile
// (per AWS docs: "This value comes from creating or updating the config-file
// field of a sync-configuration"). For CFN_STACK_SYNC -- the only SyncType
// gopherstack supports -- AWS docs state "the parent and target resource are
// the same", so Parent and Target both equal ResourceName.
func (b *InMemoryBackend) ListRepositorySyncDefinitions(
	ctx context.Context,
	repositoryLinkID, syncType string,
) ([]RepositorySyncDefinition, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("ListRepositorySyncDefinitions")
	defer b.mu.RUnlock()

	if !b.repositoryLinks.Has(regionKey(region, repositoryLinkID)) {
		return nil, fmt.Errorf("%w: repository link not found: %s", ErrNotFound, repositoryLinkID)
	}

	cfgs := b.syncConfigurationsByRegion.Get(region)
	result := make([]RepositorySyncDefinition, 0, len(cfgs))

	for _, cfg := range cfgs {
		if cfg.RepositoryLinkID != repositoryLinkID {
			continue
		}

		if syncType != "" && cfg.SyncType != syncType {
			continue
		}

		result = append(result, RepositorySyncDefinition{
			Branch:    cfg.Branch,
			Directory: cfg.ConfigFile,
			Parent:    cfg.ResourceName,
			Target:    cfg.ResourceName,
		})
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Target < result[j].Target
	})

	return result, nil
}

// ListSyncConfigurations returns all sync configurations for a given repository link and sync type.
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
		return nil, fmt.Errorf("%w: repository link not found: %s", ErrNotFound, repositoryLinkID)
	}

	// ConnectionArn/EncryptionKeyArn are not part of the repositoryLinks key
	// (region|RepositoryLinkID) or the byRegion index, so mutating the
	// stored *RepositoryLink in place is safe -- no Delete+Put needed.
	if connectionArn != "" {
		link.ConnectionArn = connectionArn
	}

	if encryptionKeyArn != "" {
		link.EncryptionKeyArn = encryptionKeyArn
	}

	cp := *link

	return &cp, nil
}

// UpdateSyncConfiguration updates branch, config file, role ARN, or repository link for a sync configuration.
func (b *InMemoryBackend) UpdateSyncConfiguration(
	ctx context.Context,
	resourceName, syncType, branch, configFile, repositoryLinkID, roleArn string,
) (*SyncConfiguration, error) {
	return b.UpdateSyncConfigurationFull(
		ctx, resourceName, syncType, branch, configFile, repositoryLinkID, roleArn, "", "",
	)
}

// UpdateSyncConfigurationFull updates a sync configuration including optional publish/trigger fields.
func (b *InMemoryBackend) UpdateSyncConfigurationFull(
	ctx context.Context,
	resourceName, syncType, branch, configFile, repositoryLinkID, roleArn,
	publishDeploymentStatus, triggerResourceUpdateOn string,
) (*SyncConfiguration, error) {
	if syncType != "" && !validSyncTypes()[syncType] {
		return nil, fmt.Errorf("%w: invalid SyncType %q", ErrValidation, syncType)
	}

	if publishDeploymentStatus != "" && !validPublishDeploymentStatus()[publishDeploymentStatus] {
		return nil, fmt.Errorf("%w: invalid PublishDeploymentStatus %q", ErrValidation, publishDeploymentStatus)
	}

	if triggerResourceUpdateOn != "" && !validTriggerResourceUpdateOn()[triggerResourceUpdateOn] {
		return nil, fmt.Errorf("%w: invalid TriggerResourceUpdateOn %q", ErrValidation, triggerResourceUpdateOn)
	}

	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("UpdateSyncConfiguration")
	defer b.mu.Unlock()

	key := regionKey(region, syncConfigKey(resourceName, syncType))

	cfg, ok := b.syncConfigurations.Get(key)
	if !ok {
		return nil, fmt.Errorf("%w: sync configuration not found: %s/%s", ErrNotFound, resourceName, syncType)
	}

	// None of the fields below (Branch/ConfigFile/RepositoryLinkID/RoleArn/
	// PublishDeploymentStatus/TriggerResourceUpdateOn) are part of the
	// syncConfigurations key (region|ResourceName/SyncType) or the byRegion
	// index, so mutating the stored *SyncConfiguration in place is safe --
	// no Delete+Put needed.
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
