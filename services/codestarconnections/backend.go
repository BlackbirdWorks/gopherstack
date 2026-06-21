// Package codestarconnections provides an in-memory implementation of the AWS CodeStar Connections service.
package codestarconnections

import (
	"context"
	"fmt"
	"maps"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
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
	HostStatusAvailable = "AVAILABLE"
	HostStatusPending   = "PENDING"
)

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
	// ErrAlreadyExists is returned when a resource with the same name already exists.
	ErrAlreadyExists = awserr.New("InvalidInputException", awserr.ErrAlreadyExists)
	// ErrValidation is returned when input validation fails.
	ErrValidation = awserr.New("ValidationException", awserr.ErrInvalidParameter)
	// ErrResourceInUse is returned when a resource cannot be deleted because it is referenced by another resource.
	ErrResourceInUse = awserr.New("ResourceInUseException", awserr.ErrConflict)
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

// syncConfigKey returns the composite map key for a sync configuration.
func syncConfigKey(resourceName, syncType string) string {
	return resourceName + "/" + syncType
}

// sortedTagKeys returns the keys of the tags map in sorted order for deterministic output.
func sortedTagKeys(tags map[string]string) []string {
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}

	sort.Strings(keys)

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

// repositorySyncStatusKey is the composite key for per-branch/syncType sync status.
func repositorySyncStatusKey(repositoryLinkID, branch, syncType string) string {
	return repositoryLinkID + "/" + branch + "/" + syncType
}

// InMemoryBackend is a thread-safe in-memory store for CodeStar Connections resources.
//
// All resource maps are nested by region (outer key = region) so that
// same-named resources are isolated across regions. The per-region inner maps
// are created lazily via the *Store helpers. Callers must hold b.mu while
// accessing the inner maps.
type InMemoryBackend struct {
	connections            map[string]map[string]*Connection           // region → ARN → Connection
	connectionsByName      map[string]map[string]string                // region → name → ARN
	hosts                  map[string]map[string]*Host                 // region → ARN → Host
	hostsByName            map[string]map[string]string                // region → name → ARN
	repositoryLinks        map[string]map[string]*RepositoryLink       // region → ID → RepositoryLink
	syncConfigurations     map[string]map[string]*SyncConfiguration    // region → key → SyncConfiguration
	repositorySyncStatuses map[string]map[string]*RepositorySyncStatus // region → statusKey → status
	resourceSyncStatuses   map[string]map[string]*ResourceSyncStatus   // region → key → status
	syncBlockers           map[string]map[string]*SyncBlocker          // region → blockerID → blocker
	syncBlockersByResource map[string]map[string][]string              // region → configKey → []blockerID
	mu                     *lockmetrics.RWMutex
	accountID              string
	defaultRegion          string
}

// NewInMemoryBackend creates a new backend for the given account and region.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		connections:            make(map[string]map[string]*Connection),
		connectionsByName:      make(map[string]map[string]string),
		hosts:                  make(map[string]map[string]*Host),
		hostsByName:            make(map[string]map[string]string),
		repositoryLinks:        make(map[string]map[string]*RepositoryLink),
		syncConfigurations:     make(map[string]map[string]*SyncConfiguration),
		repositorySyncStatuses: make(map[string]map[string]*RepositorySyncStatus),
		resourceSyncStatuses:   make(map[string]map[string]*ResourceSyncStatus),
		syncBlockers:           make(map[string]map[string]*SyncBlocker),
		syncBlockersByResource: make(map[string]map[string][]string),
		accountID:              accountID,
		defaultRegion:          region,
		mu:                     lockmetrics.New("codestarconnections"),
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

func (b *InMemoryBackend) repositorySyncStatusesStore(region string) map[string]*RepositorySyncStatus {
	if b.repositorySyncStatuses[region] == nil {
		b.repositorySyncStatuses[region] = make(map[string]*RepositorySyncStatus)
	}

	return b.repositorySyncStatuses[region]
}

func (b *InMemoryBackend) resourceSyncStatusesStore(region string) map[string]*ResourceSyncStatus {
	if b.resourceSyncStatuses[region] == nil {
		b.resourceSyncStatuses[region] = make(map[string]*ResourceSyncStatus)
	}

	return b.resourceSyncStatuses[region]
}

func (b *InMemoryBackend) syncBlockersStore(region string) map[string]*SyncBlocker {
	if b.syncBlockers[region] == nil {
		b.syncBlockers[region] = make(map[string]*SyncBlocker)
	}

	return b.syncBlockers[region]
}

func (b *InMemoryBackend) syncBlockersByResourceStore(region string) map[string][]string {
	if b.syncBlockersByResource[region] == nil {
		b.syncBlockersByResource[region] = make(map[string][]string)
	}

	return b.syncBlockersByResource[region]
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
	b.repositorySyncStatuses = make(map[string]map[string]*RepositorySyncStatus)
	b.resourceSyncStatuses = make(map[string]map[string]*ResourceSyncStatus)
	b.syncBlockers = make(map[string]map[string]*SyncBlocker)
	b.syncBlockersByResource = make(map[string]map[string][]string)
}

// Region returns the default region for this backend instance.
func (b *InMemoryBackend) Region() string { return b.defaultRegion }

// AccountID returns the account ID for this backend instance.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// findResourceTagsLocked returns the tag map for a resource ARN within the resolved region.
// Must be called with at least an RLock held.
func (b *InMemoryBackend) findResourceTagsLocked(region, resourceArn string) (map[string]string, bool) {
	if conns := b.connections[region]; conns != nil {
		if conn, ok := conns[resourceArn]; ok {
			return conn.Tags, true
		}
	}

	if hs := b.hosts[region]; hs != nil {
		if host, ok := hs[resourceArn]; ok {
			return host.Tags, true
		}
	}

	return nil, false
}

// ensureTagsLocked returns a non-nil tag map for the resource, initialising it when nil.
// Must be called with a write lock held.
func (b *InMemoryBackend) ensureTagsLocked(region, resourceArn string) (map[string]string, bool) {
	if conns := b.connections[region]; conns != nil {
		if conn, ok := conns[resourceArn]; ok {
			if conn.Tags == nil {
				conn.Tags = make(map[string]string)
			}

			return conn.Tags, true
		}
	}

	if hs := b.hosts[region]; hs != nil {
		if host, ok := hs[resourceArn]; ok {
			if host.Tags == nil {
				host.Tags = make(map[string]string)
			}

			return host.Tags, true
		}
	}

	return nil, false
}

// connectionHasReferenceToHostLocked returns true if any connection in the region references hostArn.
// Must be called with at least an RLock held.
func (b *InMemoryBackend) connectionHasReferenceToHostLocked(region, hostArn string) bool {
	conns := b.connections[region]
	for _, conn := range conns {
		if conn.HostArn == hostArn {
			return true
		}
	}

	return false
}

// syncConfigHasReferenceToLinkLocked returns true if any sync config references the given repositoryLinkID.
// Must be called with at least an RLock held.
func (b *InMemoryBackend) syncConfigHasReferenceToLinkLocked(region, repositoryLinkID string) bool {
	cfgs := b.syncConfigurations[region]
	for _, cfg := range cfgs {
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

	byName := b.connectionsByNameStore(region)
	if _, exists := byName[name]; exists {
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
	b.connectionsStore(region)[connArn] = conn
	byName[name] = connArn

	cp := *conn
	cp.Tags = make(map[string]string, len(conn.Tags))
	maps.Copy(cp.Tags, conn.Tags)

	return &cp, nil
}

// GetConnection returns a connection by ARN.
func (b *InMemoryBackend) GetConnection(ctx context.Context, connectionArn string) (*Connection, error) {
	region := regionFromARN(connectionArn, getRegion(ctx, b.defaultRegion))

	b.mu.RLock("GetConnection")
	defer b.mu.RUnlock()

	conns := b.connections[region]
	if conns == nil {
		return nil, fmt.Errorf("%w: connection not found: %s", ErrNotFound, connectionArn)
	}

	conn, ok := conns[connectionArn]
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

	conns := b.connections[region]
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
func (b *InMemoryBackend) DeleteConnection(ctx context.Context, connectionArn string) error {
	region := regionFromARN(connectionArn, getRegion(ctx, b.defaultRegion))

	b.mu.Lock("DeleteConnection")
	defer b.mu.Unlock()

	conns := b.connections[region]
	if conns == nil {
		return fmt.Errorf("%w: connection not found: %s", ErrNotFound, connectionArn)
	}

	conn, ok := conns[connectionArn]
	if !ok {
		return fmt.Errorf("%w: connection not found: %s", ErrNotFound, connectionArn)
	}

	delete(b.connectionsByNameStore(region), conn.ConnectionName)
	delete(conns, connectionArn)

	return nil
}

// CreateHost creates a new CodeStar host.
func (b *InMemoryBackend) CreateHost(
	ctx context.Context,
	name, providerType, providerEndpoint string,
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

	byName := b.hostsByNameStore(region)
	if _, exists := byName[name]; exists {
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
		Status:           HostStatusAvailable,
		Tags:             tagsCopy,
	}
	b.hostsStore(region)[hostArn] = host
	byName[name] = hostArn

	cp := *host
	cp.Tags = make(map[string]string, len(host.Tags))
	maps.Copy(cp.Tags, host.Tags)

	return &cp, nil
}

// GetHost returns a host by ARN.
func (b *InMemoryBackend) GetHost(ctx context.Context, hostArn string) (*Host, error) {
	region := regionFromARN(hostArn, getRegion(ctx, b.defaultRegion))

	b.mu.RLock("GetHost")
	defer b.mu.RUnlock()

	hs := b.hosts[region]
	if hs == nil {
		return nil, fmt.Errorf("%w: host not found: %s", ErrNotFound, hostArn)
	}

	host, ok := hs[hostArn]
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

	hs := b.hosts[region]
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

	hs := b.hosts[region]
	if hs == nil {
		return fmt.Errorf("%w: host not found: %s", ErrNotFound, hostArn)
	}

	host, ok := hs[hostArn]
	if !ok {
		return fmt.Errorf("%w: host not found: %s", ErrNotFound, hostArn)
	}

	if b.connectionHasReferenceToHostLocked(region, hostArn) {
		return fmt.Errorf("%w: host %q has active connections; delete them first", ErrResourceInUse, host.Name)
	}

	delete(b.hostsByNameStore(region), host.Name)
	delete(hs, hostArn)

	return nil
}

// UpdateHost updates the provider endpoint for a host.
func (b *InMemoryBackend) UpdateHost(ctx context.Context, hostArn, providerEndpoint string) error {
	if providerEndpoint != "" && len(providerEndpoint) > maxProviderEndpointLen {
		return fmt.Errorf("%w: ProviderEndpoint must not exceed %d characters", ErrValidation, maxProviderEndpointLen)
	}

	region := regionFromARN(hostArn, getRegion(ctx, b.defaultRegion))

	b.mu.Lock("UpdateHost")
	defer b.mu.Unlock()

	hs := b.hosts[region]
	if hs == nil {
		return fmt.Errorf("%w: host not found: %s", ErrNotFound, hostArn)
	}

	host, ok := hs[hostArn]
	if !ok {
		return fmt.Errorf("%w: host not found: %s", ErrNotFound, hostArn)
	}

	if providerEndpoint != "" {
		host.ProviderEndpoint = providerEndpoint
	}

	return nil
}

// ListTagsForResource returns the tags for a resource by ARN.
func (b *InMemoryBackend) ListTagsForResource(ctx context.Context, resourceArn string) (map[string]string, error) {
	region := regionFromARN(resourceArn, getRegion(ctx, b.defaultRegion))

	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	existing, ok := b.findResourceTagsLocked(region, resourceArn)
	if !ok {
		return nil, fmt.Errorf("%w: resource not found: %s", ErrNotFound, resourceArn)
	}

	result := make(map[string]string, len(existing))
	maps.Copy(result, existing)

	return result, nil
}

// TagResource adds or updates tags on a resource.
func (b *InMemoryBackend) TagResource(ctx context.Context, resourceArn string, tags map[string]string) error {
	if err := validateTags(tags); err != nil {
		return err
	}

	region := regionFromARN(resourceArn, getRegion(ctx, b.defaultRegion))

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	existing, ok := b.ensureTagsLocked(region, resourceArn)
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
func (b *InMemoryBackend) UntagResource(ctx context.Context, resourceArn string, tagKeys []string) error {
	region := regionFromARN(resourceArn, getRegion(ctx, b.defaultRegion))

	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	existing, ok := b.findResourceTagsLocked(region, resourceArn)
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
	region := regionFromARN(conn.ConnectionArn, b.defaultRegion)

	b.mu.Lock("AddConnectionInternal")
	defer b.mu.Unlock()

	b.connectionsStore(region)[conn.ConnectionArn] = conn
	b.connectionsByNameStore(region)[conn.ConnectionName] = conn.ConnectionArn
}

// AddHostInternal seeds a host directly for testing.
func (b *InMemoryBackend) AddHostInternal(host *Host) {
	region := regionFromARN(host.HostArn, b.defaultRegion)

	b.mu.Lock("AddHostInternal")
	defer b.mu.Unlock()

	b.hostsStore(region)[host.HostArn] = host
	b.hostsByNameStore(region)[host.Name] = host.HostArn
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
	ctx context.Context,
	connectionArn, ownerID, repoName, encryptionKeyArn string,
) (*RepositoryLink, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("CreateRepositoryLink")
	defer b.mu.Unlock()

	// Derive provider type from the connection if it exists in the same region.
	providerType := ""
	connRegion := regionFromARN(connectionArn, region)
	if conns := b.connections[connRegion]; conns != nil {
		if conn, ok := conns[connectionArn]; ok {
			providerType = conn.ProviderType
		}
	}

	// Check for duplicate: same connection + owner + repo.
	links := b.repositoryLinks[region]
	for _, existing := range links {
		if existing.ConnectionArn == connectionArn &&
			existing.OwnerID == ownerID &&
			existing.RepositoryName == repoName {
			return nil, fmt.Errorf("%w: repository link for %s/%s already exists", ErrAlreadyExists, ownerID, repoName)
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
	}

	b.repositoryLinksStore(region)[id] = link

	cp := *link

	return &cp, nil
}

// GetRepositoryLink retrieves a repository link by ID.
func (b *InMemoryBackend) GetRepositoryLink(ctx context.Context, repositoryLinkID string) (*RepositoryLink, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("GetRepositoryLink")
	defer b.mu.RUnlock()

	links := b.repositoryLinks[region]
	if links == nil {
		return nil, fmt.Errorf("%w: repository link not found: %s", ErrNotFound, repositoryLinkID)
	}

	link, ok := links[repositoryLinkID]
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

	links := b.repositoryLinks[region]
	if links == nil {
		return fmt.Errorf("%w: repository link not found: %s", ErrNotFound, repositoryLinkID)
	}

	if _, ok := links[repositoryLinkID]; !ok {
		return fmt.Errorf("%w: repository link not found: %s", ErrNotFound, repositoryLinkID)
	}

	if b.syncConfigHasReferenceToLinkLocked(region, repositoryLinkID) {
		return fmt.Errorf("%w: repository link %q has active sync configurations; delete them first",
			ErrResourceInUse, repositoryLinkID)
	}

	delete(links, repositoryLinkID)

	return nil
}

// ListRepositoryLinks returns all repository links sorted by ID.
func (b *InMemoryBackend) ListRepositoryLinks(ctx context.Context) []*RepositoryLink {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("ListRepositoryLinks")
	defer b.mu.RUnlock()

	links := b.repositoryLinks[region]
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

	b.repositoryLinksStore(region)[link.RepositoryLinkID] = link
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

	if links := b.repositoryLinks[region]; links != nil {
		if link, ok := links[repositoryLinkID]; ok {
			ownerID = link.OwnerID
			providerType = link.ProviderType
			repoName = link.RepositoryName
		}
	}

	// Check for duplicate.
	cfgs := b.syncConfigurationsStore(region)
	key := syncConfigKey(resourceName, syncType)

	if _, exists := cfgs[key]; exists {
		return nil, fmt.Errorf("%w: sync configuration for %q/%q already exists",
			ErrAlreadyExists, resourceName, syncType)
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
	}

	cfgs[key] = cfg

	// Seed an initial sync status for this resource.
	rsStore := b.resourceSyncStatusesStore(region)
	rsStore[key] = &ResourceSyncStatus{
		StartedAt: time.Now().UTC(),
		Status:    SyncStatusSucceeded,
		Events:    []SyncEvent{},
	}

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

	cfgs := b.syncConfigurations[region]
	if cfgs == nil {
		return nil, fmt.Errorf("%w: sync configuration not found: %s/%s", ErrNotFound, resourceName, syncType)
	}

	cfg, ok := cfgs[syncConfigKey(resourceName, syncType)]
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

	cfgs := b.syncConfigurations[region]
	if cfgs == nil {
		return fmt.Errorf("%w: sync configuration not found: %s/%s", ErrNotFound, resourceName, syncType)
	}

	key := syncConfigKey(resourceName, syncType)
	if _, ok := cfgs[key]; !ok {
		return fmt.Errorf("%w: sync configuration not found: %s/%s", ErrNotFound, resourceName, syncType)
	}

	delete(cfgs, key)

	// Remove associated sync statuses and blockers.
	if rsStore := b.resourceSyncStatuses[region]; rsStore != nil {
		delete(rsStore, key)
	}

	if bByRes := b.syncBlockersByResource[region]; bByRes != nil {
		for _, bid := range bByRes[key] {
			if bStore := b.syncBlockers[region]; bStore != nil {
				delete(bStore, bid)
			}
		}

		delete(bByRes, key)
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
	Events    []SyncEvent
}

// GetRepositorySyncStatus returns the latest sync status for a repository link and branch.
func (b *InMemoryBackend) GetRepositorySyncStatus(
	ctx context.Context,
	repositoryLinkID, branch, syncType string,
) (*RepositorySyncStatus, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("GetRepositorySyncStatus")
	defer b.mu.RUnlock()

	links := b.repositoryLinks[region]
	if links == nil {
		return nil, fmt.Errorf("%w: repository link not found: %s", ErrNotFound, repositoryLinkID)
	}

	if _, ok := links[repositoryLinkID]; !ok {
		return nil, fmt.Errorf("%w: repository link not found: %s", ErrNotFound, repositoryLinkID)
	}

	key := repositorySyncStatusKey(repositoryLinkID, branch, syncType)
	statusStore := b.repositorySyncStatuses[region]

	if statusStore != nil {
		if s, ok := statusStore[key]; ok {
			cp := *s
			cp.Events = append([]SyncEvent(nil), s.Events...)

			return &cp, nil
		}
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

	store := b.repositorySyncStatusesStore(region)
	key := repositorySyncStatusKey(repositoryLinkID, branch, syncType)
	store[key] = &RepositorySyncStatus{
		StartedAt: time.Now().UTC(),
		Status:    status,
		Events:    events,
	}
}

// ResourceSyncStatus holds the latest sync attempt for an AWS resource.
type ResourceSyncStatus struct {
	StartedAt time.Time
	Status    string
	Events    []SyncEvent
}

// GetResourceSyncStatus returns the latest sync status for a resource.
func (b *InMemoryBackend) GetResourceSyncStatus(
	ctx context.Context,
	resourceName, syncType string,
) (*ResourceSyncStatus, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("GetResourceSyncStatus")
	defer b.mu.RUnlock()

	cfgs := b.syncConfigurations[region]
	if cfgs == nil {
		return nil, fmt.Errorf("%w: sync configuration not found: %s/%s", ErrNotFound, resourceName, syncType)
	}

	key := syncConfigKey(resourceName, syncType)
	if _, ok := cfgs[key]; !ok {
		return nil, fmt.Errorf("%w: sync configuration not found: %s/%s", ErrNotFound, resourceName, syncType)
	}

	statusStore := b.resourceSyncStatuses[region]
	if statusStore != nil {
		if s, ok := statusStore[key]; ok {
			cp := *s
			cp.Events = append([]SyncEvent(nil), s.Events...)

			return &cp, nil
		}
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

	store := b.resourceSyncStatusesStore(region)
	key := syncConfigKey(resourceName, syncType)
	store[key] = &ResourceSyncStatus{
		StartedAt: time.Now().UTC(),
		Status:    status,
		Events:    events,
	}
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
}

// GetSyncBlockerSummary returns the sync blocker summary for a resource.
func (b *InMemoryBackend) GetSyncBlockerSummary(
	ctx context.Context,
	resourceName, syncType string,
) (*SyncBlockerSummary, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("GetSyncBlockerSummary")
	defer b.mu.RUnlock()

	cfgs := b.syncConfigurations[region]
	if cfgs == nil {
		return nil, fmt.Errorf("%w: sync configuration not found: %s/%s", ErrNotFound, resourceName, syncType)
	}

	key := syncConfigKey(resourceName, syncType)
	if _, ok := cfgs[key]; !ok {
		return nil, fmt.Errorf("%w: sync configuration not found: %s/%s", ErrNotFound, resourceName, syncType)
	}

	summary := &SyncBlockerSummary{
		ResourceName:   resourceName,
		LatestBlockers: []SyncBlocker{},
	}

	bByRes := b.syncBlockersByResource[region]
	if bByRes == nil {
		return summary, nil
	}

	blockerIDs := bByRes[key]
	bStore := b.syncBlockers[region]

	if bStore == nil {
		return summary, nil
	}

	blockers := make([]SyncBlocker, 0, len(blockerIDs))

	for _, bid := range blockerIDs {
		if blocker, ok := bStore[bid]; ok {
			blockers = append(blockers, *blocker)
		}
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

	cfgs := b.syncConfigurations[region]
	if cfgs == nil {
		return nil, fmt.Errorf("%w: sync configuration not found: %s/%s", ErrNotFound, resourceName, syncType)
	}

	key := syncConfigKey(resourceName, syncType)
	if _, ok := cfgs[key]; !ok {
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
	}

	bStore := b.syncBlockersStore(region)
	bStore[id] = blocker

	bByRes := b.syncBlockersByResourceStore(region)
	bByRes[key] = append(bByRes[key], id)

	cp := *blocker

	return &cp, nil
}

// UpdateSyncBlocker resolves a sync blocker by ID. If the blocker ID is not found,
// returns an empty summary (AWS accepts resolution of unknown blockers gracefully).
func (b *InMemoryBackend) UpdateSyncBlocker(
	ctx context.Context,
	id, resolvedReason string,
) (*SyncBlockerSummary, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("UpdateSyncBlocker")
	defer b.mu.Unlock()

	bStore := b.syncBlockers[region]
	if bStore == nil {
		return &SyncBlockerSummary{LatestBlockers: []SyncBlocker{}}, nil
	}

	blocker, ok := bStore[id]
	if !ok {
		return &SyncBlockerSummary{LatestBlockers: []SyncBlocker{}}, nil
	}

	now := time.Now().UTC()
	blocker.Status = SyncBlockerStatusResolved
	blocker.ResolvedReason = resolvedReason
	blocker.ResolvedAt = &now

	// Return summary for the resource that owns this blocker.
	key := syncConfigKey(blocker.ResourceName, blocker.SyncType)
	bByRes := b.syncBlockersByResource[region]

	summary := &SyncBlockerSummary{
		ResourceName:   blocker.ResourceName,
		LatestBlockers: []SyncBlocker{},
	}

	if bByRes != nil {
		for _, bid := range bByRes[key] {
			if b2, ok2 := bStore[bid]; ok2 {
				summary.LatestBlockers = append(summary.LatestBlockers, *b2)
			}
		}

		sort.Slice(summary.LatestBlockers, func(i, j int) bool {
			return summary.LatestBlockers[i].CreatedAt.After(summary.LatestBlockers[j].CreatedAt)
		})
	}

	return summary, nil
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

	links := b.repositoryLinks[region]
	if links == nil {
		return nil, fmt.Errorf("%w: repository link not found: %s", ErrNotFound, repositoryLinkID)
	}

	if _, ok := links[repositoryLinkID]; !ok {
		return nil, fmt.Errorf("%w: repository link not found: %s", ErrNotFound, repositoryLinkID)
	}

	_ = syncType

	return []RepositorySyncDefinition{}, nil
}

// ListSyncConfigurations returns all sync configurations for a given repository link and sync type.
func (b *InMemoryBackend) ListSyncConfigurations(
	ctx context.Context,
	repositoryLinkID, syncType string,
) []*SyncConfiguration {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("ListSyncConfigurations")
	defer b.mu.RUnlock()

	cfgs := b.syncConfigurations[region]
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

	links := b.repositoryLinks[region]
	if links == nil {
		return nil, fmt.Errorf("%w: repository link not found: %s", ErrNotFound, repositoryLinkID)
	}

	link, ok := links[repositoryLinkID]
	if !ok {
		return nil, fmt.Errorf("%w: repository link not found: %s", ErrNotFound, repositoryLinkID)
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

	cfgs := b.syncConfigurations[region]
	if cfgs == nil {
		return nil, fmt.Errorf("%w: sync configuration not found: %s/%s", ErrNotFound, resourceName, syncType)
	}

	key := syncConfigKey(resourceName, syncType)
	cfg, ok := cfgs[key]

	if !ok {
		return nil, fmt.Errorf("%w: sync configuration not found: %s/%s", ErrNotFound, resourceName, syncType)
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

	if publishDeploymentStatus != "" {
		cfg.PublishDeploymentStatus = publishDeploymentStatus
	}

	if triggerResourceUpdateOn != "" {
		cfg.TriggerResourceUpdateOn = triggerResourceUpdateOn
	}

	cp := *cfg

	return &cp, nil
}
