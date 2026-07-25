package memorydb

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	snapshotSourceManual = "manual"
	// networkTypeIPv4 is the default NetworkType/IPDiscovery/SupportedNetworkTypes
	// value for resources this mock only ever creates as IPv4.
	networkTypeIPv4 = "ipv4"
)

const (
	// Engine type constants.
	engineRedis  = "redis"
	engineValkey = "valkey"
	// defaultValkeyEngineVersion is the default version for new valkey clusters.
	defaultValkeyEngineVersion = "7.2"
	// authTypeIAM is the IAM authentication type.
	authTypeIAM = "iam"
	// authTypePassword is the password authentication type.
	authTypePassword = "password"
	// authTypeNoPasswordRequired is an accepted request-side alias for
	// authTypeNoPassword. It is never stored or returned on the wire: real
	// AWS's Authentication.Type output enum only knows "password", "iam", and
	// "no-password" (see aws-sdk-go-v2/service/memorydb/types.AuthenticationType).
	authTypeNoPasswordRequired = "no-password-required"
	// authTypeNoPassword is the canonical stored/wire value for a user that
	// does not require a password to authenticate.
	authTypeNoPassword = "no-password"
	// snapshotSourceAutomated is the source type for automated snapshots.
	snapshotSourceAutomated = "automated"
)

const (
	// openAccessACL is the default ACL name that allows all connections.
	openAccessACL = "open-access"
	// defaultEngineVersion is the default Redis version for new clusters.
	defaultEngineVersion = "7.0"
	// defaultNodeType is the default node type for new clusters.
	defaultNodeType = "db.r6g.large"
	// defaultReservedNodeType is the node type used in reserved node offerings.
	defaultReservedNodeType = "db.r6g.xlarge"
	// defaultPort is the default MemoryDB port.
	defaultPort = int32(6379)
	// clusterStatusAvailable is the status for a running cluster.
	clusterStatusAvailable = "available"
	// aclStatusActive is the status for an active ACL.
	aclStatusActive = "active"
	// userStatusActive is the status for an active user.
	userStatusActive = "active"
	// snapshotStatusAvailable is the status for a completed snapshot.
	snapshotStatusAvailable = "available"
	// multiRegionClusterStatusAvailable is the status for a running multi-region cluster.
	multiRegionClusterStatusAvailable = "available"
	// snsTopicStatusActive is the active value for SnsTopicStatus on a cluster.
	snsTopicStatusActive = "active"
	// snsTopicStatusInactive is the inactive value for SnsTopicStatus on a cluster.
	snsTopicStatusInactive = "inactive"
	// maxEvents is the maximum number of events retained in memory.
	maxEvents = 1000

	// Reserved node offering durations (in seconds).
	reservedDuration1Year  = int32(31_536_000) // 365 days
	reservedDuration3Years = int32(94_608_000) // 3 × 365 days

	// Reserved node offering prices (USD).
	reservedFixedPriceLarge1Y  = 1200.00
	reservedFixedPriceLarge3Y  = 2000.00
	reservedFixedPriceXLarge1Y = 2400.00
	reservedChargeRateLarge    = 0.14
	reservedChargeRateXLarge   = 0.28

	// splitParts is the number of parts expected when splitting window formats.
	splitParts = 2

	// Engine family constants.
	familyRedis6  = "memorydb_redis6"
	familyRedis7  = "memorydb_redis7"
	familyValkey7 = "memorydb_valkey7"
	familyValkey8 = "memorydb_valkey8"

	// Supported engine version constants.
	engineVersion62 = "6.2"
	engineVersion70 = "7.0"
	engineVersion71 = "7.1"
	engineVersion72 = "7.2"
	engineVersion80 = "8.0"

	// paramValueYes is the string paramValueYes used in default parameter values.
	paramValueYes = "yes"

	// Resource kind constants for tag routing.
	resourceKindCluster            = "cluster"
	resourceKindACL                = "acl"
	resourceKindSubnetGroup        = "subnetgroup"
	resourceKindUser               = "user"
	resourceKindParameterGroup     = "parametergroup"
	resourceKindSnapshot           = "snapshot"
	resourceKindMultiRegionCluster = "multiregioncluster"

	// maxResourceNameLen is the maximum allowed length for resource names.
	maxResourceNameLen = 40
)

// validateResourceName validates that name is 1-40 characters, contains only
// lowercase letters, numbers, and hyphens, and starts with a letter.
func validateResourceName(name string, resourceType string) error {
	if len(name) == 0 {
		return fmt.Errorf("%s name cannot be empty: %w", resourceType, ErrValidation)
	}

	if len(name) > maxResourceNameLen {
		return fmt.Errorf("%s name %q exceeds %d characters: %w", resourceType, name, maxResourceNameLen, ErrValidation)
	}

	if name[0] < 'a' || name[0] > 'z' {
		return fmt.Errorf("%s name %q must start with a lowercase letter: %w", resourceType, name, ErrValidation)
	}

	for _, ch := range name {
		if (ch < 'a' || ch > 'z') && (ch < '0' || ch > '9') && ch != '-' {
			return fmt.Errorf(
				"%s name %q contains invalid character %q (only lowercase alphanumeric and hyphens allowed): %w",
				resourceType, name, ch, ErrValidation,
			)
		}
	}

	return nil
}

type regionContextKey struct{}

func getRegion(ctx context.Context, defaultRegion string) string {
	if r, ok := ctx.Value(regionContextKey{}).(string); ok && r != "" {
		return r
	}

	return defaultRegion
}

// InMemoryBackend is the in-memory implementation of StorageBackend.
//
// multiRegionClusters, multiRegionParameterGroups, and serviceUpdates are
// partition-scoped (like AWS) and therefore NOT region-nested; they are
// registered on registry so Reset/Snapshot/Restore collapse to one Registry
// call each. Every other resource is nested per-region
// (map[string]*store.Table[T], outer key is region) via the lazy "*Store"
// accessors below. Per-region tables are deliberately NOT registered on
// registry because the set of regions is only known at runtime -- see
// store_setup.go and persistence.go for how they are (de)serialized instead.
//
// arnToResource and events are left as plain maps: arnToResource's value
// (resourceRef) has no key of its own to feed a Table's keyFn (the ARN is the
// map key, not a value field), and events is slice-valued
// (map[string][]*Event), neither of which store.Table can represent.
type InMemoryBackend struct {
	registry                   *store.Registry
	multiRegionClusters        *store.Table[MultiRegionCluster]
	multiRegionParameterGroups *store.Table[MultiRegionParameterGroup]
	serviceUpdates             *store.Table[ServiceUpdate]
	clusters                   map[string]*store.Table[Cluster]
	acls                       map[string]*store.Table[ACL]
	subnetGroups               map[string]*store.Table[SubnetGroup]
	users                      map[string]*store.Table[User]
	parameterGroups            map[string]*store.Table[ParameterGroup]
	snapshots                  map[string]*store.Table[Snapshot]
	reservedNodes              map[string]*store.Table[ReservedNode]
	arnToResource              map[string]map[string]resourceRef
	events                     map[string][]*Event
	clock                      func() time.Time
	accountID                  string
	defaultRegion              string
	lifecycleDelay             time.Duration
	mu                         sync.RWMutex
}

type resourceRef struct {
	Kind string `json:"kind"`
	Name string `json:"name"`
}

func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return newInMemoryBackendWithDefaults(region, accountID)
}

func newInMemoryBackendWithDefaults(region, accountID string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:        store.NewRegistry(),
		clusters:        make(map[string]*store.Table[Cluster]),
		acls:            make(map[string]*store.Table[ACL]),
		subnetGroups:    make(map[string]*store.Table[SubnetGroup]),
		users:           make(map[string]*store.Table[User]),
		parameterGroups: make(map[string]*store.Table[ParameterGroup]),
		snapshots:       make(map[string]*store.Table[Snapshot]),
		reservedNodes:   make(map[string]*store.Table[ReservedNode]),
		events:          make(map[string][]*Event),
		arnToResource:   make(map[string]map[string]resourceRef),
		accountID:       accountID,
		defaultRegion:   region,
	}
	b.multiRegionClusters = store.Register(b.registry, "multiRegionClusters", store.New(multiRegionClusterKeyFn))
	b.multiRegionParameterGroups = store.Register(
		b.registry, "multiRegionParameterGroups", store.New(multiRegionParameterGroupKeyFn),
	)
	b.serviceUpdates = store.Register(b.registry, "serviceUpdates", store.New(serviceUpdateKeyFn))

	openAccessARN := arn.Build("memorydb", region, accountID, "acl/"+openAccessACL)
	b.aclsStore(region).Put(&ACL{
		Name:      openAccessACL,
		ARN:       openAccessARN,
		Status:    aclStatusActive,
		UserNames: []string{},
		CreatedAt: time.Now(),
		Tags:      make(map[string]string),
	})
	b.arnToResourceStore(region)[openAccessARN] = resourceRef{Kind: resourceKindACL, Name: openAccessACL}

	for _, su := range defaultServiceUpdates() {
		b.serviceUpdates.Put(su)
	}

	b.seedDefaultParameterGroupsLocked()

	return b
}

func (b *InMemoryBackend) clustersStore(region string) *store.Table[Cluster] {
	if b.clusters[region] == nil {
		b.clusters[region] = store.New(clusterKeyFn)
	}

	return b.clusters[region]
}

func (b *InMemoryBackend) aclsStore(region string) *store.Table[ACL] {
	if b.acls[region] == nil {
		b.acls[region] = store.New(aclKeyFn)
		// Seed the open-access ACL into every region so CreateCluster works across regions.
		openAccessARN := arn.Build("memorydb", region, b.accountID, "acl/"+openAccessACL)
		b.acls[region].Put(&ACL{
			Name:      openAccessACL,
			ARN:       openAccessARN,
			Status:    aclStatusActive,
			UserNames: []string{},
			CreatedAt: time.Now(),
			Tags:      make(map[string]string),
		})
	}

	return b.acls[region]
}

func (b *InMemoryBackend) subnetGroupsStore(region string) *store.Table[SubnetGroup] {
	if b.subnetGroups[region] == nil {
		b.subnetGroups[region] = store.New(subnetGroupKeyFn)
	}

	return b.subnetGroups[region]
}

func (b *InMemoryBackend) usersStore(region string) *store.Table[User] {
	if b.users[region] == nil {
		b.users[region] = store.New(userKeyFn)
	}

	return b.users[region]
}

func (b *InMemoryBackend) parameterGroupsStore(region string) *store.Table[ParameterGroup] {
	if b.parameterGroups[region] == nil {
		b.parameterGroups[region] = store.New(parameterGroupKeyFn)
	}

	return b.parameterGroups[region]
}

func (b *InMemoryBackend) snapshotsStore(region string) *store.Table[Snapshot] {
	if b.snapshots[region] == nil {
		b.snapshots[region] = store.New(snapshotKeyFn)
	}

	return b.snapshots[region]
}

func (b *InMemoryBackend) reservedNodesStore(region string) *store.Table[ReservedNode] {
	if b.reservedNodes[region] == nil {
		b.reservedNodes[region] = store.New(reservedNodeKeyFn)
	}

	return b.reservedNodes[region]
}

func (b *InMemoryBackend) arnToResourceStore(region string) map[string]resourceRef {
	if b.arnToResource[region] == nil {
		b.arnToResource[region] = make(map[string]resourceRef)
	}

	return b.arnToResource[region]
}

func (b *InMemoryBackend) Region() string { return b.defaultRegion }

// Reset clears all state and re-seeds defaults, returning the backend to a clean state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.resetLocked()
}

// resetLocked does the work of Reset without acquiring b.mu, so it can also be
// called from Restore's incompatible-snapshot-version guard, which already
// holds the lock (calling the public, self-locking Reset there would
// deadlock). Must hold b.mu.
func (b *InMemoryBackend) resetLocked() {
	b.clusters = make(map[string]*store.Table[Cluster])
	b.acls = make(map[string]*store.Table[ACL])
	b.subnetGroups = make(map[string]*store.Table[SubnetGroup])
	b.users = make(map[string]*store.Table[User])
	b.parameterGroups = make(map[string]*store.Table[ParameterGroup])
	b.snapshots = make(map[string]*store.Table[Snapshot])
	b.reservedNodes = make(map[string]*store.Table[ReservedNode])
	b.events = make(map[string][]*Event)
	b.arnToResource = make(map[string]map[string]resourceRef)
	// b.multiRegionClusters, b.multiRegionParameterGroups, and b.serviceUpdates
	// are registered on b.registry at construction and must keep their
	// identity (store.Register panics on a duplicate name), so they are
	// cleared in place via the registry rather than reassigned.
	b.registry.ResetAll()

	openAccessARN := arn.Build("memorydb", b.defaultRegion, b.accountID, "acl/"+openAccessACL)
	b.aclsStore(b.defaultRegion).Put(&ACL{
		Name:      openAccessACL,
		ARN:       openAccessARN,
		Status:    aclStatusActive,
		UserNames: []string{},
		CreatedAt: time.Now(),
		Tags:      make(map[string]string),
	})
	b.arnToResourceStore(b.defaultRegion)[openAccessARN] = resourceRef{Kind: resourceKindACL, Name: openAccessACL}

	for _, su := range defaultServiceUpdates() {
		b.serviceUpdates.Put(su)
	}

	b.seedDefaultParameterGroupsLocked()
}
