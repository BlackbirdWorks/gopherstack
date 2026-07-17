package elasticache

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
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

const (
	familyRedis7              = "redis7"
	engineMemcached           = "memcached"
	versionRedis710           = "7.1.0"
	nodeTypeT3Micro           = "cache.t3.micro"
	statusAvailable           = "available"
	statusServerlessAvailable = "available"
	statusDisabled            = "disabled"
	statusEnabled             = "enabled"
	statusActive              = "active"
)

const (
	engineValkey    = "valkey"
	familyValkey8   = "valkey8"
	familyValkey7   = "valkey7"
	versionValkey82 = "8.2.0"
)

// engineValkeyCap is the display-name capitalisation for Valkey.
const engineValkeyCap = "Valkey"

const (
	snapshotSourceManual        = "manual"
	snapshotSourceAutomated     = "automated"
	dataTypeString              = "string"
	dataTypeInteger             = "integer"
	allowedValuesYesNo          = "yes,no"
	allowedValuesMaxInt32       = "0-2147483647"
	allowedValuesEvictionPolicy = "noeviction,allkeys-lru,volatile-lru,allkeys-random," +
		"volatile-random,volatile-ttl,allkeys-lfu,volatile-lfu"
)

const (
	randomSuffixLen     = 3
	engineRedis         = "redis"
	tagCandidateInitCap = 16
)

// Engine mode constants.
const (
	EngineEmbedded = "embedded"
	EngineDocker   = "docker"
	EngineStub     = "stub"
)

// NewInMemoryBackend creates a new backend with the given engine mode.
func NewInMemoryBackend(engineMode, accountID, region string, allocator *portalloc.Allocator) *InMemoryBackend {
	if engineMode == "" {
		engineMode = EngineEmbedded
	}

	b := &InMemoryBackend{
		registry:                  store.NewRegistry(),
		clusters:                  make(map[string]*store.Table[Cluster]),
		replicationGroups:         make(map[string]*store.Table[ReplicationGroup]),
		parameterGroups:           make(map[string]*store.Table[CacheParameterGroup]),
		subnetGroups:              make(map[string]*store.Table[CacheSubnetGroup]),
		snapshots:                 make(map[string]*store.Table[CacheSnapshot]),
		cacheSecurityGroups:       make(map[string]*store.Table[CacheSecurityGroup]),
		cacheSecurityGroupIngress: make(map[string]map[string][]EC2SecurityGroupMembership),
		serverlessCaches:          make(map[string]*store.Table[ServerlessCache]),
		serverlessCacheSnapshots:  make(map[string]*store.Table[ServerlessCacheSnapshot]),
		users:                     make(map[string]*store.Table[User]),
		userGroups:                make(map[string]*store.Table[UserGroup]),
		reservedCacheNodes:        make(map[string]*store.Table[ReservedCacheNode]),
		updateActions:             nil,
		events:                    newEventRing(maxEvents),
		engineMode:                engineMode,
		accountID:                 accountID,
		region:                    region,
		allocator:                 allocator,
		mu:                        lockmetrics.New("elasticache"),
	}
	b.globalReplicationGroups = store.Register(
		b.registry, "globalReplicationGroups", store.New(globalReplicationGroupKeyFn),
	)

	b.initDefaultParameterGroups()

	return b
}

// Region returns the backend's default AWS region.
func (b *InMemoryBackend) Region() string { return b.region }

// buildARN is a helper to build an ElastiCache ARN with an explicit region.
func buildARN(resource, region, accountID string) string {
	return arn.Build("elasticache", region, accountID, resource)
}

// SetDNSRegistrar wires a DNS server so cache cluster hostnames are
// automatically registered on create and deregistered on delete.
func (b *InMemoryBackend) SetDNSRegistrar(r DNSRegistrar) {
	b.mu.Lock("SetDNSRegistrar")
	b.dnsRegistrar = r
	b.mu.Unlock()
}

// Reset closes all miniredis instances, clears all state, and re-initialises default parameter groups.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.resetLocked()
}

// resetLocked does the work of Reset without acquiring b.mu, so it can also be
// called from Restore's incompatible-snapshot-version guard, which already
// holds the lock (calling the public, self-locking Reset there would
// deadlock). Must hold b.mu.
func (b *InMemoryBackend) resetLocked() {
	for _, regionClusters := range b.clusters {
		for _, c := range regionClusters.All() {
			b.releaseClusterLocked(c)
		}
	}

	b.clusters = make(map[string]*store.Table[Cluster])
	b.replicationGroups = make(map[string]*store.Table[ReplicationGroup])
	b.parameterGroups = make(map[string]*store.Table[CacheParameterGroup])
	b.subnetGroups = make(map[string]*store.Table[CacheSubnetGroup])
	b.snapshots = make(map[string]*store.Table[CacheSnapshot])
	b.cacheSecurityGroups = make(map[string]*store.Table[CacheSecurityGroup])
	b.cacheSecurityGroupIngress = make(map[string]map[string][]EC2SecurityGroupMembership)
	b.serverlessCaches = make(map[string]*store.Table[ServerlessCache])
	b.serverlessCacheSnapshots = make(map[string]*store.Table[ServerlessCacheSnapshot])
	b.users = make(map[string]*store.Table[User])
	b.userGroups = make(map[string]*store.Table[UserGroup])
	b.reservedCacheNodes = make(map[string]*store.Table[ReservedCacheNode])
	b.updateActions = nil
	b.events.reset()
	// b.globalReplicationGroups is registered on b.registry at construction
	// and must keep its identity (store.Register panics on a duplicate name),
	// so it is cleared in place via the registry rather than reassigned.
	b.registry.ResetAll()
	b.initDefaultParameterGroups()
}
