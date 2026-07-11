package elasticache

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	gopherDNS "github.com/blackbirdworks/gopherstack/pkgs/dns"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
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
)

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

var (
	ErrClusterNotFound                    = errors.New("CacheClusterNotFound")
	ErrClusterAlreadyExists               = errors.New("CacheClusterAlreadyExists")
	ErrReplicationGroupNotFound           = errors.New("ReplicationGroupNotFound")
	ErrReplicationGroupAlreadyExists      = errors.New("ReplicationGroupAlreadyExists")
	ErrResourceNotFound                   = errors.New("resource not found")
	ErrParameterGroupNotFound             = errors.New("CacheParameterGroupNotFound")
	ErrParameterGroupAlreadyExists        = errors.New("CacheParameterGroupAlreadyExists")
	ErrParameterGroupDefaultNotModifiable = errors.New("default parameter group cannot be deleted or modified")
	ErrInvalidParameterGroupFamily        = errors.New("InvalidParameterGroupFamily")
	ErrSubnetGroupNotFound                = errors.New("CacheSubnetGroupNotFound")
	ErrSubnetGroupAlreadyExists           = errors.New("CacheSubnetGroupAlreadyExists")
	ErrSnapshotNotFound                   = errors.New("SnapshotNotFound")
	ErrSnapshotAlreadyExists              = errors.New("SnapshotAlreadyExistsFault")
	ErrInvalidSnapshotSource              = errors.New(
		"exactly one of CacheClusterId or ReplicationGroupId must be specified",
	)
)

const maxEvents = 1000

// CacheEvent represents a recorded ElastiCache operation event.
type CacheEvent struct {
	Date             time.Time `json:"date"`
	SourceIdentifier string    `json:"sourceIdentifier"`
	SourceType       string    `json:"sourceType"`
	Message          string    `json:"message"`
}

// eventRing is a fixed-capacity circular ring buffer for CacheEvents.
// It stores up to size events, overwriting the oldest when full.
// Reads allocate and return events in insertion order.
type eventRing struct {
	buf  []CacheEvent
	head int // index of oldest entry
	n    int // number of valid entries
	size int
}

func newEventRing(size int) *eventRing {
	return &eventRing{buf: make([]CacheEvent, size), size: size}
}

// push appends e, overwriting the oldest entry when the ring is full.
func (r *eventRing) push(e CacheEvent) {
	if r.n < r.size {
		r.buf[(r.head+r.n)%r.size] = e
		r.n++
	} else {
		// Full: overwrite oldest.
		r.buf[r.head] = e
		r.head = (r.head + 1) % r.size
	}
}

// all returns a snapshot of all events in insertion order.
func (r *eventRing) all() []CacheEvent {
	out := make([]CacheEvent, r.n)
	for i := range r.n {
		out[i] = r.buf[(r.head+i)%r.size]
	}

	return out
}

// reset clears the ring without reallocating the backing buffer.
func (r *eventRing) reset() {
	r.head = 0
	r.n = 0
}

// marshalJSON exports events in insertion order for persistence.
func (r *eventRing) marshalJSON() []CacheEvent {
	return r.all()
}

// restoreFromSlice loads previously-persisted events back into the ring.
func (r *eventRing) restoreFromSlice(events []CacheEvent) {
	r.reset()
	for _, e := range events {
		r.push(e)
	}
}

// Cluster represents an ElastiCache cluster.
type Cluster struct {
	CreatedAt                  time.Time
	Tags                       *tags.Tags
	mini                       *miniredis.Miniredis
	ClusterID                  string
	Engine                     string
	EngineVersion              string
	Status                     string
	Endpoint                   string
	NodeType                   string
	ARN                        string
	Region                     string
	CacheParameterGroupName    string
	PreferredMaintenanceWindow string
	SnapshotWindow             string
	ReplicationGroupID         string
	KmsKeyID                   string
	TransitEncryptionMode      string
	ConnectAddress             string
	PendingStatus              string
	AvailableAt                time.Time
	Members                    []CacheNodeMember
	Port                       int
	AllocatedPort              int
	NumCacheNodes              int
	TransitEncryptionEnabled   bool
	AtRestEncryptionEnabled    bool
}

// ReplicationGroup represents an ElastiCache replication group.
type ReplicationGroup struct {
	CreatedAt                  time.Time                `json:"createdAt"`
	AuthTokenLastModifiedDate  *time.Time               `json:"authTokenLastModifiedDate,omitempty"`
	AvailableAt                time.Time                `json:"availableAt,omitzero"`
	PendingModifiedValues      *RGPendingModifiedValues `json:"pendingModifiedValues,omitempty"`
	Tags                       *tags.Tags               `json:"tags,omitempty"`
	ReplicationGroupID         string                   `json:"replicationGroupID"`
	Description                string                   `json:"description"`
	Status                     string                   `json:"status"`
	PendingStatus              string                   `json:"pendingStatus,omitempty"`
	ARN                        string                   `json:"arn"`
	Engine                     string                   `json:"engine,omitempty"`
	CacheParameterGroupName    string                   `json:"cacheParameterGroupName,omitempty"`
	AutomaticFailover          string                   `json:"automaticFailover,omitempty"`
	EngineVersion              string                   `json:"engineVersion,omitempty"`
	CacheNodeType              string                   `json:"cacheNodeType,omitempty"`
	PreferredMaintenanceWindow string                   `json:"preferredMaintenanceWindow,omitempty"`
	SnapshotWindow             string                   `json:"snapshotWindow,omitempty"`
	AuthToken                  string                   `json:"authToken,omitempty"`
	KmsKeyID                   string                   `json:"kmsKeyId,omitempty"`
	NotificationTopicArn       string                   `json:"notificationTopicArn,omitempty"`
	TransitEncryptionMode      string                   `json:"transitEncryptionMode,omitempty"`
	NodeGroups                 []NodeGroup              `json:"nodeGroups,omitempty"`
	LogDeliveryConfigurations  []LogDeliveryConfig      `json:"logDeliveryConfigurations,omitempty"`
	UserGroupIDs               []string                 `json:"userGroupIds,omitempty"`
	SnapshotRetentionLimit     int                      `json:"snapshotRetentionLimit,omitempty"`
	ReplicaCount               int32                    `json:"replicaCount,omitempty"`
	ClusterModeEnabled         bool                     `json:"clusterModeEnabled,omitempty"`
	AuthTokenEnabled           bool                     `json:"authTokenEnabled,omitempty"`
	AtRestEncryptionEnabled    bool                     `json:"atRestEncryptionEnabled,omitempty"`
	TransitEncryptionEnabled   bool                     `json:"transitEncryptionEnabled,omitempty"`
	DataTieringEnabled         bool                     `json:"dataTieringEnabled,omitempty"`
	MultiAZEnabled             bool                     `json:"multiAZEnabled,omitempty"`
}

// CacheParameterGroup represents an ElastiCache parameter group.
type CacheParameterGroup struct {
	Tags        *tags.Tags        `json:"tags,omitempty"`
	Parameters  map[string]string `json:"parameters"`
	Name        string            `json:"name"`
	Family      string            `json:"family"`
	Description string            `json:"description"`
	ARN         string            `json:"arn"`
	IsGlobal    bool              `json:"isGlobal"`
}

// CacheSubnetGroup represents an ElastiCache subnet group.
type CacheSubnetGroup struct {
	Tags        *tags.Tags `json:"tags,omitempty"`
	Name        string     `json:"name"`
	Description string     `json:"description"`
	VpcID       string     `json:"vpcId"`
	ARN         string     `json:"arn"`
	SubnetIDs   []string   `json:"subnetIds"`
}

// CacheSnapshot represents an ElastiCache snapshot.
type CacheSnapshot struct {
	CreatedAt          time.Time  `json:"createdAt"`
	AvailableAt        time.Time  `json:"availableAt,omitzero"`
	Tags               *tags.Tags `json:"tags,omitempty"`
	SnapshotName       string     `json:"snapshotName"`
	CacheClusterID     string     `json:"cacheClusterId"`
	ReplicationGroupID string     `json:"replicationGroupId"`
	Status             string     `json:"status"`
	PendingStatus      string     `json:"pendingStatus,omitempty"`
	ARN                string     `json:"arn"`
	Engine             string     `json:"engine"`
	EngineVersion      string     `json:"engineVersion"`
	NodeType           string     `json:"nodeType"`
	KmsKeyID           string     `json:"kmsKeyId,omitempty"`
	SnapshotSource     string     `json:"snapshotSource"` // "manual" or "automated"
}

// StorageBackend defines the interface for the ElastiCache in-memory store.
type StorageBackend interface {
	CreateCluster(ctx context.Context, id, engine, nodeType string, port int) (*Cluster, error)
	CreateClusterWithOptions(
		ctx context.Context,
		id, engine, nodeType, paramGroupName, maintenanceWindow, snapshotWindow string,
		numCacheNodes, port int,
	) (*Cluster, error)
	DeleteCluster(ctx context.Context, id string) error
	DescribeClusters(ctx context.Context, id, marker string, maxRecords int, notInRG bool) (page.Page[Cluster], error)
	ModifyCluster(
		ctx context.Context,
		id, nodeType, paramGroupName, engineVersion, maintenanceWindow, snapshotWindow string,
		numCacheNodes int,
	) (*Cluster, error)
	ListTagsForResource(ctx context.Context, arn string) (map[string]string, error)
	AddTagsToResource(ctx context.Context, arn string, newTags map[string]string) error
	RemoveTagsFromResource(ctx context.Context, arn string, tagKeys []string) error
	CreateReplicationGroup(ctx context.Context, id, description string) (*ReplicationGroup, error)
	CreateReplicationGroupWithOptions(
		ctx context.Context,
		id, description, paramGroupName, maintenanceWindow, snapshotWindow string,
	) (*ReplicationGroup, error)
	DeleteReplicationGroup(ctx context.Context, id string) error
	DescribeReplicationGroups(
		ctx context.Context,
		id, marker string,
		maxRecords int,
	) (page.Page[ReplicationGroup], error)
	ModifyReplicationGroup(
		ctx context.Context,
		id, description, paramGroupName, engineVersion, cacheNodeType, maintenanceWindow, snapshotWindow string,
		automaticFailoverEnabled, multiAZEnabled *bool,
	) (*ReplicationGroup, error)
	FailoverReplicationGroup(ctx context.Context, id, nodeGroupID string) (*ReplicationGroup, error)
	CreateParameterGroup(ctx context.Context, name, family, description string) (*CacheParameterGroup, error)
	DeleteParameterGroup(ctx context.Context, name string) error
	DescribeParameterGroups(
		ctx context.Context,
		name, marker string,
		maxRecords int,
	) (page.Page[CacheParameterGroup], error)
	ModifyParameterGroup(ctx context.Context, name string, params map[string]string) (*CacheParameterGroup, error)
	ResetParameterGroup(
		ctx context.Context,
		name string,
		paramNames []string,
		resetAll bool,
	) (*CacheParameterGroup, error)
	DescribeParameters(ctx context.Context, name, marker string, maxRecords int) (page.Page[CacheParameter], error)
	CreateSubnetGroup(ctx context.Context, name, description string, subnetIDs []string) (*CacheSubnetGroup, error)
	CreateSubnetGroupFull(
		ctx context.Context,
		name, description, vpcID string,
		subnetIDs []string,
	) (*CacheSubnetGroup, error)
	DeleteSubnetGroup(ctx context.Context, name string) error
	DescribeSubnetGroups(ctx context.Context, name, marker string, maxRecords int) (page.Page[CacheSubnetGroup], error)
	ModifySubnetGroup(ctx context.Context, name, description string, subnetIDs []string) (*CacheSubnetGroup, error)
	CreateSnapshot(ctx context.Context, snapshotName, clusterID, replicationGroupID string) (*CacheSnapshot, error)
	DeleteSnapshot(ctx context.Context, snapshotName string) (*CacheSnapshot, error)
	DescribeSnapshots(
		ctx context.Context,
		snapshotName, clusterID, replicationGroupID, snapshotSource, marker string,
		maxRecords int,
	) (page.Page[CacheSnapshot], error)
	CopySnapshot(ctx context.Context, sourceSnapshotName, targetSnapshotName string) (*CacheSnapshot, error)
	CopySnapshotFull(
		ctx context.Context,
		sourceSnapshotName, targetSnapshotName, kmsKeyID string,
	) (*CacheSnapshot, error)
	DescribeEvents(
		ctx context.Context,
		sourceIdentifier, sourceType, marker string,
		startTime, endTime time.Time,
		duration, maxRecords int,
	) (page.Page[CacheEvent], error)
	// New ops
	CreateCacheSecurityGroup(ctx context.Context, name, description string) (*CacheSecurityGroup, error)
	AuthorizeCacheSecurityGroupIngress(
		ctx context.Context,
		name, ec2SecurityGroupName, ec2SecurityGroupOwnerID string,
	) (*CacheSecurityGroup, error)
	CreateGlobalReplicationGroup(
		ctx context.Context,
		globalReplicationGroupIDSuffix, description, primaryReplicationGroupID string,
	) (*GlobalReplicationGroup, error)
	CreateServerlessCache(ctx context.Context, name, description, engine string) (*ServerlessCache, error)
	CreateServerlessCacheSnapshot(
		ctx context.Context,
		snapshotName, serverlessCacheName string,
	) (*ServerlessCacheSnapshot, error)
	CopyServerlessCacheSnapshot(
		ctx context.Context,
		sourceSnapshotName, targetSnapshotName string,
	) (*ServerlessCacheSnapshot, error)
	CreateUser(
		ctx context.Context,
		userID, userName, accessString, engine string,
		noPasswordRequired bool,
	) (*User, error)
	BatchApplyUpdateAction(
		ctx context.Context,
		replicationGroupIDs, cacheClusterIDs []string,
		serviceUpdateName string,
	) (*BatchUpdateResult, error)
	BatchStopUpdateAction(
		ctx context.Context,
		replicationGroupIDs, cacheClusterIDs []string,
		serviceUpdateName string,
	) (*BatchUpdateResult, error)
	CompleteMigration(ctx context.Context, replicationGroupID string, force bool) (*ReplicationGroup, error)
	// User operations
	DeleteUser(ctx context.Context, userID string) (*User, error)
	DescribeUsers(ctx context.Context, userID, marker string, maxRecords int) (page.Page[User], error)
	ModifyUser(ctx context.Context, userID, accessString string, noPasswordRequired bool) (*User, error)
	// UserGroup operations
	CreateUserGroup(ctx context.Context, groupID, description, engine string, userIDs []string) (*UserGroup, error)
	CreateUserGroupValidated(
		ctx context.Context,
		groupID, description, engine string,
		userIDs []string,
	) (*UserGroup, error)
	DeleteUserGroup(ctx context.Context, groupID string) (*UserGroup, error)
	DescribeUserGroups(ctx context.Context, groupID, marker string, maxRecords int) (page.Page[UserGroup], error)
	ModifyUserGroup(ctx context.Context, groupID string, userIDsToAdd, userIDsToRemove []string) (*UserGroup, error)
	// GlobalReplicationGroup operations
	DeleteGlobalReplicationGroup(
		ctx context.Context,
		id string,
		retainPrimaryReplicationGroup bool,
	) (*GlobalReplicationGroup, error)
	DescribeGlobalReplicationGroups(
		ctx context.Context,
		id, marker string,
		maxRecords int,
	) (page.Page[GlobalReplicationGroup], error)
	DisassociateGlobalReplicationGroup(
		ctx context.Context,
		id, replicationGroupID, replicationGroupRegion string,
	) (*GlobalReplicationGroup, error)
	FailoverGlobalReplicationGroup(
		ctx context.Context,
		id, primaryRegion, primaryReplicationGroupID string,
	) (*GlobalReplicationGroup, error)
	IncreaseNodeGroupsInGlobalReplicationGroup(
		ctx context.Context,
		id string,
		nodeGroupCount int32,
	) (*GlobalReplicationGroup, error)
	DecreaseNodeGroupsInGlobalReplicationGroup(
		ctx context.Context,
		id string,
		nodeGroupCount int32,
	) (*GlobalReplicationGroup, error)
	ModifyGlobalReplicationGroup(
		ctx context.Context,
		id, description, engineVersion string,
		automaticFailoverEnabled bool,
	) (*GlobalReplicationGroup, error)
	RebalanceSlotsInGlobalReplicationGroup(ctx context.Context, id string) (*GlobalReplicationGroup, error)
	// ReservedCacheNodes operations
	DescribeReservedCacheNodes(
		ctx context.Context,
		id, cacheNodeType, offeringType, marker string,
		maxRecords int,
	) (page.Page[ReservedCacheNode], error)
	DescribeReservedCacheNodesOfferings(
		ctx context.Context,
		offeringID, cacheNodeType, offeringType, marker string,
		maxRecords int,
	) (page.Page[ReservedCacheNodesOffering], error)
	PurchaseReservedCacheNodesOffering(
		ctx context.Context,
		offeringID, reservedCacheNodeID string,
		cacheNodeCount int32,
	) (*ReservedCacheNode, error)
	// ServerlessCache operations
	DeleteServerlessCache(ctx context.Context, name string) (*ServerlessCache, error)
	DeleteServerlessCacheSnapshot(ctx context.Context, name string) (*ServerlessCacheSnapshot, error)
	DescribeServerlessCaches(
		ctx context.Context,
		name, marker string,
		maxRecords int,
	) (page.Page[ServerlessCache], error)
	DescribeServerlessCacheSnapshots(
		ctx context.Context,
		serverlessCacheName, snapshotName, marker string,
		maxRecords int,
	) (page.Page[ServerlessCacheSnapshot], error)
	ExportServerlessCacheSnapshot(
		ctx context.Context,
		snapshotName, s3BucketName string,
	) (*ServerlessCacheSnapshot, error)
	ModifyServerlessCache(ctx context.Context, name, description string) (*ServerlessCache, error)
	CreateServerlessCacheFull(ctx context.Context, opts ServerlessCreateOpts) (*ServerlessCache, error)
	ModifyServerlessCacheFull(ctx context.Context, name string, opts ServerlessModifyOpts) (*ServerlessCache, error)
	// Migration operations
	StartMigration(ctx context.Context, replicationGroupID string) (*ReplicationGroup, error)
	TestMigration(ctx context.Context, replicationGroupID string) (*ReplicationGroup, error)
	IncreaseReplicaCount(
		ctx context.Context,
		replicationGroupID string,
		newReplicaCount int32,
	) (*ReplicationGroup, error)
	DecreaseReplicaCount(
		ctx context.Context,
		replicationGroupID string,
		newReplicaCount int32,
	) (*ReplicationGroup, error)
	ModifyReplicationGroupShardConfiguration(
		ctx context.Context,
		replicationGroupID string,
		nodeGroupCount int32,
	) (*ReplicationGroup, error)
	// Cache info operations
	DescribeCacheEngineVersions(
		ctx context.Context,
		engine, family, engineVersion, marker string,
		maxRecords int,
	) (page.Page[CacheEngineVersion], error)
	RebootCacheCluster(ctx context.Context, clusterID string, nodeIDs []string) (*Cluster, error)
	DeleteCacheSecurityGroup(ctx context.Context, name string) error
	DescribeCacheSecurityGroups(
		ctx context.Context,
		name, marker string,
		maxRecords int,
	) (page.Page[CacheSecurityGroup], error)
	RevokeCacheSecurityGroupIngress(
		ctx context.Context,
		name, ec2SecurityGroupName, ec2SecurityGroupOwnerID string,
	) (*CacheSecurityGroup, error)
	DescribeEngineDefaultParameters(
		ctx context.Context,
		cacheParameterGroupFamily, marker string,
		maxRecords int,
	) (page.Page[CacheParameter], error)
	DescribeServiceUpdates(
		ctx context.Context,
		serviceUpdateName, marker string,
		maxRecords int,
		status []string,
	) (page.Page[ServiceUpdate], error)
	DescribeUpdateActions(
		ctx context.Context,
		serviceUpdateName, marker string,
		maxRecords int,
	) (page.Page[UpdateAction], error)
	ListAllowedNodeTypeModifications(ctx context.Context, clusterID, replicationGroupID string) ([]string, error)
	// Audit1: extended create/modify with new fields
	CreateReplicationGroupFull(ctx context.Context, opts ReplicationGroupCreateOpts) (*ReplicationGroup, error)
	ModifyReplicationGroupFull(
		ctx context.Context,
		id string,
		opts ReplicationGroupModifyOpts,
	) (*ReplicationGroup, error)
	// Audit1: auto snapshot scheduling
	TriggerAutoSnapshot(ctx context.Context, replicationGroupID string) (*CacheSnapshot, error)
	// Batch-2: update action tracking
	AppendUpdateActions(actions []*UpdateAction)
	ListUpdateActionsByServiceUpdate(serviceUpdateName string) []*UpdateAction
	// Region returns the backend's default AWS region.
	Region() string
}

// CacheParameter represents a single cache parameter (for DescribeParameters response).
type CacheParameter struct {
	Name          string
	Value         string
	Description   string
	DataType      string
	AllowedValues string
	IsModifiable  bool
}

// DNSRegistrar can register and deregister hostnames with an embedded DNS server.
type DNSRegistrar interface {
	Register(hostname string)
	Deregister(hostname string)
}

// dnsRecordRegistrar is an optional capability: a registrar that can bind a
// hostname to a specific record value (e.g. an A record to the real bound IP of
// the embedded redis). The concrete embedded DNS server implements this, so the
// published synthetic endpoint resolves to a genuinely connectable address.
type dnsRecordRegistrar interface {
	RegisterRecord(hostname, recordType string, values []string)
}

// builtinParameterGroupFamilies returns the well-known default parameter group families.
func builtinParameterGroupFamilies() []struct{ family, name string } {
	return []struct{ family, name string }{
		{familyRedis7, "default.redis7"},
		{"redis6.x", "default.redis6.x"},
		{"redis5.0", "default.redis5.0"},
		{"redis4.0", "default.redis4.0"},
		{"redis3.2", "default.redis3.2"},
		{"redis2.8", "default.redis2.8"},
		{"memcached1.6", "default.memcached1.6"},
		{"memcached1.5", "default.memcached1.5"},
		{familyValkey8, "default.valkey8"},
		{familyValkey7, "default.valkey7"},
	}
}

// InMemoryBackend is an in-memory ElastiCache backend.
// All regional resource maps are nested by region (outer key = region) so that
// same-named resources in different regions are fully isolated. GlobalReplicationGroups
// are global/partition-scoped (like AWS) and therefore are NOT region-nested.
type InMemoryBackend struct {
	dnsRegistrar              DNSRegistrar
	registry                  *store.Registry
	globalReplicationGroups   *store.Table[GlobalReplicationGroup]
	serverlessCaches          map[string]*store.Table[ServerlessCache]
	userGroups                map[string]*store.Table[UserGroup]
	parameterGroups           map[string]*store.Table[CacheParameterGroup]
	snapshots                 map[string]*store.Table[CacheSnapshot]
	cacheSecurityGroups       map[string]*store.Table[CacheSecurityGroup]
	cacheSecurityGroupIngress map[string]map[string][]EC2SecurityGroupMembership
	clusters                  map[string]*store.Table[Cluster]
	replicationGroups         map[string]*store.Table[ReplicationGroup]
	reservedCacheNodes        map[string]*store.Table[ReservedCacheNode]
	serverlessCacheSnapshots  map[string]*store.Table[ServerlessCacheSnapshot]
	subnetGroups              map[string]*store.Table[CacheSubnetGroup]
	users                     map[string]*store.Table[User]
	events                    *eventRing
	mu                        *lockmetrics.RWMutex
	allocator                 *portalloc.Allocator
	clock                     func() time.Time
	region                    string
	engineMode                string
	accountID                 string
	updateActions             []*UpdateAction
	lifecycleDelay            time.Duration
}

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

// initDefaultParameterGroups seeds the well-known default parameter groups for the default region.
func (b *InMemoryBackend) initDefaultParameterGroups() {
	b.initDefaultParameterGroupsForRegion(b.region)
}

// initDefaultParameterGroupsForRegion seeds default parameter groups for the given region.
// Callers must NOT hold b.mu (it allocates directly into the map).
func (b *InMemoryBackend) initDefaultParameterGroupsForRegion(region string) {
	t := b.parameterGroups[region]
	if t == nil {
		t = store.New(cacheParameterGroupKeyFn)
		b.parameterGroups[region] = t
	}

	for _, dpg := range builtinParameterGroupFamilies() {
		pg := &CacheParameterGroup{
			Name:        dpg.name,
			Family:      dpg.family,
			Description: "Default parameter group for " + dpg.family,
			ARN:         buildARN("parametergroup:"+dpg.name, region, b.accountID),
			IsGlobal:    true,
			Parameters:  make(map[string]string),
			Tags:        tags.New("elasticache.pg." + dpg.name + ".tags"),
		}
		t.Put(pg)
	}
}

// The following lazy per-region store helpers return the resource map for the
// given region, creating it on first use. Callers must hold b.mu.

func (b *InMemoryBackend) clustersStore(region string) *store.Table[Cluster] {
	if b.clusters[region] == nil {
		b.clusters[region] = store.New(clusterKeyFn)
	}

	return b.clusters[region]
}

func (b *InMemoryBackend) replicationGroupsStore(region string) *store.Table[ReplicationGroup] {
	if b.replicationGroups[region] == nil {
		b.replicationGroups[region] = store.New(replicationGroupKeyFn)
	}

	return b.replicationGroups[region]
}

func (b *InMemoryBackend) parameterGroupsStore(region string) *store.Table[CacheParameterGroup] {
	if b.parameterGroups[region] == nil {
		b.initDefaultParameterGroupsForRegion(region)
	}

	return b.parameterGroups[region]
}

func (b *InMemoryBackend) subnetGroupsStore(region string) *store.Table[CacheSubnetGroup] {
	if b.subnetGroups[region] == nil {
		b.subnetGroups[region] = store.New(cacheSubnetGroupKeyFn)
	}

	return b.subnetGroups[region]
}

func (b *InMemoryBackend) snapshotsStore(region string) *store.Table[CacheSnapshot] {
	if b.snapshots[region] == nil {
		b.snapshots[region] = store.New(cacheSnapshotKeyFn)
	}

	return b.snapshots[region]
}

func (b *InMemoryBackend) cacheSecurityGroupsStore(region string) *store.Table[CacheSecurityGroup] {
	if b.cacheSecurityGroups[region] == nil {
		b.cacheSecurityGroups[region] = store.New(cacheSecurityGroupKeyFn)
	}

	return b.cacheSecurityGroups[region]
}

func (b *InMemoryBackend) cacheSecurityGroupIngressStore(region string) map[string][]EC2SecurityGroupMembership {
	if b.cacheSecurityGroupIngress[region] == nil {
		b.cacheSecurityGroupIngress[region] = make(map[string][]EC2SecurityGroupMembership)
	}

	return b.cacheSecurityGroupIngress[region]
}

func (b *InMemoryBackend) serverlessCachesStore(region string) *store.Table[ServerlessCache] {
	if b.serverlessCaches[region] == nil {
		b.serverlessCaches[region] = store.New(serverlessCacheKeyFn)
	}

	return b.serverlessCaches[region]
}

func (b *InMemoryBackend) serverlessCacheSnapshotsStore(region string) *store.Table[ServerlessCacheSnapshot] {
	if b.serverlessCacheSnapshots[region] == nil {
		b.serverlessCacheSnapshots[region] = store.New(serverlessCacheSnapshotKeyFn)
	}

	return b.serverlessCacheSnapshots[region]
}

func (b *InMemoryBackend) usersStore(region string) *store.Table[User] {
	if b.users[region] == nil {
		b.users[region] = store.New(userKeyFn)
	}

	return b.users[region]
}

func (b *InMemoryBackend) userGroupsStore(region string) *store.Table[UserGroup] {
	if b.userGroups[region] == nil {
		b.userGroups[region] = store.New(userGroupKeyFn)
	}

	return b.userGroups[region]
}

func (b *InMemoryBackend) reservedCacheNodesStore(region string) *store.Table[ReservedCacheNode] {
	if b.reservedCacheNodes[region] == nil {
		b.reservedCacheNodes[region] = store.New(reservedCacheNodeKeyFn)
	}

	return b.reservedCacheNodes[region]
}

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

func (b *InMemoryBackend) clusterARN(region, id string) string {
	return arn.Build("elasticache", region, b.accountID, "cluster:"+id)
}

func (b *InMemoryBackend) replicationGroupARN(region, id string) string {
	return arn.Build("elasticache", region, b.accountID, "replicationgroup:"+id)
}

func (b *InMemoryBackend) parameterGroupARN(name string) string {
	return arn.Build("elasticache", b.region, b.accountID, "parametergroup:"+name)
}

func (b *InMemoryBackend) subnetGroupARN(region, name string) string {
	return arn.Build("elasticache", region, b.accountID, "subnetgroup:"+name)
}

func (b *InMemoryBackend) snapshotARN(region, name string) string {
	return arn.Build("elasticache", region, b.accountID, "snapshot:"+name)
}

// appendEventLocked records a new event. Must be called with b.mu write-locked.
func (b *InMemoryBackend) appendEventLocked(sourceIdentifier, sourceType, message string) {
	b.events.push(CacheEvent{
		Date:             time.Now(),
		SourceIdentifier: sourceIdentifier,
		SourceType:       sourceType,
		Message:          message,
	})
}

func validateParamGroupFamily(engine, family string) error {
	switch engine {
	case engineMemcached:
		if !strings.HasPrefix(family, engineMemcached) {
			return fmt.Errorf(
				"parameter group family %q does not match engine memcached: %w",
				family,
				ErrInvalidParameterGroupFamily,
			)
		}
	case engineValkey:
		if !strings.HasPrefix(family, engineValkey) {
			return fmt.Errorf(
				"parameter group family %q does not match engine valkey: %w",
				family,
				ErrInvalidParameterGroupFamily,
			)
		}
	default:
		if !strings.HasPrefix(family, "redis") {
			return fmt.Errorf(
				"parameter group family %q does not match engine redis: %w",
				family,
				ErrInvalidParameterGroupFamily,
			)
		}
	}

	return nil
}

// defaultEngineVersion returns the realistic default version for the given engine.
func defaultEngineVersion(engine string) string {
	switch engine {
	case engineMemcached:
		return "1.6.17"
	case engineValkey:
		return versionValkey82
	default:
		return versionRedis710
	}
}

// clusterEngine holds the started data-plane engine (embedded redis or a
// pass-through port) for a cluster, produced outside the backend lock.
type clusterEngine struct {
	mini          *miniredis.Miniredis
	connectAddr   string
	port          int
	allocatedPort int
}

// startClusterEngine starts the data-plane engine for a cluster WITHOUT holding
// b.mu. Listener startup (miniredis.StartAddr) is the slow part and must not
// serialise every other backend op behind it. b.engineMode/b.allocator are set
// once at construction and never mutated, so they are safe to read lock-free.
func (b *InMemoryBackend) startClusterEngine(port int) (*clusterEngine, error) {
	if b.engineMode != EngineEmbedded {
		p := port
		if p <= 0 {
			p = 6379
		}

		return &clusterEngine{port: p}, nil
	}

	mr := miniredis.NewMiniRedis()

	eng := &clusterEngine{mini: mr}
	if b.allocator != nil {
		allocatedPort, err := b.allocator.Acquire("elasticache")
		if err != nil {
			return nil, fmt.Errorf("allocate miniredis port: %w", err)
		}
		if startErr := mr.StartAddr(fmt.Sprintf("0.0.0.0:%d", allocatedPort)); startErr != nil {
			_ = b.allocator.Release(allocatedPort)

			return nil, fmt.Errorf("start miniredis on port %d: %w", allocatedPort, startErr)
		}
		eng.allocatedPort = allocatedPort
	} else if err := mr.Start(); err != nil {
		return nil, fmt.Errorf("start miniredis: %w", err)
	}

	addr := mr.Server().Addr()
	eng.port = addr.Port
	host := addr.IP.String()
	if addr.IP.IsUnspecified() {
		// 0.0.0.0 is a bind wildcard, not a connectable target; publish loopback.
		host = "127.0.0.1"
	}
	eng.connectAddr = net.JoinHostPort(host, strconv.Itoa(addr.Port))

	return eng, nil
}

// releaseEngine discards an engine that was started but never inserted (lost a
// create race). Safe to call with a nil or pass-through engine.
func (b *InMemoryBackend) releaseEngine(eng *clusterEngine) {
	if eng == nil {
		return
	}
	if eng.mini != nil {
		eng.mini.Close()
	}
	if b.allocator != nil && eng.allocatedPort > 0 {
		_ = b.allocator.Release(eng.allocatedPort)
	}
}

// insertClusterLocked builds and stores a cluster from an already-started
// engine. Must hold b.mu.
func (b *InMemoryBackend) insertClusterLocked(
	region, id, engine, nodeType, paramGroupName, maintenanceWindow, snapshotWindow string,
	numCacheNodes int,
	eng *clusterEngine,
) *Cluster {
	if engine == "" {
		engine = engineRedis
	}
	if nodeType == "" {
		nodeType = nodeTypeT3Micro
	}
	if numCacheNodes <= 0 {
		numCacheNodes = 1
	}

	c := &Cluster{
		ClusterID:                  id,
		Engine:                     engine,
		EngineVersion:              defaultEngineVersion(engine),
		Status:                     statusAvailable,
		NodeType:                   nodeType,
		NumCacheNodes:              numCacheNodes,
		ARN:                        b.clusterARN(region, id),
		Region:                     region,
		Tags:                       tags.New("elasticache.cluster." + id + ".tags"),
		CreatedAt:                  time.Now(),
		CacheParameterGroupName:    paramGroupName,
		PreferredMaintenanceWindow: maintenanceWindow,
		SnapshotWindow:             snapshotWindow,
		mini:                       eng.mini,
		Port:                       eng.port,
		AllocatedPort:              eng.allocatedPort,
		ConnectAddress:             eng.connectAddr,
	}
	b.markCreatingLocked(&c.PendingStatus, &c.AvailableAt)

	c.Endpoint = gopherDNS.SyntheticHostname(id, randomSuffix(), region, "cache")
	b.registerClusterDNSLocked(c)

	b.clustersStore(region).Put(c)
	b.appendEventLocked(id, "cache-cluster", "cluster created")

	return c
}

// registerClusterDNSLocked registers the cluster's synthetic endpoint hostname
// and, when the registrar supports it, binds an A record to the real bound IP so
// the published endpoint is genuinely reachable. Must hold b.mu.
func (b *InMemoryBackend) registerClusterDNSLocked(c *Cluster) {
	if b.dnsRegistrar == nil || c.Endpoint == "" {
		return
	}
	b.dnsRegistrar.Register(c.Endpoint)

	if c.ConnectAddress == "" {
		return
	}
	if host, _, err := net.SplitHostPort(c.ConnectAddress); err == nil && host != "" {
		if rr, ok := b.dnsRegistrar.(dnsRecordRegistrar); ok {
			rr.RegisterRecord(c.Endpoint, "A", []string{host})
		}
	}
}

// CreateCluster creates a new cache cluster.
func (b *InMemoryBackend) CreateCluster(ctx context.Context, id, engine, nodeType string, port int) (*Cluster, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateCluster.reserve")
	b.pruneRegionLocked(region)
	_, exists := b.clustersStore(region).Get(id)
	b.mu.Unlock()
	if exists {
		return nil, ErrClusterAlreadyExists
	}

	eng, err := b.startClusterEngine(port)
	if err != nil {
		return nil, err
	}

	b.mu.Lock("CreateCluster.insert")
	defer b.mu.Unlock()
	if _, dup := b.clustersStore(region).Get(id); dup {
		b.releaseEngine(eng)

		return nil, ErrClusterAlreadyExists
	}

	c := b.insertClusterLocked(region, id, engine, nodeType, "", "", "", 1, eng)

	return b.clusterView(c), nil
}

// CreateClusterWithOptions creates a new cache cluster with optional parameter group and scheduling windows.
func (b *InMemoryBackend) CreateClusterWithOptions(
	ctx context.Context,
	id, engine, nodeType, paramGroupName, maintenanceWindow, snapshotWindow string,
	numCacheNodes, port int,
) (*Cluster, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateClusterWithOptions.reserve")
	b.pruneRegionLocked(region)
	if _, exists := b.clustersStore(region).Get(id); exists {
		b.mu.Unlock()

		return nil, ErrClusterAlreadyExists
	}
	if paramGroupName != "" {
		pg, ok := b.parameterGroupsStore(region).Get(paramGroupName)
		if !ok {
			b.mu.Unlock()

			return nil, ErrParameterGroupNotFound
		}
		if err := validateParamGroupFamily(engine, pg.Family); err != nil {
			b.mu.Unlock()

			return nil, err
		}
	}
	b.mu.Unlock()

	eng, err := b.startClusterEngine(port)
	if err != nil {
		return nil, err
	}

	b.mu.Lock("CreateClusterWithOptions.insert")
	defer b.mu.Unlock()
	if _, exists := b.clustersStore(region).Get(id); exists {
		b.releaseEngine(eng)

		return nil, ErrClusterAlreadyExists
	}

	c := b.insertClusterLocked(
		region, id, engine, nodeType, paramGroupName, maintenanceWindow, snapshotWindow, numCacheNodes, eng,
	)

	return b.clusterView(c), nil
}

// DeleteCluster stops and removes a cluster.
func (b *InMemoryBackend) DeleteCluster(ctx context.Context, id string) error {
	b.mu.Lock("DeleteCluster")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	b.pruneRegionLocked(region)
	tbl := b.clustersStore(region)
	c, exists := tbl.Get(id)
	if !exists || isReaped(b.now(), c.PendingStatus, c.AvailableAt) {
		return ErrClusterNotFound
	}

	// With a lifecycle delay, dwell in "deleting" so waiters can observe it; the
	// engine stays live and the entry is reaped by the next write op once the
	// deadline passes. Without a delay (default), delete synchronously.
	if d := b.pendingUntil(); !d.IsZero() {
		c.PendingStatus = statusDeleting
		c.AvailableAt = d
		b.appendEventLocked(id, "cache-cluster", "cluster deleting")

		return nil
	}

	b.releaseClusterLocked(c)
	tbl.Delete(id)
	b.appendEventLocked(id, "cache-cluster", "cluster deleted")

	return nil
}

const elasticacheDefaultMaxRecords = 100

// DescribeClusters returns one cluster by id, or a paginated list of all clusters when id is empty.
// When notInRG is true, only clusters with no ReplicationGroupID are returned (standalone clusters).
func (b *InMemoryBackend) DescribeClusters(
	ctx context.Context,
	id, marker string,
	maxRecords int,
	notInRG bool,
) (page.Page[Cluster], error) {
	b.mu.RLock("DescribeClusters")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	var filter func(Cluster) bool
	if notInRG {
		filter = func(c Cluster) bool { return c.ReplicationGroupID == "" }
	}

	p, err := describePaged(b.clustersStore(region), id, ErrClusterNotFound, filter,
		func(c Cluster) string { return c.ClusterID }, marker, maxRecords)

	return b.finalizeClusterPage(id, p, err)
}

// tagEntry holds the tags pointer and the metric name used to initialise tags when nil.
type tagEntry struct {
	ptr      **tags.Tags
	initName string
}

// tagCandidate bundles an ARN with the tagEntry to return when it matches.
type tagCandidate struct {
	arn   string
	entry tagEntry
}

// collectTagCandidatesLocked builds a flat list of all taggable resources for ARN lookup.
// It iterates over all regions so that ARN-addressed operations find the correct resource.
func (b *InMemoryBackend) collectTagCandidatesLocked() []tagCandidate {
	candidates := make([]tagCandidate, 0, tagCandidateInitCap)
	candidates = b.appendClusterTagCandidates(candidates)
	candidates = b.appendNetworkTagCandidates(candidates)
	candidates = b.appendServerlessTagCandidates(candidates)
	candidates = b.appendUserTagCandidates(candidates)

	return candidates
}

func (b *InMemoryBackend) appendClusterTagCandidates(candidates []tagCandidate) []tagCandidate {
	for _, regionClusters := range b.clusters {
		for _, c := range regionClusters.All() {
			candidates = append(candidates,
				tagCandidate{c.ARN, tagEntry{&c.Tags, "elasticache.cluster." + c.ClusterID + ".tags"}})
		}
	}
	for _, regionRGs := range b.replicationGroups {
		for _, rg := range regionRGs.All() {
			candidates = append(candidates,
				tagCandidate{rg.ARN, tagEntry{&rg.Tags, "elasticache.rg." + rg.ReplicationGroupID + ".tags"}})
		}
	}
	for _, regionPGs := range b.parameterGroups {
		for _, pg := range regionPGs.All() {
			candidates = append(candidates,
				tagCandidate{pg.ARN, tagEntry{&pg.Tags, "elasticache.pg." + pg.Name + ".tags"}})
		}
	}
	for _, regionSnaps := range b.snapshots {
		for _, snap := range regionSnaps.All() {
			candidates = append(candidates,
				tagCandidate{snap.ARN, tagEntry{&snap.Tags, "elasticache.snapshot." + snap.SnapshotName + ".tags"}})
		}
	}
	for _, regionCSGs := range b.cacheSecurityGroups {
		for _, sg := range regionCSGs.All() {
			candidates = append(candidates,
				tagCandidate{sg.ARN, tagEntry{&sg.Tags, "elasticache.sg." + sg.Name + ".tags"}})
		}
	}
	for _, grg := range b.globalReplicationGroups.All() {
		candidates = append(candidates,
			tagCandidate{grg.ARN, tagEntry{&grg.Tags, "elasticache.grg." + grg.GlobalReplicationGroupID + ".tags"}})
	}

	return candidates
}

func (b *InMemoryBackend) appendNetworkTagCandidates(candidates []tagCandidate) []tagCandidate {
	for _, regionSGs := range b.subnetGroups {
		for _, sg := range regionSGs.All() {
			candidates = append(candidates,
				tagCandidate{sg.ARN, tagEntry{&sg.Tags, "elasticache.sg." + sg.Name + ".tags"}})
		}
	}

	return candidates
}

func (b *InMemoryBackend) appendServerlessTagCandidates(candidates []tagCandidate) []tagCandidate {
	for _, regionSCs := range b.serverlessCaches {
		for _, sc := range regionSCs.All() {
			candidates = append(candidates,
				tagCandidate{sc.ARN, tagEntry{&sc.Tags, "elasticache.serverless." + sc.Name + ".tags"}})
		}
	}
	for _, regionScSnaps := range b.serverlessCacheSnapshots {
		for _, snap := range regionScSnaps.All() {
			candidates = append(candidates,
				tagCandidate{snap.ARN, tagEntry{&snap.Tags, "elasticache.serverlesssnap." + snap.Name + ".tags"}})
		}
	}

	return candidates
}

func (b *InMemoryBackend) appendUserTagCandidates(candidates []tagCandidate) []tagCandidate {
	for _, regionUsers := range b.users {
		for _, u := range regionUsers.All() {
			candidates = append(candidates,
				tagCandidate{u.ARN, tagEntry{&u.Tags, "elasticache.user." + u.UserID + ".tags"}})
		}
	}
	for _, regionUGs := range b.userGroups {
		for _, ug := range regionUGs.All() {
			candidates = append(candidates,
				tagCandidate{ug.ARN, tagEntry{&ug.Tags, "elasticache.usergroup." + ug.UserGroupID + ".tags"}})
		}
	}

	return candidates
}

// findTagsByARNLocked returns the tagEntry for the resource with the given ARN, or nil if not found.
func (b *InMemoryBackend) findTagsByARNLocked(arn string) *tagEntry {
	for _, c := range b.collectTagCandidatesLocked() {
		if c.arn == arn {
			entry := c.entry

			return &entry
		}
	}

	return nil
}

// ListTagsForResource returns tags for the given ARN.
func (b *InMemoryBackend) ListTagsForResource(_ context.Context, arn string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	entry := b.findTagsByARNLocked(arn)
	if entry == nil {
		return nil, fmt.Errorf("resource with ARN %s: %w", arn, ErrResourceNotFound)
	}

	if *entry.ptr == nil {
		return map[string]string{}, nil
	}

	return (*entry.ptr).Clone(), nil
}

// AddTagsToResource adds or updates tags on the resource identified by resourceARN.
func (b *InMemoryBackend) AddTagsToResource(_ context.Context, resourceARN string, newTags map[string]string) error {
	b.mu.Lock("AddTagsToResource")
	defer b.mu.Unlock()

	entry := b.findTagsByARNLocked(resourceARN)
	if entry == nil {
		return fmt.Errorf("resource with ARN %s: %w", resourceARN, ErrResourceNotFound)
	}

	if *entry.ptr == nil {
		*entry.ptr = tags.FromMap(entry.initName, newTags)
	} else {
		(*entry.ptr).Merge(newTags)
	}

	return nil
}

// RemoveTagsFromResource removes the specified tag keys from the resource identified by resourceARN.
func (b *InMemoryBackend) RemoveTagsFromResource(_ context.Context, resourceARN string, tagKeys []string) error {
	b.mu.Lock("RemoveTagsFromResource")
	defer b.mu.Unlock()

	entry := b.findTagsByARNLocked(resourceARN)
	if entry == nil {
		return fmt.Errorf("resource with ARN %s: %w", resourceARN, ErrResourceNotFound)
	}

	if *entry.ptr != nil {
		(*entry.ptr).DeleteKeys(tagKeys)
	}

	return nil
}

// createReplicationGroupLocked creates a replication group assuming b.mu is already held.
func (b *InMemoryBackend) createReplicationGroupLocked(
	region, id, description, paramGroupName, maintenanceWindow, snapshotWindow string,
) *ReplicationGroup {
	rg := &ReplicationGroup{
		ReplicationGroupID:         id,
		Description:                description,
		Status:                     statusAvailable,
		ARN:                        b.replicationGroupARN(region, id),
		Tags:                       tags.New("elasticache.rg." + id + ".tags"),
		CreatedAt:                  time.Now(),
		CacheParameterGroupName:    paramGroupName,
		PreferredMaintenanceWindow: maintenanceWindow,
		SnapshotWindow:             snapshotWindow,
	}
	b.markCreatingLocked(&rg.PendingStatus, &rg.AvailableAt)
	b.replicationGroupsStore(region).Put(rg)
	b.appendEventLocked(id, "replication-group", "replication group created")

	return rg
}

// CreateReplicationGroup creates a replication group.
func (b *InMemoryBackend) CreateReplicationGroup(
	ctx context.Context,
	id, description string,
) (*ReplicationGroup, error) {
	b.mu.Lock("CreateReplicationGroup")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	b.pruneRegionLocked(region)
	if _, exists := b.replicationGroupsStore(region).Get(id); exists {
		return nil, ErrReplicationGroupAlreadyExists
	}

	return b.replicationGroupView(b.createReplicationGroupLocked(region, id, description, "", "", "")), nil
}

// CreateReplicationGroupWithOptions creates a replication group with optional parameter group and scheduling windows.
func (b *InMemoryBackend) CreateReplicationGroupWithOptions(
	ctx context.Context,
	id, description, paramGroupName, maintenanceWindow, snapshotWindow string,
) (*ReplicationGroup, error) {
	b.mu.Lock("CreateReplicationGroupWithOptions")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	if _, exists := b.replicationGroupsStore(region).Get(id); exists {
		return nil, ErrReplicationGroupAlreadyExists
	}

	if paramGroupName != "" {
		if _, ok := b.parameterGroupsStore(region).Get(paramGroupName); !ok {
			return nil, ErrParameterGroupNotFound
		}
	}

	return b.replicationGroupView(b.createReplicationGroupLocked(
		region,
		id,
		description,
		paramGroupName,
		maintenanceWindow,
		snapshotWindow,
	)), nil
}

// DeleteReplicationGroup removes a replication group.
func (b *InMemoryBackend) DeleteReplicationGroup(ctx context.Context, id string) error {
	b.mu.Lock("DeleteReplicationGroup")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	b.pruneRegionLocked(region)
	tbl := b.replicationGroupsStore(region)
	rg, exists := tbl.Get(id)
	if !exists || isReaped(b.now(), rg.PendingStatus, rg.AvailableAt) {
		return ErrReplicationGroupNotFound
	}

	if d := b.pendingUntil(); !d.IsZero() {
		rg.PendingStatus = statusDeleting
		rg.AvailableAt = d
		b.appendEventLocked(id, "replication-group", "replication group deleting")

		return nil
	}

	rg.Tags.Close()
	tbl.Delete(id)
	b.appendEventLocked(id, "replication-group", "replication group deleted")

	return nil
}

// DescribeReplicationGroups returns one replication group by id, or a paginated list of all when id is empty.
func (b *InMemoryBackend) DescribeReplicationGroups(
	ctx context.Context,
	id, marker string,
	maxRecords int,
) (page.Page[ReplicationGroup], error) {
	b.mu.RLock("DescribeReplicationGroups")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	p, err := describePaged(b.replicationGroupsStore(region), id, ErrReplicationGroupNotFound, nil,
		func(rg ReplicationGroup) string { return rg.ReplicationGroupID }, marker, maxRecords)

	return b.finalizeReplicationGroupPage(id, p, err)
}

// randomSuffix generates a short random hex string for synthetic hostnames.
func randomSuffix() string {
	b := make([]byte, randomSuffixLen)
	_, _ = rand.Read(b)

	return hex.EncodeToString(b)
}

// ListAll returns all clusters across all regions (used by dashboard).
func (b *InMemoryBackend) ListAll() []Cluster {
	b.mu.RLock("ListAll")
	defer b.mu.RUnlock()
	now := b.now()
	var out []Cluster
	for _, regionClusters := range b.clusters {
		for _, c := range regionClusters.All() {
			if isReaped(now, c.PendingStatus, c.AvailableAt) {
				continue
			}
			cp := *c
			cp.Status = overlayStatus(now, c.Status, c.PendingStatus, c.AvailableAt)
			out = append(out, cp)
		}
	}

	return out
}

// ModifyCluster modifies an existing cache cluster.
func (b *InMemoryBackend) ModifyCluster(
	ctx context.Context,
	id, nodeType, paramGroupName, engineVersion, maintenanceWindow, snapshotWindow string,
	numCacheNodes int,
) (*Cluster, error) {
	b.mu.Lock("ModifyCluster")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	c, exists := b.clustersStore(region).Get(id)
	if !exists {
		return nil, ErrClusterNotFound
	}

	if nodeType != "" {
		c.NodeType = nodeType
	}

	if paramGroupName != "" {
		if _, ok := b.parameterGroupsStore(region).Get(paramGroupName); !ok {
			return nil, ErrParameterGroupNotFound
		}
		c.CacheParameterGroupName = paramGroupName
	}

	if engineVersion != "" {
		c.EngineVersion = engineVersion
	}

	if numCacheNodes > 0 {
		c.NumCacheNodes = numCacheNodes
	}

	if maintenanceWindow != "" {
		c.PreferredMaintenanceWindow = maintenanceWindow
	}

	if snapshotWindow != "" {
		c.SnapshotWindow = snapshotWindow
	}

	b.markTransitionLocked(&c.PendingStatus, &c.AvailableAt, statusModifying)
	b.appendEventLocked(id, "cache-cluster", "cluster modified")

	return b.clusterView(c), nil
}

// ModifyReplicationGroup modifies an existing replication group.
func (b *InMemoryBackend) ModifyReplicationGroup(
	ctx context.Context,
	id, description, paramGroupName, engineVersion, cacheNodeType, maintenanceWindow, snapshotWindow string,
	automaticFailoverEnabled, multiAZEnabled *bool,
) (*ReplicationGroup, error) {
	b.mu.Lock("ModifyReplicationGroup")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	rg, exists := b.replicationGroupsStore(region).Get(id)
	if !exists {
		return nil, ErrReplicationGroupNotFound
	}

	if description != "" {
		rg.Description = description
	}

	if paramGroupName != "" {
		if _, ok := b.parameterGroupsStore(region).Get(paramGroupName); !ok {
			return nil, ErrParameterGroupNotFound
		}
		rg.CacheParameterGroupName = paramGroupName
	}

	if engineVersion != "" {
		rg.EngineVersion = engineVersion
	}

	if cacheNodeType != "" {
		rg.CacheNodeType = cacheNodeType
	}

	if automaticFailoverEnabled != nil {
		if *automaticFailoverEnabled {
			rg.AutomaticFailover = statusEnabled
		} else {
			rg.AutomaticFailover = statusDisabled
		}
	}

	if multiAZEnabled != nil {
		rg.MultiAZEnabled = *multiAZEnabled
	}

	if maintenanceWindow != "" {
		rg.PreferredMaintenanceWindow = maintenanceWindow
	}

	if snapshotWindow != "" {
		rg.SnapshotWindow = snapshotWindow
	}

	b.markTransitionLocked(&rg.PendingStatus, &rg.AvailableAt, statusModifying)
	b.appendEventLocked(id, "replication-group", "replication group modified")

	return b.replicationGroupView(rg), nil
}

// FailoverReplicationGroup simulates a failover for the given replication group.
func (b *InMemoryBackend) FailoverReplicationGroup(ctx context.Context, id, _ string) (*ReplicationGroup, error) {
	b.mu.Lock("FailoverReplicationGroup")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	rg, exists := b.replicationGroupsStore(region).Get(id)
	if !exists {
		return nil, ErrReplicationGroupNotFound
	}

	rg.Status = statusAvailable
	b.markTransitionLocked(&rg.PendingStatus, &rg.AvailableAt, statusFailingOver)
	b.appendEventLocked(id, "replication-group", "failover completed")

	return b.replicationGroupView(rg), nil
}

// CreateParameterGroup creates a new cache parameter group.
func (b *InMemoryBackend) CreateParameterGroup(
	ctx context.Context,
	name, family, description string,
) (*CacheParameterGroup, error) {
	b.mu.Lock("CreateParameterGroup")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	tbl := b.parameterGroupsStore(region)
	if _, exists := tbl.Get(name); exists {
		return nil, ErrParameterGroupAlreadyExists
	}

	pg := &CacheParameterGroup{
		Name:        name,
		Family:      family,
		Description: description,
		ARN:         buildARN("parametergroup:"+name, region, b.accountID),
		IsGlobal:    false,
		Parameters:  make(map[string]string),
		Tags:        tags.New("elasticache.pg." + name + ".tags"),
	}
	tbl.Put(pg)

	return pg, nil
}

// DeleteParameterGroup removes a cache parameter group.
func (b *InMemoryBackend) DeleteParameterGroup(ctx context.Context, name string) error {
	b.mu.Lock("DeleteParameterGroup")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	tbl := b.parameterGroupsStore(region)
	pg, exists := tbl.Get(name)
	if !exists {
		return ErrParameterGroupNotFound
	}

	if pg.IsGlobal {
		return ErrParameterGroupDefaultNotModifiable
	}

	pg.Tags.Close()
	tbl.Delete(name)

	return nil
}

// DescribeParameterGroups returns one parameter group by name, or a paginated list of all.
func (b *InMemoryBackend) DescribeParameterGroups(
	ctx context.Context,
	name, marker string,
	maxRecords int,
) (page.Page[CacheParameterGroup], error) {
	b.mu.RLock("DescribeParameterGroups")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return describePaged(b.parameterGroupsStore(region), name, ErrParameterGroupNotFound, nil,
		func(pg CacheParameterGroup) string { return pg.Name }, marker, maxRecords)
}

// ModifyParameterGroup updates parameters in a cache parameter group.
func (b *InMemoryBackend) ModifyParameterGroup(
	ctx context.Context,
	name string,
	params map[string]string,
) (*CacheParameterGroup, error) {
	b.mu.Lock("ModifyParameterGroup")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	pg, exists := b.parameterGroupsStore(region).Get(name)
	if !exists {
		return nil, ErrParameterGroupNotFound
	}

	if pg.IsGlobal {
		return nil, ErrParameterGroupDefaultNotModifiable
	}

	maps.Copy(pg.Parameters, params)

	cp := *pg

	return &cp, nil
}

// ResetParameterGroup resets parameters in a cache parameter group to defaults.
func (b *InMemoryBackend) ResetParameterGroup(
	ctx context.Context,
	name string,
	paramNames []string,
	resetAll bool,
) (*CacheParameterGroup, error) {
	b.mu.Lock("ResetParameterGroup")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	pg, exists := b.parameterGroupsStore(region).Get(name)
	if !exists {
		return nil, ErrParameterGroupNotFound
	}

	if pg.IsGlobal {
		return nil, ErrParameterGroupDefaultNotModifiable
	}

	if resetAll {
		pg.Parameters = make(map[string]string)
	} else {
		for _, pname := range paramNames {
			delete(pg.Parameters, pname)
		}
	}

	cp := *pg

	return &cp, nil
}

// DescribeParameters lists parameters in a cache parameter group.
func (b *InMemoryBackend) DescribeParameters(
	ctx context.Context,
	name, marker string,
	maxRecords int,
) (page.Page[CacheParameter], error) {
	b.mu.RLock("DescribeParameters")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	pg, exists := b.parameterGroupsStore(region).Get(name)
	if !exists {
		return page.Page[CacheParameter]{}, ErrParameterGroupNotFound
	}

	out := make([]CacheParameter, 0, len(pg.Parameters))
	for k, v := range pg.Parameters {
		out = append(out, CacheParameter{
			Name:         k,
			Value:        v,
			DataType:     dataTypeString,
			IsModifiable: true,
		})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return page.New(out, marker, maxRecords, elasticacheDefaultMaxRecords), nil
}

// CreateSubnetGroup creates a new cache subnet group.
func (b *InMemoryBackend) CreateSubnetGroup(
	ctx context.Context,
	name, description string,
	subnetIDs []string,
) (*CacheSubnetGroup, error) {
	b.mu.Lock("CreateSubnetGroup")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	tbl := b.subnetGroupsStore(region)
	if _, exists := tbl.Get(name); exists {
		return nil, ErrSubnetGroupAlreadyExists
	}

	sg := &CacheSubnetGroup{
		Name:        name,
		Description: description,
		SubnetIDs:   subnetIDs,
		ARN:         b.subnetGroupARN(region, name),
		Tags:        tags.New("elasticache.sg." + name + ".tags"),
	}
	tbl.Put(sg)

	return sg, nil
}

// DeleteSubnetGroup removes a cache subnet group.
func (b *InMemoryBackend) DeleteSubnetGroup(ctx context.Context, name string) error {
	b.mu.Lock("DeleteSubnetGroup")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	tbl := b.subnetGroupsStore(region)
	sg, exists := tbl.Get(name)
	if !exists {
		return ErrSubnetGroupNotFound
	}

	sg.Tags.Close()
	tbl.Delete(name)

	return nil
}

// DescribeSubnetGroups returns one subnet group by name, or a paginated list of all.
func (b *InMemoryBackend) DescribeSubnetGroups(
	ctx context.Context,
	name, marker string,
	maxRecords int,
) (page.Page[CacheSubnetGroup], error) {
	b.mu.RLock("DescribeSubnetGroups")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return describePaged(b.subnetGroupsStore(region), name, ErrSubnetGroupNotFound, nil,
		func(sg CacheSubnetGroup) string { return sg.Name }, marker, maxRecords)
}

// ModifySubnetGroup updates a cache subnet group.
func (b *InMemoryBackend) ModifySubnetGroup(
	ctx context.Context,
	name, description string,
	subnetIDs []string,
) (*CacheSubnetGroup, error) {
	b.mu.Lock("ModifySubnetGroup")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	sg, exists := b.subnetGroupsStore(region).Get(name)
	if !exists {
		return nil, ErrSubnetGroupNotFound
	}

	if description != "" {
		sg.Description = description
	}

	if len(subnetIDs) > 0 {
		sg.SubnetIDs = subnetIDs
	}

	cp := *sg

	return &cp, nil
}

// CreateSnapshot creates a manual snapshot of a cluster or replication group.
func (b *InMemoryBackend) CreateSnapshot(
	ctx context.Context,
	snapshotName, clusterID, replicationGroupID string,
) (*CacheSnapshot, error) {
	b.mu.Lock("CreateSnapshot")
	defer b.mu.Unlock()

	// Exactly one source identifier must be provided.
	if (clusterID == "") == (replicationGroupID == "") {
		return nil, ErrInvalidSnapshotSource
	}

	region := getRegion(ctx, b.region)
	snapStore := b.snapshotsStore(region)
	if _, exists := snapStore.Get(snapshotName); exists {
		return nil, ErrSnapshotAlreadyExists
	}

	snap := &CacheSnapshot{
		SnapshotName:       snapshotName,
		CacheClusterID:     clusterID,
		ReplicationGroupID: replicationGroupID,
		Status:             statusAvailable,
		ARN:                b.snapshotARN(region, snapshotName),
		SnapshotSource:     snapshotSourceManual,
		CreatedAt:          time.Now(),
		Tags:               tags.New("elasticache.snapshot." + snapshotName + ".tags"),
	}
	b.markCreatingLocked(&snap.PendingStatus, &snap.AvailableAt)

	if clusterID != "" {
		c, ok := b.clustersStore(region).Get(clusterID)
		if !ok {
			return nil, ErrClusterNotFound
		}
		snap.Engine = c.Engine
		snap.EngineVersion = c.EngineVersion
		snap.NodeType = c.NodeType
	}

	if replicationGroupID != "" {
		rg, ok := b.replicationGroupsStore(region).Get(replicationGroupID)
		if !ok {
			return nil, ErrReplicationGroupNotFound
		}
		snap.Engine = engineRedis
		ev := rg.EngineVersion
		if ev == "" {
			ev = defaultEngineVersion(engineRedis)
		}
		snap.EngineVersion = ev
		snap.ReplicationGroupID = rg.ReplicationGroupID
	}

	snapStore.Put(snap)
	sourceID := clusterID
	if sourceID == "" {
		sourceID = replicationGroupID
	}
	b.appendEventLocked(sourceID, "cache-cluster", "snapshot "+snapshotName+" created")

	cp := *snap
	cp.Status = overlayStatus(b.now(), snap.Status, snap.PendingStatus, snap.AvailableAt)

	return &cp, nil
}

// DeleteSnapshot removes a snapshot and returns the deleted snapshot.
func (b *InMemoryBackend) DeleteSnapshot(ctx context.Context, snapshotName string) (*CacheSnapshot, error) {
	b.mu.Lock("DeleteSnapshot")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	b.pruneRegionLocked(region)
	tbl := b.snapshotsStore(region)
	snap, exists := tbl.Get(snapshotName)
	if !exists || isReaped(b.now(), snap.PendingStatus, snap.AvailableAt) {
		return nil, ErrSnapshotNotFound
	}

	if d := b.pendingUntil(); !d.IsZero() {
		snap.PendingStatus = statusDeleting
		snap.AvailableAt = d
		b.appendEventLocked(snapshotName, "cache-snapshot", "snapshot deleting")
		cp := *snap
		cp.Status = statusDeleting

		return &cp, nil
	}

	cp := *snap
	snap.Tags.Close()
	tbl.Delete(snapshotName)
	b.appendEventLocked(snapshotName, "cache-snapshot", "snapshot deleted")

	return &cp, nil
}

// DescribeSnapshots returns one snapshot by name, or a paginated list filtered by cluster/rg/source.
// snapshotSource mirrors the real AWS filter values: "system" matches automated snapshots,
// "user" matches manual snapshots, and "" returns all.
func (b *InMemoryBackend) DescribeSnapshots(
	ctx context.Context,
	snapshotName, clusterID, replicationGroupID, snapshotSource, marker string,
	maxRecords int,
) (page.Page[CacheSnapshot], error) {
	b.mu.RLock("DescribeSnapshots")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	// Map AWS filter values ("system"/"user") to stored values ("automated"/"manual").
	wantSource := ""
	switch snapshotSource {
	case "system":
		wantSource = snapshotSourceAutomated
	case "user":
		wantSource = snapshotSourceManual
	}

	p, err := describePaged(b.snapshotsStore(region), snapshotName, ErrSnapshotNotFound, func(s CacheSnapshot) bool {
		return (clusterID == "" || s.CacheClusterID == clusterID) &&
			(replicationGroupID == "" || s.ReplicationGroupID == replicationGroupID) &&
			(wantSource == "" || s.SnapshotSource == wantSource)
	},
		func(s CacheSnapshot) string { return s.SnapshotName }, marker, maxRecords)

	return b.finalizeSnapshotPage(snapshotName, p, err)
}

// CopySnapshot copies an existing snapshot to a new name.
func (b *InMemoryBackend) CopySnapshot(
	ctx context.Context,
	sourceSnapshotName, targetSnapshotName string,
) (*CacheSnapshot, error) {
	b.mu.Lock("CopySnapshot")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	tbl := b.snapshotsStore(region)

	src, ok := tbl.Get(sourceSnapshotName)
	if !ok {
		return nil, ErrSnapshotNotFound
	}

	if _, targetExists := tbl.Get(targetSnapshotName); targetExists {
		return nil, ErrSnapshotAlreadyExists
	}

	cp := *src
	cp.SnapshotName = targetSnapshotName
	cp.ARN = b.snapshotARN(region, targetSnapshotName)
	cp.CreatedAt = time.Now()
	cp.Tags = tags.New("elasticache.snapshot." + targetSnapshotName + ".tags")
	tbl.Put(&cp)
	b.appendEventLocked(targetSnapshotName, "cache-snapshot", "snapshot copied from "+sourceSnapshotName)

	result := cp

	return &result, nil
}

// DescribeEvents returns a paginated list of recorded events, optionally filtered by source and time.
func (b *InMemoryBackend) DescribeEvents(
	_ context.Context,
	sourceIdentifier, sourceType, marker string,
	startTime, endTime time.Time,
	duration, maxRecords int,
) (page.Page[CacheEvent], error) {
	b.mu.RLock("DescribeEvents")
	defer b.mu.RUnlock()

	// If duration (seconds) is specified, derive startTime from it.
	effectiveStart := startTime
	if duration > 0 {
		effectiveStart = time.Now().Add(-time.Duration(duration) * time.Second)
	}

	all := b.events.all()
	out := make([]CacheEvent, 0, len(all))
	for _, e := range all {
		if sourceIdentifier != "" && e.SourceIdentifier != sourceIdentifier {
			continue
		}
		if sourceType != "" && e.SourceType != sourceType {
			continue
		}
		if !effectiveStart.IsZero() && e.Date.Before(effectiveStart) {
			continue
		}
		if !endTime.IsZero() && e.Date.After(endTime) {
			continue
		}
		out = append(out, e)
	}

	return page.New(out, marker, maxRecords, elasticacheDefaultMaxRecords), nil
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

func (b *InMemoryBackend) getGlobalReplicationGroup(id string) (*GlobalReplicationGroup, bool) {
	return b.globalReplicationGroups.Get(id)
}

func (b *InMemoryBackend) listGlobalReplicationGroups() []*GlobalReplicationGroup {
	return b.globalReplicationGroups.All()
}

func (b *InMemoryBackend) putGlobalReplicationGroup(_ string, grg *GlobalReplicationGroup) {
	b.globalReplicationGroups.Put(grg)
}

func (b *InMemoryBackend) deleteGlobalReplicationGroup(id string) {
	b.globalReplicationGroups.Delete(id)
}

func (b *InMemoryBackend) appendUpdateActionsLocked(actions ...*UpdateAction) {
	b.updateActions = append(b.updateActions, actions...)
	const maxUpdateActions = 1000
	if len(b.updateActions) > maxUpdateActions {
		b.updateActions = b.updateActions[len(b.updateActions)-maxUpdateActions:]
	}
}
