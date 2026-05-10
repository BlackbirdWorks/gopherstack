package elasticache

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	gopherDNS "github.com/blackbirdworks/gopherstack/pkgs/dns"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

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
	Members                    []CacheNodeMember
	ClusterID                  string
	Engine                     string
	EngineVersion              string
	Status                     string
	Endpoint                   string
	NodeType                   string
	ARN                        string
	CacheParameterGroupName    string
	PreferredMaintenanceWindow string
	SnapshotWindow             string
	ReplicationGroupID         string
	KmsKeyId                   string
	TransitEncryptionMode      string
	Port                       int
	NumCacheNodes              int
	TransitEncryptionEnabled   bool
	AtRestEncryptionEnabled    bool
}

// ReplicationGroup represents an ElastiCache replication group.
type ReplicationGroup struct {
	CreatedAt                  time.Time                `json:"createdAt"`
	AuthTokenLastModifiedDate  *time.Time               `json:"authTokenLastModifiedDate,omitempty"`
	PendingModifiedValues      *RGPendingModifiedValues `json:"pendingModifiedValues,omitempty"`
	Tags                       *tags.Tags               `json:"tags,omitempty"`
	NodeGroups                 []NodeGroup              `json:"nodeGroups,omitempty"`
	LogDeliveryConfigurations  []LogDeliveryConfig      `json:"logDeliveryConfigurations,omitempty"`
	ReplicationGroupID         string                   `json:"replicationGroupID"`
	Description                string                   `json:"description"`
	Status                     string                   `json:"status"`
	ARN                        string                   `json:"arn"`
	Engine                     string                   `json:"engine,omitempty"`
	CacheParameterGroupName    string                   `json:"cacheParameterGroupName,omitempty"`
	AutomaticFailover          string                   `json:"automaticFailover,omitempty"`
	EngineVersion              string                   `json:"engineVersion,omitempty"`
	CacheNodeType              string                   `json:"cacheNodeType,omitempty"`
	PreferredMaintenanceWindow string                   `json:"preferredMaintenanceWindow,omitempty"`
	SnapshotWindow             string                   `json:"snapshotWindow,omitempty"`
	AuthToken                  string                   `json:"authToken,omitempty"`
	KmsKeyId                   string                   `json:"kmsKeyId,omitempty"`
	NotificationTopicArn       string                   `json:"notificationTopicArn,omitempty"`
	TransitEncryptionMode      string                   `json:"transitEncryptionMode,omitempty"`
	ReplicaCount               int32                    `json:"replicaCount,omitempty"`
	SnapshotRetentionLimit     int                      `json:"snapshotRetentionLimit,omitempty"`
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
	Tags               *tags.Tags `json:"tags,omitempty"`
	SnapshotName       string     `json:"snapshotName"`
	CacheClusterID     string     `json:"cacheClusterId"`
	ReplicationGroupID string     `json:"replicationGroupId"`
	Status             string     `json:"status"`
	ARN                string     `json:"arn"`
	Engine             string     `json:"engine"`
	EngineVersion      string     `json:"engineVersion"`
	NodeType           string     `json:"nodeType"`
	SnapshotSource     string     `json:"snapshotSource"` // "manual" or "automated"
}

// StorageBackend defines the interface for the ElastiCache in-memory store.
type StorageBackend interface {
	CreateCluster(id, engine, nodeType string, port int) (*Cluster, error)
	CreateClusterWithOptions(
		id, engine, nodeType, paramGroupName, maintenanceWindow, snapshotWindow string,
		numCacheNodes, port int,
	) (*Cluster, error)
	DeleteCluster(id string) error
	DescribeClusters(id, marker string, maxRecords int) (page.Page[Cluster], error)
	ModifyCluster(
		id, nodeType, paramGroupName, engineVersion, maintenanceWindow, snapshotWindow string,
		numCacheNodes int,
	) (*Cluster, error)
	ListTagsForResource(arn string) (map[string]string, error)
	AddTagsToResource(arn string, newTags map[string]string) error
	RemoveTagsFromResource(arn string, tagKeys []string) error
	CreateReplicationGroup(id, description string) (*ReplicationGroup, error)
	CreateReplicationGroupWithOptions(
		id, description, paramGroupName, maintenanceWindow, snapshotWindow string,
	) (*ReplicationGroup, error)
	DeleteReplicationGroup(id string) error
	DescribeReplicationGroups(id, marker string, maxRecords int) (page.Page[ReplicationGroup], error)
	ModifyReplicationGroup(
		id, description, paramGroupName, engineVersion, cacheNodeType, maintenanceWindow, snapshotWindow string,
		automaticFailoverEnabled, multiAZEnabled *bool,
	) (*ReplicationGroup, error)
	FailoverReplicationGroup(id, nodeGroupID string) (*ReplicationGroup, error)
	CreateParameterGroup(name, family, description string) (*CacheParameterGroup, error)
	DeleteParameterGroup(name string) error
	DescribeParameterGroups(name, marker string, maxRecords int) (page.Page[CacheParameterGroup], error)
	ModifyParameterGroup(name string, params map[string]string) (*CacheParameterGroup, error)
	ResetParameterGroup(name string, paramNames []string, resetAll bool) (*CacheParameterGroup, error)
	DescribeParameters(name, marker string, maxRecords int) (page.Page[CacheParameter], error)
	CreateSubnetGroup(name, description string, subnetIDs []string) (*CacheSubnetGroup, error)
	DeleteSubnetGroup(name string) error
	DescribeSubnetGroups(name, marker string, maxRecords int) (page.Page[CacheSubnetGroup], error)
	ModifySubnetGroup(name, description string, subnetIDs []string) (*CacheSubnetGroup, error)
	CreateSnapshot(snapshotName, clusterID, replicationGroupID string) (*CacheSnapshot, error)
	DeleteSnapshot(snapshotName string) (*CacheSnapshot, error)
	DescribeSnapshots(
		snapshotName, clusterID, replicationGroupID, marker string,
		maxRecords int,
	) (page.Page[CacheSnapshot], error)
	CopySnapshot(sourceSnapshotName, targetSnapshotName string) (*CacheSnapshot, error)
	DescribeEvents(
		sourceIdentifier, sourceType, marker string,
		startTime, endTime time.Time,
		duration, maxRecords int,
	) (page.Page[CacheEvent], error)
	// New ops
	CreateCacheSecurityGroup(name, description string) (*CacheSecurityGroup, error)
	AuthorizeCacheSecurityGroupIngress(
		name, ec2SecurityGroupName, ec2SecurityGroupOwnerID string,
	) (*CacheSecurityGroup, error)
	CreateGlobalReplicationGroup(
		globalReplicationGroupIDSuffix, description, primaryReplicationGroupID string,
	) (*GlobalReplicationGroup, error)
	CreateServerlessCache(name, description, engine string) (*ServerlessCache, error)
	CreateServerlessCacheSnapshot(snapshotName, serverlessCacheName string) (*ServerlessCacheSnapshot, error)
	CopyServerlessCacheSnapshot(sourceSnapshotName, targetSnapshotName string) (*ServerlessCacheSnapshot, error)
	CreateUser(userID, userName, accessString, engine string, noPasswordRequired bool) (*User, error)
	BatchApplyUpdateAction(
		replicationGroupIDs, cacheClusterIDs []string,
		serviceUpdateName string,
	) (*BatchUpdateResult, error)
	BatchStopUpdateAction(
		replicationGroupIDs, cacheClusterIDs []string,
		serviceUpdateName string,
	) (*BatchUpdateResult, error)
	CompleteMigration(replicationGroupID string, force bool) (*ReplicationGroup, error)
	// User operations
	DeleteUser(userID string) (*User, error)
	DescribeUsers(userID, marker string, maxRecords int) (page.Page[User], error)
	ModifyUser(userID, accessString string, noPasswordRequired bool) (*User, error)
	// UserGroup operations
	CreateUserGroup(groupID, description, engine string, userIDs []string) (*UserGroup, error)
	DeleteUserGroup(groupID string) (*UserGroup, error)
	DescribeUserGroups(groupID, marker string, maxRecords int) (page.Page[UserGroup], error)
	ModifyUserGroup(groupID string, userIDsToAdd, userIDsToRemove []string) (*UserGroup, error)
	// GlobalReplicationGroup operations
	DeleteGlobalReplicationGroup(id string, retainPrimaryReplicationGroup bool) (*GlobalReplicationGroup, error)
	DescribeGlobalReplicationGroups(id, marker string, maxRecords int) (page.Page[GlobalReplicationGroup], error)
	DisassociateGlobalReplicationGroup(
		id, replicationGroupID, replicationGroupRegion string,
	) (*GlobalReplicationGroup, error)
	FailoverGlobalReplicationGroup(id, primaryRegion, primaryReplicationGroupID string) (*GlobalReplicationGroup, error)
	IncreaseNodeGroupsInGlobalReplicationGroup(id string, nodeGroupCount int32) (*GlobalReplicationGroup, error)
	DecreaseNodeGroupsInGlobalReplicationGroup(id string, nodeGroupCount int32) (*GlobalReplicationGroup, error)
	ModifyGlobalReplicationGroup(
		id, description, engineVersion string,
		automaticFailoverEnabled bool,
	) (*GlobalReplicationGroup, error)
	RebalanceSlotsInGlobalReplicationGroup(id string) (*GlobalReplicationGroup, error)
	// ReservedCacheNodes operations
	DescribeReservedCacheNodes(
		id, cacheNodeType, offeringType, marker string,
		maxRecords int,
	) (page.Page[ReservedCacheNode], error)
	DescribeReservedCacheNodesOfferings(
		offeringID, cacheNodeType, offeringType, marker string,
		maxRecords int,
	) (page.Page[ReservedCacheNodesOffering], error)
	PurchaseReservedCacheNodesOffering(
		offeringID, reservedCacheNodeID string,
		cacheNodeCount int32,
	) (*ReservedCacheNode, error)
	// ServerlessCache operations
	DeleteServerlessCache(name string) (*ServerlessCache, error)
	DeleteServerlessCacheSnapshot(name string) (*ServerlessCacheSnapshot, error)
	DescribeServerlessCaches(name, marker string, maxRecords int) (page.Page[ServerlessCache], error)
	DescribeServerlessCacheSnapshots(
		serverlessCacheName, snapshotName, marker string,
		maxRecords int,
	) (page.Page[ServerlessCacheSnapshot], error)
	ExportServerlessCacheSnapshot(snapshotName, s3BucketName string) (*ServerlessCacheSnapshot, error)
	ModifyServerlessCache(name, description string) (*ServerlessCache, error)
	// Migration operations
	StartMigration(replicationGroupID string) (*ReplicationGroup, error)
	TestMigration(replicationGroupID string) (*ReplicationGroup, error)
	IncreaseReplicaCount(replicationGroupID string, newReplicaCount int32) (*ReplicationGroup, error)
	DecreaseReplicaCount(replicationGroupID string, newReplicaCount int32) (*ReplicationGroup, error)
	ModifyReplicationGroupShardConfiguration(replicationGroupID string, nodeGroupCount int32) (*ReplicationGroup, error)
	// Cache info operations
	DescribeCacheEngineVersions(
		engine, family, engineVersion, marker string,
		maxRecords int,
	) (page.Page[CacheEngineVersion], error)
	RebootCacheCluster(clusterID string, nodeIDs []string) (*Cluster, error)
	DeleteCacheSecurityGroup(name string) error
	DescribeCacheSecurityGroups(name, marker string, maxRecords int) (page.Page[CacheSecurityGroup], error)
	RevokeCacheSecurityGroupIngress(
		name, ec2SecurityGroupName, ec2SecurityGroupOwnerID string,
	) (*CacheSecurityGroup, error)
	DescribeEngineDefaultParameters(
		cacheParameterGroupFamily, marker string,
		maxRecords int,
	) (page.Page[CacheParameter], error)
	DescribeServiceUpdates(
		serviceUpdateName, marker string,
		maxRecords int,
		status []string,
	) (page.Page[ServiceUpdate], error)
	DescribeUpdateActions(serviceUpdateName, marker string, maxRecords int) (page.Page[UpdateAction], error)
	ListAllowedNodeTypeModifications(clusterID, replicationGroupID string) ([]string, error)
	// Audit1: extended create/modify with new fields
	CreateReplicationGroupFull(opts ReplicationGroupCreateOpts) (*ReplicationGroup, error)
	ModifyReplicationGroupFull(id string, opts ReplicationGroupModifyOpts) (*ReplicationGroup, error)
	// Audit1: auto snapshot scheduling
	TriggerAutoSnapshot(replicationGroupID string) (*CacheSnapshot, error)
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
type InMemoryBackend struct {
	dnsRegistrar              DNSRegistrar
	globalReplicationGroups   map[string]*GlobalReplicationGroup
	users                     map[string]*User
	parameterGroups           map[string]*CacheParameterGroup
	subnetGroups              map[string]*CacheSubnetGroup
	snapshots                 map[string]*CacheSnapshot
	cacheSecurityGroups       map[string]*CacheSecurityGroup
	cacheSecurityGroupIngress map[string][]EC2SecurityGroupMembership
	clusters                  map[string]*Cluster
	replicationGroups         map[string]*ReplicationGroup
	serverlessCaches          map[string]*ServerlessCache
	serverlessCacheSnapshots  map[string]*ServerlessCacheSnapshot
	userGroups                map[string]*UserGroup
	reservedCacheNodes        map[string]*ReservedCacheNode
	mu                        *lockmetrics.RWMutex
	events                    *eventRing
	accountID                 string
	region                    string
	engineMode                string
}

// NewInMemoryBackend creates a new backend with the given engine mode.
func NewInMemoryBackend(engineMode, accountID, region string) *InMemoryBackend {
	if engineMode == "" {
		engineMode = EngineEmbedded
	}

	b := &InMemoryBackend{
		clusters:                  make(map[string]*Cluster),
		replicationGroups:         make(map[string]*ReplicationGroup),
		parameterGroups:           make(map[string]*CacheParameterGroup),
		subnetGroups:              make(map[string]*CacheSubnetGroup),
		snapshots:                 make(map[string]*CacheSnapshot),
		cacheSecurityGroups:       make(map[string]*CacheSecurityGroup),
		cacheSecurityGroupIngress: make(map[string][]EC2SecurityGroupMembership),
		globalReplicationGroups:   make(map[string]*GlobalReplicationGroup),
		serverlessCaches:          make(map[string]*ServerlessCache),
		serverlessCacheSnapshots:  make(map[string]*ServerlessCacheSnapshot),
		users:                     make(map[string]*User),
		userGroups:                make(map[string]*UserGroup),
		reservedCacheNodes:        make(map[string]*ReservedCacheNode),
		events:                    newEventRing(maxEvents),
		engineMode:                engineMode,
		accountID:                 accountID,
		region:                    region,
		mu:                        lockmetrics.New("elasticache"),
	}

	b.initDefaultParameterGroups()

	return b
}

// initDefaultParameterGroups seeds the well-known default parameter groups.
func (b *InMemoryBackend) initDefaultParameterGroups() {
	for _, dpg := range builtinParameterGroupFamilies() {
		pg := &CacheParameterGroup{
			Name:        dpg.name,
			Family:      dpg.family,
			Description: "Default parameter group for " + dpg.family,
			ARN:         b.parameterGroupARN(dpg.name),
			IsGlobal:    true,
			Parameters:  make(map[string]string),
			Tags:        tags.New("elasticache.pg." + dpg.name + ".tags"),
		}
		b.parameterGroups[dpg.name] = pg
	}
}

// SetDNSRegistrar wires a DNS server so cache cluster hostnames are
// automatically registered on create and deregistered on delete.
func (b *InMemoryBackend) SetDNSRegistrar(r DNSRegistrar) {
	b.mu.Lock("SetDNSRegistrar")
	b.dnsRegistrar = r
	b.mu.Unlock()
}

func (b *InMemoryBackend) clusterARN(id string) string {
	return arn.Build("elasticache", b.region, b.accountID, "cluster:"+id)
}

func (b *InMemoryBackend) replicationGroupARN(id string) string {
	return arn.Build("elasticache", b.region, b.accountID, "replicationgroup:"+id)
}

func (b *InMemoryBackend) parameterGroupARN(name string) string {
	return arn.Build("elasticache", b.region, b.accountID, "parametergroup:"+name)
}

func (b *InMemoryBackend) subnetGroupARN(name string) string {
	return arn.Build("elasticache", b.region, b.accountID, "subnetgroup:"+name)
}

func (b *InMemoryBackend) snapshotARN(name string) string {
	return arn.Build("elasticache", b.region, b.accountID, "snapshot:"+name)
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

// createClusterLocked creates a cluster assuming b.mu is already held.
func (b *InMemoryBackend) createClusterLocked(
	id, engine, nodeType, paramGroupName, maintenanceWindow, snapshotWindow string,
	numCacheNodes, port int,
) (*Cluster, error) {
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
		ARN:                        b.clusterARN(id),
		Tags:                       tags.New("elasticache.cluster." + id + ".tags"),
		CreatedAt:                  time.Now(),
		CacheParameterGroupName:    paramGroupName,
		PreferredMaintenanceWindow: maintenanceWindow,
		SnapshotWindow:             snapshotWindow,
	}

	switch b.engineMode {
	case EngineEmbedded:
		mr, err := miniredis.Run()
		if err != nil {
			return nil, fmt.Errorf("start miniredis: %w", err)
		}
		c.mini = mr
		c.Port = mr.Server().Addr().Port
	default:
		if port > 0 {
			c.Port = port
		} else {
			c.Port = 6379
		}
	}

	c.Endpoint = gopherDNS.SyntheticHostname(id, randomSuffix(), b.region, "cache")
	if b.dnsRegistrar != nil {
		b.dnsRegistrar.Register(c.Endpoint)
	}

	b.clusters[id] = c
	b.appendEventLocked(id, "cache-cluster", "cluster created")

	return c, nil
}

// CreateCluster creates a new cache cluster.
func (b *InMemoryBackend) CreateCluster(id, engine, nodeType string, port int) (*Cluster, error) {
	b.mu.Lock("CreateCluster")
	defer b.mu.Unlock()

	if _, exists := b.clusters[id]; exists {
		return nil, ErrClusterAlreadyExists
	}

	return b.createClusterLocked(id, engine, nodeType, "", "", "", 1, port)
}

// CreateClusterWithOptions creates a new cache cluster with optional parameter group and scheduling windows.
func (b *InMemoryBackend) CreateClusterWithOptions(
	id, engine, nodeType, paramGroupName, maintenanceWindow, snapshotWindow string,
	numCacheNodes, port int,
) (*Cluster, error) {
	b.mu.Lock("CreateClusterWithOptions")
	defer b.mu.Unlock()

	if _, exists := b.clusters[id]; exists {
		return nil, ErrClusterAlreadyExists
	}

	if paramGroupName != "" {
		pg, ok := b.parameterGroups[paramGroupName]
		if !ok {
			return nil, ErrParameterGroupNotFound
		}

		if err := validateParamGroupFamily(engine, pg.Family); err != nil {
			return nil, err
		}
	}

	return b.createClusterLocked(
		id,
		engine,
		nodeType,
		paramGroupName,
		maintenanceWindow,
		snapshotWindow,
		numCacheNodes,
		port,
	)
}

// DeleteCluster stops and removes a cluster.
func (b *InMemoryBackend) DeleteCluster(id string) error {
	b.mu.Lock("DeleteCluster")
	defer b.mu.Unlock()

	c, exists := b.clusters[id]
	if !exists {
		return ErrClusterNotFound
	}

	if b.dnsRegistrar != nil && c.Endpoint != "" {
		b.dnsRegistrar.Deregister(c.Endpoint)
	}

	if c.mini != nil {
		c.mini.Close()
	}
	c.Tags.Close()
	delete(b.clusters, id)
	b.appendEventLocked(id, "cache-cluster", "cluster deleted")

	return nil
}

const elasticacheDefaultMaxRecords = 100

// DescribeClusters returns one cluster by id, or a paginated list of all clusters when id is empty.
func (b *InMemoryBackend) DescribeClusters(id, marker string, maxRecords int) (page.Page[Cluster], error) {
	b.mu.RLock("DescribeClusters")
	defer b.mu.RUnlock()

	if id != "" {
		c, exists := b.clusters[id]
		if !exists {
			return page.Page[Cluster]{}, ErrClusterNotFound
		}

		return page.Page[Cluster]{Data: []Cluster{*c}}, nil
	}

	out := make([]Cluster, 0, len(b.clusters))
	for _, c := range b.clusters {
		out = append(out, *c)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ClusterID < out[j].ClusterID })

	return page.New(out, marker, maxRecords, elasticacheDefaultMaxRecords), nil
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
func (b *InMemoryBackend) collectTagCandidatesLocked() []tagCandidate {
	candidates := make([]tagCandidate, 0, tagCandidateInitCap)
	for _, c := range b.clusters {
		candidates = append(
			candidates,
			tagCandidate{c.ARN, tagEntry{&c.Tags, "elasticache.cluster." + c.ClusterID + ".tags"}},
		)
	}
	for _, rg := range b.replicationGroups {
		candidates = append(
			candidates,
			tagCandidate{rg.ARN, tagEntry{&rg.Tags, "elasticache.rg." + rg.ReplicationGroupID + ".tags"}},
		)
	}
	for _, pg := range b.parameterGroups {
		candidates = append(candidates, tagCandidate{pg.ARN, tagEntry{&pg.Tags, "elasticache.pg." + pg.Name + ".tags"}})
	}
	for _, sg := range b.subnetGroups {
		candidates = append(candidates, tagCandidate{sg.ARN, tagEntry{&sg.Tags, "elasticache.sg." + sg.Name + ".tags"}})
	}
	for _, snap := range b.snapshots {
		candidates = append(
			candidates,
			tagCandidate{snap.ARN, tagEntry{&snap.Tags, "elasticache.snapshot." + snap.SnapshotName + ".tags"}},
		)
	}
	for _, sg := range b.cacheSecurityGroups {
		candidates = append(candidates, tagCandidate{sg.ARN, tagEntry{&sg.Tags, "elasticache.sg." + sg.Name + ".tags"}})
	}
	for _, grg := range b.globalReplicationGroups {
		candidates = append(candidates,
			tagCandidate{grg.ARN, tagEntry{&grg.Tags, "elasticache.grg." + grg.GlobalReplicationGroupID + ".tags"}})
	}
	for _, sc := range b.serverlessCaches {
		candidates = append(
			candidates,
			tagCandidate{sc.ARN, tagEntry{&sc.Tags, "elasticache.serverless." + sc.Name + ".tags"}},
		)
	}
	for _, snap := range b.serverlessCacheSnapshots {
		candidates = append(candidates,
			tagCandidate{snap.ARN, tagEntry{&snap.Tags, "elasticache.serverlesssnap." + snap.Name + ".tags"}})
	}
	for _, u := range b.users {
		candidates = append(
			candidates,
			tagCandidate{u.ARN, tagEntry{&u.Tags, "elasticache.user." + u.UserID + ".tags"}},
		)
	}
	for _, ug := range b.userGroups {
		candidates = append(
			candidates,
			tagCandidate{ug.ARN, tagEntry{&ug.Tags, "elasticache.usergroup." + ug.UserGroupID + ".tags"}},
		)
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
func (b *InMemoryBackend) ListTagsForResource(arn string) (map[string]string, error) {
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
func (b *InMemoryBackend) AddTagsToResource(resourceARN string, newTags map[string]string) error {
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
func (b *InMemoryBackend) RemoveTagsFromResource(resourceARN string, tagKeys []string) error {
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
	id, description, paramGroupName, maintenanceWindow, snapshotWindow string,
) *ReplicationGroup {
	rg := &ReplicationGroup{
		ReplicationGroupID:         id,
		Description:                description,
		Status:                     statusAvailable,
		ARN:                        b.replicationGroupARN(id),
		Tags:                       tags.New("elasticache.rg." + id + ".tags"),
		CreatedAt:                  time.Now(),
		CacheParameterGroupName:    paramGroupName,
		PreferredMaintenanceWindow: maintenanceWindow,
		SnapshotWindow:             snapshotWindow,
	}
	b.replicationGroups[id] = rg
	b.appendEventLocked(id, "replication-group", "replication group created")

	return rg
}

// CreateReplicationGroup creates a replication group.
func (b *InMemoryBackend) CreateReplicationGroup(id, description string) (*ReplicationGroup, error) {
	b.mu.Lock("CreateReplicationGroup")
	defer b.mu.Unlock()

	if _, exists := b.replicationGroups[id]; exists {
		return nil, ErrReplicationGroupAlreadyExists
	}

	return b.createReplicationGroupLocked(id, description, "", "", ""), nil
}

// CreateReplicationGroupWithOptions creates a replication group with optional parameter group and scheduling windows.
func (b *InMemoryBackend) CreateReplicationGroupWithOptions(
	id, description, paramGroupName, maintenanceWindow, snapshotWindow string,
) (*ReplicationGroup, error) {
	b.mu.Lock("CreateReplicationGroupWithOptions")
	defer b.mu.Unlock()

	if _, exists := b.replicationGroups[id]; exists {
		return nil, ErrReplicationGroupAlreadyExists
	}

	if paramGroupName != "" {
		if _, ok := b.parameterGroups[paramGroupName]; !ok {
			return nil, ErrParameterGroupNotFound
		}
	}

	return b.createReplicationGroupLocked(id, description, paramGroupName, maintenanceWindow, snapshotWindow), nil
}

// DeleteReplicationGroup removes a replication group.
func (b *InMemoryBackend) DeleteReplicationGroup(id string) error {
	b.mu.Lock("DeleteReplicationGroup")
	defer b.mu.Unlock()

	rg, exists := b.replicationGroups[id]
	if !exists {
		return ErrReplicationGroupNotFound
	}
	rg.Tags.Close()
	delete(b.replicationGroups, id)
	b.appendEventLocked(id, "replication-group", "replication group deleted")

	return nil
}

// DescribeReplicationGroups returns one replication group by id, or a paginated list of all when id is empty.
func (b *InMemoryBackend) DescribeReplicationGroups(
	id, marker string,
	maxRecords int,
) (page.Page[ReplicationGroup], error) {
	b.mu.RLock("DescribeReplicationGroups")
	defer b.mu.RUnlock()

	if id != "" {
		rg, exists := b.replicationGroups[id]
		if !exists {
			return page.Page[ReplicationGroup]{}, ErrReplicationGroupNotFound
		}

		return page.Page[ReplicationGroup]{Data: []ReplicationGroup{*rg}}, nil
	}

	out := make([]ReplicationGroup, 0, len(b.replicationGroups))
	for _, rg := range b.replicationGroups {
		out = append(out, *rg)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ReplicationGroupID < out[j].ReplicationGroupID })

	return page.New(out, marker, maxRecords, elasticacheDefaultMaxRecords), nil
}

// randomSuffix generates a short random hex string for synthetic hostnames.
func randomSuffix() string {
	b := make([]byte, randomSuffixLen)
	_, _ = rand.Read(b)

	return hex.EncodeToString(b)
}

// ListAll returns all clusters (used by dashboard).
func (b *InMemoryBackend) ListAll() []Cluster {
	b.mu.RLock("ListAll")
	defer b.mu.RUnlock()
	out := make([]Cluster, 0, len(b.clusters))
	for _, c := range b.clusters {
		cp := *c
		out = append(out, cp)
	}

	return out
}

// ModifyCluster modifies an existing cache cluster.
func (b *InMemoryBackend) ModifyCluster(
	id, nodeType, paramGroupName, engineVersion, maintenanceWindow, snapshotWindow string,
	numCacheNodes int,
) (*Cluster, error) {
	b.mu.Lock("ModifyCluster")
	defer b.mu.Unlock()

	c, exists := b.clusters[id]
	if !exists {
		return nil, ErrClusterNotFound
	}

	if nodeType != "" {
		c.NodeType = nodeType
	}

	if paramGroupName != "" {
		if _, ok := b.parameterGroups[paramGroupName]; !ok {
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

	b.appendEventLocked(id, "cache-cluster", "cluster modified")

	cp := *c

	return &cp, nil
}

// ModifyReplicationGroup modifies an existing replication group.
func (b *InMemoryBackend) ModifyReplicationGroup(
	id, description, paramGroupName, engineVersion, cacheNodeType, maintenanceWindow, snapshotWindow string,
	automaticFailoverEnabled, multiAZEnabled *bool,
) (*ReplicationGroup, error) {
	b.mu.Lock("ModifyReplicationGroup")
	defer b.mu.Unlock()

	rg, exists := b.replicationGroups[id]
	if !exists {
		return nil, ErrReplicationGroupNotFound
	}

	if description != "" {
		rg.Description = description
	}

	if paramGroupName != "" {
		if _, ok := b.parameterGroups[paramGroupName]; !ok {
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

	b.appendEventLocked(id, "replication-group", "replication group modified")

	cp := *rg

	return &cp, nil
}

// FailoverReplicationGroup simulates a failover for the given replication group.
func (b *InMemoryBackend) FailoverReplicationGroup(id, _ string) (*ReplicationGroup, error) {
	b.mu.Lock("FailoverReplicationGroup")
	defer b.mu.Unlock()

	rg, exists := b.replicationGroups[id]
	if !exists {
		return nil, ErrReplicationGroupNotFound
	}

	rg.Status = statusAvailable
	b.appendEventLocked(id, "replication-group", "failover completed")

	cp := *rg

	return &cp, nil
}

// CreateParameterGroup creates a new cache parameter group.
func (b *InMemoryBackend) CreateParameterGroup(name, family, description string) (*CacheParameterGroup, error) {
	b.mu.Lock("CreateParameterGroup")
	defer b.mu.Unlock()

	if _, exists := b.parameterGroups[name]; exists {
		return nil, ErrParameterGroupAlreadyExists
	}

	pg := &CacheParameterGroup{
		Name:        name,
		Family:      family,
		Description: description,
		ARN:         b.parameterGroupARN(name),
		IsGlobal:    false,
		Parameters:  make(map[string]string),
		Tags:        tags.New("elasticache.pg." + name + ".tags"),
	}
	b.parameterGroups[name] = pg

	return pg, nil
}

// DeleteParameterGroup removes a cache parameter group.
func (b *InMemoryBackend) DeleteParameterGroup(name string) error {
	b.mu.Lock("DeleteParameterGroup")
	defer b.mu.Unlock()

	pg, exists := b.parameterGroups[name]
	if !exists {
		return ErrParameterGroupNotFound
	}

	if pg.IsGlobal {
		return ErrParameterGroupDefaultNotModifiable
	}

	pg.Tags.Close()
	delete(b.parameterGroups, name)

	return nil
}

// DescribeParameterGroups returns one parameter group by name, or a paginated list of all.
func (b *InMemoryBackend) DescribeParameterGroups(
	name, marker string,
	maxRecords int,
) (page.Page[CacheParameterGroup], error) {
	b.mu.RLock("DescribeParameterGroups")
	defer b.mu.RUnlock()

	if name != "" {
		pg, exists := b.parameterGroups[name]
		if !exists {
			return page.Page[CacheParameterGroup]{}, ErrParameterGroupNotFound
		}

		return page.Page[CacheParameterGroup]{Data: []CacheParameterGroup{*pg}}, nil
	}

	out := make([]CacheParameterGroup, 0, len(b.parameterGroups))
	for _, pg := range b.parameterGroups {
		out = append(out, *pg)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return page.New(out, marker, maxRecords, elasticacheDefaultMaxRecords), nil
}

// ModifyParameterGroup updates parameters in a cache parameter group.
func (b *InMemoryBackend) ModifyParameterGroup(name string, params map[string]string) (*CacheParameterGroup, error) {
	b.mu.Lock("ModifyParameterGroup")
	defer b.mu.Unlock()

	pg, exists := b.parameterGroups[name]
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
	name string,
	paramNames []string,
	resetAll bool,
) (*CacheParameterGroup, error) {
	b.mu.Lock("ResetParameterGroup")
	defer b.mu.Unlock()

	pg, exists := b.parameterGroups[name]
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
func (b *InMemoryBackend) DescribeParameters(name, marker string, maxRecords int) (page.Page[CacheParameter], error) {
	b.mu.RLock("DescribeParameters")
	defer b.mu.RUnlock()

	pg, exists := b.parameterGroups[name]
	if !exists {
		return page.Page[CacheParameter]{}, ErrParameterGroupNotFound
	}

	out := make([]CacheParameter, 0, len(pg.Parameters))
	for k, v := range pg.Parameters {
		out = append(out, CacheParameter{
			Name:         k,
			Value:        v,
			DataType:     "string",
			IsModifiable: true,
		})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return page.New(out, marker, maxRecords, elasticacheDefaultMaxRecords), nil
}

// CreateSubnetGroup creates a new cache subnet group.
func (b *InMemoryBackend) CreateSubnetGroup(name, description string, subnetIDs []string) (*CacheSubnetGroup, error) {
	b.mu.Lock("CreateSubnetGroup")
	defer b.mu.Unlock()

	if _, exists := b.subnetGroups[name]; exists {
		return nil, ErrSubnetGroupAlreadyExists
	}

	sg := &CacheSubnetGroup{
		Name:        name,
		Description: description,
		SubnetIDs:   subnetIDs,
		ARN:         b.subnetGroupARN(name),
		Tags:        tags.New("elasticache.sg." + name + ".tags"),
	}
	b.subnetGroups[name] = sg

	return sg, nil
}

// DeleteSubnetGroup removes a cache subnet group.
func (b *InMemoryBackend) DeleteSubnetGroup(name string) error {
	b.mu.Lock("DeleteSubnetGroup")
	defer b.mu.Unlock()

	sg, exists := b.subnetGroups[name]
	if !exists {
		return ErrSubnetGroupNotFound
	}

	sg.Tags.Close()
	delete(b.subnetGroups, name)

	return nil
}

// DescribeSubnetGroups returns one subnet group by name, or a paginated list of all.
func (b *InMemoryBackend) DescribeSubnetGroups(
	name, marker string,
	maxRecords int,
) (page.Page[CacheSubnetGroup], error) {
	b.mu.RLock("DescribeSubnetGroups")
	defer b.mu.RUnlock()

	if name != "" {
		sg, exists := b.subnetGroups[name]
		if !exists {
			return page.Page[CacheSubnetGroup]{}, ErrSubnetGroupNotFound
		}

		return page.Page[CacheSubnetGroup]{Data: []CacheSubnetGroup{*sg}}, nil
	}

	out := make([]CacheSubnetGroup, 0, len(b.subnetGroups))
	for _, sg := range b.subnetGroups {
		out = append(out, *sg)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return page.New(out, marker, maxRecords, elasticacheDefaultMaxRecords), nil
}

// ModifySubnetGroup updates a cache subnet group.
func (b *InMemoryBackend) ModifySubnetGroup(name, description string, subnetIDs []string) (*CacheSubnetGroup, error) {
	b.mu.Lock("ModifySubnetGroup")
	defer b.mu.Unlock()

	sg, exists := b.subnetGroups[name]
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
func (b *InMemoryBackend) CreateSnapshot(snapshotName, clusterID, replicationGroupID string) (*CacheSnapshot, error) {
	b.mu.Lock("CreateSnapshot")
	defer b.mu.Unlock()

	// Exactly one source identifier must be provided.
	if (clusterID == "") == (replicationGroupID == "") {
		return nil, ErrInvalidSnapshotSource
	}

	if _, exists := b.snapshots[snapshotName]; exists {
		return nil, ErrSnapshotAlreadyExists
	}

	snap := &CacheSnapshot{
		SnapshotName:       snapshotName,
		CacheClusterID:     clusterID,
		ReplicationGroupID: replicationGroupID,
		Status:             statusAvailable,
		ARN:                b.snapshotARN(snapshotName),
		SnapshotSource:     "manual",
		CreatedAt:          time.Now(),
		Tags:               tags.New("elasticache.snapshot." + snapshotName + ".tags"),
	}

	if clusterID != "" {
		c, ok := b.clusters[clusterID]
		if !ok {
			return nil, ErrClusterNotFound
		}
		snap.Engine = c.Engine
		snap.EngineVersion = c.EngineVersion
		snap.NodeType = c.NodeType
	}

	if replicationGroupID != "" {
		rg, ok := b.replicationGroups[replicationGroupID]
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

	b.snapshots[snapshotName] = snap
	sourceID := clusterID
	if sourceID == "" {
		sourceID = replicationGroupID
	}
	b.appendEventLocked(sourceID, "cache-cluster", "snapshot "+snapshotName+" created")

	return snap, nil
}

// DeleteSnapshot removes a snapshot and returns the deleted snapshot.
func (b *InMemoryBackend) DeleteSnapshot(snapshotName string) (*CacheSnapshot, error) {
	b.mu.Lock("DeleteSnapshot")
	defer b.mu.Unlock()

	snap, exists := b.snapshots[snapshotName]
	if !exists {
		return nil, ErrSnapshotNotFound
	}

	cp := *snap
	snap.Tags.Close()
	delete(b.snapshots, snapshotName)
	b.appendEventLocked(snapshotName, "cache-snapshot", "snapshot deleted")

	return &cp, nil
}

// DescribeSnapshots returns one snapshot by name, or a paginated list filtered by cluster/rg.
func (b *InMemoryBackend) DescribeSnapshots(
	snapshotName, clusterID, replicationGroupID, marker string,
	maxRecords int,
) (page.Page[CacheSnapshot], error) {
	b.mu.RLock("DescribeSnapshots")
	defer b.mu.RUnlock()

	if snapshotName != "" {
		snap, exists := b.snapshots[snapshotName]
		if !exists {
			return page.Page[CacheSnapshot]{}, ErrSnapshotNotFound
		}

		return page.Page[CacheSnapshot]{Data: []CacheSnapshot{*snap}}, nil
	}

	out := make([]CacheSnapshot, 0, len(b.snapshots))
	for k := range b.snapshots {
		snap := b.snapshots[k]
		if clusterID != "" && snap.CacheClusterID != clusterID {
			continue
		}
		if replicationGroupID != "" && snap.ReplicationGroupID != replicationGroupID {
			continue
		}
		out = append(out, *snap)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].SnapshotName < out[j].SnapshotName })

	return page.New(out, marker, maxRecords, elasticacheDefaultMaxRecords), nil
}

// CopySnapshot copies an existing snapshot to a new name.
func (b *InMemoryBackend) CopySnapshot(sourceSnapshotName, targetSnapshotName string) (*CacheSnapshot, error) {
	b.mu.Lock("CopySnapshot")
	defer b.mu.Unlock()

	src, ok := b.snapshots[sourceSnapshotName]
	if !ok {
		return nil, ErrSnapshotNotFound
	}

	if _, targetExists := b.snapshots[targetSnapshotName]; targetExists {
		return nil, ErrSnapshotAlreadyExists
	}

	cp := *src
	cp.SnapshotName = targetSnapshotName
	cp.ARN = b.snapshotARN(targetSnapshotName)
	cp.CreatedAt = time.Now()
	cp.Tags = tags.New("elasticache.snapshot." + targetSnapshotName + ".tags")
	b.snapshots[targetSnapshotName] = &cp
	b.appendEventLocked(targetSnapshotName, "cache-snapshot", "snapshot copied from "+sourceSnapshotName)

	result := cp

	return &result, nil
}

// DescribeEvents returns a paginated list of recorded events, optionally filtered by source and time.
func (b *InMemoryBackend) DescribeEvents(
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

	for _, c := range b.clusters {
		if c.mini != nil {
			c.mini.Close()
		}
	}

	b.clusters = make(map[string]*Cluster)
	b.replicationGroups = make(map[string]*ReplicationGroup)
	b.parameterGroups = make(map[string]*CacheParameterGroup)
	b.subnetGroups = make(map[string]*CacheSubnetGroup)
	b.snapshots = make(map[string]*CacheSnapshot)
	b.cacheSecurityGroups = make(map[string]*CacheSecurityGroup)
	b.cacheSecurityGroupIngress = make(map[string][]EC2SecurityGroupMembership)
	b.globalReplicationGroups = make(map[string]*GlobalReplicationGroup)
	b.serverlessCaches = make(map[string]*ServerlessCache)
	b.serverlessCacheSnapshots = make(map[string]*ServerlessCacheSnapshot)
	b.users = make(map[string]*User)
	b.userGroups = make(map[string]*UserGroup)
	b.reservedCacheNodes = make(map[string]*ReservedCacheNode)
	b.events.reset()
	b.initDefaultParameterGroups()
}
