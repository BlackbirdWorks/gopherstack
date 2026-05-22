package memorydb

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

const (
	snapshotSourceManual = "manual"
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
	// authTypeNoPasswordRequired is the no-password authentication type.
	authTypeNoPasswordRequired = "no-password-required"
	// authTypeNoPassword is the alias for no-password-required.
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

// ErrValidation is returned when input validation fails.
var ErrValidation = awserr.New("InvalidParameterValueException", awserr.ErrInvalidParameter)

// Errors used by the backend.
var (
	// ErrClusterNotFound is returned when a cluster does not exist.
	ErrClusterNotFound = awserr.New("ClusterNotFoundFault: cluster not found", awserr.ErrNotFound)
	// ErrClusterAlreadyExists is returned when a cluster already exists.
	ErrClusterAlreadyExists = awserr.New("ClusterAlreadyExistsFault: cluster already exists", awserr.ErrAlreadyExists)
	// ErrACLNotFound is returned when an ACL does not exist.
	ErrACLNotFound = awserr.New("ACLNotFoundFault: ACL not found", awserr.ErrNotFound)
	// ErrACLAlreadyExists is returned when an ACL already exists.
	ErrACLAlreadyExists = awserr.New("ACLAlreadyExistsFault: ACL already exists", awserr.ErrAlreadyExists)
	// ErrSubnetGroupNotFound is returned when a subnet group does not exist.
	ErrSubnetGroupNotFound = awserr.New("SubnetGroupNotFoundFault: subnet group not found", awserr.ErrNotFound)
	// ErrSubnetGroupAlreadyExists is returned when a subnet group already exists.
	ErrSubnetGroupAlreadyExists = awserr.New(
		"SubnetGroupAlreadyExistsFault: subnet group already exists",
		awserr.ErrAlreadyExists,
	)
	// ErrUserNotFound is returned when a user does not exist.
	ErrUserNotFound = awserr.New("UserNotFoundFault: user not found", awserr.ErrNotFound)
	// ErrUserAlreadyExists is returned when a user already exists.
	ErrUserAlreadyExists = awserr.New("UserAlreadyExistsFault: user already exists", awserr.ErrAlreadyExists)
	// ErrParameterGroupNotFound is returned when a parameter group does not exist.
	ErrParameterGroupNotFound = awserr.New("ParameterGroupNotFoundFault: parameter group not found", awserr.ErrNotFound)
	// ErrParameterGroupAlreadyExists is returned when a parameter group already exists.
	ErrParameterGroupAlreadyExists = awserr.New(
		"ParameterGroupAlreadyExistsFault: parameter group already exists",
		awserr.ErrAlreadyExists,
	)
	// ErrSnapshotNotFound is returned when a snapshot does not exist.
	ErrSnapshotNotFound = awserr.New("SnapshotNotFoundFault: snapshot not found", awserr.ErrNotFound)
	// ErrSnapshotAlreadyExists is returned when a snapshot already exists.
	ErrSnapshotAlreadyExists = awserr.New(
		"SnapshotAlreadyExistsFault: snapshot already exists",
		awserr.ErrAlreadyExists,
	)
	// ErrMultiRegionClusterNotFound is returned when a multi-region cluster does not exist.
	ErrMultiRegionClusterNotFound = awserr.New(
		"MultiRegionClusterNotFoundFault: multi-region cluster not found",
		awserr.ErrNotFound,
	)
	// ErrMultiRegionClusterAlreadyExists is returned when a multi-region cluster already exists.
	ErrMultiRegionClusterAlreadyExists = awserr.New(
		"MultiRegionClusterAlreadyExistsFault: multi-region cluster already exists",
		awserr.ErrAlreadyExists,
	)
	// ErrMultiRegionParameterGroupNotFound is returned when a multi-region parameter group does not exist.
	ErrMultiRegionParameterGroupNotFound = awserr.New(
		"ParameterGroupNotFoundFault: multi-region parameter group not found",
		awserr.ErrNotFound,
	)
	// ErrACLInUse is returned when an ACL cannot be deleted because it is assigned to a cluster.
	ErrACLInUse = awserr.New("ACLInUseFault: ACL is currently associated with a cluster", awserr.ErrConflict)
	// ErrUserInUse is returned when a user cannot be deleted because it is a member of an ACL.
	ErrUserInUse = awserr.New("UserInUseFault: user is currently a member of an ACL", awserr.ErrConflict)
	// ErrReservationAlreadyExists is returned when a reserved node with the same ID already exists.
	ErrReservationAlreadyExists = awserr.New(
		"ReservedNodeAlreadyExistsFault: reserved node already exists",
		awserr.ErrAlreadyExists,
	)
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

// compile-time assertion that InMemoryBackend satisfies StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)

// StorageBackend is the interface for the MemoryDB in-memory backend.
type StorageBackend interface {
	// Cluster operations
	CreateCluster(region, accountID string, req *createClusterRequest) (*Cluster, error)
	DescribeClusters(name string) ([]*Cluster, error)
	DeleteCluster(name string) (*Cluster, error)
	DeleteClusterWithSnapshot(region, accountID, clusterName, snapshotName string) (*Cluster, error)
	UpdateCluster(req *updateClusterRequest) (*Cluster, error)

	// ACL operations
	CreateACL(region, accountID string, req *createACLRequest) (*ACL, error)
	DescribeACLs(name string) ([]*ACL, error)
	DeleteACL(name string) (*ACL, error)
	UpdateACL(req *updateACLRequest) (*ACL, error)

	// SubnetGroup operations
	CreateSubnetGroup(region, accountID string, req *createSubnetGroupRequest) (*SubnetGroup, error)
	DescribeSubnetGroups(name string) ([]*SubnetGroup, error)
	DeleteSubnetGroup(name string) (*SubnetGroup, error)
	UpdateSubnetGroup(req *updateSubnetGroupRequest) (*SubnetGroup, error)

	// User operations
	CreateUser(region, accountID string, req *createUserRequest) (*User, error)
	DescribeUsers(name string) ([]*User, error)
	DeleteUser(name string) (*User, error)
	UpdateUser(req *updateUserRequest) (*User, error)

	// ParameterGroup operations
	CreateParameterGroup(region, accountID string, req *createParameterGroupRequest) (*ParameterGroup, error)
	DescribeParameterGroups(name string) ([]*ParameterGroup, error)
	DeleteParameterGroup(name string) (*ParameterGroup, error)
	UpdateParameterGroup(req *updateParameterGroupRequest) (*ParameterGroup, error)

	// Tag operations
	ListTags(resourceArn string) (map[string]string, error)
	TagResource(resourceArn string, tags map[string]string) error
	UntagResource(resourceArn string, tagKeys []string) error

	// Snapshot operations
	CreateSnapshot(region, accountID string, req *createSnapshotRequest) (*Snapshot, error)
	DescribeSnapshots(name, clusterName, snapshotType, source string) ([]*Snapshot, error)
	CopySnapshot(region, accountID string, req *copySnapshotRequest) (*Snapshot, error)
	DeleteSnapshot(name string) (*Snapshot, error)

	// EngineVersion operations
	DescribeEngineVersions(req *describeEngineVersionsRequest) ([]*EngineVersion, error)

	// Event operations
	DescribeEvents(req *describeEventsRequest) ([]*Event, error)

	// MultiRegionCluster operations
	CreateMultiRegionCluster(
		region, accountID string,
		req *createMultiRegionClusterRequest,
	) (*MultiRegionCluster, error)
	DeleteMultiRegionCluster(name string) (*MultiRegionCluster, error)
	DescribeMultiRegionClusters(name string) ([]*MultiRegionCluster, error)
	UpdateMultiRegionCluster(req *updateMultiRegionClusterRequest) (*MultiRegionCluster, error)

	// MultiRegionParameterGroup operations
	DescribeMultiRegionParameterGroups(name string) ([]*MultiRegionParameterGroup, error)

	// ParameterGroup operations
	DescribeParameters(parameterGroupName string) (map[string]string, error)
	ResetParameterGroup(name string) (*ParameterGroup, error)

	// Shard operations
	FailoverShard(clusterName, shardConfiguration string) (*Cluster, error)

	// Node type update operations
	ListAllowedNodeTypeUpdates(clusterName string) ([]string, error)
	ListAllowedMultiRegionClusterUpdates(clusterName string) ([]string, error)

	// BatchUpdateCluster operation
	BatchUpdateCluster(clusterNames []string) map[string]*Cluster

	// ReservedNode operations
	DescribeReservedNodes(req *describeReservedNodesRequest) ([]*ReservedNode, error)
	DescribeReservedNodesOfferings(req *describeReservedNodesOfferingsRequest) ([]*ReservedNodesOffering, error)
	PurchaseReservedNodesOffering(
		region, accountID string,
		req *purchaseReservedNodesOfferingRequest,
	) (*ReservedNode, error)

	// MultiRegionParameters operations
	DescribeMultiRegionParameters(parameterGroupName string) (map[string]string, error)

	// ServiceUpdates operations
	DescribeServiceUpdates(req *describeServiceUpdatesRequest) ([]*ServiceUpdate, error)

	// Lifecycle
	Reset()
	Snapshot() []byte
	Restore(data []byte) error
}

// InMemoryBackend is the in-memory implementation of StorageBackend.
type InMemoryBackend struct {
	multiRegionClusters        map[string]*MultiRegionCluster
	acls                       map[string]*ACL
	subnetGroups               map[string]*SubnetGroup
	users                      map[string]*User
	parameterGroups            map[string]*ParameterGroup
	snapshots                  map[string]*Snapshot
	clusters                   map[string]*Cluster
	multiRegionParameterGroups map[string]*MultiRegionParameterGroup
	reservedNodes              map[string]*ReservedNode
	serviceUpdates             map[string]*ServiceUpdate
	arnToResource              map[string]resourceRef
	accountID                  string
	region                     string
	events                     []*Event
	mu                         sync.RWMutex
}

type resourceRef struct {
	Kind string `json:"kind"`
	Name string `json:"name"`
}

// NewInMemoryBackend creates a new MemoryDB in-memory backend.
// It pre-seeds the "open-access" ACL which is required by most clusters.
func NewInMemoryBackend() *InMemoryBackend {
	return newInMemoryBackendWithDefaults("us-east-1", "000000000000")
}

// newInMemoryBackendWithDefaults creates a backend pre-seeded with the given region and account.
func newInMemoryBackendWithDefaults(region, accountID string) *InMemoryBackend {
	b := &InMemoryBackend{
		clusters:                   make(map[string]*Cluster),
		acls:                       make(map[string]*ACL),
		subnetGroups:               make(map[string]*SubnetGroup),
		users:                      make(map[string]*User),
		parameterGroups:            make(map[string]*ParameterGroup),
		snapshots:                  make(map[string]*Snapshot),
		multiRegionClusters:        make(map[string]*MultiRegionCluster),
		multiRegionParameterGroups: make(map[string]*MultiRegionParameterGroup),
		reservedNodes:              make(map[string]*ReservedNode),
		serviceUpdates:             make(map[string]*ServiceUpdate),
		events:                     []*Event{},
		arnToResource:              make(map[string]resourceRef),
		accountID:                  accountID,
		region:                     region,
	}

	// Pre-seed the open-access ACL so Terraform resources that omit an explicit
	// ACL name can reference it without first creating it.
	openAccessARN := arn.Build("memorydb", region, accountID, "acl/"+openAccessACL)
	b.acls[openAccessACL] = &ACL{
		Name:      openAccessACL,
		ARN:       openAccessARN,
		Status:    aclStatusActive,
		UserNames: []string{},
		CreatedAt: time.Now(),
		Tags:      make(map[string]string),
	}
	b.arnToResource[openAccessARN] = resourceRef{Kind: resourceKindACL, Name: openAccessACL}

	// Seed service update fixtures.
	b.serviceUpdates["memorydb-20240601-redis-security"] = &ServiceUpdate{
		ServiceUpdateName:   "memorydb-20240601-redis-security",
		ReleaseDate:         "2024-06-01",
		Description:         "Security update for Redis 7.x clusters",
		Status:              "available",
		Type:                "security-update",
		AutoUpdateStartDate: "2024-07-01",
	}
	b.serviceUpdates["memorydb-20240801-engine-update"] = &ServiceUpdate{
		ServiceUpdateName:   "memorydb-20240801-engine-update",
		ReleaseDate:         "2024-08-01",
		Description:         "Engine update with performance improvements",
		Status:              "available",
		Type:                "engine-update",
		AutoUpdateStartDate: "2024-09-01",
	}

	return b
}

// Reset clears all state and re-seeds defaults, returning the backend to a clean state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.clusters = make(map[string]*Cluster)
	b.acls = make(map[string]*ACL)
	b.subnetGroups = make(map[string]*SubnetGroup)
	b.users = make(map[string]*User)
	b.parameterGroups = make(map[string]*ParameterGroup)
	b.snapshots = make(map[string]*Snapshot)
	b.multiRegionClusters = make(map[string]*MultiRegionCluster)
	b.multiRegionParameterGroups = make(map[string]*MultiRegionParameterGroup)
	b.reservedNodes = make(map[string]*ReservedNode)
	b.serviceUpdates = make(map[string]*ServiceUpdate)
	b.events = []*Event{}
	b.arnToResource = make(map[string]resourceRef)

	// Re-seed open-access ACL.
	openAccessARN := arn.Build("memorydb", b.region, b.accountID, "acl/"+openAccessACL)
	b.acls[openAccessACL] = &ACL{
		Name:      openAccessACL,
		ARN:       openAccessARN,
		Status:    aclStatusActive,
		UserNames: []string{},
		CreatedAt: time.Now(),
		Tags:      make(map[string]string),
	}
	b.arnToResource[openAccessARN] = resourceRef{Kind: resourceKindACL, Name: openAccessACL}

	// Re-seed service update fixtures.
	b.serviceUpdates["memorydb-20240601-redis-security"] = &ServiceUpdate{
		ServiceUpdateName:   "memorydb-20240601-redis-security",
		ReleaseDate:         "2024-06-01",
		Description:         "Security update for Redis 7.x clusters",
		Status:              "available",
		Type:                "security-update",
		AutoUpdateStartDate: "2024-07-01",
	}
	b.serviceUpdates["memorydb-20240801-engine-update"] = &ServiceUpdate{
		ServiceUpdateName:   "memorydb-20240801-engine-update",
		ReleaseDate:         "2024-08-01",
		Description:         "Engine update with performance improvements",
		Status:              "available",
		Type:                "engine-update",
		AutoUpdateStartDate: "2024-09-01",
	}
}

// -- Cluster operations ----------------------------------------------------------

// clusterDefaults holds resolved default values for a new cluster.
type clusterDefaults struct {
	engine        string
	engineVersion string
	nodeType      string
	port          int32
	numShards     int32
	numReplicas   int32
	tlsEnabled    bool
}

// isSupportedEngineVersion reports whether v is a supported MemoryDB engine version.
func isSupportedEngineVersion(v string) bool {
	switch v {
	case "6.2", "7.0", "7.1", "7.2", "8.0":
		return true
	default:
		return false
	}
}

// validateCreateClusterRefs checks that ACL, subnet group, and parameter group referenced in
// req all exist in the backend (caller must hold b.mu).
func (b *InMemoryBackend) validateCreateClusterRefs(req *createClusterRequest) (string, error) {
	aclName := req.ACLName
	if aclName == "" {
		aclName = openAccessACL
	}

	if _, ok := b.acls[aclName]; !ok {
		return "", fmt.Errorf("ACL %q not found: %w", aclName, ErrACLNotFound)
	}

	if req.SubnetGroupName != "" {
		if _, ok := b.subnetGroups[req.SubnetGroupName]; !ok {
			return "", fmt.Errorf("subnet group %q not found: %w", req.SubnetGroupName, ErrSubnetGroupNotFound)
		}
	}

	if req.ParameterGroupName != "" {
		if _, ok := b.parameterGroups[req.ParameterGroupName]; !ok {
			return "", fmt.Errorf("parameter group %q not found: %w", req.ParameterGroupName, ErrParameterGroupNotFound)
		}
	}

	return aclName, nil
}

// resolveClusterDefaults fills in default values for optional cluster fields.
func resolveClusterDefaults(req *createClusterRequest) (clusterDefaults, error) {
	d := clusterDefaults{
		engine:        req.Engine,
		engineVersion: req.EngineVersion,
		nodeType:      req.NodeType,
		port:          defaultPort,
		numShards:     1,
		numReplicas:   1,
		tlsEnabled:    true,
	}

	if d.engine == "" {
		d.engine = engineRedis
	}

	if d.engine != engineRedis && d.engine != engineValkey {
		return d, fmt.Errorf("engine %q is not supported (must be redis or valkey): %w", d.engine, ErrValidation)
	}

	if d.engineVersion == "" {
		if d.engine == engineValkey {
			d.engineVersion = defaultValkeyEngineVersion
		} else {
			d.engineVersion = defaultEngineVersion
		}
	}

	if !isSupportedEngineVersion(d.engineVersion) {
		return d, fmt.Errorf("engine version %q is not supported: %w", d.engineVersion, ErrValidation)
	}

	if d.nodeType == "" {
		d.nodeType = defaultNodeType
	}

	if !strings.HasPrefix(d.nodeType, "db.") {
		return d, fmt.Errorf("node type %q is invalid: must begin with 'db.': %w", d.nodeType, ErrValidation)
	}

	if req.Port != nil {
		d.port = *req.Port
	}

	if req.NumShards != nil {
		d.numShards = *req.NumShards
	}

	if req.NumReplicasPerShard != nil {
		d.numReplicas = *req.NumReplicasPerShard
	}

	if req.TLSEnabled != nil {
		d.tlsEnabled = *req.TLSEnabled
	}

	return d, nil
}

// applySnapshotRestoreConfig overrides cluster defaults from a source snapshot config.
func applySnapshotRestoreConfig(d *clusterDefaults, snap *Snapshot) {
	if snap.ClusterConfiguration.EngineVersion != "" {
		d.engineVersion = snap.ClusterConfiguration.EngineVersion
	}

	if snap.ClusterConfiguration.NodeType != "" {
		d.nodeType = snap.ClusterConfiguration.NodeType
	}

	if snap.ClusterConfiguration.NumShards > 0 {
		d.numShards = snap.ClusterConfiguration.NumShards
	}

	if snap.ClusterConfiguration.Port > 0 {
		d.port = snap.ClusterConfiguration.Port
	}
}

// seedAutomatedSnapshotLocked creates an automated snapshot for a new cluster if retention is configured.
// Caller must hold b.mu.
func (b *InMemoryBackend) seedAutomatedSnapshotLocked(region, accountID string, c *Cluster) {
	autoName := "automatic." + c.Name + "-" + time.Now().UTC().Format("20060102150405")
	autoARN := arn.Build("memorydb", region, accountID, "snapshot/"+autoName)
	autoSnap := &Snapshot{
		Name:         autoName,
		ARN:          autoARN,
		ClusterName:  c.Name,
		Status:       snapshotStatusAvailable,
		SnapshotType: snapshotSourceAutomated,
		Source:       snapshotSourceAutomated,
		Tags:         make(map[string]string),
		CreatedAt:    time.Now(),
		ClusterConfiguration: snapshotClusterConfig{
			Name:          c.Name,
			NodeType:      c.NodeType,
			EngineVersion: c.EngineVersion,
			Description:   c.Description,
			Port:          c.Port,
			NumShards:     c.NumShards,
		},
	}
	b.snapshots[autoName] = autoSnap
	b.arnToResource[autoARN] = resourceRef{Kind: resourceKindSnapshot, Name: autoName}
}

// CreateCluster creates a new MemoryDB cluster.
func (b *InMemoryBackend) CreateCluster(region, accountID string, req *createClusterRequest) (*Cluster, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if err := validateResourceName(req.ClusterName, "cluster"); err != nil {
		return nil, err
	}

	if _, exists := b.clusters[req.ClusterName]; exists {
		return nil, ErrClusterAlreadyExists
	}

	aclName, err := b.validateCreateClusterRefs(req)
	if err != nil {
		return nil, err
	}

	// If restoring from snapshot, look it up and use its config.
	var restoreSnap *Snapshot
	if req.SnapshotName != "" {
		s, ok := b.snapshots[req.SnapshotName]
		if !ok {
			return nil, fmt.Errorf("snapshot %q not found: %w", req.SnapshotName, ErrSnapshotNotFound)
		}
		restoreSnap = s
	}

	d, err := resolveClusterDefaults(req)
	if err != nil {
		return nil, err
	}

	if restoreSnap != nil {
		applySnapshotRestoreConfig(&d, restoreSnap)
	}

	clusterARN := arn.Build("memorydb", region, accountID, "cluster/"+req.ClusterName)

	c := &Cluster{
		Name:                    req.ClusterName,
		ARN:                     clusterARN,
		Description:             req.Description,
		NodeType:                d.nodeType,
		EngineVersion:           d.engineVersion,
		Engine:                  d.engine,
		ACLName:                 aclName,
		SubnetGroupName:         req.SubnetGroupName,
		ParameterGroupName:      req.ParameterGroupName,
		KmsKeyID:                req.KmsKeyID,
		SnsTopicArn:             req.SnsTopicArn,
		MaintenanceWindow:       req.MaintenanceWindow,
		SnapshotWindow:          req.SnapshotWindow,
		NumShards:               d.numShards,
		NumReplicasPerShard:     d.numReplicas,
		Port:                    d.port,
		TLSEnabled:              d.tlsEnabled,
		Status:                  clusterStatusAvailable,
		Tags:                    tagsFromSlice(req.Tags),
		CreatedAt:               time.Now(),
		Region:                  region,
		SecurityGroupIDs:        req.SecurityGroupIDs,
		AutoMinorVersionUpgrade: req.AutoMinorVersionUpgrade == nil || *req.AutoMinorVersionUpgrade,
	}

	if req.DataTiering != nil && *req.DataTiering {
		c.DataTiering = "true"
	} else {
		c.DataTiering = "false"
	}

	c.NetworkType = req.NetworkType
	if c.NetworkType == "" {
		c.NetworkType = "ipv4"
	}
	c.IpDiscovery = req.IpDiscovery
	if c.IpDiscovery == "" {
		c.IpDiscovery = "ipv4"
	}

	if req.SnapshotRetentionLimit != nil {
		c.SnapshotRetentionLimit = *req.SnapshotRetentionLimit
	}

	availabilityMode := "SingleAZ"
	if d.numReplicas > 0 {
		availabilityMode = "MultiAZ"
	}

	c.AvailabilityMode = availabilityMode
	c.Endpoint = req.ClusterName + ".memorydb." + region + ".amazonaws.com"

	b.clusters[req.ClusterName] = c
	b.arnToResource[clusterARN] = resourceRef{Kind: resourceKindCluster, Name: req.ClusterName}

	// Emit cluster created event.
	b.appendEventLocked(&Event{
		Date:       time.Now(),
		SourceName: req.ClusterName,
		SourceType: resourceKindCluster,
		Message:    "Cluster " + req.ClusterName + " created",
	})

	// Seed automated snapshot when retention limit > 0.
	if c.SnapshotRetentionLimit > 0 {
		b.seedAutomatedSnapshotLocked(region, accountID, c)
	}

	return cloneCluster(c), nil
}

// DescribeClusters returns clusters, optionally filtered by name.
func (b *InMemoryBackend) DescribeClusters(name string) ([]*Cluster, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if name != "" {
		c, ok := b.clusters[name]
		if !ok {
			return nil, ErrClusterNotFound
		}

		return []*Cluster{cloneCluster(c)}, nil
	}

	result := make([]*Cluster, 0, len(b.clusters))

	for _, c := range b.clusters {
		result = append(result, cloneCluster(c))
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return result, nil
}

// DeleteCluster removes a cluster, optionally taking a final snapshot first.
func (b *InMemoryBackend) DeleteCluster(name string) (*Cluster, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	c, ok := b.clusters[name]
	if !ok {
		return nil, ErrClusterNotFound
	}

	delete(b.clusters, name)
	delete(b.arnToResource, c.ARN)

	b.appendEventLocked(&Event{
		Date:       time.Now(),
		SourceName: name,
		SourceType: resourceKindCluster,
		Message:    "Cluster " + name + " deleted",
	})

	return cloneCluster(c), nil
}

// DeleteClusterWithSnapshot removes a cluster, first creating a snapshot with the given name.
func (b *InMemoryBackend) DeleteClusterWithSnapshot(
	region, accountID, clusterName, snapshotName string,
) (*Cluster, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	c, ok := b.clusters[clusterName]
	if !ok {
		return nil, ErrClusterNotFound
	}

	if snapshotName != "" {
		snapshotARN := arn.Build("memorydb", region, accountID, "snapshot/"+snapshotName)
		s := &Snapshot{
			Name:         snapshotName,
			ARN:          snapshotARN,
			ClusterName:  clusterName,
			Status:       snapshotStatusAvailable,
			SnapshotType: snapshotSourceManual,
			Tags:         make(map[string]string),
			CreatedAt:    time.Now(),
			ClusterConfiguration: snapshotClusterConfig{
				Name:          c.Name,
				NodeType:      c.NodeType,
				EngineVersion: c.EngineVersion,
				Description:   c.Description,
				Port:          c.Port,
				NumShards:     c.NumShards,
			},
		}
		b.snapshots[snapshotName] = s
		b.arnToResource[snapshotARN] = resourceRef{Kind: resourceKindSnapshot, Name: snapshotName}
	}

	delete(b.clusters, clusterName)
	delete(b.arnToResource, c.ARN)

	return cloneCluster(c), nil
}

// applyClusterStringUpdates applies non-nil string field updates from req to c.
func applyClusterStringUpdates(c *Cluster, req *updateClusterRequest) {
	if req.Description != "" {
		c.Description = req.Description
	}

	if req.ACLName != "" {
		c.ACLName = req.ACLName
	}

	if req.NodeType != "" {
		c.NodeType = req.NodeType
	}

	if req.EngineVersion != "" {
		c.EngineVersion = req.EngineVersion
	}

	if req.MaintenanceWindow != "" {
		c.MaintenanceWindow = req.MaintenanceWindow
	}

	if req.SnapshotWindow != "" {
		c.SnapshotWindow = req.SnapshotWindow
	}

	if req.SnsTopicArn != "" {
		c.SnsTopicArn = req.SnsTopicArn
	}

	if req.NetworkType != "" {
		c.NetworkType = req.NetworkType
	}

	if req.IpDiscovery != "" {
		c.IpDiscovery = req.IpDiscovery
	}
}

// UpdateCluster modifies an existing cluster.
func (b *InMemoryBackend) UpdateCluster(req *updateClusterRequest) (*Cluster, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	c, ok := b.clusters[req.ClusterName]
	if !ok {
		return nil, ErrClusterNotFound
	}

	applyClusterStringUpdates(c, req)

	if req.SnapshotRetentionLimit != nil {
		c.SnapshotRetentionLimit = *req.SnapshotRetentionLimit
	}

	if req.ReplicaConfiguration != nil && req.ReplicaConfiguration.ReplicaCount != nil {
		c.NumReplicasPerShard = *req.ReplicaConfiguration.ReplicaCount
	}

	if req.ShardConfiguration != nil && req.ShardConfiguration.ShardCount != nil {
		c.NumShards = *req.ShardConfiguration.ShardCount
	}

	if req.AutoMinorVersionUpgrade != nil {
		c.AutoMinorVersionUpgrade = *req.AutoMinorVersionUpgrade
	}

	b.appendEventLocked(&Event{
		Date:       time.Now(),
		SourceName: req.ClusterName,
		SourceType: resourceKindCluster,
		Message:    "Cluster " + req.ClusterName + " modified",
	})

	return cloneCluster(c), nil
}

// -- ACL operations --------------------------------------------------------------

// CreateACL creates a new ACL.
func (b *InMemoryBackend) CreateACL(region, accountID string, req *createACLRequest) (*ACL, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if err := validateResourceName(req.ACLName, "ACL"); err != nil {
		return nil, err
	}

	if _, exists := b.acls[req.ACLName]; exists {
		return nil, ErrACLAlreadyExists
	}

	aclARN := arn.Build("memorydb", region, accountID, "acl/"+req.ACLName)

	userNames := req.UserNames
	if userNames == nil {
		userNames = []string{}
	}

	a := &ACL{
		Name:      req.ACLName,
		ARN:       aclARN,
		Status:    aclStatusActive,
		UserNames: userNames,
		Tags:      tagsFromSlice(req.Tags),
		CreatedAt: time.Now(),
	}

	b.acls[req.ACLName] = a
	b.arnToResource[aclARN] = resourceRef{Kind: resourceKindACL, Name: req.ACLName}

	b.appendEventLocked(&Event{
		Date:       time.Now(),
		SourceName: req.ACLName,
		SourceType: resourceKindACL,
		Message:    "ACL " + req.ACLName + " created",
	})

	return cloneACL(a), nil
}

// DescribeACLs returns ACLs, optionally filtered by name.
func (b *InMemoryBackend) DescribeACLs(name string) ([]*ACL, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if name != "" {
		a, ok := b.acls[name]
		if !ok {
			return nil, ErrACLNotFound
		}

		return []*ACL{cloneACL(a)}, nil
	}

	result := make([]*ACL, 0, len(b.acls))

	for _, a := range b.acls {
		result = append(result, cloneACL(a))
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return result, nil
}

// DeleteACL removes an ACL.
func (b *InMemoryBackend) DeleteACL(name string) (*ACL, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	a, ok := b.acls[name]
	if !ok {
		return nil, ErrACLNotFound
	}

	if name == openAccessACL {
		return nil, fmt.Errorf("cannot delete system ACL %q: %w", name, ErrValidation)
	}

	for _, c := range b.clusters {
		if c.ACLName == name {
			return nil, fmt.Errorf("ACL %q is associated with cluster %q: %w", name, c.Name, ErrACLInUse)
		}
	}

	delete(b.acls, name)
	delete(b.arnToResource, a.ARN)

	b.appendEventLocked(&Event{
		Date:       time.Now(),
		SourceName: name,
		SourceType: resourceKindACL,
		Message:    "ACL " + name + " deleted",
	})

	return a, nil
}

// UpdateACL modifies an existing ACL.
func (b *InMemoryBackend) UpdateACL(req *updateACLRequest) (*ACL, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	a, ok := b.acls[req.ACLName]
	if !ok {
		return nil, ErrACLNotFound
	}

	// Add users (dedup).
	existing := make(map[string]bool, len(a.UserNames))

	for _, u := range a.UserNames {
		existing[u] = true
	}

	for _, u := range req.UserNamesToAdd {
		if _, exists := b.users[u]; !exists {
			return nil, fmt.Errorf("user %q not found: %w", u, ErrUserNotFound)
		}
	}

	for _, u := range req.UserNamesToAdd {
		if !existing[u] {
			a.UserNames = append(a.UserNames, u)
			existing[u] = true
		}
	}

	// Remove users — allocate a fresh slice to avoid backing-array aliasing.
	if len(req.UserNamesToRemove) > 0 {
		toRemove := make(map[string]bool, len(req.UserNamesToRemove))

		for _, u := range req.UserNamesToRemove {
			toRemove[u] = true
		}

		filtered := make([]string, 0, len(a.UserNames))

		for _, u := range a.UserNames {
			if !toRemove[u] {
				filtered = append(filtered, u)
			}
		}

		a.UserNames = filtered
	}

	b.appendEventLocked(&Event{
		Date:       time.Now(),
		SourceName: req.ACLName,
		SourceType: resourceKindACL,
		Message:    "ACL " + req.ACLName + " modified",
	})

	return cloneACL(a), nil
}

// -- SubnetGroup operations -------------------------------------------------------

// CreateSubnetGroup creates a new subnet group.
func (b *InMemoryBackend) CreateSubnetGroup(
	region, accountID string,
	req *createSubnetGroupRequest,
) (*SubnetGroup, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if err := validateResourceName(req.SubnetGroupName, "subnet group"); err != nil {
		return nil, err
	}

	if _, exists := b.subnetGroups[req.SubnetGroupName]; exists {
		return nil, ErrSubnetGroupAlreadyExists
	}

	sgARN := arn.Build("memorydb", region, accountID, "subnetgroup/"+req.SubnetGroupName)

	sg := &SubnetGroup{
		Name:        req.SubnetGroupName,
		ARN:         sgARN,
		Description: req.Description,
		SubnetIDs:   req.SubnetIDs,
		Tags:        tagsFromSlice(req.Tags),
		CreatedAt:   time.Now(),
	}

	b.subnetGroups[req.SubnetGroupName] = sg
	b.arnToResource[sgARN] = resourceRef{Kind: resourceKindSubnetGroup, Name: req.SubnetGroupName}

	return cloneSubnetGroup(sg), nil
}

// DescribeSubnetGroups returns subnet groups, optionally filtered by name.
func (b *InMemoryBackend) DescribeSubnetGroups(name string) ([]*SubnetGroup, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if name != "" {
		sg, ok := b.subnetGroups[name]
		if !ok {
			return nil, ErrSubnetGroupNotFound
		}

		return []*SubnetGroup{cloneSubnetGroup(sg)}, nil
	}

	result := make([]*SubnetGroup, 0, len(b.subnetGroups))

	for _, sg := range b.subnetGroups {
		result = append(result, cloneSubnetGroup(sg))
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return result, nil
}

// DeleteSubnetGroup removes a subnet group.
func (b *InMemoryBackend) DeleteSubnetGroup(name string) (*SubnetGroup, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	sg, ok := b.subnetGroups[name]
	if !ok {
		return nil, ErrSubnetGroupNotFound
	}

	delete(b.subnetGroups, name)
	delete(b.arnToResource, sg.ARN)

	return sg, nil
}

// UpdateSubnetGroup modifies an existing subnet group.
func (b *InMemoryBackend) UpdateSubnetGroup(req *updateSubnetGroupRequest) (*SubnetGroup, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	sg, ok := b.subnetGroups[req.SubnetGroupName]
	if !ok {
		return nil, ErrSubnetGroupNotFound
	}

	if req.Description != "" {
		sg.Description = req.Description
	}

	if len(req.SubnetIDs) > 0 {
		sg.SubnetIDs = req.SubnetIDs
	}

	return cloneSubnetGroup(sg), nil
}

// -- User operations -------------------------------------------------------------

// CreateUser creates a new MemoryDB user.
func (b *InMemoryBackend) CreateUser(region, accountID string, req *createUserRequest) (*User, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if err := validateResourceName(req.UserName, "user"); err != nil {
		return nil, err
	}

	if _, exists := b.users[req.UserName]; exists {
		return nil, ErrUserAlreadyExists
	}

	authType := strings.ToLower(req.AuthenticationMode.Type)
	if authType == "" {
		authType = authTypeNoPasswordRequired
	}
	// Normalize legacy alias.
	if authType == authTypeNoPassword {
		authType = authTypeNoPasswordRequired
	}
	if authType != authTypePassword && authType != authTypeIAM && authType != authTypeNoPasswordRequired {
		return nil, fmt.Errorf(
			"AuthenticationMode.Type must be password, iam, or no-password-required: %w",
			ErrValidation,
		)
	}
	if authType == authTypeIAM && len(req.AuthenticationMode.Passwords) > 0 {
		return nil, fmt.Errorf("passwords cannot be set when AuthenticationMode.Type is iam: %w", ErrValidation)
	}

	userARN := arn.Build("memorydb", region, accountID, "user/"+req.UserName)

	u := &User{
		Name:         req.UserName,
		ARN:          userARN,
		AccessString: req.AccessString,
		Status:       userStatusActive,
		AuthType:     authType,
		Passwords:    req.AuthenticationMode.Passwords,
		Tags:         tagsFromSlice(req.Tags),
		CreatedAt:    time.Now(),
	}

	b.users[req.UserName] = u
	b.arnToResource[userARN] = resourceRef{Kind: resourceKindUser, Name: req.UserName}

	b.appendEventLocked(&Event{
		Date:       time.Now(),
		SourceName: req.UserName,
		SourceType: "user",
		Message:    "User " + req.UserName + " created",
	})

	return cloneUser(u), nil
}

// DescribeUsers returns users, optionally filtered by name.
func (b *InMemoryBackend) DescribeUsers(name string) ([]*User, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if name != "" {
		u, ok := b.users[name]
		if !ok {
			return nil, ErrUserNotFound
		}

		return []*User{cloneUser(u)}, nil
	}

	result := make([]*User, 0, len(b.users))

	for _, u := range b.users {
		result = append(result, cloneUser(u))
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return result, nil
}

// DeleteUser removes a user.
func (b *InMemoryBackend) DeleteUser(name string) (*User, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	u, ok := b.users[name]
	if !ok {
		return nil, ErrUserNotFound
	}

	for _, a := range b.acls {
		if slices.Contains(a.UserNames, name) {
			return nil, fmt.Errorf("user %q is a member of ACL %q: %w", name, a.Name, ErrUserInUse)
		}
	}

	delete(b.users, name)
	delete(b.arnToResource, u.ARN)

	return u, nil
}

// UpdateUser modifies an existing user.
func (b *InMemoryBackend) UpdateUser(req *updateUserRequest) (*User, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	u, ok := b.users[req.UserName]
	if !ok {
		return nil, ErrUserNotFound
	}

	if req.AccessString != "" {
		u.AccessString = req.AccessString
	}

	if req.AuthenticationMode != nil {
		if req.AuthenticationMode.Type != "" {
			u.AuthType = req.AuthenticationMode.Type
		}

		if len(req.AuthenticationMode.Passwords) > 0 {
			u.Passwords = req.AuthenticationMode.Passwords
		}
	}

	return cloneUser(u), nil
}

// -- ParameterGroup operations ---------------------------------------------------

// CreateParameterGroup creates a new parameter group.
func (b *InMemoryBackend) CreateParameterGroup(
	region, accountID string,
	req *createParameterGroupRequest,
) (*ParameterGroup, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if req.Family == "" {
		return nil, fmt.Errorf("family is required: %w", ErrValidation)
	}

	if err := validateResourceName(req.ParameterGroupName, "parameter group"); err != nil {
		return nil, err
	}

	if _, exists := b.parameterGroups[req.ParameterGroupName]; exists {
		return nil, ErrParameterGroupAlreadyExists
	}

	pgARN := arn.Build("memorydb", region, accountID, "parametergroup/"+req.ParameterGroupName)

	pg := &ParameterGroup{
		Name:        req.ParameterGroupName,
		ARN:         pgARN,
		Description: req.Description,
		Family:      req.Family,
		Parameters:  make(map[string]string),
		Tags:        tagsFromSlice(req.Tags),
		CreatedAt:   time.Now(),
	}

	b.parameterGroups[req.ParameterGroupName] = pg
	b.arnToResource[pgARN] = resourceRef{Kind: resourceKindParameterGroup, Name: req.ParameterGroupName}

	return pg, nil
}

// DescribeParameterGroups returns parameter groups, optionally filtered by name.
func (b *InMemoryBackend) DescribeParameterGroups(name string) ([]*ParameterGroup, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if name != "" {
		pg, ok := b.parameterGroups[name]
		if !ok {
			return nil, ErrParameterGroupNotFound
		}

		return []*ParameterGroup{cloneParameterGroup(pg)}, nil
	}

	result := make([]*ParameterGroup, 0, len(b.parameterGroups))

	for _, pg := range b.parameterGroups {
		result = append(result, cloneParameterGroup(pg))
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return result, nil
}

// DeleteParameterGroup removes a parameter group.
func (b *InMemoryBackend) DeleteParameterGroup(name string) (*ParameterGroup, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	pg, ok := b.parameterGroups[name]
	if !ok {
		return nil, ErrParameterGroupNotFound
	}

	delete(b.parameterGroups, name)
	delete(b.arnToResource, pg.ARN)

	return pg, nil
}

// UpdateParameterGroup modifies parameter values in a parameter group.
func (b *InMemoryBackend) UpdateParameterGroup(req *updateParameterGroupRequest) (*ParameterGroup, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	pg, ok := b.parameterGroups[req.ParameterGroupName]
	if !ok {
		return nil, ErrParameterGroupNotFound
	}

	for _, pnv := range req.ParameterNameValues {
		pg.Parameters[pnv.ParameterName] = pnv.ParameterValue
	}

	return cloneParameterGroup(pg), nil
}

// -- Tag operations --------------------------------------------------------------

// ListTags returns the tags for a resource identified by ARN.
func (b *InMemoryBackend) ListTags(resourceArn string) (map[string]string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	ref, ok := b.arnToResource[resourceArn]
	if !ok {
		return nil, awserr.New("ResourceNotFoundFault: resource not found", awserr.ErrNotFound)
	}

	tags := b.tagsForRef(ref)

	return tags, nil
}

// TagResource adds or updates tags on a resource.
func (b *InMemoryBackend) TagResource(resourceArn string, tags map[string]string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	ref, ok := b.arnToResource[resourceArn]
	if !ok {
		return awserr.New("ResourceNotFoundFault: resource not found", awserr.ErrNotFound)
	}

	const maxTagsPerResource = 50

	existingTags := b.tagsForRef(ref)
	newTotal := len(existingTags)

	for k := range tags {
		if _, alreadyExists := existingTags[k]; !alreadyExists {
			newTotal++
		}
	}

	if newTotal > maxTagsPerResource {
		return fmt.Errorf(
			"tag limit exceeded: a resource may have at most %d tags: %w",
			maxTagsPerResource,
			ErrValidation,
		)
	}

	b.applyTags(ref, tags)

	return nil
}

// UntagResource removes tags from a resource.
func (b *InMemoryBackend) UntagResource(resourceArn string, tagKeys []string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	ref, ok := b.arnToResource[resourceArn]
	if !ok {
		return awserr.New("ResourceNotFoundFault: resource not found", awserr.ErrNotFound)
	}

	b.removeTags(ref, tagKeys)

	return nil
}

// tagsForRef returns a copy of the tags for the referenced resource (must hold at least RLock).
func (b *InMemoryBackend) tagsForRef(ref resourceRef) map[string]string {
	var src map[string]string

	switch ref.Kind {
	case resourceKindCluster:
		if c, ok := b.clusters[ref.Name]; ok {
			src = c.Tags
		}
	case resourceKindACL:
		if a, ok := b.acls[ref.Name]; ok {
			src = a.Tags
		}
	case resourceKindSubnetGroup:
		if sg, ok := b.subnetGroups[ref.Name]; ok {
			src = sg.Tags
		}
	case resourceKindUser:
		if u, ok := b.users[ref.Name]; ok {
			src = u.Tags
		}
	case resourceKindParameterGroup:
		if pg, ok := b.parameterGroups[ref.Name]; ok {
			src = pg.Tags
		}
	case resourceKindSnapshot:
		if s, ok := b.snapshots[ref.Name]; ok {
			src = s.Tags
		}
	}

	return maps.Clone(src)
}

// applyTags merges tags into the referenced resource (must hold Lock).
// mergeTags ensures dst is initialized then copies all src entries into it.
func mergeTags(dst *map[string]string, src map[string]string) {
	if *dst == nil {
		*dst = make(map[string]string, len(src))
	}

	maps.Copy(*dst, src)
}

func (b *InMemoryBackend) applyTags(ref resourceRef, tags map[string]string) {
	switch ref.Kind {
	case resourceKindCluster:
		if c, ok := b.clusters[ref.Name]; ok {
			mergeTags(&c.Tags, tags)
		}
	case resourceKindACL:
		if a, ok := b.acls[ref.Name]; ok {
			mergeTags(&a.Tags, tags)
		}
	case resourceKindSubnetGroup:
		if sg, ok := b.subnetGroups[ref.Name]; ok {
			mergeTags(&sg.Tags, tags)
		}
	case resourceKindUser:
		if u, ok := b.users[ref.Name]; ok {
			mergeTags(&u.Tags, tags)
		}
	case resourceKindParameterGroup:
		if pg, ok := b.parameterGroups[ref.Name]; ok {
			mergeTags(&pg.Tags, tags)
		}
	case resourceKindSnapshot:
		if s, ok := b.snapshots[ref.Name]; ok {
			mergeTags(&s.Tags, tags)
		}
	}
}

// removeTags deletes the given tag keys from the referenced resource (must hold Lock).
func (b *InMemoryBackend) removeTags(ref resourceRef, tagKeys []string) {
	m := b.tagsMapForRef(ref)
	if m == nil {
		return
	}

	for _, k := range tagKeys {
		delete(m, k)
	}
}

// tagsMapForRef returns a direct (mutable) reference to the tag map for a resource (must hold Lock).
func (b *InMemoryBackend) tagsMapForRef(ref resourceRef) map[string]string {
	switch ref.Kind {
	case resourceKindCluster:
		if c, ok := b.clusters[ref.Name]; ok {
			return c.Tags
		}
	case resourceKindACL:
		if a, ok := b.acls[ref.Name]; ok {
			return a.Tags
		}
	case resourceKindSubnetGroup:
		if sg, ok := b.subnetGroups[ref.Name]; ok {
			return sg.Tags
		}
	case resourceKindUser:
		if u, ok := b.users[ref.Name]; ok {
			return u.Tags
		}
	case resourceKindParameterGroup:
		if pg, ok := b.parameterGroups[ref.Name]; ok {
			return pg.Tags
		}
	case resourceKindSnapshot:
		if s, ok := b.snapshots[ref.Name]; ok {
			return s.Tags
		}
	}

	return nil
}

// -- Snapshot operations --------------------------------------------------------

// CreateSnapshot creates a snapshot of a cluster.
func (b *InMemoryBackend) CreateSnapshot(region, accountID string, req *createSnapshotRequest) (*Snapshot, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Validate the source cluster exists.
	c, ok := b.clusters[req.ClusterName]
	if !ok {
		return nil, ErrClusterNotFound
	}

	if _, exists := b.snapshots[req.SnapshotName]; exists {
		return nil, ErrSnapshotAlreadyExists
	}

	snapshotARN := arn.Build("memorydb", region, accountID, "snapshot/"+req.SnapshotName)

	s := &Snapshot{
		Name:         req.SnapshotName,
		ARN:          snapshotARN,
		ClusterName:  req.ClusterName,
		Status:       snapshotStatusAvailable,
		KmsKeyID:     req.KmsKeyID,
		SnapshotType: snapshotSourceManual,
		Source:       snapshotSourceManual,
		Tags:         tagsFromSlice(req.Tags),
		CreatedAt:    time.Now(),
		ClusterConfiguration: snapshotClusterConfig{
			Name:          c.Name,
			NodeType:      c.NodeType,
			EngineVersion: c.EngineVersion,
			Description:   c.Description,
			Port:          c.Port,
			NumShards:     c.NumShards,
		},
	}

	b.snapshots[req.SnapshotName] = s
	b.arnToResource[snapshotARN] = resourceRef{Kind: resourceKindSnapshot, Name: req.SnapshotName}

	b.appendEventLocked(&Event{
		Date:       time.Now(),
		SourceName: req.SnapshotName,
		SourceType: resourceKindSnapshot,
		Message:    "Snapshot " + req.SnapshotName + " created for cluster " + req.ClusterName,
	})

	return s, nil
}

// DescribeSnapshots returns snapshots, optionally filtered by name, cluster name, snapshot type, or source.
func (b *InMemoryBackend) DescribeSnapshots(name, clusterName, snapshotType, source string) ([]*Snapshot, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if name != "" {
		s, ok := b.snapshots[name]
		if !ok {
			return nil, ErrSnapshotNotFound
		}

		return []*Snapshot{cloneSnapshot(s)}, nil
	}

	result := make([]*Snapshot, 0, len(b.snapshots))

	for _, s := range b.snapshots {
		if clusterName != "" && s.ClusterName != clusterName {
			continue
		}

		if snapshotType != "" && s.SnapshotType != snapshotType {
			continue
		}

		if source != "" && s.Source != source {
			continue
		}

		result = append(result, cloneSnapshot(s))
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return result, nil
}

// CopySnapshot copies an existing snapshot to a new name.
func (b *InMemoryBackend) CopySnapshot(region, accountID string, req *copySnapshotRequest) (*Snapshot, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	src, ok := b.snapshots[req.SourceSnapshotName]
	if !ok {
		return nil, ErrSnapshotNotFound
	}

	// When TargetBucket is set, we're exporting to S3 — just return the source snapshot.
	if req.TargetBucket != "" {
		return cloneSnapshot(src), nil
	}

	if _, exists := b.snapshots[req.TargetSnapshotName]; exists {
		return nil, ErrSnapshotAlreadyExists
	}

	targetARN := arn.Build("memorydb", region, accountID, "snapshot/"+req.TargetSnapshotName)

	kmsKeyID := req.KmsKeyID
	if kmsKeyID == "" {
		kmsKeyID = src.KmsKeyID
	}

	// Inherit tags from source if none supplied.
	var tags map[string]string
	if len(req.Tags) > 0 {
		tags = tagsFromSlice(req.Tags)
	} else {
		tags = maps.Clone(src.Tags)
	}

	dst := &Snapshot{
		Name:                 req.TargetSnapshotName,
		ARN:                  targetARN,
		ClusterName:          src.ClusterName,
		Status:               snapshotStatusAvailable,
		KmsKeyID:             kmsKeyID,
		SnapshotType:         snapshotSourceManual,
		Tags:                 tags,
		CreatedAt:            time.Now(),
		ClusterConfiguration: src.ClusterConfiguration,
	}

	b.snapshots[req.TargetSnapshotName] = dst
	b.arnToResource[targetARN] = resourceRef{Kind: resourceKindSnapshot, Name: req.TargetSnapshotName}

	return dst, nil
}

// DeleteSnapshot removes a snapshot.
func (b *InMemoryBackend) DeleteSnapshot(name string) (*Snapshot, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	s, ok := b.snapshots[name]
	if !ok {
		return nil, ErrSnapshotNotFound
	}

	delete(b.snapshots, name)
	delete(b.arnToResource, s.ARN)

	b.appendEventLocked(&Event{
		Date:       time.Now(),
		SourceName: name,
		SourceType: resourceKindSnapshot,
		Message:    "Snapshot " + name + " deleted",
	})

	return s, nil
}

// -- EngineVersion operations ---------------------------------------------------

// defaultEngineVersions returns the built-in list of supported engine versions.
func defaultEngineVersions() []*EngineVersion {
	return []*EngineVersion{
		{Engine: "valkey", EngineVersion: "8.0", EnginePatchVersion: "8.0.1", ParameterGroupFamily: "memorydb_valkey8", Description: "Valkey 8.0"},
		{Engine: "valkey", EngineVersion: "7.2", EnginePatchVersion: "7.2.4", ParameterGroupFamily: "memorydb_valkey7", Description: "Valkey 7.2"},
		{Engine: "redis", EngineVersion: "7.1", EnginePatchVersion: "7.1.0", ParameterGroupFamily: "memorydb_redis7", Description: "Redis 7.1"},
		{Engine: "redis", EngineVersion: "7.0", EnginePatchVersion: "7.0.7", ParameterGroupFamily: "memorydb_redis7", Description: "Redis 7.0"},
		{Engine: "redis", EngineVersion: "6.2", EnginePatchVersion: "6.2.6", ParameterGroupFamily: "memorydb_redis6", Description: "Redis 6.2"},
	}
}

// DescribeEngineVersions returns supported engine versions, optionally filtered.
func (b *InMemoryBackend) DescribeEngineVersions(req *describeEngineVersionsRequest) ([]*EngineVersion, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	all := defaultEngineVersions()

	result := make([]*EngineVersion, 0, len(all))

	for _, ev := range all {
		if req.ParameterGroupFamily != "" && ev.ParameterGroupFamily != req.ParameterGroupFamily {
			continue
		}

		if req.Engine != "" && ev.Engine != req.Engine {
			continue
		}

		cp := *ev
		result = append(result, &cp)
	}

	if req.DefaultOnly && len(result) > 0 {
		result = result[:1]
	}

	return result, nil
}

// -- Event operations -----------------------------------------------------------

// AddEvent appends an event to the backend event log (used internally for seeding).
// Events are capped at maxEvents; oldest entries are dropped when the cap is reached.
func (b *InMemoryBackend) AddEvent(ev *Event) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.appendEventLocked(ev)
}

// appendEventLocked appends an event without acquiring the lock (caller must hold b.mu).
func (b *InMemoryBackend) appendEventLocked(ev *Event) {
	b.events = append(b.events, ev)

	if len(b.events) > maxEvents {
		b.events = b.events[len(b.events)-maxEvents:]
	}
}

// DescribeEvents returns events, optionally filtered by source name and type.
func (b *InMemoryBackend) DescribeEvents(req *describeEventsRequest) ([]*Event, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var startTime *time.Time
	if req.StartTime != nil {
		startTime = req.StartTime
	} else if req.Duration != nil {
		t := time.Now().Add(-time.Duration(*req.Duration) * time.Minute)
		startTime = &t
	}
	// if neither is set, startTime stays nil → no time filter applied

	result := make([]*Event, 0, len(b.events))

	for _, ev := range b.events {
		if req.SourceName != "" && ev.SourceName != req.SourceName {
			continue
		}

		if req.SourceType != "" && ev.SourceType != req.SourceType {
			continue
		}

		if startTime != nil && ev.Date.Before(*startTime) {
			continue
		}

		if req.EndTime != nil && ev.Date.After(*req.EndTime) {
			continue
		}

		result = append(result, cloneEvent(ev))
	}

	return result, nil
}

// cloneEvent returns a shallow copy of an Event.
func cloneEvent(e *Event) *Event {
	cp := *e
	return &cp
}

// -- MultiRegionCluster operations ----------------------------------------------

// CreateMultiRegionCluster creates a new multi-region cluster.
func (b *InMemoryBackend) CreateMultiRegionCluster(
	region, accountID string,
	req *createMultiRegionClusterRequest,
) (*MultiRegionCluster, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// AWS generates the full name by prepending "virv-" to the suffix.
	fullName := "virv-" + req.MultiRegionClusterNameSuffix

	if _, exists := b.multiRegionClusters[fullName]; exists {
		return nil, ErrMultiRegionClusterAlreadyExists
	}

	mrARN := arn.Build("memorydb", region, accountID, "multiregioncluster/"+fullName)

	engineVersion := req.EngineVersion
	if engineVersion == "" {
		engineVersion = defaultEngineVersion
	}

	engine := req.Engine
	if engine == "" {
		engine = engineRedis
	}

	mrc := &MultiRegionCluster{
		MultiRegionClusterName:        fullName,
		ARN:                           mrARN,
		Description:                   req.Description,
		NodeType:                      req.NodeType,
		Engine:                        engine,
		EngineVersion:                 engineVersion,
		MultiRegionParameterGroupName: req.MultiRegionParameterGroupName,
		Status:                        multiRegionClusterStatusAvailable,
		Tags:                          tagsFromSlice(req.Tags),
		CreatedAt:                     time.Now(),
	}

	b.multiRegionClusters[fullName] = mrc
	b.arnToResource[mrARN] = resourceRef{Kind: resourceKindMultiRegionCluster, Name: fullName}

	return mrc, nil
}

// DeleteMultiRegionCluster removes a multi-region cluster.
func (b *InMemoryBackend) DeleteMultiRegionCluster(name string) (*MultiRegionCluster, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	mrc, ok := b.multiRegionClusters[name]
	if !ok {
		return nil, ErrMultiRegionClusterNotFound
	}

	delete(b.multiRegionClusters, name)
	delete(b.arnToResource, mrc.ARN)

	return cloneMultiRegionCluster(mrc), nil
}

// DescribeMultiRegionClusters returns multi-region clusters, optionally filtered by name.
func (b *InMemoryBackend) DescribeMultiRegionClusters(name string) ([]*MultiRegionCluster, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if name != "" {
		mrc, ok := b.multiRegionClusters[name]
		if !ok {
			return nil, ErrMultiRegionClusterNotFound
		}

		return []*MultiRegionCluster{cloneMultiRegionCluster(mrc)}, nil
	}

	result := make([]*MultiRegionCluster, 0, len(b.multiRegionClusters))

	for _, mrc := range b.multiRegionClusters {
		result = append(result, cloneMultiRegionCluster(mrc))
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].MultiRegionClusterName < result[j].MultiRegionClusterName
	})

	return result, nil
}

// UpdateMultiRegionCluster modifies an existing multi-region cluster.
func (b *InMemoryBackend) UpdateMultiRegionCluster(req *updateMultiRegionClusterRequest) (*MultiRegionCluster, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	mrc, ok := b.multiRegionClusters[req.MultiRegionClusterName]
	if !ok {
		return nil, ErrMultiRegionClusterNotFound
	}

	if req.Description != "" {
		mrc.Description = req.Description
	}

	if req.NodeType != "" {
		mrc.NodeType = req.NodeType
	}

	if req.EngineVersion != "" {
		mrc.EngineVersion = req.EngineVersion
	}

	if req.MultiRegionParameterGroupName != "" {
		mrc.MultiRegionParameterGroupName = req.MultiRegionParameterGroupName
	}

	return cloneMultiRegionCluster(mrc), nil
}

// -- MultiRegionParameterGroup operations ----------------------------------------

// DescribeMultiRegionParameterGroups returns multi-region parameter groups, optionally filtered by name.
func (b *InMemoryBackend) DescribeMultiRegionParameterGroups(name string) ([]*MultiRegionParameterGroup, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if name != "" {
		mrpg, ok := b.multiRegionParameterGroups[name]
		if !ok {
			return nil, ErrMultiRegionParameterGroupNotFound
		}

		return []*MultiRegionParameterGroup{cloneMultiRegionParameterGroup(mrpg)}, nil
	}

	result := make([]*MultiRegionParameterGroup, 0, len(b.multiRegionParameterGroups))

	for _, mrpg := range b.multiRegionParameterGroups {
		result = append(result, cloneMultiRegionParameterGroup(mrpg))
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return result, nil
}

// -- ParameterGroup parameter operations -----------------------------------------

// DescribeParameters returns the parameters map for a given parameter group.
func (b *InMemoryBackend) DescribeParameters(parameterGroupName string) (map[string]string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if parameterGroupName == "" {
		return nil, fmt.Errorf("parameter group name is required: %w", ErrValidation)
	}

	pg, ok := b.parameterGroups[parameterGroupName]
	if !ok {
		return nil, ErrParameterGroupNotFound
	}

	return maps.Clone(pg.Parameters), nil
}

// ResetParameterGroup resets all parameters in a parameter group to their default (empty) values.
func (b *InMemoryBackend) ResetParameterGroup(name string) (*ParameterGroup, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	pg, ok := b.parameterGroups[name]
	if !ok {
		return nil, ErrParameterGroupNotFound
	}

	pg.Parameters = make(map[string]string)

	return cloneParameterGroup(pg), nil
}

// -- Shard operations -----------------------------------------------------------

// FailoverShard simulates a shard failover for a cluster, returning the cluster state.
func (b *InMemoryBackend) FailoverShard(clusterName, shardName string) (*Cluster, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	c, ok := b.clusters[clusterName]
	if !ok {
		return nil, ErrClusterNotFound
	}

	msg := "Failover initiated"
	if shardName != "" {
		msg = "Failover initiated for shard " + shardName
	}

	b.appendEventLocked(&Event{
		Date:       time.Now(),
		SourceName: clusterName,
		SourceType: resourceKindCluster,
		Message:    msg,
	})

	return cloneCluster(c), nil
}

// -- Node type update operations ------------------------------------------------

// allowedNodeTypes returns the set of node types available for upgrade/downgrade.
func allowedNodeTypes() []string {
	return []string{
		defaultNodeType,
		"db.r6g.xlarge",
		"db.r6g.2xlarge",
		"db.r6g.4xlarge",
		"db.r6gd.xlarge",
		"db.t4g.small",
		"db.t4g.medium",
	}
}

// ListAllowedNodeTypeUpdates returns the set of node types a cluster can be updated to.
func (b *InMemoryBackend) ListAllowedNodeTypeUpdates(clusterName string) ([]string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, ok := b.clusters[clusterName]; !ok {
		return nil, ErrClusterNotFound
	}

	return allowedNodeTypes(), nil
}

// ListAllowedMultiRegionClusterUpdates returns the set of node types a multi-region cluster can be updated to.
func (b *InMemoryBackend) ListAllowedMultiRegionClusterUpdates(clusterName string) ([]string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, ok := b.multiRegionClusters[clusterName]; !ok {
		return nil, ErrMultiRegionClusterNotFound
	}

	return allowedNodeTypes(), nil
}

// BatchUpdateCluster looks up each named cluster and returns a map of name→cluster
// for all clusters that were found. Unknown names are omitted from the result.
// The caller is responsible for deciding which names are processed vs unprocessed.
func (b *InMemoryBackend) BatchUpdateCluster(clusterNames []string) map[string]*Cluster {
	b.mu.RLock()
	defer b.mu.RUnlock()

	result := make(map[string]*Cluster, len(clusterNames))

	for _, name := range clusterNames {
		if c, ok := b.clusters[name]; ok {
			result[name] = cloneCluster(c)
		}
	}

	return result
}

// -- ReservedNode operations ----------------------------------------------------

// defaultReservedNodesOfferings returns the built-in reserved node offerings.
func defaultReservedNodesOfferings() []*ReservedNodesOffering {
	return []*ReservedNodesOffering{
		{
			ReservedNodesOfferingID: "aaa00000-1111-2222-3333-444444444444",
			NodeType:                defaultNodeType,
			Duration:                reservedDuration1Year,
			FixedPrice:              reservedFixedPriceLarge1Y,
			OfferingType:            "No Upfront",
			RecurringCharges: []recurringChargeObject{
				{RecurringChargeAmount: reservedChargeRateLarge, RecurringChargeFrequency: "Hourly"},
			},
		},
		{
			ReservedNodesOfferingID: "bbb00000-1111-2222-3333-444444444444",
			NodeType:                "db.r6g.xlarge",
			Duration:                reservedDuration1Year,
			FixedPrice:              reservedFixedPriceXLarge1Y,
			OfferingType:            "No Upfront",
			RecurringCharges: []recurringChargeObject{
				{RecurringChargeAmount: reservedChargeRateXLarge, RecurringChargeFrequency: "Hourly"},
			},
		},
		{
			ReservedNodesOfferingID: "ccc00000-1111-2222-3333-444444444444",
			NodeType:                defaultNodeType,
			Duration:                reservedDuration3Years,
			FixedPrice:              reservedFixedPriceLarge3Y,
			OfferingType:            "All Upfront",
			RecurringCharges:        []recurringChargeObject{},
		},
	}
}

// DescribeReservedNodes returns reserved nodes, optionally filtered by reservation ID or node type.
func (b *InMemoryBackend) DescribeReservedNodes(req *describeReservedNodesRequest) ([]*ReservedNode, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	result := make([]*ReservedNode, 0, len(b.reservedNodes))

	for _, rn := range b.reservedNodes {
		if req.ReservedNodeID != "" && rn.ReservedNodeID != req.ReservedNodeID {
			continue
		}

		if req.ReservationID != "" && rn.ReservationID != req.ReservationID {
			continue
		}

		if req.NodeType != "" && rn.NodeType != req.NodeType {
			continue
		}

		if req.OfferingType != "" && rn.OfferingType != req.OfferingType {
			continue
		}

		cp := *rn
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ReservedNodeID < result[j].ReservedNodeID
	})

	return result, nil
}

// DescribeReservedNodesOfferings returns available reserved node offerings.
func (b *InMemoryBackend) DescribeReservedNodesOfferings(
	req *describeReservedNodesOfferingsRequest,
) ([]*ReservedNodesOffering, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	all := defaultReservedNodesOfferings()

	result := make([]*ReservedNodesOffering, 0, len(all))

	for _, o := range all {
		if req.ReservedNodesOfferingID != "" && o.ReservedNodesOfferingID != req.ReservedNodesOfferingID {
			continue
		}

		if req.NodeType != "" && o.NodeType != req.NodeType {
			continue
		}

		if req.OfferingType != "" && o.OfferingType != req.OfferingType {
			continue
		}

		if req.Duration != "" {
			dSec := parseDurationToSeconds(req.Duration)
			if dSec > 0 && o.Duration != dSec {
				continue
			}
		}

		result = append(result, o)
	}

	return result, nil
}

// parseDurationToSeconds converts a duration string to seconds for reserved node filtering.
func parseDurationToSeconds(d string) int32 {
	switch d {
	case "1", "31536000":
		return reservedDuration1Year
	case "3", "94608000":
		return reservedDuration3Years
	default:
		return 0
	}
}

// PurchaseReservedNodesOffering creates a new reserved node from an offering.
func (b *InMemoryBackend) PurchaseReservedNodesOffering(
	region, accountID string,
	req *purchaseReservedNodesOfferingRequest,
) (*ReservedNode, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if req.ReservedNodesOfferingID == "" {
		return nil, fmt.Errorf("ReservedNodesOfferingId is required: %w", ErrValidation)
	}

	// Find the offering.
	var offering *ReservedNodesOffering

	for _, o := range defaultReservedNodesOfferings() {
		if o.ReservedNodesOfferingID == req.ReservedNodesOfferingID {
			offering = o

			break
		}
	}

	if offering == nil {
		return nil, fmt.Errorf("reserved nodes offering not found: %w", ErrValidation)
	}

	reservationID := req.ReservationID
	if reservationID == "" {
		reservationID = req.ReservedNodesOfferingID + "-reservation"
	}

	if _, exists := b.reservedNodes[reservationID]; exists {
		return nil, fmt.Errorf("reserved node %q already exists: %w", reservationID, ErrReservationAlreadyExists)
	}

	nodeCount := int32(1)
	if req.NodeCount != nil {
		nodeCount = *req.NodeCount
	}

	rnARN := arn.Build("memorydb", region, accountID, "reservednode/"+reservationID)

	rn := &ReservedNode{
		ReservedNodeID:   reservationID,
		ReservationID:    req.ReservedNodesOfferingID,
		NodeType:         offering.NodeType,
		Duration:         offering.Duration,
		FixedPrice:       offering.FixedPrice,
		UsagePrice:       offering.UsagePrice,
		OfferingType:     offering.OfferingType,
		RecurringCharges: offering.RecurringCharges,
		State:            "active",
		StartTime:        time.Now().UTC().Format(time.RFC3339),
		NodeCount:        nodeCount,
		ARN:              rnARN,
	}

	b.reservedNodes[reservationID] = rn

	cp := *rn

	return &cp, nil
}

// -- DescribeMultiRegionParameters operation ------------------------------------

// DescribeMultiRegionParameters returns the parameters for a multi-region parameter group.
func (b *InMemoryBackend) DescribeMultiRegionParameters(parameterGroupName string) (map[string]string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if parameterGroupName == "" {
		return nil, fmt.Errorf("parameter group name is required: %w", ErrValidation)
	}

	mrpg, ok := b.multiRegionParameterGroups[parameterGroupName]
	if !ok {
		return nil, ErrMultiRegionParameterGroupNotFound
	}

	return maps.Clone(mrpg.Parameters), nil
}

// DescribeServiceUpdates returns service updates, optionally filtered.
func (b *InMemoryBackend) DescribeServiceUpdates(req *describeServiceUpdatesRequest) ([]*ServiceUpdate, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	result := make([]*ServiceUpdate, 0, len(b.serviceUpdates))

	for _, su := range b.serviceUpdates {
		if req.ServiceUpdateName != "" && su.ServiceUpdateName != req.ServiceUpdateName {
			continue
		}
		if len(req.Status) > 0 {
			found := false
			for _, s := range req.Status {
				if su.Status == s {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}
		cp := *su
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ServiceUpdateName < result[j].ServiceUpdateName
	})

	return result, nil
}

// tagsFromSlice converts []tagEntry to map[string]string.
func tagsFromSlice(tags []tagEntry) map[string]string {
	result := make(map[string]string, len(tags))

	for _, t := range tags {
		result[t.Key] = t.Value
	}

	return result
}

// tagsToSlice converts map[string]string to []tagEntry sorted by key.
func tagsToSlice(tags map[string]string) []tagEntry {
	result := make([]tagEntry, 0, len(tags))

	for k, v := range tags {
		result = append(result, tagEntry{Key: k, Value: v})
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Key < result[j].Key
	})

	return result
}

// ListClusters returns all clusters for use by the dashboard.
func (b *InMemoryBackend) ListClusters() []*Cluster {
	b.mu.RLock()
	defer b.mu.RUnlock()

	result := make([]*Cluster, 0, len(b.clusters))

	for _, c := range b.clusters {
		result = append(result, c)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return result
}

// Purge removes all MemoryDB resources created before the cutoff time.
func (b *InMemoryBackend) Purge(ctx context.Context, cutoff time.Time) {
	if ctx.Err() != nil {
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	purgeMemoryDBMap(ctx, b.clusters, cutoff,
		func(c *Cluster) time.Time { return c.CreatedAt },
		func(_ string, c *Cluster) { delete(b.arnToResource, c.ARN) },
	)

	purgeMemoryDBMapFiltered(ctx, b.acls, cutoff,
		func(name string, _ *ACL) bool { return name == openAccessACL },
		func(a *ACL) time.Time { return a.CreatedAt },
		func(_ string, a *ACL) { delete(b.arnToResource, a.ARN) },
	)

	purgeMemoryDBMap(ctx, b.subnetGroups, cutoff,
		func(sg *SubnetGroup) time.Time { return sg.CreatedAt },
		func(_ string, sg *SubnetGroup) { delete(b.arnToResource, sg.ARN) },
	)

	purgeMemoryDBMap(ctx, b.users, cutoff,
		func(u *User) time.Time { return u.CreatedAt },
		func(_ string, u *User) { delete(b.arnToResource, u.ARN) },
	)

	purgeMemoryDBMap(ctx, b.parameterGroups, cutoff,
		func(pg *ParameterGroup) time.Time { return pg.CreatedAt },
		func(_ string, pg *ParameterGroup) { delete(b.arnToResource, pg.ARN) },
	)

	purgeMemoryDBMap(ctx, b.snapshots, cutoff,
		func(s *Snapshot) time.Time { return s.CreatedAt },
		func(_ string, s *Snapshot) { delete(b.arnToResource, s.ARN) },
	)

	purgeMemoryDBMap(ctx, b.multiRegionClusters, cutoff,
		func(mrc *MultiRegionCluster) time.Time { return mrc.CreatedAt },
		func(_ string, _ *MultiRegionCluster) {},
	)

	// Truncate events older than cutoff.
	if ctx.Err() != nil {
		return
	}

	filtered := b.events[:0]

	for _, ev := range b.events {
		if !ev.Date.IsZero() && ev.Date.Before(cutoff) {
			continue
		}

		filtered = append(filtered, ev)
	}

	b.events = filtered
}

// purgeMemoryDBMap deletes entries from m that were created before cutoff,
// calling cleanup for each deleted entry.
func purgeMemoryDBMap[V any](
	ctx context.Context,
	m map[string]V,
	cutoff time.Time,
	getTime func(V) time.Time,
	cleanup func(string, V),
) {
	for k, v := range m {
		if ctx.Err() != nil {
			return
		}
		if getTime(v).Before(cutoff) {
			cleanup(k, v)
			delete(m, k)
		}
	}
}

// purgeMemoryDBMapFiltered is like purgeMemoryDBMap but skips entries where skip returns true.
func purgeMemoryDBMapFiltered[V any](
	ctx context.Context,
	m map[string]V,
	cutoff time.Time,
	skip func(string, V) bool,
	getTime func(V) time.Time,
	cleanup func(string, V),
) {
	for k, v := range m {
		if ctx.Err() != nil {
			return
		}
		if skip(k, v) {
			continue
		}
		if getTime(v).Before(cutoff) {
			cleanup(k, v)
			delete(m, k)
		}
	}
}

// -- Deep-copy helpers -----------------------------------------------------------

// cloneCluster returns a shallow copy of the cluster with a separate tags map.
func cloneCluster(c *Cluster) *Cluster {
	if c == nil {
		return nil
	}

	cp := *c
	cp.Tags = maps.Clone(c.Tags)
	cp.SecurityGroupIDs = append([]string(nil), c.SecurityGroupIDs...)

	return &cp
}

// cloneACL returns a shallow copy of the ACL with separate tag and user slices.
func cloneACL(a *ACL) *ACL {
	if a == nil {
		return nil
	}

	cp := *a
	cp.Tags = maps.Clone(a.Tags)
	cp.UserNames = append([]string(nil), a.UserNames...)

	return &cp
}

// cloneSubnetGroup returns a shallow copy of the subnet group with separate slices.
func cloneSubnetGroup(sg *SubnetGroup) *SubnetGroup {
	if sg == nil {
		return nil
	}

	cp := *sg
	cp.Tags = maps.Clone(sg.Tags)
	cp.SubnetIDs = append([]string(nil), sg.SubnetIDs...)

	return &cp
}

// cloneUser returns a shallow copy of the user with separate tag and password slices.
func cloneUser(u *User) *User {
	if u == nil {
		return nil
	}

	cp := *u
	cp.Tags = maps.Clone(u.Tags)
	cp.Passwords = append([]string(nil), u.Passwords...)

	return &cp
}

// cloneParameterGroup returns a shallow copy of the parameter group with separate maps.
func cloneParameterGroup(pg *ParameterGroup) *ParameterGroup {
	if pg == nil {
		return nil
	}

	cp := *pg
	cp.Tags = maps.Clone(pg.Tags)
	cp.Parameters = maps.Clone(pg.Parameters)

	return &cp
}

// cloneSnapshot returns a shallow copy of the snapshot with a separate tags map.
func cloneSnapshot(s *Snapshot) *Snapshot {
	if s == nil {
		return nil
	}

	cp := *s
	cp.Tags = maps.Clone(s.Tags)

	return &cp
}

// cloneMultiRegionCluster returns a shallow copy of the multi-region cluster with separate tags.
func cloneMultiRegionCluster(mrc *MultiRegionCluster) *MultiRegionCluster {
	if mrc == nil {
		return nil
	}

	cp := *mrc
	cp.Tags = maps.Clone(mrc.Tags)

	return &cp
}

// cloneMultiRegionParameterGroup returns a shallow copy with separate tags.
func cloneMultiRegionParameterGroup(mrpg *MultiRegionParameterGroup) *MultiRegionParameterGroup {
	if mrpg == nil {
		return nil
	}

	cp := *mrpg
	cp.Tags = maps.Clone(mrpg.Tags)

	return &cp
}

// -- Seed helpers (for testing) --------------------------------------------------

// AddClusterInternal inserts a cluster directly into the backend for testing.
func (b *InMemoryBackend) AddClusterInternal(name, nodeType string) *Cluster {
	b.mu.Lock()
	defer b.mu.Unlock()

	clusterARN := arn.Build("memorydb", b.region, b.accountID, "cluster/"+name)
	c := &Cluster{
		Name:      name,
		ARN:       clusterARN,
		NodeType:  nodeType,
		Status:    clusterStatusAvailable,
		ACLName:   openAccessACL,
		Tags:      make(map[string]string),
		CreatedAt: time.Now(),
		Region:    b.region,
	}
	b.clusters[name] = c
	b.arnToResource[clusterARN] = resourceRef{Kind: resourceKindCluster, Name: name}

	return c
}

// AddACLInternal inserts an ACL directly into the backend for testing.
func (b *InMemoryBackend) AddACLInternal(name string) *ACL {
	b.mu.Lock()
	defer b.mu.Unlock()

	aclARN := arn.Build("memorydb", b.region, b.accountID, "acl/"+name)
	a := &ACL{
		Name:      name,
		ARN:       aclARN,
		Status:    aclStatusActive,
		UserNames: []string{},
		Tags:      make(map[string]string),
		CreatedAt: time.Now(),
	}
	b.acls[name] = a
	b.arnToResource[aclARN] = resourceRef{Kind: resourceKindACL, Name: name}

	return a
}

// AddSnapshotInternal inserts a snapshot directly into the backend for testing.
func (b *InMemoryBackend) AddSnapshotInternal(name, clusterName string) *Snapshot {
	b.mu.Lock()
	defer b.mu.Unlock()

	snapshotARN := arn.Build("memorydb", b.region, b.accountID, "snapshot/"+name)
	s := &Snapshot{
		Name:        name,
		ARN:         snapshotARN,
		ClusterName: clusterName,
		Status:      snapshotStatusAvailable,
		Tags:        make(map[string]string),
		CreatedAt:   time.Now(),
	}
	b.snapshots[name] = s
	b.arnToResource[snapshotARN] = resourceRef{Kind: resourceKindSnapshot, Name: name}

	return s
}

// AddUserInternal inserts a user directly into the backend for testing.
func (b *InMemoryBackend) AddUserInternal(name, accessString string) *User {
	b.mu.Lock()
	defer b.mu.Unlock()

	userARN := arn.Build("memorydb", b.region, b.accountID, "user/"+name)
	u := &User{
		Name:         name,
		ARN:          userARN,
		AccessString: accessString,
		Status:       userStatusActive,
		Tags:         make(map[string]string),
		CreatedAt:    time.Now(),
	}
	b.users[name] = u
	b.arnToResource[userARN] = resourceRef{Kind: resourceKindUser, Name: name}

	return u
}

// AddSubnetGroupInternal inserts a subnet group directly into the backend for testing.
func (b *InMemoryBackend) AddSubnetGroupInternal(name string) *SubnetGroup {
	b.mu.Lock()
	defer b.mu.Unlock()

	sgARN := arn.Build("memorydb", b.region, b.accountID, "subnetgroup/"+name)
	sg := &SubnetGroup{
		Name:      name,
		ARN:       sgARN,
		Tags:      make(map[string]string),
		CreatedAt: time.Now(),
	}
	b.subnetGroups[name] = sg
	b.arnToResource[sgARN] = resourceRef{Kind: resourceKindSubnetGroup, Name: name}

	return sg
}

// AddParameterGroupInternal inserts a parameter group directly into the backend for testing.
func (b *InMemoryBackend) AddParameterGroupInternal(name, family string) *ParameterGroup {
	b.mu.Lock()
	defer b.mu.Unlock()

	pgARN := arn.Build("memorydb", b.region, b.accountID, "parametergroup/"+name)
	pg := &ParameterGroup{
		Name:       name,
		ARN:        pgARN,
		Family:     family,
		Parameters: make(map[string]string),
		Tags:       make(map[string]string),
		CreatedAt:  time.Now(),
	}
	b.parameterGroups[name] = pg
	b.arnToResource[pgARN] = resourceRef{Kind: resourceKindParameterGroup, Name: name}

	return pg
}

// AddMultiRegionParameterGroupInternal inserts a multi-region parameter group directly into the backend for testing.
func (b *InMemoryBackend) AddMultiRegionParameterGroupInternal(name, family string) *MultiRegionParameterGroup {
	b.mu.Lock()
	defer b.mu.Unlock()

	mrpgARN := arn.Build("memorydb", b.region, b.accountID, "multiregionparametergroup/"+name)
	mrpg := &MultiRegionParameterGroup{
		Name:       name,
		ARN:        mrpgARN,
		Family:     family,
		Tags:       make(map[string]string),
		Parameters: make(map[string]string),
		CreatedAt:  time.Now(),
	}
	b.multiRegionParameterGroups[name] = mrpg

	return mrpg
}
