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

// validateMaintenanceWindow validates the AWS MemoryDB maintenance window format ddd:hh24:mi-ddd:hh24:mi.
func validateMaintenanceWindow(w string) error {
	if w == "" {
		return nil
	}
	parts := strings.SplitN(w, "-", splitParts)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return fmt.Errorf("MaintenanceWindow must be in format ddd:hh24:mi-ddd:hh24:mi: %w", ErrValidation)
	}

	return nil
}

// validateSnapshotWindow validates the AWS MemoryDB snapshot window format hh24:mi-hh24:mi.
func validateSnapshotWindow(w string) error {
	if w == "" {
		return nil
	}
	parts := strings.SplitN(w, "-", splitParts)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return fmt.Errorf("SnapshotWindow must be in format hh24:mi-hh24:mi: %w", ErrValidation)
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

// compile-time assertion that InMemoryBackend satisfies StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)

// StorageBackend is the interface for the MemoryDB in-memory backend.
type StorageBackend interface {
	CreateCluster(ctx context.Context, req *createClusterRequest) (*Cluster, error)
	DescribeClusters(ctx context.Context, name string) ([]*Cluster, error)
	DeleteCluster(ctx context.Context, name string) (*Cluster, error)
	DeleteClusterWithSnapshot(ctx context.Context, clusterName, snapshotName string) (*Cluster, error)
	UpdateCluster(ctx context.Context, req *updateClusterRequest) (*Cluster, error)
	CreateACL(ctx context.Context, req *createACLRequest) (*ACL, error)
	DescribeACLs(ctx context.Context, name string) ([]*ACL, error)
	DeleteACL(ctx context.Context, name string) (*ACL, error)
	UpdateACL(ctx context.Context, req *updateACLRequest) (*ACL, error)
	CreateSubnetGroup(ctx context.Context, req *createSubnetGroupRequest) (*SubnetGroup, error)
	DescribeSubnetGroups(ctx context.Context, name string) ([]*SubnetGroup, error)
	DeleteSubnetGroup(ctx context.Context, name string) (*SubnetGroup, error)
	UpdateSubnetGroup(ctx context.Context, req *updateSubnetGroupRequest) (*SubnetGroup, error)
	CreateUser(ctx context.Context, req *createUserRequest) (*User, error)
	DescribeUsers(ctx context.Context, name string) ([]*User, error)
	DeleteUser(ctx context.Context, name string) (*User, error)
	UpdateUser(ctx context.Context, req *updateUserRequest) (*User, error)
	CreateParameterGroup(ctx context.Context, req *createParameterGroupRequest) (*ParameterGroup, error)
	DescribeParameterGroups(ctx context.Context, name string) ([]*ParameterGroup, error)
	DeleteParameterGroup(ctx context.Context, name string) (*ParameterGroup, error)
	UpdateParameterGroup(ctx context.Context, req *updateParameterGroupRequest) (*ParameterGroup, error)
	ListTags(ctx context.Context, resourceArn string) (map[string]string, error)
	TagResource(ctx context.Context, resourceArn string, tags map[string]string) error
	UntagResource(ctx context.Context, resourceArn string, tagKeys []string) error
	CreateSnapshot(ctx context.Context, req *createSnapshotRequest) (*Snapshot, error)
	DescribeSnapshots(ctx context.Context, name, clusterName, snapshotType, source string) ([]*Snapshot, error)
	CopySnapshot(ctx context.Context, req *copySnapshotRequest) (*Snapshot, error)
	DeleteSnapshot(ctx context.Context, name string) (*Snapshot, error)
	DescribeEngineVersions(ctx context.Context, req *describeEngineVersionsRequest) ([]*EngineVersion, error)
	DescribeEvents(ctx context.Context, req *describeEventsRequest) ([]*Event, error)
	CreateMultiRegionCluster(ctx context.Context, req *createMultiRegionClusterRequest) (*MultiRegionCluster, error)
	DeleteMultiRegionCluster(ctx context.Context, name string) (*MultiRegionCluster, error)
	DescribeMultiRegionClusters(ctx context.Context, name string) ([]*MultiRegionCluster, error)
	UpdateMultiRegionCluster(ctx context.Context, req *updateMultiRegionClusterRequest) (*MultiRegionCluster, error)
	DescribeMultiRegionParameterGroups(ctx context.Context, name string) ([]*MultiRegionParameterGroup, error)
	DescribeParameters(ctx context.Context, parameterGroupName string) (map[string]string, error)
	ResetParameterGroup(ctx context.Context, name string, parameterNames []string, allParameters bool) (*ParameterGroup, error)
	FailoverShard(ctx context.Context, clusterName, shardConfiguration string) (*Cluster, error)
	ListAllowedNodeTypeUpdates(ctx context.Context, clusterName string) ([]string, error)
	ListAllowedMultiRegionClusterUpdates(ctx context.Context, clusterName string) ([]string, error)
	BatchUpdateCluster(ctx context.Context, clusterNames []string) map[string]*Cluster
	DescribeReservedNodes(ctx context.Context, req *describeReservedNodesRequest) ([]*ReservedNode, error)
	DescribeReservedNodesOfferings(ctx context.Context, req *describeReservedNodesOfferingsRequest) ([]*ReservedNodesOffering, error)
	PurchaseReservedNodesOffering(ctx context.Context, req *purchaseReservedNodesOfferingRequest) (*ReservedNode, error)
	DescribeMultiRegionParameters(ctx context.Context, parameterGroupName string) (map[string]string, error)
	DescribeServiceUpdates(ctx context.Context, req *describeServiceUpdatesRequest) ([]*ServiceUpdate, error)
	Region() string
	Reset()
	Snapshot() []byte
	Restore(data []byte) error
}

// InMemoryBackend is the in-memory implementation of StorageBackend.
type InMemoryBackend struct {
	multiRegionClusters        map[string]*MultiRegionCluster
	multiRegionParameterGroups map[string]*MultiRegionParameterGroup
	serviceUpdates             map[string]*ServiceUpdate
	clusters                   map[string]map[string]*Cluster
	acls                       map[string]map[string]*ACL
	subnetGroups               map[string]map[string]*SubnetGroup
	users                      map[string]map[string]*User
	parameterGroups            map[string]map[string]*ParameterGroup
	snapshots                  map[string]map[string]*Snapshot
	reservedNodes              map[string]map[string]*ReservedNode
	arnToResource              map[string]map[string]resourceRef
	events                     map[string][]*Event
	accountID                  string
	defaultRegion              string
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
		clusters:                   make(map[string]map[string]*Cluster),
		acls:                       make(map[string]map[string]*ACL),
		subnetGroups:               make(map[string]map[string]*SubnetGroup),
		users:                      make(map[string]map[string]*User),
		parameterGroups:            make(map[string]map[string]*ParameterGroup),
		snapshots:                  make(map[string]map[string]*Snapshot),
		multiRegionClusters:        make(map[string]*MultiRegionCluster),
		multiRegionParameterGroups: make(map[string]*MultiRegionParameterGroup),
		reservedNodes:              make(map[string]map[string]*ReservedNode),
		serviceUpdates:             make(map[string]*ServiceUpdate),
		events:                     make(map[string][]*Event),
		arnToResource:              make(map[string]map[string]resourceRef),
		accountID:                  accountID,
		defaultRegion:              region,
	}

	openAccessARN := arn.Build("memorydb", region, accountID, "acl/"+openAccessACL)
	b.aclsStore(region)[openAccessACL] = &ACL{
		Name:      openAccessACL,
		ARN:       openAccessARN,
		Status:    aclStatusActive,
		UserNames: []string{},
		CreatedAt: time.Now(),
		Tags:      make(map[string]string),
	}
	b.arnToResourceStore(region)[openAccessARN] = resourceRef{Kind: resourceKindACL, Name: openAccessACL}

	b.serviceUpdates["memorydb-20240601-redis-security"] = &ServiceUpdate{
		ServiceUpdateName:   "memorydb-20240601-redis-security",
		ReleaseDate:         "2024-06-01",
		Description:         "Security update for Redis 7.x clusters",
		Status:              clusterStatusAvailable,
		Type:                "security-update",
		AutoUpdateStartDate: "2024-07-01",
	}
	b.serviceUpdates["memorydb-20240801-engine-update"] = &ServiceUpdate{
		ServiceUpdateName:   "memorydb-20240801-engine-update",
		ReleaseDate:         "2024-08-01",
		Description:         "Engine update with performance improvements",
		Status:              clusterStatusAvailable,
		Type:                "engine-update",
		AutoUpdateStartDate: "2024-09-01",
	}

	b.seedDefaultParameterGroupsLocked()

	return b
}
func (b *InMemoryBackend) clustersStore(region string) map[string]*Cluster {
	if b.clusters[region] == nil {
		b.clusters[region] = make(map[string]*Cluster)
	}

	return b.clusters[region]
}
func (b *InMemoryBackend) aclsStore(region string) map[string]*ACL {
	if b.acls[region] == nil {
		b.acls[region] = make(map[string]*ACL)
		// Seed the open-access ACL into every region so CreateCluster works across regions.
		openAccessARN := arn.Build("memorydb", region, b.accountID, "acl/"+openAccessACL)
		b.acls[region][openAccessACL] = &ACL{
			Name:      openAccessACL,
			ARN:       openAccessARN,
			Status:    aclStatusActive,
			UserNames: []string{},
			CreatedAt: time.Now(),
			Tags:      make(map[string]string),
		}
	}

	return b.acls[region]
}
func (b *InMemoryBackend) subnetGroupsStore(region string) map[string]*SubnetGroup {
	if b.subnetGroups[region] == nil {
		b.subnetGroups[region] = make(map[string]*SubnetGroup)
	}

	return b.subnetGroups[region]
}
func (b *InMemoryBackend) usersStore(region string) map[string]*User {
	if b.users[region] == nil {
		b.users[region] = make(map[string]*User)
	}

	return b.users[region]
}

func (b *InMemoryBackend) parameterGroupsStore(region string) map[string]*ParameterGroup {
	if b.parameterGroups[region] == nil {
		b.parameterGroups[region] = make(map[string]*ParameterGroup)
	}

	return b.parameterGroups[region]
}
func (b *InMemoryBackend) snapshotsStore(region string) map[string]*Snapshot {
	if b.snapshots[region] == nil {
		b.snapshots[region] = make(map[string]*Snapshot)
	}

	return b.snapshots[region]
}
func (b *InMemoryBackend) reservedNodesStore(region string) map[string]*ReservedNode {
	if b.reservedNodes[region] == nil {
		b.reservedNodes[region] = make(map[string]*ReservedNode)
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

	b.clusters = make(map[string]map[string]*Cluster)
	b.acls = make(map[string]map[string]*ACL)
	b.subnetGroups = make(map[string]map[string]*SubnetGroup)
	b.users = make(map[string]map[string]*User)
	b.parameterGroups = make(map[string]map[string]*ParameterGroup)
	b.snapshots = make(map[string]map[string]*Snapshot)
	b.multiRegionClusters = make(map[string]*MultiRegionCluster)
	b.multiRegionParameterGroups = make(map[string]*MultiRegionParameterGroup)
	b.reservedNodes = make(map[string]map[string]*ReservedNode)
	b.serviceUpdates = make(map[string]*ServiceUpdate)
	b.events = make(map[string][]*Event)
	b.arnToResource = make(map[string]map[string]resourceRef)

	openAccessARN := arn.Build("memorydb", b.defaultRegion, b.accountID, "acl/"+openAccessACL)
	b.aclsStore(b.defaultRegion)[openAccessACL] = &ACL{
		Name:      openAccessACL,
		ARN:       openAccessARN,
		Status:    aclStatusActive,
		UserNames: []string{},
		CreatedAt: time.Now(),
		Tags:      make(map[string]string),
	}
	b.arnToResourceStore(b.defaultRegion)[openAccessARN] = resourceRef{Kind: resourceKindACL, Name: openAccessACL}

	b.serviceUpdates["memorydb-20240601-redis-security"] = &ServiceUpdate{
		ServiceUpdateName:   "memorydb-20240601-redis-security",
		ReleaseDate:         "2024-06-01",
		Description:         "Security update for Redis 7.x clusters",
		Status:              clusterStatusAvailable,
		Type:                "security-update",
		AutoUpdateStartDate: "2024-07-01",
	}
	b.serviceUpdates["memorydb-20240801-engine-update"] = &ServiceUpdate{
		ServiceUpdateName:   "memorydb-20240801-engine-update",
		ReleaseDate:         "2024-08-01",
		Description:         "Engine update with performance improvements",
		Status:              clusterStatusAvailable,
		Type:                "engine-update",
		AutoUpdateStartDate: "2024-09-01",
	}

	b.seedDefaultParameterGroupsLocked()
}

// seedDefaultParameterGroupsLocked seeds the built-in default single-region and
// multi-region parameter groups. Caller must hold b.mu or be in the constructor.
func (b *InMemoryBackend) seedDefaultParameterGroupsLocked() {
	families := []struct {
		name   string
		family string
		desc   string
	}{
		{"default.memorydb-redis6", familyRedis6, "Default parameter group for MemoryDB Redis 6.2"},
		{"default.memorydb-redis7", familyRedis7, "Default parameter group for MemoryDB Redis 7.x"},
		{"default.memorydb-valkey7", familyValkey7, "Default parameter group for MemoryDB Valkey 7.x"},
		{"default.memorydb-valkey8", familyValkey8, "Default parameter group for MemoryDB Valkey 8.x"},
	}
	for _, f := range families {
		pgARN := arn.Build("memorydb", b.defaultRegion, b.accountID, "parametergroup/"+f.name)
		pg := &ParameterGroup{
			Name:        f.name,
			ARN:         pgARN,
			Description: f.desc,
			Family:      f.family,
			Parameters:  defaultParametersByFamily(f.family),
			Tags:        make(map[string]string),
			CreatedAt:   time.Now(),
		}
		b.parameterGroupsStore(b.defaultRegion)[f.name] = pg
		b.arnToResourceStore(b.defaultRegion)[pgARN] = resourceRef{Kind: resourceKindParameterGroup, Name: f.name}
	}

	mrFamilies := []struct {
		name   string
		family string
		desc   string
	}{
		{
			"default.memorydb-redis6.multiregion",
			familyRedis6,
			"Default multi-region parameter group for MemoryDB Redis 6.2",
		},
		{
			"default.memorydb-redis7.multiregion",
			familyRedis7,
			"Default multi-region parameter group for MemoryDB Redis 7.x",
		},
		{
			"default.memorydb-valkey7.multiregion",
			familyValkey7,
			"Default multi-region parameter group for MemoryDB Valkey 7.x",
		},
		{
			"default.memorydb-valkey8.multiregion",
			familyValkey8,
			"Default multi-region parameter group for MemoryDB Valkey 8.x",
		},
	}
	for _, f := range mrFamilies {
		mrARN := arn.Build("memorydb", b.defaultRegion, b.accountID, "multiregionparametergroup/"+f.name)
		mrpg := &MultiRegionParameterGroup{
			Name:        f.name,
			ARN:         mrARN,
			Description: f.desc,
			Family:      f.family,
			Parameters:  defaultParametersByFamily(f.family),
			Tags:        make(map[string]string),
			CreatedAt:   time.Now(),
		}
		b.multiRegionParameterGroups[f.name] = mrpg
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
	case engineVersion62, engineVersion70, engineVersion71, engineVersion72, engineVersion80:
		return true
	default:
		return false
	}
}

// validateCreateClusterRefs checks that ACL, subnet group, and parameter group referenced in
// req all exist in the backend (caller must hold b.mu).
func (b *InMemoryBackend) validateCreateClusterRefs(region string, req *createClusterRequest) (string, error) {
	aclName := req.ACLName
	if aclName == "" {
		aclName = openAccessACL
	}

	if _, ok := b.aclsStore(region)[aclName]; !ok {
		return "", fmt.Errorf("ACL %q not found: %w", aclName, ErrACLNotFound)
	}

	if req.SubnetGroupName != "" {
		if _, ok := b.subnetGroupsStore(region)[req.SubnetGroupName]; !ok {
			return "", fmt.Errorf("subnet group %q not found: %w", req.SubnetGroupName, ErrSubnetGroupNotFound)
		}
	}

	if req.ParameterGroupName != "" {
		if _, ok := b.parameterGroupsStore(region)[req.ParameterGroupName]; !ok {
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

	if err := resolveEngineDefaults(&d); err != nil {
		return d, err
	}

	if err := resolveNodeDefaults(&d); err != nil {
		return d, err
	}

	applyOptionalOverrides(&d, req)

	if err := validateClusterBounds(&d); err != nil {
		return d, err
	}

	return d, nil
}

// resolveEngineDefaults sets engine and version defaults, returning an error for unsupported values.
func resolveEngineDefaults(d *clusterDefaults) error {
	if d.engine == "" {
		d.engine = engineRedis
	}

	if d.engine != engineRedis && d.engine != engineValkey {
		return fmt.Errorf("engine %q is not supported (must be redis or valkey): %w", d.engine, ErrValidation)
	}

	if d.engineVersion == "" {
		if d.engine == engineValkey {
			d.engineVersion = defaultValkeyEngineVersion
		} else {
			d.engineVersion = defaultEngineVersion
		}
	}

	if !isSupportedEngineVersion(d.engineVersion) {
		return fmt.Errorf("engine version %q is not supported: %w", d.engineVersion, ErrValidation)
	}

	return nil
}

// resolveNodeDefaults sets the node type default and validates the prefix.
func resolveNodeDefaults(d *clusterDefaults) error {
	if d.nodeType == "" {
		d.nodeType = defaultNodeType
	}

	if !strings.HasPrefix(d.nodeType, "db.") {
		return fmt.Errorf("node type %q is invalid: must begin with 'db.': %w", d.nodeType, ErrValidation)
	}

	return nil
}

// applyOptionalOverrides applies caller-provided overrides for port, shards, replicas, and TLS.
func applyOptionalOverrides(d *clusterDefaults, req *createClusterRequest) {
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
}

// validateClusterBounds checks that shard and replica counts are within allowed ranges.
func validateClusterBounds(d *clusterDefaults) error {
	if d.numShards < 1 || d.numShards > 500 {
		return fmt.Errorf("NumShards must be between 1 and 500: %w", ErrValidation)
	}

	if d.numReplicas < 0 || d.numReplicas > 5 {
		return fmt.Errorf("NumReplicasPerShard must be between 0 and 5: %w", ErrValidation)
	}

	return nil
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
			Name:                   c.Name,
			NodeType:               c.NodeType,
			EngineVersion:          c.EngineVersion,
			Description:            c.Description,
			Port:                   c.Port,
			NumShards:              c.NumShards,
			Engine:                 c.Engine,
			MaintenanceWindow:      c.MaintenanceWindow,
			TopicArn:               c.SnsTopicArn,
			ParameterGroupName:     c.ParameterGroupName,
			SubnetGroupName:        c.SubnetGroupName,
			SnapshotRetentionLimit: c.SnapshotRetentionLimit,
			SnapshotWindow:         c.SnapshotWindow,
		},
	}
	b.snapshotsStore(region)[autoName] = autoSnap
	b.arnToResourceStore(region)[autoARN] = resourceRef{Kind: resourceKindSnapshot, Name: autoName}
}

// resolveDataTiering converts the optional DataTiering request field to the AWS string value.
func resolveDataTiering(req *createClusterRequest) string {
	if req.DataTiering != nil && *req.DataTiering {
		return "true"
	}

	return "false"
}

// applyClusterNetworkDefaults sets NetworkType and IPDiscovery defaults.
func applyClusterNetworkDefaults(c *Cluster, req *createClusterRequest) {
	c.NetworkType = req.NetworkType
	if c.NetworkType == "" {
		c.NetworkType = "ipv4"
	}
	c.IPDiscovery = req.IPDiscovery
	if c.IPDiscovery == "" {
		c.IPDiscovery = "ipv4"
	}
}

// CreateCluster creates a new MemoryDB cluster.
func buildCluster(region, clusterARN, aclName string, req *createClusterRequest, d clusterDefaults) *Cluster {
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
	c.DataTiering = resolveDataTiering(req)
	applyClusterNetworkDefaults(c, req)
	if req.SnapshotRetentionLimit != nil {
		c.SnapshotRetentionLimit = *req.SnapshotRetentionLimit
	}
	if d.numReplicas > 0 {
		c.AvailabilityMode = "MultiAZ"
	} else {
		c.AvailabilityMode = "SingleAZ"
	}
	c.Endpoint = req.ClusterName + ".memorydb." + region + ".amazonaws.com"

	return c
}

func (b *InMemoryBackend) CreateCluster(ctx context.Context, req *createClusterRequest) (*Cluster, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	if err := validateResourceName(req.ClusterName, "cluster"); err != nil {
		return nil, err
	}

	if _, exists := b.clustersStore(region)[req.ClusterName]; exists {
		return nil, ErrClusterAlreadyExists
	}

	aclName, err := b.validateCreateClusterRefs(region, req)
	if err != nil {
		return nil, err
	}

	var restoreSnap *Snapshot
	if req.SnapshotName != "" {
		s, ok := b.snapshotsStore(region)[req.SnapshotName]
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

	if errMW := validateMaintenanceWindow(req.MaintenanceWindow); errMW != nil {
		return nil, errMW
	}
	if errSW := validateSnapshotWindow(req.SnapshotWindow); errSW != nil {
		return nil, errSW
	}

	clusterARN := arn.Build("memorydb", region, b.accountID, "cluster/"+req.ClusterName)
	c := buildCluster(region, clusterARN, aclName, req, d)

	b.clustersStore(region)[req.ClusterName] = c
	b.arnToResourceStore(region)[clusterARN] = resourceRef{Kind: resourceKindCluster, Name: req.ClusterName}

	b.appendEventLocked(region, &Event{
		Date:       time.Now(),
		SourceName: req.ClusterName,
		SourceType: resourceKindCluster,
		Message:    "Cluster " + req.ClusterName + " created",
	})

	if c.SnapshotRetentionLimit > 0 {
		b.seedAutomatedSnapshotLocked(region, b.accountID, c)
	}

	return cloneCluster(c), nil
}

// DescribeClusters returns clusters, optionally filtered by name.
func (b *InMemoryBackend) DescribeClusters(ctx context.Context, name string) ([]*Cluster, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)
	store := b.clusters[region]

	if name != "" {
		c, ok := store[name]
		if !ok {
			return nil, ErrClusterNotFound
		}

		return []*Cluster{cloneCluster(c)}, nil
	}

	result := make([]*Cluster, 0, len(store))
	for _, c := range store {
		result = append(result, cloneCluster(c))
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return result, nil
}

// DeleteCluster removes a cluster.
func (b *InMemoryBackend) DeleteCluster(ctx context.Context, name string) (*Cluster, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	c, ok := b.clustersStore(region)[name]
	if !ok {
		return nil, ErrClusterNotFound
	}

	delete(b.clustersStore(region), name)
	delete(b.arnToResourceStore(region), c.ARN)

	b.appendEventLocked(region, &Event{
		Date:       time.Now(),
		SourceName: name,
		SourceType: resourceKindCluster,
		Message:    "Cluster " + name + " deleted",
	})

	return cloneCluster(c), nil
}

// DeleteClusterWithSnapshot removes a cluster, first creating a snapshot with the given name.
func (b *InMemoryBackend) DeleteClusterWithSnapshot(ctx context.Context, clusterName, snapshotName string) (*Cluster, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	c, ok := b.clustersStore(region)[clusterName]
	if !ok {
		return nil, ErrClusterNotFound
	}

	if snapshotName != "" {
		snapshotARN := arn.Build("memorydb", region, b.accountID, "snapshot/"+snapshotName)
		s := &Snapshot{
			Name:         snapshotName,
			ARN:          snapshotARN,
			ClusterName:  clusterName,
			Status:       snapshotStatusAvailable,
			SnapshotType: snapshotSourceManual,
			Tags:         make(map[string]string),
			CreatedAt:    time.Now(),
			ClusterConfiguration: snapshotClusterConfig{
				Name:                   c.Name,
				NodeType:               c.NodeType,
				EngineVersion:          c.EngineVersion,
				Description:            c.Description,
				Port:                   c.Port,
				NumShards:              c.NumShards,
				Engine:                 c.Engine,
				MaintenanceWindow:      c.MaintenanceWindow,
				TopicArn:               c.SnsTopicArn,
				ParameterGroupName:     c.ParameterGroupName,
				SubnetGroupName:        c.SubnetGroupName,
				SnapshotRetentionLimit: c.SnapshotRetentionLimit,
				SnapshotWindow:         c.SnapshotWindow,
			},
		}
		b.snapshotsStore(region)[snapshotName] = s
		b.arnToResourceStore(region)[snapshotARN] = resourceRef{Kind: resourceKindSnapshot, Name: snapshotName}
	}

	delete(b.clustersStore(region), clusterName)
	delete(b.arnToResourceStore(region), c.ARN)

	b.appendEventLocked(region, &Event{
		Date:       time.Now(),
		SourceName: clusterName,
		SourceType: resourceKindCluster,
		Message:    "Cluster " + clusterName + " deleted",
	})

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

	if req.IPDiscovery != "" {
		c.IPDiscovery = req.IPDiscovery
	}

	if req.SnsTopicStatus != "" {
		c.SnsTopicStatus = req.SnsTopicStatus
	}
}

// validateUpdateClusterRequest validates the update cluster request fields.
func validateUpdateClusterRequest(req *updateClusterRequest) error {
	if req.MaintenanceWindow != "" {
		if err := validateMaintenanceWindow(req.MaintenanceWindow); err != nil {
			return err
		}
	}

	if req.SnapshotWindow != "" {
		if err := validateSnapshotWindow(req.SnapshotWindow); err != nil {
			return err
		}
	}

	if req.ReplicaConfiguration != nil && req.ReplicaConfiguration.ReplicaCount != nil {
		rc := *req.ReplicaConfiguration.ReplicaCount
		if rc < 0 || rc > 5 {
			return fmt.Errorf("NumReplicasPerShard must be between 0 and 5: %w", ErrValidation)
		}
	}

	if req.ShardConfiguration != nil && req.ShardConfiguration.ShardCount != nil {
		sc := *req.ShardConfiguration.ShardCount
		if sc < 1 || sc > 500 {
			return fmt.Errorf("NumShards must be between 1 and 500: %w", ErrValidation)
		}
	}

	return nil
}

// applyClusterUpdates applies validated optional fields from the update request to the cluster.
func applyClusterUpdates(c *Cluster, req *updateClusterRequest) {
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
}

// UpdateCluster modifies an existing cluster.
func (b *InMemoryBackend) UpdateCluster(ctx context.Context, req *updateClusterRequest) (*Cluster, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	c, ok := b.clustersStore(region)[req.ClusterName]
	if !ok {
		return nil, ErrClusterNotFound
	}

	if err := validateUpdateClusterRequest(req); err != nil {
		return nil, err
	}

	applyClusterUpdates(c, req)

	b.appendEventLocked(region, &Event{
		Date:       time.Now(),
		SourceName: req.ClusterName,
		SourceType: resourceKindCluster,
		Message:    "Cluster " + req.ClusterName + " modified",
	})

	return cloneCluster(c), nil
}

// -- ACL operations --------------------------------------------------------------

// CreateACL creates a new ACL.
func (b *InMemoryBackend) CreateACL(ctx context.Context, req *createACLRequest) (*ACL, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	if err := validateResourceName(req.ACLName, "ACL"); err != nil {
		return nil, err
	}

	if _, exists := b.aclsStore(region)[req.ACLName]; exists {
		return nil, ErrACLAlreadyExists
	}

	aclARN := arn.Build("memorydb", region, b.accountID, "acl/"+req.ACLName)

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

	b.aclsStore(region)[req.ACLName] = a
	b.arnToResourceStore(region)[aclARN] = resourceRef{Kind: resourceKindACL, Name: req.ACLName}

	b.appendEventLocked(region, &Event{
		Date:       time.Now(),
		SourceName: req.ACLName,
		SourceType: resourceKindACL,
		Message:    "ACL " + req.ACLName + " created",
	})

	return cloneACL(a), nil
}

// DescribeACLs returns ACLs, optionally filtered by name.
func (b *InMemoryBackend) DescribeACLs(ctx context.Context, name string) ([]*ACL, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)
	store := b.acls[region]

	if name != "" {
		a, ok := store[name]
		if !ok {
			return nil, ErrACLNotFound
		}

		return []*ACL{cloneACL(a)}, nil
	}

	result := make([]*ACL, 0, len(store))
	for _, a := range store {
		result = append(result, cloneACL(a))
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return result, nil
}

// DeleteACL removes an ACL.
func (b *InMemoryBackend) DeleteACL(ctx context.Context, name string) (*ACL, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	a, ok := b.aclsStore(region)[name]
	if !ok {
		return nil, ErrACLNotFound
	}

	if name == openAccessACL {
		return nil, fmt.Errorf("cannot delete system ACL %q: %w", name, ErrValidation)
	}

	for _, c := range b.clusters[region] {
		if c.ACLName == name {
			return nil, fmt.Errorf("ACL %q is associated with cluster %q: %w", name, c.Name, ErrACLInUse)
		}
	}

	delete(b.aclsStore(region), name)
	delete(b.arnToResourceStore(region), a.ARN)

	b.appendEventLocked(region, &Event{
		Date:       time.Now(),
		SourceName: name,
		SourceType: resourceKindACL,
		Message:    "ACL " + name + " deleted",
	})

	return a, nil
}

// UpdateACL modifies an existing ACL.
func (b *InMemoryBackend) UpdateACL(ctx context.Context, req *updateACLRequest) (*ACL, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	a, ok := b.aclsStore(region)[req.ACLName]
	if !ok {
		return nil, ErrACLNotFound
	}

	existing := make(map[string]bool, len(a.UserNames))
	for _, u := range a.UserNames {
		existing[u] = true
	}

	for _, u := range req.UserNamesToAdd {
		if _, exists := b.users[region][u]; !exists {
			return nil, fmt.Errorf("user %q not found: %w", u, ErrUserNotFound)
		}
	}

	for _, u := range req.UserNamesToAdd {
		if !existing[u] {
			a.UserNames = append(a.UserNames, u)
			existing[u] = true
		}
	}

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

	b.appendEventLocked(region, &Event{
		Date:       time.Now(),
		SourceName: req.ACLName,
		SourceType: resourceKindACL,
		Message:    "ACL " + req.ACLName + " modified",
	})

	return cloneACL(a), nil
}

// -- SubnetGroup operations -------------------------------------------------------

// CreateSubnetGroup creates a new subnet group.
func (b *InMemoryBackend) CreateSubnetGroup(ctx context.Context, req *createSubnetGroupRequest) (*SubnetGroup, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	if err := validateResourceName(req.SubnetGroupName, "subnet group"); err != nil {
		return nil, err
	}

	if _, exists := b.subnetGroupsStore(region)[req.SubnetGroupName]; exists {
		return nil, ErrSubnetGroupAlreadyExists
	}

	sgARN := arn.Build("memorydb", region, b.accountID, "subnetgroup/"+req.SubnetGroupName)

	sg := &SubnetGroup{
		Name:        req.SubnetGroupName,
		ARN:         sgARN,
		Description: req.Description,
		SubnetIDs:   req.SubnetIDs,
		Tags:        tagsFromSlice(req.Tags),
		CreatedAt:   time.Now(),
	}

	b.subnetGroupsStore(region)[req.SubnetGroupName] = sg
	b.arnToResourceStore(region)[sgARN] = resourceRef{Kind: resourceKindSubnetGroup, Name: req.SubnetGroupName}

	return cloneSubnetGroup(sg), nil
}

// DescribeSubnetGroups returns subnet groups, optionally filtered by name.
func (b *InMemoryBackend) DescribeSubnetGroups(ctx context.Context, name string) ([]*SubnetGroup, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)
	store := b.subnetGroups[region]

	if name != "" {
		sg, ok := store[name]

		if !ok {
			return nil, ErrSubnetGroupNotFound
		}

		return []*SubnetGroup{cloneSubnetGroup(sg)}, nil
	}

	result := make([]*SubnetGroup, 0, len(store))
	for _, sg := range store {
		result = append(result, cloneSubnetGroup(sg))
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return result, nil
}

// DeleteSubnetGroup removes a subnet group.
func (b *InMemoryBackend) DeleteSubnetGroup(ctx context.Context, name string) (*SubnetGroup, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	sg, ok := b.subnetGroupsStore(region)[name]
	if !ok {
		return nil, ErrSubnetGroupNotFound
	}

	delete(b.subnetGroupsStore(region), name)
	delete(b.arnToResourceStore(region), sg.ARN)

	return sg, nil
}

// UpdateSubnetGroup modifies an existing subnet group.
func (b *InMemoryBackend) UpdateSubnetGroup(ctx context.Context, req *updateSubnetGroupRequest) (*SubnetGroup, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	sg, ok := b.subnetGroupsStore(region)[req.SubnetGroupName]
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
func (b *InMemoryBackend) CreateUser(ctx context.Context, req *createUserRequest) (*User, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	if err := validateResourceName(req.UserName, "user"); err != nil {
		return nil, err
	}

	if _, exists := b.usersStore(region)[req.UserName]; exists {
		return nil, ErrUserAlreadyExists
	}

	authType := strings.ToLower(req.AuthenticationMode.Type)
	if authType == "" {
		authType = authTypeNoPasswordRequired
	}
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

	userARN := arn.Build("memorydb", region, b.accountID, "user/"+req.UserName)

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

	b.usersStore(region)[req.UserName] = u
	b.arnToResourceStore(region)[userARN] = resourceRef{Kind: resourceKindUser, Name: req.UserName}

	b.appendEventLocked(region, &Event{
		Date:       time.Now(),
		SourceName: req.UserName,
		SourceType: "user",
		Message:    "User " + req.UserName + " created",
	})

	return cloneUser(u), nil
}

// DescribeUsers returns users, optionally filtered by name.
func (b *InMemoryBackend) DescribeUsers(ctx context.Context, name string) ([]*User, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)
	store := b.users[region]

	if name != "" {
		u, ok := store[name]
		if !ok {
			return nil, ErrUserNotFound
		}

		return []*User{cloneUser(u)}, nil
	}

	result := make([]*User, 0, len(store))
	for _, u := range store {
		result = append(result, cloneUser(u))
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return result, nil
}

// DeleteUser removes a user.
func (b *InMemoryBackend) DeleteUser(ctx context.Context, name string) (*User, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	u, ok := b.usersStore(region)[name]
	if !ok {
		return nil, ErrUserNotFound
	}

	for _, a := range b.acls[region] {
		if slices.Contains(a.UserNames, name) {
			return nil, fmt.Errorf("user %q is a member of ACL %q: %w", name, a.Name, ErrUserInUse)
		}
	}

	delete(b.usersStore(region), name)
	delete(b.arnToResourceStore(region), u.ARN)

	return u, nil
}

// UpdateUser modifies an existing user.
func (b *InMemoryBackend) UpdateUser(ctx context.Context, req *updateUserRequest) (*User, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	u, ok := b.usersStore(region)[req.UserName]
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
func (b *InMemoryBackend) CreateParameterGroup(ctx context.Context, req *createParameterGroupRequest) (*ParameterGroup, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	if req.Family == "" {
		return nil, fmt.Errorf("family is required: %w", ErrValidation)
	}

	if err := validateResourceName(req.ParameterGroupName, "parameter group"); err != nil {
		return nil, err
	}

	if _, exists := b.parameterGroupsStore(region)[req.ParameterGroupName]; exists {
		return nil, ErrParameterGroupAlreadyExists
	}

	pgARN := arn.Build("memorydb", region, b.accountID, "parametergroup/"+req.ParameterGroupName)

	pg := &ParameterGroup{
		Name:        req.ParameterGroupName,
		ARN:         pgARN,
		Description: req.Description,
		Family:      req.Family,
		Parameters:  defaultParametersByFamily(req.Family),
		Tags:        tagsFromSlice(req.Tags),
		CreatedAt:   time.Now(),
	}

	b.parameterGroupsStore(region)[req.ParameterGroupName] = pg
	b.arnToResourceStore(region)[pgARN] = resourceRef{Kind: resourceKindParameterGroup, Name: req.ParameterGroupName}

	return pg, nil
}

// DescribeParameterGroups returns parameter groups, optionally filtered by name.
func (b *InMemoryBackend) DescribeParameterGroups(ctx context.Context, name string) ([]*ParameterGroup, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)
	store := b.parameterGroups[region]

	if name != "" {
		pg, ok := store[name]
		if !ok {
			return nil, ErrParameterGroupNotFound
		}

		return []*ParameterGroup{cloneParameterGroup(pg)}, nil
	}

	result := make([]*ParameterGroup, 0, len(store))
	for _, pg := range store {
		result = append(result, cloneParameterGroup(pg))
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return result, nil
}

// DeleteParameterGroup removes a parameter group.
func (b *InMemoryBackend) DeleteParameterGroup(ctx context.Context, name string) (*ParameterGroup, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	pg, ok := b.parameterGroupsStore(region)[name]
	if !ok {
		return nil, ErrParameterGroupNotFound
	}

	delete(b.parameterGroupsStore(region), name)
	delete(b.arnToResourceStore(region), pg.ARN)

	return pg, nil
}

// UpdateParameterGroup modifies parameter values in a parameter group.
func (b *InMemoryBackend) UpdateParameterGroup(ctx context.Context, req *updateParameterGroupRequest) (*ParameterGroup, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	pg, ok := b.parameterGroupsStore(region)[req.ParameterGroupName]
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
func (b *InMemoryBackend) ListTags(_ context.Context, resourceArn string) (map[string]string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	region, ref, ok := b.findARN(resourceArn)
	if !ok {
		return nil, awserr.New("ResourceNotFoundFault: resource not found", awserr.ErrNotFound)
	}

	tags := b.tagsForRef(region, ref)

	return tags, nil
}

// TagResource adds or updates tags on a resource.
func (b *InMemoryBackend) TagResource(_ context.Context, resourceArn string, tags map[string]string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	region, ref, ok := b.findARN(resourceArn)
	if !ok {
		return awserr.New("ResourceNotFoundFault: resource not found", awserr.ErrNotFound)
	}

	const maxTagsPerResource = 50

	existingTags := b.tagsForRef(region, ref)
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

	b.applyTags(region, ref, tags)

	return nil
}

// UntagResource removes tags from a resource.
func (b *InMemoryBackend) UntagResource(_ context.Context, resourceArn string, tagKeys []string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	region, ref, ok := b.findARN(resourceArn)
	if !ok {
		return awserr.New("ResourceNotFoundFault: resource not found", awserr.ErrNotFound)
	}

	b.removeTags(region, ref, tagKeys)

	return nil
}

// findARN searches all regions' arnToResource maps for the given ARN.
func (b *InMemoryBackend) findARN(resourceArn string) (string, resourceRef, bool) {
	for region, store := range b.arnToResource {
		if ref, ok := store[resourceArn]; ok {
			return region, ref, true
		}
	}

	return "", resourceRef{}, false
}

// tagsForRef returns a copy of the tags for the referenced resource (must hold at least RLock).
func (b *InMemoryBackend) tagsForRef(region string, ref resourceRef) map[string]string {
	var src map[string]string

	switch ref.Kind {
	case resourceKindCluster:
		if c, ok := b.clusters[region][ref.Name]; ok {
			src = c.Tags
		}
	case resourceKindACL:
		if a, ok := b.acls[region][ref.Name]; ok {
			src = a.Tags
		}
	case resourceKindSubnetGroup:
		if sg, ok := b.subnetGroups[region][ref.Name]; ok {
			src = sg.Tags
		}
	case resourceKindUser:
		if u, ok := b.users[region][ref.Name]; ok {
			src = u.Tags
		}
	case resourceKindParameterGroup:
		if pg, ok := b.parameterGroups[region][ref.Name]; ok {
			src = pg.Tags
		}
	case resourceKindSnapshot:
		if s, ok := b.snapshots[region][ref.Name]; ok {
			src = s.Tags
		}
	}

	return maps.Clone(src)
}

// mergeTags ensures dst is initialized then copies all src entries into it.
func mergeTags(dst *map[string]string, src map[string]string) {
	if *dst == nil {
		*dst = make(map[string]string, len(src))
	}

	maps.Copy(*dst, src)
}

func (b *InMemoryBackend) applyTags(region string, ref resourceRef, tags map[string]string) {
	switch ref.Kind {
	case resourceKindCluster:
		if c, ok := b.clusters[region][ref.Name]; ok {
			mergeTags(&c.Tags, tags)
		}
	case resourceKindACL:
		if a, ok := b.acls[region][ref.Name]; ok {
			mergeTags(&a.Tags, tags)
		}
	case resourceKindSubnetGroup:
		if sg, ok := b.subnetGroups[region][ref.Name]; ok {
			mergeTags(&sg.Tags, tags)
		}
	case resourceKindUser:
		if u, ok := b.users[region][ref.Name]; ok {
			mergeTags(&u.Tags, tags)
		}
	case resourceKindParameterGroup:
		if pg, ok := b.parameterGroups[region][ref.Name]; ok {
			mergeTags(&pg.Tags, tags)
		}
	case resourceKindSnapshot:
		if s, ok := b.snapshots[region][ref.Name]; ok {
			mergeTags(&s.Tags, tags)
		}
	}
}

// removeTags deletes the given tag keys from the referenced resource (must hold Lock).
func (b *InMemoryBackend) removeTags(region string, ref resourceRef, tagKeys []string) {
	m := b.tagsMapForRef(region, ref)
	if m == nil {
		return
	}

	for _, k := range tagKeys {
		delete(m, k)
	}
}

// tagsMapForRef returns a direct (mutable) reference to the tag map for a resource (must hold Lock).
func (b *InMemoryBackend) tagsMapForRef(region string, ref resourceRef) map[string]string {
	switch ref.Kind {
	case resourceKindCluster:
		if c, ok := b.clusters[region][ref.Name]; ok {
			return c.Tags
		}
	case resourceKindACL:
		if a, ok := b.acls[region][ref.Name]; ok {
			return a.Tags
		}
	case resourceKindSubnetGroup:
		if sg, ok := b.subnetGroups[region][ref.Name]; ok {
			return sg.Tags
		}
	case resourceKindUser:
		if u, ok := b.users[region][ref.Name]; ok {
			return u.Tags
		}
	case resourceKindParameterGroup:
		if pg, ok := b.parameterGroups[region][ref.Name]; ok {
			return pg.Tags
		}
	case resourceKindSnapshot:
		if s, ok := b.snapshots[region][ref.Name]; ok {
			return s.Tags
		}
	}

	return nil
}

// -- Snapshot operations --------------------------------------------------------

// CreateSnapshot creates a snapshot of a cluster.
func (b *InMemoryBackend) CreateSnapshot(ctx context.Context, req *createSnapshotRequest) (*Snapshot, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	c, ok := b.clustersStore(region)[req.ClusterName]
	if !ok {
		return nil, ErrClusterNotFound
	}

	if _, exists := b.snapshotsStore(region)[req.SnapshotName]; exists {
		return nil, ErrSnapshotAlreadyExists
	}

	snapshotARN := arn.Build("memorydb", region, b.accountID, "snapshot/"+req.SnapshotName)

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
			Name:                   c.Name,
			NodeType:               c.NodeType,
			EngineVersion:          c.EngineVersion,
			Description:            c.Description,
			Port:                   c.Port,
			NumShards:              c.NumShards,
			Engine:                 c.Engine,
			MaintenanceWindow:      c.MaintenanceWindow,
			TopicArn:               c.SnsTopicArn,
			ParameterGroupName:     c.ParameterGroupName,
			SubnetGroupName:        c.SubnetGroupName,
			SnapshotRetentionLimit: c.SnapshotRetentionLimit,
			SnapshotWindow:         c.SnapshotWindow,
		},
	}

	b.snapshotsStore(region)[req.SnapshotName] = s
	b.arnToResourceStore(region)[snapshotARN] = resourceRef{Kind: resourceKindSnapshot, Name: req.SnapshotName}

	b.appendEventLocked(region, &Event{
		Date:       time.Now(),
		SourceName: req.SnapshotName,
		SourceType: resourceKindSnapshot,

		Message: "Snapshot " + req.SnapshotName + " created for cluster " + req.ClusterName,
	})

	return s, nil
}

// DescribeSnapshots returns snapshots, optionally filtered by name, cluster name, snapshot type, or source.
func (b *InMemoryBackend) DescribeSnapshots(ctx context.Context, name, clusterName, snapshotType, source string) ([]*Snapshot, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)
	store := b.snapshots[region]

	if name != "" {
		s, ok := store[name]
		if !ok {
			return nil, ErrSnapshotNotFound
		}

		return []*Snapshot{cloneSnapshot(s)}, nil
	}

	result := make([]*Snapshot, 0, len(store))
	for _, s := range store {
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
func (b *InMemoryBackend) CopySnapshot(ctx context.Context, req *copySnapshotRequest) (*Snapshot, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	src, ok := b.snapshotsStore(region)[req.SourceSnapshotName]
	if !ok {
		return nil, ErrSnapshotNotFound
	}

	if req.TargetBucket != "" {
		return cloneSnapshot(src), nil
	}

	if _, exists := b.snapshotsStore(region)[req.TargetSnapshotName]; exists {
		return nil, ErrSnapshotAlreadyExists
	}

	targetARN := arn.Build("memorydb", region, b.accountID, "snapshot/"+req.TargetSnapshotName)

	kmsKeyID := req.KmsKeyID
	if kmsKeyID == "" {
		kmsKeyID = src.KmsKeyID
	}

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

	b.snapshotsStore(region)[req.TargetSnapshotName] = dst
	b.arnToResourceStore(region)[targetARN] = resourceRef{Kind: resourceKindSnapshot, Name: req.TargetSnapshotName}

	return dst, nil
}

// DeleteSnapshot removes a snapshot.
func (b *InMemoryBackend) DeleteSnapshot(ctx context.Context, name string) (*Snapshot, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	s, ok := b.snapshotsStore(region)[name]
	if !ok {
		return nil, ErrSnapshotNotFound
	}

	delete(b.snapshotsStore(region), name)
	delete(b.arnToResourceStore(region), s.ARN)

	b.appendEventLocked(region, &Event{
		Date:       time.Now(),
		SourceName: name,
		SourceType: resourceKindSnapshot,
		Message:    "Snapshot " + name + " deleted",
	})

	return s, nil
}

// -- EngineVersion operations ---------------------------------------------------

// defaultParametersByFamily returns the built-in parameter defaults for each engine family.
func defaultParametersByFamily(family string) map[string]string {
	base := map[string]string{
		"maxmemory-policy":              "noeviction",
		"timeout":                       "0",
		"tcp-keepalive":                 "300",
		"lazyfree-lazy-eviction":        "no",
		"lazyfree-lazy-expire":          "no",
		"lazyfree-lazy-server-del":      "no",
		"replica-lazy-flush":            "no",
		"activedefrag":                  "no",
		"active-expire-enabled":         "1",
		"active-expire-effort":          "1",
		"lfu-log-factor":                "10",
		"lfu-decay-time":                "1",
		"hash-max-listpack-entries":     "128",
		"hash-max-listpack-value":       "64",
		"list-max-listpack-size":        "-2",
		"list-compress-depth":           "0",
		"set-max-intset-entries":        "512",
		"zset-max-listpack-entries":     "128",
		"zset-max-listpack-value":       "64",
		"activerehashing":               paramValueYes,
		"hz":                            "10",
		"dynamic-hz":                    paramValueYes,
		"aof-rewrite-incremental-fsync": paramValueYes,
		"rdb-save-incremental-fsync":    paramValueYes,
		"jemalloc-bg-thread":            paramValueYes,
		"close-on-slave-write":          paramValueYes,
		"repl-backlog-size":             "1048576",
		"repl-backlog-ttl":              "3600",
		"slowlog-log-slower-than":       "10000",
		"slowlog-max-len":               "128",
		"latency-monitor-threshold":     "0",
		"tracking-table-max-keys":       "0",
		"list-max-ziplist-size":         "-2",
		"cluster-node-timeout":          "15000",
		"cluster-migration-barrier":     "1",
		"cluster-require-full-coverage": paramValueYes,
		"cluster-allow-reads-when-down": "no",
	}
	_ = family // family-specific overrides could go here

	return base
}

// defaultEngineVersions returns the built-in list of supported engine versions.
func defaultEngineVersions() []*EngineVersion {
	return []*EngineVersion{
		{
			Engine:               "valkey",
			EngineVersion:        engineVersion80,
			EnginePatchVersion:   "8.0.1",
			ParameterGroupFamily: familyValkey8,
			Description:          "Valkey 8.0",
		},
		{
			Engine:               "valkey",
			EngineVersion:        engineVersion72,
			EnginePatchVersion:   "7.2.4",
			ParameterGroupFamily: familyValkey7,
			Description:          "Valkey 7.2",
		},
		{
			Engine:               engineRedis,
			EngineVersion:        engineVersion71,
			EnginePatchVersion:   "7.1.0",
			ParameterGroupFamily: familyRedis7,
			Description:          "Redis 7.1",
		},
		{
			Engine:               engineRedis,
			EngineVersion:        engineVersion70,
			EnginePatchVersion:   "7.0.7",
			ParameterGroupFamily: familyRedis7,
			Description:          "Redis 7.0",
		},
		{
			Engine:               engineRedis,
			EngineVersion:        engineVersion62,
			EnginePatchVersion:   "6.2.6",
			ParameterGroupFamily: familyRedis6,
			Description:          "Redis 6.2",
		},
	}
}

// DescribeEngineVersions returns supported engine versions, optionally filtered.
func (b *InMemoryBackend) DescribeEngineVersions(_ context.Context, req *describeEngineVersionsRequest) ([]*EngineVersion, error) {
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
	b.appendEventLocked(b.defaultRegion, ev)
}

// appendEventLocked appends an event without acquiring the lock (caller must hold b.mu).
func (b *InMemoryBackend) appendEventLocked(region string, ev *Event) {
	b.events[region] = append(b.events[region], ev)

	if len(b.events[region]) > maxEvents {
		trimmed := make([]*Event, maxEvents)
		copy(trimmed, b.events[region][len(b.events[region])-maxEvents:])
		b.events[region] = trimmed
	}
}

// DescribeEvents returns events, optionally filtered by source name and type.
func (b *InMemoryBackend) DescribeEvents(ctx context.Context, req *describeEventsRequest) ([]*Event, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var startTime *time.Time
	if req.StartTime != nil {
		startTime = req.StartTime
	} else if req.Duration != nil {
		t := time.Now().Add(-time.Duration(*req.Duration) * time.Minute)
		startTime = &t
	}

	var result []*Event

	for _, evs := range b.events {
		for _, ev := range evs {
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
func (b *InMemoryBackend) CreateMultiRegionCluster(ctx context.Context, req *createMultiRegionClusterRequest) (*MultiRegionCluster, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	fullName := "virv-" + req.MultiRegionClusterNameSuffix

	if _, exists := b.multiRegionClusters[fullName]; exists {
		return nil, ErrMultiRegionClusterAlreadyExists
	}

	mrARN := arn.Build("memorydb", region, b.accountID, "multiregioncluster/"+fullName)

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
	b.arnToResourceStore(region)[mrARN] = resourceRef{Kind: resourceKindMultiRegionCluster, Name: fullName}

	return mrc, nil
}

// DeleteMultiRegionCluster removes a multi-region cluster.
func (b *InMemoryBackend) DeleteMultiRegionCluster(ctx context.Context, name string) (*MultiRegionCluster, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	mrc, ok := b.multiRegionClusters[name]
	if !ok {
		return nil, ErrMultiRegionClusterNotFound
	}

	delete(b.multiRegionClusters, name)
	delete(b.arnToResourceStore(region), mrc.ARN)

	return cloneMultiRegionCluster(mrc), nil
}

// DescribeMultiRegionClusters returns multi-region clusters, optionally filtered by name.
func (b *InMemoryBackend) DescribeMultiRegionClusters(_ context.Context, name string) ([]*MultiRegionCluster, error) {
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
func (b *InMemoryBackend) UpdateMultiRegionCluster(_ context.Context, req *updateMultiRegionClusterRequest) (*MultiRegionCluster, error) {
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
func (b *InMemoryBackend) DescribeMultiRegionParameterGroups(_ context.Context, name string) ([]*MultiRegionParameterGroup, error) {
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
func (b *InMemoryBackend) DescribeParameters(ctx context.Context, parameterGroupName string) (map[string]string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)

	if parameterGroupName == "" {
		return nil, fmt.Errorf("parameter group name is required: %w", ErrValidation)
	}

	pg, ok := b.parameterGroups[region][parameterGroupName]
	if !ok {
		return nil, ErrParameterGroupNotFound
	}

	return maps.Clone(pg.Parameters), nil
}

// ResetParameterGroup resets parameters in a parameter group back to family defaults.
// If parameterNames is non-empty and allParameters is false, only those keys are reset.
// If allParameters is true or parameterNames is empty, all parameters are reset.
func (b *InMemoryBackend) ResetParameterGroup(ctx context.Context, name string, parameterNames []string, allParameters bool) (*ParameterGroup, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	pg, ok := b.parameterGroupsStore(region)[name]
	if !ok {
		return nil, ErrParameterGroupNotFound
	}

	defaults := defaultParametersByFamily(pg.Family)

	if len(parameterNames) > 0 && !allParameters {
		for _, pn := range parameterNames {
			if dv, found := defaults[pn]; found {
				pg.Parameters[pn] = dv
			} else {
				delete(pg.Parameters, pn)
			}
		}
	} else {
		pg.Parameters = maps.Clone(defaults)
	}

	return cloneParameterGroup(pg), nil
}

// -- Shard operations -----------------------------------------------------------

// FailoverShard simulates a shard failover for a cluster, returning the cluster state.
func (b *InMemoryBackend) FailoverShard(ctx context.Context, clusterName, shardName string) (*Cluster, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	c, ok := b.clustersStore(region)[clusterName]
	if !ok {
		return nil, ErrClusterNotFound
	}

	msg := "Failover initiated"
	if shardName != "" {
		msg = "Failover initiated for shard " + shardName
	}

	b.appendEventLocked(region, &Event{
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
		defaultReservedNodeType,
		"db.r6g.2xlarge",
		"db.r6g.4xlarge",
		"db.r6gd.xlarge",
		"db.t4g.small",
		"db.t4g.medium",
	}
}

// ListAllowedNodeTypeUpdates returns the set of node types a cluster can be updated to.
func (b *InMemoryBackend) ListAllowedNodeTypeUpdates(ctx context.Context, clusterName string) ([]string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)

	if _, ok := b.clusters[region][clusterName]; !ok {
		return nil, ErrClusterNotFound
	}

	return allowedNodeTypes(), nil
}

// ListAllowedMultiRegionClusterUpdates returns the set of node types a multi-region cluster can be updated to.
func (b *InMemoryBackend) ListAllowedMultiRegionClusterUpdates(_ context.Context, clusterName string) ([]string, error) {
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
func (b *InMemoryBackend) BatchUpdateCluster(ctx context.Context, clusterNames []string) map[string]*Cluster {
	b.mu.RLock()
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)

	result := make(map[string]*Cluster, len(clusterNames))
	for _, name := range clusterNames {
		if c, ok := b.clusters[region][name]; ok {
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
			NodeType:                defaultReservedNodeType,
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
func (b *InMemoryBackend) DescribeReservedNodes(ctx context.Context, req *describeReservedNodesRequest) ([]*ReservedNode, error) {
	b.mu.RLock()

	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)
	store := b.reservedNodes[region]

	result := make([]*ReservedNode, 0, len(store))
	for _, rn := range store {
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
func (b *InMemoryBackend) DescribeReservedNodesOfferings(_ context.Context, req *describeReservedNodesOfferingsRequest) ([]*ReservedNodesOffering, error) {
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
func (b *InMemoryBackend) PurchaseReservedNodesOffering(ctx context.Context, req *purchaseReservedNodesOfferingRequest) (*ReservedNode, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	if req.ReservedNodesOfferingID == "" {
		return nil, fmt.Errorf("ReservedNodesOfferingId is required: %w", ErrValidation)
	}

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

	if _, exists := b.reservedNodesStore(region)[reservationID]; exists {
		return nil, fmt.Errorf("reserved node %q already exists: %w", reservationID, ErrReservationAlreadyExists)
	}

	nodeCount := int32(1)
	if req.NodeCount != nil {
		nodeCount = *req.NodeCount
	}

	rnARN := arn.Build("memorydb", region, b.accountID, "reservednode/"+reservationID)

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

	b.reservedNodesStore(region)[reservationID] = rn

	cp := *rn

	return &cp, nil
}

// -- DescribeMultiRegionParameters operation ------------------------------------

// DescribeMultiRegionParameters returns the parameters for a multi-region parameter group.
func (b *InMemoryBackend) DescribeMultiRegionParameters(_ context.Context, parameterGroupName string) (map[string]string, error) {
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
func (b *InMemoryBackend) DescribeServiceUpdates(_ context.Context, req *describeServiceUpdatesRequest) ([]*ServiceUpdate, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	result := make([]*ServiceUpdate, 0, len(b.serviceUpdates))
	for _, su := range b.serviceUpdates {
		if req.ServiceUpdateName != "" && su.ServiceUpdateName != req.ServiceUpdateName {
			continue
		}
		if len(req.Status) > 0 && !slices.Contains(req.Status, su.Status) {
			continue
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

	var result []*Cluster
	for _, regionClusters := range b.clusters {
		for _, c := range regionClusters {
			result = append(result, cloneCluster(c))
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })

	return result
}

// Purge removes all MemoryDB resources created before the cutoff time.
func (b *InMemoryBackend) Purge(ctx context.Context, cutoff time.Time) {
	if ctx.Err() != nil {
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	for region, regionClusters := range b.clusters {
		purgeMemoryDBMap(ctx, regionClusters, cutoff,
			func(c *Cluster) time.Time { return c.CreatedAt },
			func(_ string, c *Cluster) { delete(b.arnToResource[region], c.ARN) },
		)
	}
	for region, regionACLs := range b.acls {
		purgeMemoryDBMapFiltered(ctx, regionACLs, cutoff,
			func(name string, _ *ACL) bool { return name == openAccessACL },
			func(a *ACL) time.Time { return a.CreatedAt },
			func(_ string, a *ACL) { delete(b.arnToResource[region], a.ARN) },
		)
	}
	for region, regionSGs := range b.subnetGroups {
		purgeMemoryDBMap(ctx, regionSGs, cutoff,
			func(sg *SubnetGroup) time.Time { return sg.CreatedAt },
			func(_ string, sg *SubnetGroup) { delete(b.arnToResource[region], sg.ARN) },
		)
	}
	for region, regionUsers := range b.users {
		purgeMemoryDBMap(ctx, regionUsers, cutoff,
			func(u *User) time.Time { return u.CreatedAt },
			func(_ string, u *User) { delete(b.arnToResource[region], u.ARN) },
		)
	}
	for region, regionPGs := range b.parameterGroups {
		purgeMemoryDBMap(ctx, regionPGs, cutoff,
			func(pg *ParameterGroup) time.Time { return pg.CreatedAt },
			func(_ string, pg *ParameterGroup) { delete(b.arnToResource[region], pg.ARN) },
		)
	}
	for region, regionSnaps := range b.snapshots {
		purgeMemoryDBMap(ctx, regionSnaps, cutoff,
			func(s *Snapshot) time.Time { return s.CreatedAt },
			func(_ string, s *Snapshot) { delete(b.arnToResource[region], s.ARN) },
		)
	}
	purgeMemoryDBMap(ctx, b.multiRegionClusters, cutoff,
		func(mrc *MultiRegionCluster) time.Time { return mrc.CreatedAt },
		func(_ string, _ *MultiRegionCluster) {},
	)

	if ctx.Err() != nil {
		return
	}

	for region, evs := range b.events {
		filtered := evs[:0]
		for _, ev := range evs {
			if !ev.Date.IsZero() && ev.Date.Before(cutoff) {
				continue
			}
			filtered = append(filtered, ev)
		}
		b.events[region] = filtered
	}
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

	clusterARN := arn.Build("memorydb", b.defaultRegion, b.accountID, "cluster/"+name)
	c := &Cluster{
		Name:      name,
		ARN:       clusterARN,
		NodeType:  nodeType,
		Status:    clusterStatusAvailable,
		ACLName:   openAccessACL,
		Tags:      make(map[string]string),
		CreatedAt: time.Now(),
		Region:    b.defaultRegion,
	}
	b.clustersStore(b.defaultRegion)[name] = c
	b.arnToResourceStore(b.defaultRegion)[clusterARN] = resourceRef{Kind: resourceKindCluster, Name: name}

	return c
}

// AddACLInternal inserts an ACL directly into the backend for testing.
func (b *InMemoryBackend) AddACLInternal(name string) *ACL {
	b.mu.Lock()
	defer b.mu.Unlock()

	aclARN := arn.Build("memorydb", b.defaultRegion, b.accountID, "acl/"+name)
	a := &ACL{
		Name:      name,
		ARN:       aclARN,
		Status:    aclStatusActive,
		UserNames: []string{},
		Tags:      make(map[string]string),
		CreatedAt: time.Now(),
	}
	b.aclsStore(b.defaultRegion)[name] = a
	b.arnToResourceStore(b.defaultRegion)[aclARN] = resourceRef{Kind: resourceKindACL, Name: name}

	return a
}

// AddSnapshotInternal inserts a snapshot directly into the backend for testing.
func (b *InMemoryBackend) AddSnapshotInternal(name, clusterName string) *Snapshot {
	b.mu.Lock()
	defer b.mu.Unlock()

	snapshotARN := arn.Build("memorydb", b.defaultRegion, b.accountID, "snapshot/"+name)
	s := &Snapshot{
		Name:        name,
		ARN:         snapshotARN,
		ClusterName: clusterName,
		Status:      snapshotStatusAvailable,
		Tags:        make(map[string]string),
		CreatedAt:   time.Now(),
	}
	b.snapshotsStore(b.defaultRegion)[name] = s
	b.arnToResourceStore(b.defaultRegion)[snapshotARN] = resourceRef{Kind: resourceKindSnapshot, Name: name}

	return s
}

// AddUserInternal inserts a user directly into the backend for testing.
func (b *InMemoryBackend) AddUserInternal(name, accessString string) *User {
	b.mu.Lock()
	defer b.mu.Unlock()

	userARN := arn.Build("memorydb", b.defaultRegion, b.accountID, "user/"+name)
	u := &User{
		Name:         name,
		ARN:          userARN,
		AccessString: accessString,
		Status:       userStatusActive,
		Tags:         make(map[string]string),
		CreatedAt:    time.Now(),
	}
	b.usersStore(b.defaultRegion)[name] = u
	b.arnToResourceStore(b.defaultRegion)[userARN] = resourceRef{Kind: resourceKindUser, Name: name}

	return u
}

// AddSubnetGroupInternal inserts a subnet group directly into the backend for testing.
func (b *InMemoryBackend) AddSubnetGroupInternal(name string) *SubnetGroup {
	b.mu.Lock()
	defer b.mu.Unlock()

	sgARN := arn.Build("memorydb", b.defaultRegion, b.accountID, "subnetgroup/"+name)
	sg := &SubnetGroup{
		Name:      name,
		ARN:       sgARN,
		Tags:      make(map[string]string),
		CreatedAt: time.Now(),
	}
	b.subnetGroupsStore(b.defaultRegion)[name] = sg
	b.arnToResourceStore(b.defaultRegion)[sgARN] = resourceRef{Kind: resourceKindSubnetGroup, Name: name}

	return sg
}

// AddParameterGroupInternal inserts a parameter group directly into the backend for testing.
func (b *InMemoryBackend) AddParameterGroupInternal(name, family string) *ParameterGroup {
	b.mu.Lock()
	defer b.mu.Unlock()

	pgARN := arn.Build("memorydb", b.defaultRegion, b.accountID, "parametergroup/"+name)
	pg := &ParameterGroup{
		Name:       name,
		ARN:        pgARN,
		Family:     family,
		Parameters: make(map[string]string),
		Tags:       make(map[string]string),
		CreatedAt:  time.Now(),
	}
	b.parameterGroupsStore(b.defaultRegion)[name] = pg
	b.arnToResourceStore(b.defaultRegion)[pgARN] = resourceRef{Kind: resourceKindParameterGroup, Name: name}

	return pg
}

// AddMultiRegionParameterGroupInternal inserts a multi-region parameter group directly into the backend for testing.
func (b *InMemoryBackend) AddMultiRegionParameterGroupInternal(name, family string) *MultiRegionParameterGroup {
	b.mu.Lock()
	defer b.mu.Unlock()

	mrpgARN := arn.Build("memorydb", b.defaultRegion, b.accountID, "multiregionparametergroup/"+name)
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
