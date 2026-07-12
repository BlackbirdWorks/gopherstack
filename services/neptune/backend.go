package neptune

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// nowISO8601 returns the current UTC time formatted as the ISO8601/RFC3339
// wire string Neptune's query/xml deserializers expect for *CreateTime
// fields (smithytime.ParseDateTime on the client side).
func nowISO8601() string {
	return time.Now().UTC().Format(time.RFC3339)
}

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

const (
	pgFamilyDefaultNeptune13 = "default.neptune1.3"
	snapshotSourceManual     = "manual"
	engineModeProvisioned    = "provisioned"
	engineModeServerless     = "serverless"
)

var (
	ErrClusterNotFound                    = errors.New("DBClusterNotFound")
	ErrClusterAlreadyExists               = errors.New("DBClusterAlreadyExists")
	ErrInstanceNotFound                   = errors.New("DBInstanceNotFound")
	ErrInstanceAlreadyExists              = errors.New("DBInstanceAlreadyExists")
	ErrSubnetGroupNotFound                = errors.New("DBSubnetGroupNotFound")
	ErrSubnetGroupAlreadyExists           = errors.New("DBSubnetGroupAlreadyExists")
	ErrClusterParameterGroupNotFound      = errors.New("DBClusterParameterGroupNotFound")
	ErrClusterParameterGroupAlreadyExists = errors.New("DBClusterParameterGroupAlreadyExists")
	ErrClusterSnapshotNotFound            = errors.New("DBClusterSnapshotNotFound")
	ErrClusterSnapshotAlreadyExists       = errors.New("DBClusterSnapshotAlreadyExists")
	ErrParameterGroupNotFound             = errors.New("DBParameterGroupNotFound")
	ErrParameterGroupAlreadyExists        = errors.New("DBParameterGroupAlreadyExists")
	ErrClusterEndpointNotFound            = errors.New("DBClusterEndpointNotFound")
	ErrClusterEndpointAlreadyExists       = errors.New("DBClusterEndpointAlreadyExists")
	ErrSubscriptionNotFound               = errors.New("SubscriptionNotFound")
	ErrSubscriptionAlreadyExists          = errors.New("SubscriptionAlreadyExists")
	ErrGlobalClusterNotFound              = errors.New("GlobalClusterNotFound")
	ErrGlobalClusterAlreadyExists         = errors.New("GlobalClusterAlreadyExists")
	ErrInvalidParameter                   = errors.New("InvalidParameterValue")
	ErrUnknownAction                      = errors.New("InvalidAction")
	ErrInvalidDBClusterStateFault         = errors.New("InvalidDBClusterStateFault")
	ErrInvalidDBInstanceStateFault        = errors.New("InvalidDBInstanceStateFault")
	ErrInvalidDBClusterSnapshotStateFault = errors.New("InvalidDBClusterSnapshotStateFault")
	ErrSnapshotRequired                   = errors.New("InvalidParameterCombination")
)

// neptunIdentifierRE validates Neptune resource identifiers:
// 1–63 chars, start with a letter, end with letter or digit, only letters/digits/hyphens,
// no consecutive hyphens.
var neptunIdentifierRE = regexp.MustCompile(`^[a-zA-Z](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?$`)

// validateNeptuneIdentifier returns an error when id does not conform to Neptune naming rules.
func validateNeptuneIdentifier(id, fieldName string) error {
	const maxIdentifierLen = 63
	const invalidIdentifierMsg = "%w: %s %q is not a valid identifier; must start with a letter, " +
		"contain only letters/digits/hyphens, and not end with a hyphen"
	if id == "" {
		return fmt.Errorf("%w: %s is required", ErrInvalidParameter, fieldName)
	}
	if len(id) > maxIdentifierLen {
		return fmt.Errorf(
			"%w: %s %q exceeds maximum length of %d characters",
			ErrInvalidParameter, fieldName, id, maxIdentifierLen,
		)
	}
	if !neptunIdentifierRE.MatchString(id) {
		return fmt.Errorf(invalidIdentifierMsg, ErrInvalidParameter, fieldName, id)
	}
	if strings.Contains(id, "--") {
		return fmt.Errorf(
			"%w: %s %q cannot contain consecutive hyphens",
			ErrInvalidParameter, fieldName, id,
		)
	}

	return nil
}

const (
	defaultNeptunePort           = 8182
	defaultInstanceClass         = "db.r5.large"
	neptuneEngine                = "neptune"
	defaultEngineVersion         = "1.3.0.0"
	defaultBackupRetentionPeriod = 1
	clusterStatusAvailable       = "available"
	clusterStatusStopped         = "stopped"
	subscriptionStatusActive     = "active"
	maxPromotionTier             = 15
	maxTagsPerResource           = 50
	maxTagKeyLen                 = 128
	maxTagValueLen               = 256
	arnPartCount                 = 7
	endpointTypeReader           = "READER"
	endpointTypeWriter           = "WRITER"
	endpointTypeCustom           = "CUSTOM"
	endpointTypeAny              = "ANY"
	defaultMaintenanceWindow     = "sun:05:00-sun:06:00"
	defaultStorageType           = "aurora"
	defaultAllocatedStorage      = 1
	minBackupRetentionPeriod     = 1
	maxBackupRetentionPeriod     = 35
	minNeptunePort               = 1150
	maxNeptunePort               = 65535
	snapshotStatusAvailable      = "available"
	snapshotStatusCreating       = "creating"
	percentProgressComplete      = 100
	minFailoverClusterMembers    = 2
)

// ServerlessV2ScalingConfiguration holds Neptune Serverless v2 capacity settings.
type ServerlessV2ScalingConfiguration struct {
	MinCapacity float64 `json:"minCapacity"`
	MaxCapacity float64 `json:"maxCapacity"`
}

// MasterUserManagedSecret holds the ARN of the Secrets Manager secret for the master user password.
type MasterUserManagedSecret struct {
	SecretARN    string `json:"secretArn"`
	SecretStatus string `json:"secretStatus"`
}

// DBClusterCreateOptions holds optional fields for CreateDBCluster.
type DBClusterCreateOptions struct {
	ServerlessV2ScalingConfig       *ServerlessV2ScalingConfiguration
	DBSubnetGroupName               string
	StorageType                     string
	EngineVersion                   string
	EngineMode                      string
	KmsKeyID                        string
	PreferredBackupWindow           string
	MasterUsername                  string
	PreferredMaintenanceWindow      string
	AvailabilityZones               []string
	VpcSecurityGroupIDs             []string
	BackupRetentionPeriod           int
	EnableIAMDatabaseAuthentication bool
	ManageMasterUserPassword        bool
	StorageEncrypted                bool
	DeletionProtection              bool
	CopyTagsToSnapshot              bool
}

// DBClusterModifyOptions holds optional fields for ModifyDBCluster.
type DBClusterModifyOptions struct {
	ServerlessV2ScalingConfig       *ServerlessV2ScalingConfiguration
	EngineVersion                   string
	PreferredBackupWindow           string
	PreferredMaintenanceWindow      string
	VpcSecurityGroupIDs             []string
	BackupRetentionPeriod           int
	EnableIAMDatabaseAuthentication bool
	IamAuthSet                      bool
	ManageMasterUserPassword        bool
	DeletionProtection              bool
	DeletionProtectionSet           bool
	CopyTagsToSnapshot              bool
	CopyTagsToSnapshotSet           bool
	BackupRetentionPeriodSet        bool
}

// DBClusterDeleteOptions holds optional fields for DeleteDBCluster.
type DBClusterDeleteOptions struct {
	FinalDBSnapshotIdentifier string
	SkipFinalSnapshot         bool
}

// DBClusterMember represents a single DB instance member of a Neptune cluster.
type DBClusterMember struct {
	DBInstanceIdentifier string `json:"DBInstanceIdentifier"`
	IsClusterWriter      bool   `json:"IsClusterWriter"`
}

// DBCluster represents an Amazon Neptune DB cluster.
type DBCluster struct {
	// region is the AWS region this cluster belongs to. It is the outer half
	// of the composite key ("region|DBClusterIdentifier") used by the
	// backend's flat store.Table[DBCluster] (see store_setup.go), which
	// replaces the old map[string]map[string]*DBCluster nesting (outer key =
	// region). Unexported so it never appears in Neptune wire responses
	// (those are built by cloneCluster/hand-assembled describe results, never
	// by marshaling DBCluster directly), but persistence.go must carry it
	// through a DTO explicitly since json.Marshal never sees unexported fields.
	region                          string
	ServerlessV2ScalingConfig       *ServerlessV2ScalingConfiguration `json:"ServerlessV2ScalingConfiguration,omitempty"`
	MasterUserManagedSecret         *MasterUserManagedSecret          `json:"MasterUserManagedSecret,omitempty"`
	KmsKeyID                        string                            `json:"KmsKeyID"`
	HostedZoneID                    string                            `json:"HostedZoneId"`
	Engine                          string                            `json:"Engine"`
	EngineVersion                   string                            `json:"EngineVersion"`
	DBClusterIdentifier             string                            `json:"DBClusterIdentifier"`
	Status                          string                            `json:"Status"`
	DBClusterParameterGroupName     string                            `json:"DBClusterParameterGroupName"`
	DBSubnetGroupName               string                            `json:"DBSubnetGroupName"`
	Endpoint                        string                            `json:"Endpoint"`
	ReaderEndpoint                  string                            `json:"ReaderEndpoint"`
	PreferredBackupWindow           string                            `json:"PreferredBackupWindow"`
	PreferredMaintenanceWindow      string                            `json:"PreferredMaintenanceWindow"`
	DBClusterArn                    string                            `json:"DBClusterArn"`
	DBClusterResourceID             string                            `json:"DbClusterResourceId"`
	ClusterCreateTime               string                            `json:"ClusterCreateTime"`
	StorageType                     string                            `json:"StorageType"`
	EngineMode                      string                            `json:"EngineMode"`
	MasterUsername                  string                            `json:"MasterUsername"`
	AvailabilityZones               []string                          `json:"AvailabilityZones"`
	VpcSecurityGroupIDs             []string                          `json:"VpcSecurityGroupIds"`
	AssociatedRoles                 []string                          `json:"AssociatedRoles"`
	DBClusterMembers                []DBClusterMember                 `json:"DBClusterMembers"`
	Port                            int                               `json:"Port"`
	BackupRetentionPeriod           int                               `json:"BackupRetentionPeriod"`
	AllocatedStorage                int                               `json:"AllocatedStorage"`
	EnableIAMDatabaseAuthentication bool                              `json:"EnableIAMDatabaseAuthentication"`
	StorageEncrypted                bool                              `json:"StorageEncrypted"`
	MultiAZ                         bool                              `json:"MultiAZ"`
	DeletionProtection              bool                              `json:"DeletionProtection"`
	CopyTagsToSnapshot              bool                              `json:"CopyTagsToSnapshot"`
}

// DBInstance represents an Amazon Neptune DB instance.
type DBInstance struct {
	// region is the AWS region this instance belongs to; see DBCluster.region
	// for the composite-key rationale (store_setup.go/persistence.go).
	region                          string
	DBInstanceIdentifier            string `json:"DBInstanceIdentifier"`
	DBInstanceArn                   string `json:"DBInstanceArn"`
	DBClusterIdentifier             string `json:"DBClusterIdentifier"`
	DBInstanceClass                 string `json:"DBInstanceClass"`
	Engine                          string `json:"Engine"`
	EngineVersion                   string `json:"EngineVersion"`
	DBInstanceStatus                string `json:"DBInstanceStatus"`
	InstanceCreateTime              string `json:"InstanceCreateTime"`
	Endpoint                        string `json:"Endpoint"`
	DBSubnetGroupName               string `json:"DBSubnetGroupName"`
	DBParameterGroupName            string `json:"DBParameterGroupName"`
	PreferredMaintenanceWindow      string `json:"PreferredMaintenanceWindow"`
	PreferredBackupWindow           string `json:"PreferredBackupWindow"`
	AvailabilityZone                string `json:"AvailabilityZone"`
	Port                            int    `json:"Port"`
	PromotionTier                   int    `json:"PromotionTier"`
	StorageEncrypted                bool   `json:"StorageEncrypted"`
	AutoMinorVersionUpgrade         bool   `json:"AutoMinorVersionUpgrade"`
	CopyTagsToSnapshot              bool   `json:"CopyTagsToSnapshot"`
	EnableIAMDatabaseAuthentication bool   `json:"EnableIAMDatabaseAuthentication"`
	MultiAZ                         bool   `json:"MultiAZ"`
	PubliclyAccessible              bool   `json:"PubliclyAccessible"`
}

// DBInstanceCreateOptions holds optional fields for CreateDBInstance.
type DBInstanceCreateOptions struct {
	DBParameterGroupName            string
	PreferredMaintenanceWindow      string
	PreferredBackupWindow           string
	AvailabilityZone                string
	PromotionTier                   int
	AutoMinorVersionUpgrade         bool
	CopyTagsToSnapshot              bool
	EnableIAMDatabaseAuthentication bool
	StorageEncrypted                bool
}

// DBInstanceModifyOptions holds optional fields for ModifyDBInstance.
type DBInstanceModifyOptions struct {
	DBParameterGroupName            string
	PreferredMaintenanceWindow      string
	PreferredBackupWindow           string
	AvailabilityZone                string
	PromotionTier                   int
	AutoMinorVersionUpgrade         bool
	AutoMinorVersionUpgradeSet      bool
	CopyTagsToSnapshot              bool
	CopyTagsToSnapshotSet           bool
	EnableIAMDatabaseAuthentication bool
	IamAuthSet                      bool
	PromotionTierSet                bool
}

// DBSubnetGroup represents a Neptune DB subnet group.
type DBSubnetGroup struct {
	// region is the AWS region this subnet group belongs to; see
	// DBCluster.region for the composite-key rationale.
	region                   string
	DBSubnetGroupName        string   `json:"DBSubnetGroupName"`
	DBSubnetGroupArn         string   `json:"DBSubnetGroupArn"`
	DBSubnetGroupDescription string   `json:"DBSubnetGroupDescription"`
	VpcID                    string   `json:"VpcID"`
	Status                   string   `json:"Status"`
	SubnetIDs                []string `json:"SubnetIDs"`
}

// Tag is a key-value pair tag.
type Tag struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// DBClusterParameterGroup represents a Neptune DB cluster parameter group.
type DBClusterParameterGroup struct {
	// region is the AWS region this cluster parameter group belongs to; see
	// DBCluster.region for the composite-key rationale.
	region                      string
	DBClusterParameterGroupName string `json:"DBClusterParameterGroupName"`
	DBClusterParameterGroupArn  string `json:"DBClusterParameterGroupArn"`
	DBParameterGroupFamily      string `json:"DBParameterGroupFamily"`
	Description                 string `json:"Description"`
}

// DBClusterSnapshot represents a Neptune DB cluster snapshot.
type DBClusterSnapshot struct {
	// region is the AWS region this cluster snapshot belongs to; see
	// DBCluster.region for the composite-key rationale.
	region                      string
	DBClusterSnapshotIdentifier string `json:"DBClusterSnapshotIdentifier"`
	DBClusterSnapshotArn        string `json:"DBClusterSnapshotArn"`
	DBClusterIdentifier         string `json:"DBClusterIdentifier"`
	Engine                      string `json:"Engine"`
	EngineVersion               string `json:"EngineVersion"`
	Status                      string `json:"Status"`
	SnapshotType                string `json:"SnapshotType"`
	SnapshotCreateTime          string `json:"SnapshotCreateTime"`
	ClusterCreateTime           string `json:"ClusterCreateTime"`
	KmsKeyID                    string `json:"KmsKeyId"`
	VpcID                       string `json:"VpcId"`
	// RestoreAttributeValues holds the account IDs (or "all") authorized to
	// copy/restore this manual snapshot via the "restore" DB cluster snapshot
	// attribute -- the only attribute Neptune's API models (see
	// ModifyDBClusterSnapshotAttribute/DescribeDBClusterSnapshotAttributes).
	RestoreAttributeValues           []string `json:"RestoreAttributeValues"`
	Port                             int      `json:"Port"`
	PercentProgress                  int      `json:"PercentProgress"`
	AllocatedStorage                 int      `json:"AllocatedStorage"`
	StorageEncrypted                 bool     `json:"StorageEncrypted"`
	IAMDatabaseAuthenticationEnabled bool     `json:"IAMDatabaseAuthenticationEnabled"`
}

// DBParameterGroup represents a Neptune DB parameter group.
type DBParameterGroup struct {
	// region is the AWS region this parameter group belongs to; see
	// DBCluster.region for the composite-key rationale.
	region                 string
	DBParameterGroupName   string `json:"DBParameterGroupName"`
	DBParameterGroupArn    string `json:"DBParameterGroupArn"`
	DBParameterGroupFamily string `json:"DBParameterGroupFamily"`
	Description            string `json:"Description"`
}

// DBClusterEndpoint represents a Neptune DB cluster custom endpoint.
type DBClusterEndpoint struct {
	// region is the AWS region this cluster endpoint belongs to; see
	// DBCluster.region for the composite-key rationale.
	region                              string
	DBClusterEndpointIdentifier         string   `json:"DBClusterEndpointIdentifier"`
	DBClusterIdentifier                 string   `json:"DBClusterIdentifier"`
	DBClusterEndpointArn                string   `json:"DBClusterEndpointArn"`
	DBClusterEndpointResourceIdentifier string   `json:"DBClusterEndpointResourceIdentifier"`
	EndpointType                        string   `json:"EndpointType"`
	CustomEndpointType                  string   `json:"CustomEndpointType"`
	Status                              string   `json:"Status"`
	Endpoint                            string   `json:"Endpoint"`
	StaticMembers                       []string `json:"StaticMembers"`
	ExcludedMembers                     []string `json:"ExcludedMembers"`
}

// EventSubscription represents a Neptune event subscription.
type EventSubscription struct {
	// region is the AWS region this event subscription belongs to; see
	// DBCluster.region for the composite-key rationale.
	region                   string
	CustSubscriptionID       string   `json:"CustSubscriptionID"`
	SnsTopicARN              string   `json:"SnsTopicARN"`
	EventSubscriptionArn     string   `json:"EventSubscriptionArn"`
	Status                   string   `json:"Status"`
	SourceType               string   `json:"SourceType"`
	SubscriptionCreationTime string   `json:"SubscriptionCreationTime"`
	SourceIDs                []string `json:"SourceIDs"`
	EventCategoriesList      []string `json:"EventCategoriesList"`
	Enabled                  bool     `json:"Enabled"`
}

// GlobalCluster represents a Neptune global cluster.
type GlobalCluster struct {
	GlobalClusterIdentifier string                `json:"GlobalClusterIdentifier"`
	GlobalClusterArn        string                `json:"GlobalClusterArn"`
	GlobalClusterResourceID string                `json:"GlobalClusterResourceId"`
	Status                  string                `json:"Status"`
	Engine                  string                `json:"Engine"`
	EngineVersion           string                `json:"EngineVersion"`
	GlobalClusterMembers    []GlobalClusterMember `json:"GlobalClusterMembers"`
	StorageEncrypted        bool                  `json:"StorageEncrypted"`
	DeletionProtection      bool                  `json:"DeletionProtection"`
}

// GlobalClusterMember represents a member cluster in a global cluster.
type GlobalClusterMember struct {
	DBClusterARN string `json:"DBClusterARN"`
	IsWriter     bool   `json:"IsWriter"`
}

// InMemoryBackend is a thread-safe in-memory backend for Neptune.
//
// Eight resource collections that were previously nested by region (outer key
// = region, e.g. map[string]map[string]*DBCluster) are now each a single flat
// *store.Table keyed by the composite "region|id" string (see store_setup.go),
// with a companion *store.Index grouping entries by region for per-region
// scans -- the same region-qualified-table pattern services/secretsmanager
// and services/cloudwatchlogs use. GlobalClusters are global/partition-scoped
// (like AWS) and were already flat, so they became a plain (non-composite-key)
// *store.Table. clusterRoles and tags remain raw nested maps: their values
// (a bare []string / []Tag) carry no identity of their own to key a
// store.Table by (see store_setup.go's doc comment for the full rationale).
type InMemoryBackend struct {
	registry                       *store.Registry
	clusters                       *store.Table[DBCluster]
	clustersByRegion               *store.Index[DBCluster]
	instances                      *store.Table[DBInstance]
	instancesByRegion              *store.Index[DBInstance]
	subnetGroups                   *store.Table[DBSubnetGroup]
	subnetGroupsByRegion           *store.Index[DBSubnetGroup]
	clusterParameterGroups         *store.Table[DBClusterParameterGroup]
	clusterParameterGroupsByRegion *store.Index[DBClusterParameterGroup]
	clusterSnapshots               *store.Table[DBClusterSnapshot]
	clusterSnapshotsByRegion       *store.Index[DBClusterSnapshot]
	parameterGroups                *store.Table[DBParameterGroup]
	parameterGroupsByRegion        *store.Index[DBParameterGroup]
	clusterEndpoints               *store.Table[DBClusterEndpoint]
	clusterEndpointsByRegion       *store.Index[DBClusterEndpoint]
	eventSubscriptions             *store.Table[EventSubscription]
	eventSubscriptionsByRegion     *store.Index[EventSubscription]
	globalClusters                 *store.Table[GlobalCluster] // global/partition-scoped, not region-nested
	clusterRoles                   map[string]map[string][]string
	tags                           map[string]map[string][]Tag
	mu                             *lockmetrics.RWMutex
	accountID                      string
	region                         string
}

// NewInMemoryBackend creates a new in-memory Neptune backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:     store.NewRegistry(),
		clusterRoles: make(map[string]map[string][]string),
		tags:         make(map[string]map[string][]Tag),
		accountID:    accountID,
		region:       region,
		mu:           lockmetrics.New("neptune"),
	}
	registerAllTables(b)

	return b
}

// Region returns the backend's AWS region.
func (b *InMemoryBackend) Region() string { return b.region }

// regionKey builds the composite store.Table primary key ("region|id") shared
// by every region-qualified table registered in store_setup.go.
func regionKey(region, id string) string { return region + "|" + id }

// The following Get/Has/Put/Delete/InRegion helpers replace the old lazy
// per-region map accessors (clustersStore(region) etc.) with store.Table /
// store.Index operations. Callers must still hold b.mu, exactly as before --
// store.Table performs no locking of its own (see pkgs/store's package doc).

func (b *InMemoryBackend) clusterGet(region, id string) (*DBCluster, bool) {
	return b.clusters.Get(regionKey(region, id))
}

func (b *InMemoryBackend) clusterHas(region, id string) bool {
	return b.clusters.Has(regionKey(region, id))
}

func (b *InMemoryBackend) clusterPut(v *DBCluster) { b.clusters.Put(v) }

func (b *InMemoryBackend) clusterDelete(region, id string) { b.clusters.Delete(regionKey(region, id)) }

func (b *InMemoryBackend) clustersInRegion(region string) []*DBCluster {
	return b.clustersByRegion.Get(region)
}

func (b *InMemoryBackend) instanceGet(region, id string) (*DBInstance, bool) {
	return b.instances.Get(regionKey(region, id))
}

func (b *InMemoryBackend) instanceHas(region, id string) bool {
	return b.instances.Has(regionKey(region, id))
}

func (b *InMemoryBackend) instancePut(v *DBInstance) { b.instances.Put(v) }

func (b *InMemoryBackend) instanceDelete(region, id string) {
	b.instances.Delete(regionKey(region, id))
}

func (b *InMemoryBackend) instancesInRegion(region string) []*DBInstance {
	return b.instancesByRegion.Get(region)
}

func (b *InMemoryBackend) subnetGroupGet(region, name string) (*DBSubnetGroup, bool) {
	return b.subnetGroups.Get(regionKey(region, name))
}

func (b *InMemoryBackend) subnetGroupHas(region, name string) bool {
	return b.subnetGroups.Has(regionKey(region, name))
}

func (b *InMemoryBackend) subnetGroupPut(v *DBSubnetGroup) { b.subnetGroups.Put(v) }

func (b *InMemoryBackend) subnetGroupDelete(region, name string) {
	b.subnetGroups.Delete(regionKey(region, name))
}

func (b *InMemoryBackend) subnetGroupsInRegion(region string) []*DBSubnetGroup {
	return b.subnetGroupsByRegion.Get(region)
}

func (b *InMemoryBackend) clusterParameterGroupGet(
	region, name string,
) (*DBClusterParameterGroup, bool) {
	return b.clusterParameterGroups.Get(regionKey(region, name))
}

func (b *InMemoryBackend) clusterParameterGroupHas(region, name string) bool {
	return b.clusterParameterGroups.Has(regionKey(region, name))
}

func (b *InMemoryBackend) clusterParameterGroupPut(v *DBClusterParameterGroup) {
	b.clusterParameterGroups.Put(v)
}

func (b *InMemoryBackend) clusterParameterGroupDelete(region, name string) {
	b.clusterParameterGroups.Delete(regionKey(region, name))
}

func (b *InMemoryBackend) clusterParameterGroupsInRegion(region string) []*DBClusterParameterGroup {
	return b.clusterParameterGroupsByRegion.Get(region)
}

func (b *InMemoryBackend) clusterSnapshotGet(region, id string) (*DBClusterSnapshot, bool) {
	return b.clusterSnapshots.Get(regionKey(region, id))
}

func (b *InMemoryBackend) clusterSnapshotHas(region, id string) bool {
	return b.clusterSnapshots.Has(regionKey(region, id))
}

func (b *InMemoryBackend) clusterSnapshotPut(v *DBClusterSnapshot) { b.clusterSnapshots.Put(v) }

func (b *InMemoryBackend) clusterSnapshotDelete(region, id string) {
	b.clusterSnapshots.Delete(regionKey(region, id))
}

func (b *InMemoryBackend) clusterSnapshotsInRegion(region string) []*DBClusterSnapshot {
	return b.clusterSnapshotsByRegion.Get(region)
}

func (b *InMemoryBackend) parameterGroupGet(region, name string) (*DBParameterGroup, bool) {
	return b.parameterGroups.Get(regionKey(region, name))
}

func (b *InMemoryBackend) parameterGroupHas(region, name string) bool {
	return b.parameterGroups.Has(regionKey(region, name))
}

func (b *InMemoryBackend) parameterGroupPut(v *DBParameterGroup) { b.parameterGroups.Put(v) }

func (b *InMemoryBackend) parameterGroupDelete(region, name string) {
	b.parameterGroups.Delete(regionKey(region, name))
}

func (b *InMemoryBackend) parameterGroupsInRegion(region string) []*DBParameterGroup {
	return b.parameterGroupsByRegion.Get(region)
}

func (b *InMemoryBackend) clusterEndpointGet(region, id string) (*DBClusterEndpoint, bool) {
	return b.clusterEndpoints.Get(regionKey(region, id))
}

func (b *InMemoryBackend) clusterEndpointHas(region, id string) bool {
	return b.clusterEndpoints.Has(regionKey(region, id))
}

func (b *InMemoryBackend) clusterEndpointPut(v *DBClusterEndpoint) { b.clusterEndpoints.Put(v) }

func (b *InMemoryBackend) clusterEndpointDelete(region, id string) {
	b.clusterEndpoints.Delete(regionKey(region, id))
}

func (b *InMemoryBackend) clusterEndpointsInRegion(region string) []*DBClusterEndpoint {
	return b.clusterEndpointsByRegion.Get(region)
}

func (b *InMemoryBackend) eventSubscriptionGet(region, name string) (*EventSubscription, bool) {
	return b.eventSubscriptions.Get(regionKey(region, name))
}

func (b *InMemoryBackend) eventSubscriptionHas(region, name string) bool {
	return b.eventSubscriptions.Has(regionKey(region, name))
}

func (b *InMemoryBackend) eventSubscriptionPut(v *EventSubscription) { b.eventSubscriptions.Put(v) }

func (b *InMemoryBackend) eventSubscriptionDelete(region, name string) {
	b.eventSubscriptions.Delete(regionKey(region, name))
}

func (b *InMemoryBackend) eventSubscriptionsInRegion(region string) []*EventSubscription {
	return b.eventSubscriptionsByRegion.Get(region)
}

// The following lazy per-region store helpers return the resource map for the
// given region, creating it on first use. Callers must hold b.mu. clusterRoles
// and tags remain raw maps -- see the InMemoryBackend doc comment for why.

func (b *InMemoryBackend) clusterRolesStore(region string) map[string][]string {
	if b.clusterRoles[region] == nil {
		b.clusterRoles[region] = make(map[string][]string)
	}

	return b.clusterRoles[region]
}

func (b *InMemoryBackend) tagsStore(region string) map[string][]Tag {
	if b.tags[region] == nil {
		b.tags[region] = make(map[string][]Tag)
	}

	return b.tags[region]
}

// cloneCluster deep-copies a DBCluster to avoid shared slice/pointer mutation.
func cloneCluster(c *DBCluster) DBCluster {
	cp := *c
	cp.DBClusterMembers = make([]DBClusterMember, len(c.DBClusterMembers))
	copy(cp.DBClusterMembers, c.DBClusterMembers)
	cp.AssociatedRoles = make([]string, len(c.AssociatedRoles))
	copy(cp.AssociatedRoles, c.AssociatedRoles)
	cp.VpcSecurityGroupIDs = make([]string, len(c.VpcSecurityGroupIDs))
	copy(cp.VpcSecurityGroupIDs, c.VpcSecurityGroupIDs)
	cp.AvailabilityZones = make([]string, len(c.AvailabilityZones))
	copy(cp.AvailabilityZones, c.AvailabilityZones)
	if c.ServerlessV2ScalingConfig != nil {
		sv2 := *c.ServerlessV2ScalingConfig
		cp.ServerlessV2ScalingConfig = &sv2
	}
	if c.MasterUserManagedSecret != nil {
		ms := *c.MasterUserManagedSecret
		cp.MasterUserManagedSecret = &ms
	}

	return cp
}

// cloneSubnetGroup returns a deep copy of a subnet group (with its SubnetIDs slice copied).
func cloneSubnetGroup(sg *DBSubnetGroup) DBSubnetGroup {
	cp := *sg
	cp.SubnetIDs = make([]string, len(sg.SubnetIDs))
	copy(cp.SubnetIDs, sg.SubnetIDs)

	return cp
}

// cloneClusterSnapshot returns a deep copy of a cluster snapshot (with its
// RestoreAttributeValues slice copied, so a caller mutating the returned copy
// -- or a later ModifyDBClusterSnapshotAttribute call mutating the stored
// original -- cannot alias the other's backing array).
func cloneClusterSnapshot(snap *DBClusterSnapshot) DBClusterSnapshot {
	cp := *snap
	cp.RestoreAttributeValues = make([]string, len(snap.RestoreAttributeValues))
	copy(cp.RestoreAttributeValues, snap.RestoreAttributeValues)

	return cp
}

// cloneEventSubscription returns a deep copy of an event subscription (with its slices copied).
func cloneEventSubscription(sub *EventSubscription) EventSubscription {
	cp := *sub
	cp.SourceIDs = make([]string, len(sub.SourceIDs))
	copy(cp.SourceIDs, sub.SourceIDs)
	cp.EventCategoriesList = make([]string, len(sub.EventCategoriesList))
	copy(cp.EventCategoriesList, sub.EventCategoriesList)

	return cp
}

// resolveCopyDescription returns the target description for a copy operation,
// defaulting to the source's description when the requested target is empty.
func resolveCopyDescription(targetDescription, sourceDescription string) string {
	if targetDescription == "" {
		return sourceDescription
	}

	return targetDescription
}

// copyPreconditions validates the source/target names for a copy operation and
// returns the source value via get. notFound is returned when the source is
// missing; alreadyExists when the target already exists. get is a lookup
// closure (rather than a raw map) because store.Table does not expose its
// underlying map -- see e.g. CopyDBClusterParameterGroup's call site, which
// closes over the region to look up region-qualified keys.
func copyPreconditions[V any](
	get func(name string) (*V, bool),
	sourceName, targetName string,
	missingSourceMsg, missingTargetMsg string,
	notFound, alreadyExists error,
) (*V, error) {
	if sourceName == "" {
		return nil, fmt.Errorf("%w: %s", ErrInvalidParameter, missingSourceMsg)
	}

	if targetName == "" {
		return nil, fmt.Errorf("%w: %s", ErrInvalidParameter, missingTargetMsg)
	}

	src, exists := get(sourceName)
	if !exists {
		return nil, fmt.Errorf("%w: %s", notFound, sourceName)
	}

	if _, targetExists := get(targetName); targetExists {
		return nil, fmt.Errorf("%w: %s", alreadyExists, targetName)
	}

	return src, nil
}

// clusterARN returns the region-scoped ARN for a Neptune DB cluster.
func (b *InMemoryBackend) clusterARN(region, id string) string {
	return arn.Build("neptune", region, b.accountID, "cluster:"+id)
}

// instanceARN returns the region-scoped ARN for a Neptune DB instance.
func (b *InMemoryBackend) instanceARN(region, id string) string {
	return arn.Build("neptune", region, b.accountID, "db:"+id)
}

// subnetGroupARN returns the region-scoped ARN for a Neptune DB subnet group.
func (b *InMemoryBackend) subnetGroupARN(region, name string) string {
	return arn.Build("rds", region, b.accountID, "subgrp:"+name)
}

// clusterParameterGroupARN returns the region-scoped ARN for a Neptune DB cluster parameter group.
func (b *InMemoryBackend) clusterParameterGroupARN(region, name string) string {
	return arn.Build("rds", region, b.accountID, "cluster-pg:"+name)
}

// clusterSnapshotARN returns the region-scoped ARN for a Neptune DB cluster snapshot.
func (b *InMemoryBackend) clusterSnapshotARN(region, id string) string {
	return arn.Build("rds", region, b.accountID, "cluster-snapshot:"+id)
}

// parameterGroupARN returns the region-scoped ARN for a Neptune DB parameter group.
func (b *InMemoryBackend) parameterGroupARN(region, name string) string {
	return arn.Build("rds", region, b.accountID, "pg:"+name)
}

// eventSubscriptionARN returns the region-scoped ARN for a Neptune event subscription.
func (b *InMemoryBackend) eventSubscriptionARN(region, name string) string {
	return arn.Build("rds", region, b.accountID, "es:"+name)
}

// clusterEndpointARN returns the region-scoped ARN for a Neptune DB cluster endpoint.
func (b *InMemoryBackend) clusterEndpointARN(region, id string) string {
	return arn.Build("rds", region, b.accountID, "cluster-endpoint:"+id)
}

// globalClusterARN returns the partition-scoped ARN for a Neptune global cluster.
func (b *InMemoryBackend) globalClusterARN(id string) string {
	return arn.Build("rds", "", b.accountID, "global-cluster:"+id)
}

// CreateDBCluster creates a new Neptune DB cluster.
func (b *InMemoryBackend) CreateDBCluster(
	ctx context.Context,
	id, paramGroupName string,
	port int,
	opts DBClusterCreateOptions,
) (*DBCluster, error) {
	backupRetention, err := validateCreateClusterParams(id, port, opts)
	if err != nil {
		return nil, err
	}
	region := getRegion(ctx, b.region)
	b.mu.Lock("CreateDBCluster")
	defer b.mu.Unlock()
	if b.clusterHas(region, id) {
		return nil, fmt.Errorf("%w: cluster %s already exists", ErrClusterAlreadyExists, id)
	}
	cluster := b.buildNewCluster(region, id, paramGroupName, port, backupRetention, opts)
	b.clusterPut(cluster)
	cp := cloneCluster(cluster)

	return &cp, nil
}

// validateCreateClusterParams validates CreateDBCluster inputs and returns the
// effective backup retention period to use.
func validateCreateClusterParams(
	id string, port int, opts DBClusterCreateOptions,
) (int, error) {
	if err := validateNeptuneIdentifier(id, "DBClusterIdentifier"); err != nil {
		return 0, err
	}
	if port != 0 && (port < minNeptunePort || port > maxNeptunePort) {
		return 0, fmt.Errorf(
			"%w: Port %d is not a valid Neptune port; must be between %d and %d",
			ErrInvalidParameter, port, minNeptunePort, maxNeptunePort,
		)
	}
	backupRetention := defaultBackupRetentionPeriod
	if opts.BackupRetentionPeriod != 0 {
		backupRetention = opts.BackupRetentionPeriod
	}
	if backupRetention < minBackupRetentionPeriod || backupRetention > maxBackupRetentionPeriod {
		return 0, fmt.Errorf(
			"%w: BackupRetentionPeriod %d is not valid; must be between %d and %d",
			ErrInvalidParameter,
			backupRetention,
			minBackupRetentionPeriod,
			maxBackupRetentionPeriod,
		)
	}

	return backupRetention, nil
}

// buildNewCluster constructs a DBCluster from the create options, applying defaults.
func (b *InMemoryBackend) buildNewCluster(
	region, id, paramGroupName string,
	port, backupRetention int,
	opts DBClusterCreateOptions,
) *DBCluster {
	if paramGroupName == "" {
		paramGroupName = pgFamilyDefaultNeptune13
	}
	if port <= 0 {
		port = defaultNeptunePort
	}
	engineVersion := defaultEngineVersion
	if opts.EngineVersion != "" {
		engineVersion = opts.EngineVersion
	}
	engineMode := engineModeProvisioned
	if opts.EngineMode != "" {
		engineMode = opts.EngineMode
	}
	storageType := defaultStorageType
	if opts.StorageType != "" {
		storageType = opts.StorageType
	}
	endpoint := fmt.Sprintf("%s.cluster.%s.neptune.amazonaws.com", id, region)
	readerEndpoint := fmt.Sprintf(
		"%s.cluster-ro.%s.neptune.amazonaws.com",
		id,
		region,
	)
	hostedZoneID := fmt.Sprintf("Z%s", strings.ToUpper(region))
	vpcSGs := make([]string, len(opts.VpcSecurityGroupIDs))
	copy(vpcSGs, opts.VpcSecurityGroupIDs)
	azs := make([]string, len(opts.AvailabilityZones))
	copy(azs, opts.AvailabilityZones)
	cluster := &DBCluster{
		region:                          region,
		DBClusterIdentifier:             id,
		DBClusterArn:                    b.clusterARN(region, id),
		DBClusterResourceID:             fmt.Sprintf("cluster-%s", id),
		ClusterCreateTime:               nowISO8601(),
		Engine:                          neptuneEngine,
		EngineVersion:                   engineVersion,
		EngineMode:                      engineMode,
		Status:                          clusterStatusAvailable,
		DBClusterParameterGroupName:     paramGroupName,
		DBSubnetGroupName:               opts.DBSubnetGroupName,
		Endpoint:                        endpoint,
		ReaderEndpoint:                  readerEndpoint,
		Port:                            port,
		DBClusterMembers:                []DBClusterMember{},
		AssociatedRoles:                 []string{},
		VpcSecurityGroupIDs:             vpcSGs,
		AvailabilityZones:               azs,
		BackupRetentionPeriod:           backupRetention,
		AllocatedStorage:                defaultAllocatedStorage,
		StorageEncrypted:                opts.StorageEncrypted,
		EnableIAMDatabaseAuthentication: opts.EnableIAMDatabaseAuthentication,
		DeletionProtection:              opts.DeletionProtection,
		CopyTagsToSnapshot:              opts.CopyTagsToSnapshot,
		PreferredBackupWindow:           opts.PreferredBackupWindow,
		PreferredMaintenanceWindow:      opts.PreferredMaintenanceWindow,
		KmsKeyID:                        opts.KmsKeyID,
		ServerlessV2ScalingConfig:       opts.ServerlessV2ScalingConfig,
		MasterUsername:                  opts.MasterUsername,
		StorageType:                     storageType,
		HostedZoneID:                    hostedZoneID,
	}
	if opts.ManageMasterUserPassword {
		cluster.MasterUserManagedSecret = &MasterUserManagedSecret{
			SecretARN: fmt.Sprintf(
				"arn:aws:secretsmanager:%s:%s:secret:rds!cluster-%s",
				region,
				b.accountID,
				id,
			),
			SecretStatus: subscriptionStatusActive,
		}
	}

	return cluster
}

// DBClusterFilters holds filter values for DescribeDBClusters.
type DBClusterFilters struct {
	Engine        string
	EngineVersion string
	Status        string
}

// DescribeDBClusters returns all Neptune DB clusters or a specific one.
// Filters (when set) restrict results to matching clusters.
func (b *InMemoryBackend) DescribeDBClusters(
	ctx context.Context,
	id string,
	filters DBClusterFilters,
) ([]DBCluster, error) {
	region := getRegion(ctx, b.region)
	b.mu.RLock("DescribeDBClusters")
	defer b.mu.RUnlock()
	if id != "" {
		c, exists := b.clusterGet(region, id)
		if !exists {
			return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
		}

		return []DBCluster{cloneCluster(c)}, nil
	}
	clusters := b.clustersInRegion(region)
	result := make([]DBCluster, 0, len(clusters))
	for _, c := range clusters {
		if filters.Engine != "" && c.Engine != filters.Engine {
			continue
		}
		if filters.EngineVersion != "" && c.EngineVersion != filters.EngineVersion {
			continue
		}
		if filters.Status != "" && c.Status != filters.Status {
			continue
		}
		result = append(result, cloneCluster(c))
	}
	slices.SortFunc(result, func(a, b DBCluster) int {
		return strings.Compare(a.DBClusterIdentifier, b.DBClusterIdentifier)
	})

	return result, nil
}

// DeleteDBCluster deletes a Neptune DB cluster and all associated DB instances.
func (b *InMemoryBackend) DeleteDBCluster(
	ctx context.Context,
	id string,
	opts DBClusterDeleteOptions,
) (*DBCluster, error) {
	region := getRegion(ctx, b.region)
	// Validate FinalDBSnapshotIdentifier before acquiring the lock.
	if !opts.SkipFinalSnapshot && opts.FinalDBSnapshotIdentifier == "" {
		return nil, fmt.Errorf(
			"%w: FinalDBSnapshotIdentifier is required when SkipFinalSnapshot is false",
			ErrSnapshotRequired,
		)
	}
	if !opts.SkipFinalSnapshot && opts.FinalDBSnapshotIdentifier != "" {
		if err := validateNeptuneIdentifier(
			opts.FinalDBSnapshotIdentifier,
			"FinalDBSnapshotIdentifier",
		); err != nil {
			return nil, err
		}
	}
	b.mu.Lock("DeleteDBCluster")
	defer b.mu.Unlock()
	c, exists := b.clusterGet(region, id)
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}
	if c.DeletionProtection {
		return nil, fmt.Errorf(
			"%w: cluster %s cannot be deleted because deletion protection is enabled",
			ErrInvalidDBClusterStateFault,
			id,
		)
	}
	cp := cloneCluster(c)
	// Create a final snapshot when requested.
	if !opts.SkipFinalSnapshot && opts.FinalDBSnapshotIdentifier != "" {
		if !b.clusterSnapshotHas(region, opts.FinalDBSnapshotIdentifier) {
			b.clusterSnapshotPut(&DBClusterSnapshot{
				region:                      region,
				DBClusterSnapshotIdentifier: opts.FinalDBSnapshotIdentifier,
				DBClusterSnapshotArn: b.clusterSnapshotARN(
					region,
					opts.FinalDBSnapshotIdentifier,
				),
				DBClusterIdentifier:              id,
				Engine:                           neptuneEngine,
				EngineVersion:                    c.EngineVersion,
				Status:                           snapshotStatusAvailable,
				StorageEncrypted:                 c.StorageEncrypted,
				KmsKeyID:                         c.KmsKeyID,
				IAMDatabaseAuthenticationEnabled: c.EnableIAMDatabaseAuthentication,
				Port:                             c.Port,
				PercentProgress:                  percentProgressComplete,
				AllocatedStorage:                 c.AllocatedStorage,
				SnapshotType:                     snapshotSourceManual,
				SnapshotCreateTime:               nowISO8601(),
				ClusterCreateTime:                c.ClusterCreateTime,
			})
		}
	}
	b.clusterDelete(region, id)
	delete(b.tagsStore(region), b.clusterARN(region, id))
	delete(b.clusterRolesStore(region), id)

	// Clean up all instances associated with this cluster. slices.Clone first:
	// instanceDelete mutates the byRegion index that instancesInRegion returns,
	// so iterating the live slice while deleting from it would be unsafe.
	tagStore := b.tagsStore(region)
	for _, inst := range slices.Clone(b.instancesInRegion(region)) {
		if inst.DBClusterIdentifier == id {
			b.instanceDelete(region, inst.DBInstanceIdentifier)
			delete(tagStore, b.instanceARN(region, inst.DBInstanceIdentifier))
		}
	}

	// Clean up all custom endpoints associated with this cluster (same
	// clone-before-delete rationale as above).
	for _, ep := range slices.Clone(b.clusterEndpointsInRegion(region)) {
		if ep.DBClusterIdentifier == id {
			b.clusterEndpointDelete(region, ep.DBClusterEndpointIdentifier)
		}
	}

	return &cp, nil
}

// ModifyDBCluster modifies a Neptune DB cluster.
func (b *InMemoryBackend) ModifyDBCluster(
	ctx context.Context, id, paramGroupName string, opts DBClusterModifyOptions,
) (*DBCluster, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("ModifyDBCluster")
	defer b.mu.Unlock()
	c, exists := b.clusterGet(region, id)
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}
	if paramGroupName != "" {
		c.DBClusterParameterGroupName = paramGroupName
	}
	applyClusterScalarModifications(c, opts)
	if err := applyClusterBackupRetention(c, opts); err != nil {
		return nil, err
	}
	applyClusterSecurityGroups(c, opts)
	b.applyClusterMasterSecret(c, region, id, opts)
	cp := cloneCluster(c)

	return &cp, nil
}

// applyClusterScalarModifications applies the optional scalar fields of opts onto c.
func applyClusterScalarModifications(c *DBCluster, opts DBClusterModifyOptions) {
	if opts.EngineVersion != "" {
		c.EngineVersion = opts.EngineVersion
	}
	if opts.PreferredBackupWindow != "" {
		c.PreferredBackupWindow = opts.PreferredBackupWindow
	}
	if opts.PreferredMaintenanceWindow != "" {
		c.PreferredMaintenanceWindow = opts.PreferredMaintenanceWindow
	}
	if opts.IamAuthSet {
		c.EnableIAMDatabaseAuthentication = opts.EnableIAMDatabaseAuthentication
	}
	if opts.DeletionProtectionSet {
		c.DeletionProtection = opts.DeletionProtection
	}
	if opts.ServerlessV2ScalingConfig != nil {
		sv2 := *opts.ServerlessV2ScalingConfig
		c.ServerlessV2ScalingConfig = &sv2
	}
	if opts.CopyTagsToSnapshotSet {
		c.CopyTagsToSnapshot = opts.CopyTagsToSnapshot
	}
}

// applyClusterBackupRetention validates and applies the backup retention period.
func applyClusterBackupRetention(c *DBCluster, opts DBClusterModifyOptions) error {
	if !opts.BackupRetentionPeriodSet {
		return nil
	}
	if opts.BackupRetentionPeriod < minBackupRetentionPeriod ||
		opts.BackupRetentionPeriod > maxBackupRetentionPeriod {
		return fmt.Errorf(
			"%w: BackupRetentionPeriod %d is not valid; must be between %d and %d",
			ErrInvalidParameter,
			opts.BackupRetentionPeriod,
			minBackupRetentionPeriod,
			maxBackupRetentionPeriod,
		)
	}
	c.BackupRetentionPeriod = opts.BackupRetentionPeriod

	return nil
}

// applyClusterSecurityGroups replaces the cluster VPC security groups when provided.
func applyClusterSecurityGroups(c *DBCluster, opts DBClusterModifyOptions) {
	if len(opts.VpcSecurityGroupIDs) == 0 {
		return
	}
	vpcSGs := make([]string, len(opts.VpcSecurityGroupIDs))
	copy(vpcSGs, opts.VpcSecurityGroupIDs)
	c.VpcSecurityGroupIDs = vpcSGs
}

// applyClusterMasterSecret provisions a managed master-user secret when requested.
func (b *InMemoryBackend) applyClusterMasterSecret(
	c *DBCluster, region, id string, opts DBClusterModifyOptions,
) {
	if !opts.ManageMasterUserPassword || c.MasterUserManagedSecret != nil {
		return
	}
	c.MasterUserManagedSecret = &MasterUserManagedSecret{
		SecretARN: fmt.Sprintf(
			"arn:aws:secretsmanager:%s:%s:secret:rds!cluster-%s",
			region,
			b.accountID,
			id,
		),
		SecretStatus: subscriptionStatusActive,
	}
}

// StopDBCluster stops a Neptune DB cluster.
func (b *InMemoryBackend) StopDBCluster(ctx context.Context, id string) (*DBCluster, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("StopDBCluster")
	defer b.mu.Unlock()
	c, exists := b.clusterGet(region, id)
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}
	if c.Status == clusterStatusStopped {
		return nil, fmt.Errorf(
			"%w: cluster %s is already stopped",
			ErrInvalidDBClusterStateFault,
			id,
		)
	}
	c.Status = clusterStatusStopped
	cp := cloneCluster(c)

	return &cp, nil
}

// StartDBCluster starts a stopped Neptune DB cluster.
func (b *InMemoryBackend) StartDBCluster(ctx context.Context, id string) (*DBCluster, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("StartDBCluster")
	defer b.mu.Unlock()
	c, exists := b.clusterGet(region, id)
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}
	if c.Status != clusterStatusStopped {
		return nil, fmt.Errorf(
			"%w: cluster %s is not in stopped state",
			ErrInvalidDBClusterStateFault,
			id,
		)
	}
	c.Status = clusterStatusAvailable
	cp := cloneCluster(c)

	return &cp, nil
}

// FailoverDBCluster triggers a failover for a Neptune DB cluster.
func (b *InMemoryBackend) FailoverDBCluster(
	ctx context.Context, id, targetInstanceID string,
) (*DBCluster, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("FailoverDBCluster")
	defer b.mu.Unlock()
	c, exists := b.clusterGet(region, id)
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}
	if err := promoteClusterMember(c, targetInstanceID); err != nil {
		return nil, err
	}
	cp := cloneCluster(c)

	return &cp, nil
}

// promoteClusterMember performs the state change of a failover: it promotes
// one non-writer member of c to writer and demotes the current writer,
// mirroring the DBClusterMembers.IsClusterWriter flip AWS performs. When
// targetInstanceID is empty, AWS (and this backend) picks any available
// reader; when set, it must name an existing member other than the current
// writer. A cluster with fewer than two members has no reader to fail over
// to, matching AWS's InvalidDBClusterStateFault in that situation.
func promoteClusterMember(c *DBCluster, targetInstanceID string) error {
	if len(c.DBClusterMembers) < minFailoverClusterMembers {
		return fmt.Errorf(
			"%w: cluster %s has no reader instance available to fail over to",
			ErrInvalidDBClusterStateFault,
			c.DBClusterIdentifier,
		)
	}
	targetIdx := -1
	for i, m := range c.DBClusterMembers {
		if targetInstanceID != "" {
			if m.DBInstanceIdentifier == targetInstanceID {
				targetIdx = i
			}

			continue
		}
		if !m.IsClusterWriter && targetIdx == -1 {
			targetIdx = i
		}
	}
	if targetIdx == -1 {
		return fmt.Errorf(
			"%w: target instance %s is not a valid failover target for cluster %s",
			ErrInvalidDBInstanceStateFault,
			targetInstanceID,
			c.DBClusterIdentifier,
		)
	}
	if c.DBClusterMembers[targetIdx].IsClusterWriter {
		return fmt.Errorf(
			"%w: target instance %s is already the writer for cluster %s",
			ErrInvalidDBInstanceStateFault,
			targetInstanceID,
			c.DBClusterIdentifier,
		)
	}
	for i := range c.DBClusterMembers {
		c.DBClusterMembers[i].IsClusterWriter = i == targetIdx
	}

	return nil
}

// CreateDBInstance creates a new Neptune DB instance.
func (b *InMemoryBackend) CreateDBInstance(
	ctx context.Context,
	id, clusterID, instanceClass string,
	opts DBInstanceCreateOptions,
) (*DBInstance, error) {
	if err := validateNeptuneIdentifier(id, "DBInstanceIdentifier"); err != nil {
		return nil, err
	}
	if opts.PromotionTier < 0 || opts.PromotionTier > maxPromotionTier {
		return nil, fmt.Errorf(
			"%w: PromotionTier %d is not valid; must be between 0 and %d",
			ErrInvalidParameter, opts.PromotionTier, maxPromotionTier,
		)
	}
	region := getRegion(ctx, b.region)
	b.mu.Lock("CreateDBInstance")
	defer b.mu.Unlock()
	if b.instanceHas(region, id) {
		return nil, fmt.Errorf("%w: instance %s already exists", ErrInstanceAlreadyExists, id)
	}
	if clusterID != "" {
		if !b.clusterHas(region, clusterID) {
			return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, clusterID)
		}
	}
	if instanceClass == "" {
		instanceClass = defaultInstanceClass
	}
	maintenanceWindow := defaultMaintenanceWindow
	if opts.PreferredMaintenanceWindow != "" {
		maintenanceWindow = opts.PreferredMaintenanceWindow
	}
	endpoint := fmt.Sprintf("%s.neptune.%s.amazonaws.com", id, region)
	engineVersion := defaultEngineVersion
	dbSubnetGroupName := ""
	if clusterID != "" {
		if cl, ok := b.clusterGet(region, clusterID); ok {
			engineVersion = cl.EngineVersion
			dbSubnetGroupName = cl.DBSubnetGroupName
		}
	}
	inst := &DBInstance{
		region:                          region,
		DBInstanceIdentifier:            id,
		DBInstanceArn:                   b.instanceARN(region, id),
		DBClusterIdentifier:             clusterID,
		DBInstanceClass:                 instanceClass,
		Engine:                          neptuneEngine,
		EngineVersion:                   engineVersion,
		DBInstanceStatus:                clusterStatusAvailable,
		InstanceCreateTime:              nowISO8601(),
		Endpoint:                        endpoint,
		Port:                            defaultNeptunePort,
		AutoMinorVersionUpgrade:         true,
		PreferredMaintenanceWindow:      maintenanceWindow,
		DBParameterGroupName:            opts.DBParameterGroupName,
		DBSubnetGroupName:               dbSubnetGroupName,
		PreferredBackupWindow:           opts.PreferredBackupWindow,
		AvailabilityZone:                opts.AvailabilityZone,
		CopyTagsToSnapshot:              opts.CopyTagsToSnapshot,
		EnableIAMDatabaseAuthentication: opts.EnableIAMDatabaseAuthentication,
		PromotionTier:                   opts.PromotionTier,
		StorageEncrypted:                opts.StorageEncrypted,
	}
	if opts.AutoMinorVersionUpgrade {
		inst.AutoMinorVersionUpgrade = opts.AutoMinorVersionUpgrade
	}
	b.instancePut(inst)
	if clusterID != "" {
		if cl, ok := b.clusterGet(region, clusterID); ok {
			isWriter := len(cl.DBClusterMembers) == 0
			cl.DBClusterMembers = append(cl.DBClusterMembers, DBClusterMember{
				DBInstanceIdentifier: id,
				IsClusterWriter:      isWriter,
			})
		}
	}
	cp := *inst

	return &cp, nil
}

// DescribeDBInstances returns all Neptune DB instances or a specific one by ID.
// The clusterFilter (when non-empty) restricts results to instances of that cluster.
func (b *InMemoryBackend) DescribeDBInstances(
	ctx context.Context,
	id, clusterFilter string,
) ([]DBInstance, error) {
	region := getRegion(ctx, b.region)
	b.mu.RLock("DescribeDBInstances")
	defer b.mu.RUnlock()
	if id != "" {
		inst, exists := b.instanceGet(region, id)
		if !exists {
			return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, id)
		}
		cp := *inst

		return []DBInstance{cp}, nil
	}
	instances := b.instancesInRegion(region)
	result := make([]DBInstance, 0, len(instances))
	for _, inst := range instances {
		if clusterFilter != "" && inst.DBClusterIdentifier != clusterFilter {
			continue
		}
		result = append(result, *inst)
	}
	slices.SortFunc(result, func(a, b DBInstance) int {
		return strings.Compare(a.DBInstanceIdentifier, b.DBInstanceIdentifier)
	})

	return result, nil
}

// DeleteDBInstance deletes a Neptune DB instance.
func (b *InMemoryBackend) DeleteDBInstance(ctx context.Context, id string) (*DBInstance, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("DeleteDBInstance")
	defer b.mu.Unlock()
	inst, exists := b.instanceGet(region, id)
	if !exists {
		return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, id)
	}
	cp := *inst
	b.instanceDelete(region, id)
	delete(b.tagsStore(region), b.instanceARN(region, id))
	if cp.DBClusterIdentifier != "" {
		if cl, ok := b.clusterGet(region, cp.DBClusterIdentifier); ok {
			members := make([]DBClusterMember, 0, len(cl.DBClusterMembers))
			for _, m := range cl.DBClusterMembers {
				if m.DBInstanceIdentifier != id {
					members = append(members, m)
				}
			}
			cl.DBClusterMembers = members
		}
	}

	return &cp, nil
}

// ModifyDBInstance modifies a Neptune DB instance.
func (b *InMemoryBackend) ModifyDBInstance(
	ctx context.Context,
	id, instanceClass string,
	opts DBInstanceModifyOptions,
) (*DBInstance, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("ModifyDBInstance")
	defer b.mu.Unlock()
	inst, exists := b.instanceGet(region, id)
	if !exists {
		return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, id)
	}
	if instanceClass != "" {
		inst.DBInstanceClass = instanceClass
	}
	if opts.DBParameterGroupName != "" {
		inst.DBParameterGroupName = opts.DBParameterGroupName
	}
	if opts.PreferredMaintenanceWindow != "" {
		inst.PreferredMaintenanceWindow = opts.PreferredMaintenanceWindow
	}
	if opts.PreferredBackupWindow != "" {
		inst.PreferredBackupWindow = opts.PreferredBackupWindow
	}
	if opts.AutoMinorVersionUpgradeSet {
		inst.AutoMinorVersionUpgrade = opts.AutoMinorVersionUpgrade
	}
	if opts.CopyTagsToSnapshotSet {
		inst.CopyTagsToSnapshot = opts.CopyTagsToSnapshot
	}
	if opts.IamAuthSet {
		inst.EnableIAMDatabaseAuthentication = opts.EnableIAMDatabaseAuthentication
	}
	if opts.PromotionTierSet {
		inst.PromotionTier = opts.PromotionTier
	}
	cp := *inst

	return &cp, nil
}

// RebootDBInstance reboots a Neptune DB instance.
func (b *InMemoryBackend) RebootDBInstance(ctx context.Context, id string) (*DBInstance, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("RebootDBInstance")
	defer b.mu.Unlock()
	inst, exists := b.instanceGet(region, id)
	if !exists {
		return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, id)
	}
	cp := *inst

	return &cp, nil
}

// CreateDBSubnetGroup creates a new Neptune DB subnet group.
func (b *InMemoryBackend) CreateDBSubnetGroup(
	ctx context.Context,
	name, description, vpcID string,
	subnetIDs []string,
) (*DBSubnetGroup, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: DBSubnetGroupName is required", ErrInvalidParameter)
	}
	region := getRegion(ctx, b.region)
	b.mu.Lock("CreateDBSubnetGroup")
	defer b.mu.Unlock()
	if b.subnetGroupHas(region, name) {
		return nil, fmt.Errorf(
			"%w: subnet group %s already exists",
			ErrSubnetGroupAlreadyExists,
			name,
		)
	}
	ids := make([]string, len(subnetIDs))
	copy(ids, subnetIDs)
	sg := &DBSubnetGroup{
		region:                   region,
		DBSubnetGroupName:        name,
		DBSubnetGroupArn:         b.subnetGroupARN(region, name),
		DBSubnetGroupDescription: description,
		VpcID:                    vpcID,
		Status:                   "Complete",
		SubnetIDs:                ids,
	}
	b.subnetGroupPut(sg)
	cp := *sg
	cp.SubnetIDs = make([]string, len(ids))
	copy(cp.SubnetIDs, ids)

	return &cp, nil
}

// DescribeDBSubnetGroups returns all Neptune DB subnet groups or a specific one.
func (b *InMemoryBackend) DescribeDBSubnetGroups(
	ctx context.Context,
	name string,
) ([]DBSubnetGroup, error) {
	region := getRegion(ctx, b.region)
	b.mu.RLock("DescribeDBSubnetGroups")
	defer b.mu.RUnlock()
	if name != "" {
		sg, exists := b.subnetGroupGet(region, name)
		if !exists {
			return nil, fmt.Errorf("%w: subnet group %s not found", ErrSubnetGroupNotFound, name)
		}

		return []DBSubnetGroup{cloneSubnetGroup(sg)}, nil
	}
	subnetGroups := b.subnetGroupsInRegion(region)
	result := make([]DBSubnetGroup, 0, len(subnetGroups))
	for _, sg := range subnetGroups {
		result = append(result, cloneSubnetGroup(sg))
	}
	slices.SortFunc(result, func(a, b DBSubnetGroup) int {
		return strings.Compare(a.DBSubnetGroupName, b.DBSubnetGroupName)
	})

	return result, nil
}

// DeleteDBSubnetGroup deletes a Neptune DB subnet group.
func (b *InMemoryBackend) DeleteDBSubnetGroup(ctx context.Context, name string) error {
	region := getRegion(ctx, b.region)
	b.mu.Lock("DeleteDBSubnetGroup")
	defer b.mu.Unlock()
	if !b.subnetGroupHas(region, name) {
		return fmt.Errorf("%w: subnet group %s not found", ErrSubnetGroupNotFound, name)
	}
	b.subnetGroupDelete(region, name)
	delete(b.tagsStore(region), b.subnetGroupARN(region, name))

	return nil
}

// validNeptuneParameterGroupFamily returns true for known Neptune parameter group families.
func validNeptuneParameterGroupFamily(family string) bool {
	return family == pgFamilyNeptune12 || family == pgFamilyNeptune13 || family == "neptune1.4"
}

// CreateDBClusterParameterGroup creates a Neptune DB cluster parameter group.
func (b *InMemoryBackend) CreateDBClusterParameterGroup(
	ctx context.Context,
	name, family, description string,
) (*DBClusterParameterGroup, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: DBClusterParameterGroupName is required", ErrInvalidParameter)
	}
	if family == "" || !validNeptuneParameterGroupFamily(family) {
		return nil, fmt.Errorf(
			"%w: DBParameterGroupFamily %q is not valid; must be one of neptune1.2, neptune1.3, neptune1.4",
			ErrInvalidParameter,
			family,
		)
	}
	region := getRegion(ctx, b.region)
	b.mu.Lock("CreateDBClusterParameterGroup")
	defer b.mu.Unlock()
	if b.clusterParameterGroupHas(region, name) {
		return nil, fmt.Errorf(
			"%w: cluster parameter group %s already exists",
			ErrClusterParameterGroupAlreadyExists,
			name,
		)
	}
	pg := &DBClusterParameterGroup{
		region:                      region,
		DBClusterParameterGroupName: name,
		DBClusterParameterGroupArn:  b.clusterParameterGroupARN(region, name),
		DBParameterGroupFamily:      family,
		Description:                 description,
	}
	b.clusterParameterGroupPut(pg)
	cp := *pg

	return &cp, nil
}

// DescribeDBClusterParameterGroups returns all Neptune cluster parameter groups or a specific one.
func (b *InMemoryBackend) DescribeDBClusterParameterGroups(
	ctx context.Context, name string,
) ([]DBClusterParameterGroup, error) {
	region := getRegion(ctx, b.region)
	b.mu.RLock("DescribeDBClusterParameterGroups")
	defer b.mu.RUnlock()
	if name != "" {
		pg, exists := b.clusterParameterGroupGet(region, name)
		if !exists {
			return nil, fmt.Errorf(
				"%w: cluster parameter group %s not found",
				ErrClusterParameterGroupNotFound,
				name,
			)
		}
		cp := *pg

		return []DBClusterParameterGroup{cp}, nil
	}
	groups := b.clusterParameterGroupsInRegion(region)
	result := make([]DBClusterParameterGroup, 0, len(groups))
	for _, pg := range groups {
		result = append(result, *pg)
	}
	slices.SortFunc(result, func(a, b DBClusterParameterGroup) int {
		return strings.Compare(a.DBClusterParameterGroupName, b.DBClusterParameterGroupName)
	})

	return result, nil
}

// DeleteDBClusterParameterGroup deletes a Neptune DB cluster parameter group.
func (b *InMemoryBackend) DeleteDBClusterParameterGroup(ctx context.Context, name string) error {
	region := getRegion(ctx, b.region)
	b.mu.Lock("DeleteDBClusterParameterGroup")
	defer b.mu.Unlock()
	if !b.clusterParameterGroupHas(region, name) {
		return fmt.Errorf(
			"%w: cluster parameter group %s not found",
			ErrClusterParameterGroupNotFound,
			name,
		)
	}
	b.clusterParameterGroupDelete(region, name)
	delete(b.tagsStore(region), b.clusterParameterGroupARN(region, name))

	return nil
}

// ModifyDBClusterParameterGroup modifies a Neptune DB cluster parameter group.
func (b *InMemoryBackend) ModifyDBClusterParameterGroup(
	ctx context.Context, name string,
) (*DBClusterParameterGroup, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("ModifyDBClusterParameterGroup")
	defer b.mu.Unlock()
	pg, exists := b.clusterParameterGroupGet(region, name)
	if !exists {
		return nil, fmt.Errorf(
			"%w: cluster parameter group %s not found",
			ErrClusterParameterGroupNotFound,
			name,
		)
	}
	cp := *pg

	return &cp, nil
}

// CreateDBClusterSnapshot creates a Neptune DB cluster snapshot.
func (b *InMemoryBackend) CreateDBClusterSnapshot(
	ctx context.Context, snapshotID, clusterID string,
) (*DBClusterSnapshot, error) {
	if snapshotID == "" {
		return nil, fmt.Errorf("%w: DBClusterSnapshotIdentifier is required", ErrInvalidParameter)
	}
	if clusterID == "" {
		return nil, fmt.Errorf("%w: DBClusterIdentifier is required", ErrInvalidParameter)
	}
	region := getRegion(ctx, b.region)
	b.mu.Lock("CreateDBClusterSnapshot")
	defer b.mu.Unlock()
	if b.clusterSnapshotHas(region, snapshotID) {
		return nil, fmt.Errorf(
			"%w: cluster snapshot %s already exists",
			ErrClusterSnapshotAlreadyExists,
			snapshotID,
		)
	}
	cl, exists := b.clusterGet(region, clusterID)
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, clusterID)
	}
	snap := &DBClusterSnapshot{
		region:                           region,
		DBClusterSnapshotIdentifier:      snapshotID,
		DBClusterSnapshotArn:             b.clusterSnapshotARN(region, snapshotID),
		DBClusterIdentifier:              clusterID,
		Engine:                           neptuneEngine,
		EngineVersion:                    cl.EngineVersion,
		Status:                           snapshotStatusAvailable,
		StorageEncrypted:                 cl.StorageEncrypted,
		KmsKeyID:                         cl.KmsKeyID,
		IAMDatabaseAuthenticationEnabled: cl.EnableIAMDatabaseAuthentication,
		Port:                             cl.Port,
		PercentProgress:                  percentProgressComplete,
		AllocatedStorage:                 cl.AllocatedStorage,
		SnapshotType:                     snapshotSourceManual,
		SnapshotCreateTime:               nowISO8601(),
		ClusterCreateTime:                cl.ClusterCreateTime,
	}
	b.clusterSnapshotPut(snap)
	cp := cloneClusterSnapshot(snap)

	return &cp, nil
}

// DescribeDBClusterSnapshots returns all Neptune cluster snapshots or a specific one.
// If clusterID is set, results are filtered to that cluster.
func (b *InMemoryBackend) DescribeDBClusterSnapshots(
	ctx context.Context, snapshotID, clusterID, snapshotTypeFilter string,
) ([]DBClusterSnapshot, error) {
	region := getRegion(ctx, b.region)
	b.mu.RLock("DescribeDBClusterSnapshots")
	defer b.mu.RUnlock()
	if snapshotID != "" {
		snap, exists := b.clusterSnapshotGet(region, snapshotID)
		if !exists {
			return nil, fmt.Errorf(
				"%w: cluster snapshot %s not found",
				ErrClusterSnapshotNotFound,
				snapshotID,
			)
		}
		cp := cloneClusterSnapshot(snap)

		return []DBClusterSnapshot{cp}, nil
	}
	snapshots := b.clusterSnapshotsInRegion(region)
	result := make([]DBClusterSnapshot, 0, len(snapshots))
	for _, snap := range snapshots {
		if clusterID != "" && snap.DBClusterIdentifier != clusterID {
			continue
		}
		if snapshotTypeFilter != "" && snap.SnapshotType != snapshotTypeFilter {
			continue
		}
		result = append(result, cloneClusterSnapshot(snap))
	}
	slices.SortFunc(result, func(a, b DBClusterSnapshot) int {
		return strings.Compare(a.DBClusterSnapshotIdentifier, b.DBClusterSnapshotIdentifier)
	})

	return result, nil
}

// DeleteDBClusterSnapshot deletes a Neptune DB cluster snapshot.
func (b *InMemoryBackend) DeleteDBClusterSnapshot(
	ctx context.Context,
	snapshotID string,
) (*DBClusterSnapshot, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("DeleteDBClusterSnapshot")
	defer b.mu.Unlock()
	snap, exists := b.clusterSnapshotGet(region, snapshotID)
	if !exists {
		return nil, fmt.Errorf(
			"%w: cluster snapshot %s not found",
			ErrClusterSnapshotNotFound,
			snapshotID,
		)
	}
	cp := cloneClusterSnapshot(snap)
	b.clusterSnapshotDelete(region, snapshotID)
	delete(b.tagsStore(region), b.clusterSnapshotARN(region, snapshotID))

	return &cp, nil
}

// dbClusterSnapshotRestoreAttribute is the only DB cluster snapshot attribute
// name Neptune's API models: it holds the account IDs (or "all") authorized
// to copy/restore a manual snapshot. See ModifyDBClusterSnapshotAttribute /
// DescribeDBClusterSnapshotAttributes.
const dbClusterSnapshotRestoreAttribute = "restore"

// ModifyDBClusterSnapshotAttribute adds and/or removes values from a Neptune
// DB cluster snapshot's "restore" attribute (the list of accounts authorized
// to copy/restore the snapshot). It returns the updated snapshot so callers
// can render the DBClusterSnapshotAttributesResult AWS includes in both the
// Modify and Describe responses.
func (b *InMemoryBackend) ModifyDBClusterSnapshotAttribute(
	ctx context.Context,
	snapshotID, attributeName string,
	valuesToAdd, valuesToRemove []string,
) (*DBClusterSnapshot, error) {
	if attributeName != dbClusterSnapshotRestoreAttribute {
		return nil, fmt.Errorf(
			"%w: AttributeName must be %q",
			ErrInvalidParameter,
			dbClusterSnapshotRestoreAttribute,
		)
	}
	region := getRegion(ctx, b.region)
	b.mu.Lock("ModifyDBClusterSnapshotAttribute")
	defer b.mu.Unlock()
	snap, exists := b.clusterSnapshotGet(region, snapshotID)
	if !exists {
		return nil, fmt.Errorf(
			"%w: cluster snapshot %s not found",
			ErrClusterSnapshotNotFound,
			snapshotID,
		)
	}
	remove := make(map[string]bool, len(valuesToRemove))
	for _, v := range valuesToRemove {
		remove[v] = true
	}
	kept := make([]string, 0, len(snap.RestoreAttributeValues))
	for _, v := range snap.RestoreAttributeValues {
		if !remove[v] {
			kept = append(kept, v)
		}
	}
	for _, v := range valuesToAdd {
		if !slices.Contains(kept, v) {
			kept = append(kept, v)
		}
	}
	snap.RestoreAttributeValues = kept
	cp := cloneClusterSnapshot(snap)

	return &cp, nil
}

// validateResourceARN checks whether an ARN refers to a known Neptune resource in the given region.
// Must be called while holding at least a read lock.
func (b *InMemoryBackend) validateResourceARN(region, arnStr string) error {
	// ARN format: arn:partition:service:region:account:type:id
	parts := strings.SplitN(arnStr, ":", arnPartCount)
	if len(parts) < arnPartCount {
		return fmt.Errorf("%w: invalid ARN format: %s", ErrInvalidParameter, arnStr)
	}
	resType, resID := parts[5], parts[6]
	switch resType {
	case "cluster":
		if !b.clusterHas(region, resID) {
			return fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, resID)
		}
	case "db":
		if !b.instanceHas(region, resID) {
			return fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, resID)
		}
	case "cluster-snapshot":
		if !b.clusterSnapshotHas(region, resID) {
			return fmt.Errorf(
				"%w: cluster snapshot %s not found",
				ErrClusterSnapshotNotFound,
				resID,
			)
		}
	case "subgrp":
		if !b.subnetGroupHas(region, resID) {
			return fmt.Errorf("%w: subnet group %s not found", ErrSubnetGroupNotFound, resID)
		}
	case "cluster-pg":
		if !b.clusterParameterGroupHas(region, resID) {
			return fmt.Errorf(
				"%w: cluster parameter group %s not found",
				ErrClusterParameterGroupNotFound,
				resID,
			)
		}
	default:
		return fmt.Errorf("%w: unsupported resource type in ARN: %s", ErrInvalidParameter, arnStr)
	}

	return nil
}

// AddTagsToResource adds or updates tags on a Neptune resource.
// The resource's region is resolved from the ARN, falling back to the ctx region.
func (b *InMemoryBackend) AddTagsToResource(ctx context.Context, arnStr string, tags []Tag) error {
	region := regionFromARN(arnStr, getRegion(ctx, b.region))
	b.mu.Lock("AddTagsToResource")
	defer b.mu.Unlock()
	if err := b.validateResourceARN(region, arnStr); err != nil {
		return err
	}
	for _, t := range tags {
		if len(t.Key) == 0 || len(t.Key) > maxTagKeyLen {
			return fmt.Errorf(
				"%w: tag key must be 1-%d characters",
				ErrInvalidParameter,
				maxTagKeyLen,
			)
		}
		if len(t.Value) > maxTagValueLen {
			return fmt.Errorf(
				"%w: tag value must be 0-%d characters",
				ErrInvalidParameter,
				maxTagValueLen,
			)
		}
	}
	tagStore := b.tagsStore(region)
	current := tagStore[arnStr]
	idx := make(map[string]int, len(current))
	for i, t := range current {
		idx[t.Key] = i
	}
	newCount := len(current)
	for _, t := range tags {
		if _, exists := idx[t.Key]; !exists {
			newCount++
		}
	}
	if newCount > maxTagsPerResource {
		return fmt.Errorf(
			"%w: resource cannot have more than %d tags",
			ErrInvalidParameter,
			maxTagsPerResource,
		)
	}
	for _, t := range tags {
		if i, ok := idx[t.Key]; ok {
			current[i].Value = t.Value
		} else {
			idx[t.Key] = len(current)
			current = append(current, t)
		}
	}
	tagStore[arnStr] = current

	return nil
}

// RemoveTagsFromResource removes tags from a Neptune resource.
func (b *InMemoryBackend) RemoveTagsFromResource(
	ctx context.Context,
	arnStr string,
	keys []string,
) error {
	region := regionFromARN(arnStr, getRegion(ctx, b.region))
	b.mu.Lock("RemoveTagsFromResource")
	defer b.mu.Unlock()
	if err := b.validateResourceARN(region, arnStr); err != nil {
		return err
	}
	remove := make(map[string]bool, len(keys))
	for _, k := range keys {
		remove[k] = true
	}
	tagStore := b.tagsStore(region)
	current := tagStore[arnStr]
	kept := make([]Tag, 0, len(current))
	for _, t := range current {
		if !remove[t.Key] {
			kept = append(kept, t)
		}
	}
	tagStore[arnStr] = kept

	return nil
}

// ListTagsForResource returns the tags for a Neptune resource.
func (b *InMemoryBackend) ListTagsForResource(ctx context.Context, arnStr string) ([]Tag, error) {
	region := regionFromARN(arnStr, getRegion(ctx, b.region))
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()
	if err := b.validateResourceARN(region, arnStr); err != nil {
		return nil, err
	}
	src := b.tagsStore(region)[arnStr]
	cp := make([]Tag, len(src))
	copy(cp, src)

	return cp, nil
}

// AddRoleToDBCluster associates an IAM role with a Neptune DB cluster.
func (b *InMemoryBackend) AddRoleToDBCluster(ctx context.Context, clusterID, roleARN string) error {
	if clusterID == "" {
		return fmt.Errorf("%w: DBClusterIdentifier is required", ErrInvalidParameter)
	}
	if roleARN == "" {
		return fmt.Errorf("%w: RoleArn is required", ErrInvalidParameter)
	}
	region := getRegion(ctx, b.region)
	b.mu.Lock("AddRoleToDBCluster")
	defer b.mu.Unlock()
	cluster, exists := b.clusterGet(region, clusterID)
	if !exists {
		return fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, clusterID)
	}
	roles := b.clusterRolesStore(region)
	if slices.Contains(roles[clusterID], roleARN) {
		return nil
	}
	roles[clusterID] = append(roles[clusterID], roleARN)
	if !slices.Contains(cluster.AssociatedRoles, roleARN) {
		cluster.AssociatedRoles = append(cluster.AssociatedRoles, roleARN)
	}

	return nil
}

// AddSourceIdentifierToSubscription adds a source identifier to an event subscription.
func (b *InMemoryBackend) AddSourceIdentifierToSubscription(
	ctx context.Context, name, sourceID string,
) (*EventSubscription, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: SubscriptionName is required", ErrInvalidParameter)
	}
	if sourceID == "" {
		return nil, fmt.Errorf("%w: SourceIdentifier is required", ErrInvalidParameter)
	}
	region := getRegion(ctx, b.region)
	b.mu.Lock("AddSourceIdentifierToSubscription")
	defer b.mu.Unlock()
	sub, exists := b.eventSubscriptionGet(region, name)
	if !exists {
		return nil, fmt.Errorf("%w: subscription %s not found", ErrSubscriptionNotFound, name)
	}
	if !slices.Contains(sub.SourceIDs, sourceID) {
		sub.SourceIDs = append(sub.SourceIDs, sourceID)
	}
	cp := *sub
	cp.SourceIDs = make([]string, len(sub.SourceIDs))
	copy(cp.SourceIDs, sub.SourceIDs)

	return &cp, nil
}

// ApplyPendingMaintenanceAction applies a pending maintenance action to a resource.
func (b *InMemoryBackend) ApplyPendingMaintenanceAction(
	_ context.Context, resourceID, applyAction, optInType string,
) error {
	if resourceID == "" {
		return fmt.Errorf("%w: ResourceIdentifier is required", ErrInvalidParameter)
	}
	if applyAction == "" {
		return fmt.Errorf("%w: ApplyAction is required", ErrInvalidParameter)
	}
	if optInType == "" {
		return fmt.Errorf("%w: OptInType is required", ErrInvalidParameter)
	}

	return nil
}

// CopyDBClusterParameterGroup copies a Neptune DB cluster parameter group.
func (b *InMemoryBackend) CopyDBClusterParameterGroup(
	ctx context.Context,
	sourceName, targetName, targetDescription string,
) (*DBClusterParameterGroup, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("CopyDBClusterParameterGroup")
	defer b.mu.Unlock()
	src, err := copyPreconditions(
		func(n string) (*DBClusterParameterGroup, bool) { return b.clusterParameterGroupGet(region, n) },
		sourceName, targetName,
		"SourceDBClusterParameterGroupIdentifier is required",
		"TargetDBClusterParameterGroupIdentifier is required",
		ErrClusterParameterGroupNotFound, ErrClusterParameterGroupAlreadyExists,
	)
	if err != nil {
		return nil, err
	}
	pg := &DBClusterParameterGroup{
		region:                      region,
		DBClusterParameterGroupName: targetName,
		DBClusterParameterGroupArn:  b.clusterParameterGroupARN(region, targetName),
		DBParameterGroupFamily:      src.DBParameterGroupFamily,
		Description:                 resolveCopyDescription(targetDescription, src.Description),
	}
	b.clusterParameterGroupPut(pg)
	cp := *pg

	return &cp, nil
}

// CopyDBClusterSnapshot copies a Neptune DB cluster snapshot.
func (b *InMemoryBackend) CopyDBClusterSnapshot(
	ctx context.Context, sourceSnapshotID, targetSnapshotID string,
) (*DBClusterSnapshot, error) {
	if sourceSnapshotID == "" {
		return nil, fmt.Errorf(
			"%w: SourceDBClusterSnapshotIdentifier is required",
			ErrInvalidParameter,
		)
	}
	if targetSnapshotID == "" {
		return nil, fmt.Errorf(
			"%w: TargetDBClusterSnapshotIdentifier is required",
			ErrInvalidParameter,
		)
	}
	region := getRegion(ctx, b.region)
	b.mu.Lock("CopyDBClusterSnapshot")
	defer b.mu.Unlock()
	src, exists := b.clusterSnapshotGet(region, sourceSnapshotID)
	if !exists {
		return nil, fmt.Errorf(
			"%w: cluster snapshot %s not found",
			ErrClusterSnapshotNotFound,
			sourceSnapshotID,
		)
	}
	if b.clusterSnapshotHas(region, targetSnapshotID) {
		return nil, fmt.Errorf(
			"%w: cluster snapshot %s already exists",
			ErrClusterSnapshotAlreadyExists,
			targetSnapshotID,
		)
	}
	snap := &DBClusterSnapshot{
		region:                           region,
		DBClusterSnapshotIdentifier:      targetSnapshotID,
		DBClusterSnapshotArn:             b.clusterSnapshotARN(region, targetSnapshotID),
		DBClusterIdentifier:              src.DBClusterIdentifier,
		Engine:                           src.Engine,
		EngineVersion:                    src.EngineVersion,
		Status:                           snapshotStatusAvailable,
		StorageEncrypted:                 src.StorageEncrypted,
		KmsKeyID:                         src.KmsKeyID,
		VpcID:                            src.VpcID,
		IAMDatabaseAuthenticationEnabled: src.IAMDatabaseAuthenticationEnabled,
		Port:                             src.Port,
		AllocatedStorage:                 src.AllocatedStorage,
		PercentProgress:                  percentProgressComplete,
		SnapshotType:                     snapshotSourceManual,
		SnapshotCreateTime:               nowISO8601(),
		ClusterCreateTime:                src.ClusterCreateTime,
	}
	b.clusterSnapshotPut(snap)
	cp := cloneClusterSnapshot(snap)

	return &cp, nil
}

// CopyDBParameterGroup copies a Neptune DB parameter group.
func (b *InMemoryBackend) CopyDBParameterGroup(
	ctx context.Context,
	sourceName, targetName, targetDescription string,
) (*DBParameterGroup, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("CopyDBParameterGroup")
	defer b.mu.Unlock()
	src, err := copyPreconditions(
		func(n string) (*DBParameterGroup, bool) { return b.parameterGroupGet(region, n) },
		sourceName, targetName,
		"SourceDBParameterGroupIdentifier is required",
		"TargetDBParameterGroupIdentifier is required",
		ErrParameterGroupNotFound, ErrParameterGroupAlreadyExists,
	)
	if err != nil {
		return nil, err
	}
	pg := &DBParameterGroup{
		region:                 region,
		DBParameterGroupName:   targetName,
		DBParameterGroupArn:    b.parameterGroupARN(region, targetName),
		DBParameterGroupFamily: src.DBParameterGroupFamily,
		Description:            resolveCopyDescription(targetDescription, src.Description),
	}
	b.parameterGroupPut(pg)
	cp := *pg

	return &cp, nil
}

// CreateDBClusterEndpoint creates a Neptune DB cluster custom endpoint.
func (b *InMemoryBackend) CreateDBClusterEndpoint(
	ctx context.Context,
	endpointID, clusterID, endpointType string,
) (*DBClusterEndpoint, error) {
	if endpointID == "" {
		return nil, fmt.Errorf("%w: DBClusterEndpointIdentifier is required", ErrInvalidParameter)
	}
	if clusterID == "" {
		return nil, fmt.Errorf("%w: DBClusterIdentifier is required", ErrInvalidParameter)
	}
	region := getRegion(ctx, b.region)
	b.mu.Lock("CreateDBClusterEndpoint")
	defer b.mu.Unlock()
	if b.clusterEndpointHas(region, endpointID) {
		return nil, fmt.Errorf(
			"%w: cluster endpoint %s already exists",
			ErrClusterEndpointAlreadyExists,
			endpointID,
		)
	}
	if !b.clusterHas(region, clusterID) {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, clusterID)
	}
	if endpointType == "" {
		endpointType = endpointTypeReader
	}
	switch endpointType {
	case endpointTypeReader, endpointTypeWriter, endpointTypeCustom, endpointTypeAny:
	default:
		return nil, fmt.Errorf(
			"%w: EndpointType must be one of READER, WRITER, CUSTOM, ANY",
			ErrInvalidParameter,
		)
	}
	ep := &DBClusterEndpoint{
		region:                              region,
		DBClusterEndpointIdentifier:         endpointID,
		DBClusterIdentifier:                 clusterID,
		DBClusterEndpointArn:                b.clusterEndpointARN(region, endpointID),
		DBClusterEndpointResourceIdentifier: fmt.Sprintf("cluster-endpoint-%s", endpointID),
		EndpointType:                        endpointType,
		Status:                              clusterStatusAvailable,
		Endpoint: fmt.Sprintf(
			"%s.cluster-custom.neptune.%s.amazonaws.com",
			endpointID,
			region,
		),
		StaticMembers:   []string{},
		ExcludedMembers: []string{},
	}
	b.clusterEndpointPut(ep)
	cp := *ep
	cp.StaticMembers = make([]string, len(ep.StaticMembers))
	copy(cp.StaticMembers, ep.StaticMembers)
	cp.ExcludedMembers = make([]string, len(ep.ExcludedMembers))
	copy(cp.ExcludedMembers, ep.ExcludedMembers)

	return &cp, nil
}

// CreateDBParameterGroup creates a Neptune DB parameter group.
func (b *InMemoryBackend) CreateDBParameterGroup(
	ctx context.Context, name, family, description string,
) (*DBParameterGroup, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: DBParameterGroupName is required", ErrInvalidParameter)
	}
	if family == "" || !validNeptuneParameterGroupFamily(family) {
		return nil, fmt.Errorf(
			"%w: DBParameterGroupFamily %q is not valid; must be one of neptune1.2, neptune1.3, neptune1.4",
			ErrInvalidParameter,
			family,
		)
	}
	region := getRegion(ctx, b.region)
	b.mu.Lock("CreateDBParameterGroup")
	defer b.mu.Unlock()
	if b.parameterGroupHas(region, name) {
		return nil, fmt.Errorf(
			"%w: parameter group %s already exists",
			ErrParameterGroupAlreadyExists,
			name,
		)
	}
	pg := &DBParameterGroup{
		region:                 region,
		DBParameterGroupName:   name,
		DBParameterGroupArn:    b.parameterGroupARN(region, name),
		DBParameterGroupFamily: family,
		Description:            description,
	}
	b.parameterGroupPut(pg)
	cp := *pg

	return &cp, nil
}

// CreateEventSubscription creates a Neptune event notification subscription.
func (b *InMemoryBackend) CreateEventSubscription(
	ctx context.Context,
	name, snsTopicARN, sourceType string,
	sourceIDs []string,
	enabled bool,
) (*EventSubscription, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: SubscriptionName is required", ErrInvalidParameter)
	}
	if snsTopicARN == "" {
		return nil, fmt.Errorf("%w: SnsTopicArn is required", ErrInvalidParameter)
	}
	region := getRegion(ctx, b.region)
	b.mu.Lock("CreateEventSubscription")
	defer b.mu.Unlock()
	if b.eventSubscriptionHas(region, name) {
		return nil, fmt.Errorf(
			"%w: subscription %s already exists",
			ErrSubscriptionAlreadyExists,
			name,
		)
	}
	ids := make([]string, len(sourceIDs))
	copy(ids, sourceIDs)
	sub := &EventSubscription{
		region:               region,
		CustSubscriptionID:   name,
		SnsTopicARN:          snsTopicARN,
		EventSubscriptionArn: b.eventSubscriptionARN(region, name),
		Status:               subscriptionStatusActive,
		SourceType:           sourceType,
		SourceIDs:            ids,
		Enabled:              enabled,
	}
	b.eventSubscriptionPut(sub)
	cp := cloneEventSubscription(sub)

	return &cp, nil
}

// CreateGlobalCluster creates a Neptune global cluster.
// Global clusters are partition-scoped (not region-isolated), but the optional
// source DB cluster is looked up in the ctx region where it resides.
func (b *InMemoryBackend) CreateGlobalCluster(
	ctx context.Context, globalClusterID, sourceDBClusterID string,
) (*GlobalCluster, error) {
	if globalClusterID == "" {
		return nil, fmt.Errorf("%w: GlobalClusterIdentifier is required", ErrInvalidParameter)
	}
	region := getRegion(ctx, b.region)
	b.mu.Lock("CreateGlobalCluster")
	defer b.mu.Unlock()
	if b.globalClusters.Has(globalClusterID) {
		return nil, fmt.Errorf(
			"%w: global cluster %s already exists",
			ErrGlobalClusterAlreadyExists,
			globalClusterID,
		)
	}
	gc := &GlobalCluster{
		GlobalClusterIdentifier: globalClusterID,
		GlobalClusterArn:        b.globalClusterARN(globalClusterID),
		GlobalClusterResourceID: fmt.Sprintf("cluster-%s", globalClusterID),
		Status:                  clusterStatusAvailable,
		Engine:                  neptuneEngine,
		EngineVersion:           defaultEngineVersion,
	}
	if sourceDBClusterID != "" {
		if cl, exists := b.clusterGet(region, sourceDBClusterID); exists {
			gc.GlobalClusterMembers = []GlobalClusterMember{
				{
					DBClusterARN: b.clusterARN(region, cl.DBClusterIdentifier),
					IsWriter:     true,
				},
			}
			gc.EngineVersion = cl.EngineVersion
			gc.StorageEncrypted = cl.StorageEncrypted
		}
	}
	b.globalClusters.Put(gc)
	cp := *gc
	cp.GlobalClusterMembers = make([]GlobalClusterMember, len(gc.GlobalClusterMembers))
	copy(cp.GlobalClusterMembers, gc.GlobalClusterMembers)

	return &cp, nil
}

// DescribeGlobalClusters returns all Neptune global clusters.
// Global clusters are partition-scoped, so all are returned regardless of region.
func (b *InMemoryBackend) DescribeGlobalClusters(_ context.Context) []GlobalCluster {
	b.mu.RLock("DescribeGlobalClusters")
	defer b.mu.RUnlock()
	globalClusters := b.globalClusters.All()
	result := make([]GlobalCluster, 0, len(globalClusters))
	for _, gc := range globalClusters {
		cp := *gc
		cp.GlobalClusterMembers = make([]GlobalClusterMember, len(gc.GlobalClusterMembers))
		copy(cp.GlobalClusterMembers, gc.GlobalClusterMembers)
		result = append(result, cp)
	}
	slices.SortFunc(result, func(a, b GlobalCluster) int {
		return strings.Compare(a.GlobalClusterIdentifier, b.GlobalClusterIdentifier)
	})

	return result
}

// DeleteDBClusterEndpoint deletes a Neptune DB cluster custom endpoint,
// returning the deleted endpoint's details -- AWS's DeleteDBClusterEndpoint
// response echoes them back (DeleteDBClusterEndpointResult), unlike e.g.
// DeleteDBSubnetGroup which has a genuinely empty output.
func (b *InMemoryBackend) DeleteDBClusterEndpoint(
	ctx context.Context, endpointID string,
) (*DBClusterEndpoint, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("DeleteDBClusterEndpoint")
	defer b.mu.Unlock()
	ep, exists := b.clusterEndpointGet(region, endpointID)
	if !exists {
		return nil, fmt.Errorf(
			"%w: cluster endpoint %s not found",
			ErrClusterEndpointNotFound,
			endpointID,
		)
	}
	cp := *ep
	b.clusterEndpointDelete(region, endpointID)

	return &cp, nil
}

// DescribeDBClusterEndpoints returns all Neptune DB cluster endpoints or a specific one.
func (b *InMemoryBackend) DescribeDBClusterEndpoints(
	ctx context.Context, endpointID, clusterID string,
) ([]DBClusterEndpoint, error) {
	region := getRegion(ctx, b.region)
	b.mu.RLock("DescribeDBClusterEndpoints")
	defer b.mu.RUnlock()
	if endpointID != "" {
		ep, exists := b.clusterEndpointGet(region, endpointID)
		if !exists {
			return nil, fmt.Errorf(
				"%w: cluster endpoint %s not found",
				ErrClusterEndpointNotFound,
				endpointID,
			)
		}
		cp := *ep

		return []DBClusterEndpoint{cp}, nil
	}
	clusterEndpoints := b.clusterEndpointsInRegion(region)
	result := make([]DBClusterEndpoint, 0, len(clusterEndpoints))
	for _, ep := range clusterEndpoints {
		if clusterID != "" && ep.DBClusterIdentifier != clusterID {
			continue
		}
		result = append(result, *ep)
	}

	return result, nil
}

// ModifyDBClusterEndpoint modifies a Neptune DB cluster custom endpoint.
func (b *InMemoryBackend) ModifyDBClusterEndpoint(
	ctx context.Context, endpointID, endpointType string,
) (*DBClusterEndpoint, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("ModifyDBClusterEndpoint")
	defer b.mu.Unlock()
	ep, exists := b.clusterEndpointGet(region, endpointID)
	if !exists {
		return nil, fmt.Errorf(
			"%w: cluster endpoint %s not found",
			ErrClusterEndpointNotFound,
			endpointID,
		)
	}
	if endpointType != "" {
		ep.EndpointType = endpointType
	}
	cp := *ep

	return &cp, nil
}

// DeleteDBParameterGroup deletes a Neptune DB parameter group.
func (b *InMemoryBackend) DeleteDBParameterGroup(ctx context.Context, name string) error {
	region := getRegion(ctx, b.region)
	b.mu.Lock("DeleteDBParameterGroup")
	defer b.mu.Unlock()
	if !b.parameterGroupHas(region, name) {
		return fmt.Errorf("%w: parameter group %s not found", ErrParameterGroupNotFound, name)
	}
	b.parameterGroupDelete(region, name)

	return nil
}

// DescribeDBParameterGroups returns all Neptune DB parameter groups or a specific one.
func (b *InMemoryBackend) DescribeDBParameterGroups(
	ctx context.Context,
	name string,
) ([]DBParameterGroup, error) {
	region := getRegion(ctx, b.region)
	b.mu.RLock("DescribeDBParameterGroups")
	defer b.mu.RUnlock()
	if name != "" {
		pg, exists := b.parameterGroupGet(region, name)
		if !exists {
			return nil, fmt.Errorf(
				"%w: parameter group %s not found",
				ErrParameterGroupNotFound,
				name,
			)
		}
		cp := *pg

		return []DBParameterGroup{cp}, nil
	}
	groups := b.parameterGroupsInRegion(region)
	result := make([]DBParameterGroup, 0, len(groups))
	for _, pg := range groups {
		result = append(result, *pg)
	}
	slices.SortFunc(result, func(a, b DBParameterGroup) int {
		return strings.Compare(a.DBParameterGroupName, b.DBParameterGroupName)
	})

	return result, nil
}

// ModifyDBParameterGroup modifies a Neptune DB parameter group.
func (b *InMemoryBackend) ModifyDBParameterGroup(
	ctx context.Context,
	name string,
) (*DBParameterGroup, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("ModifyDBParameterGroup")
	defer b.mu.Unlock()
	pg, exists := b.parameterGroupGet(region, name)
	if !exists {
		return nil, fmt.Errorf("%w: parameter group %s not found", ErrParameterGroupNotFound, name)
	}
	cp := *pg

	return &cp, nil
}

// ResetDBParameterGroup resets a Neptune DB parameter group to its default values.
func (b *InMemoryBackend) ResetDBParameterGroup(
	ctx context.Context,
	name string,
) (*DBParameterGroup, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("ResetDBParameterGroup")
	defer b.mu.Unlock()
	pg, exists := b.parameterGroupGet(region, name)
	if !exists {
		return nil, fmt.Errorf("%w: parameter group %s not found", ErrParameterGroupNotFound, name)
	}
	cp := *pg

	return &cp, nil
}

// ResetDBClusterParameterGroup resets a Neptune DB cluster parameter group to its default values.
func (b *InMemoryBackend) ResetDBClusterParameterGroup(
	ctx context.Context, name string,
) (*DBClusterParameterGroup, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("ResetDBClusterParameterGroup")
	defer b.mu.Unlock()
	pg, exists := b.clusterParameterGroupGet(region, name)
	if !exists {
		return nil, fmt.Errorf(
			"%w: cluster parameter group %s not found",
			ErrClusterParameterGroupNotFound,
			name,
		)
	}
	cp := *pg

	return &cp, nil
}

// DeleteEventSubscription deletes a Neptune event subscription.
func (b *InMemoryBackend) DeleteEventSubscription(
	ctx context.Context,
	name string,
) (*EventSubscription, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("DeleteEventSubscription")
	defer b.mu.Unlock()
	sub, exists := b.eventSubscriptionGet(region, name)
	if !exists {
		return nil, fmt.Errorf("%w: subscription %s not found", ErrSubscriptionNotFound, name)
	}
	cp := *sub
	cp.SourceIDs = make([]string, len(sub.SourceIDs))
	copy(cp.SourceIDs, sub.SourceIDs)
	b.eventSubscriptionDelete(region, name)

	return &cp, nil
}

// DescribeEventSubscriptions returns all event subscriptions or a specific one.
func (b *InMemoryBackend) DescribeEventSubscriptions(
	ctx context.Context,
	name string,
) ([]EventSubscription, error) {
	region := getRegion(ctx, b.region)
	b.mu.RLock("DescribeEventSubscriptions")
	defer b.mu.RUnlock()
	if name != "" {
		sub, exists := b.eventSubscriptionGet(region, name)
		if !exists {
			return nil, fmt.Errorf("%w: subscription %s not found", ErrSubscriptionNotFound, name)
		}

		return []EventSubscription{cloneEventSubscription(sub)}, nil
	}
	subs := b.eventSubscriptionsInRegion(region)
	result := make([]EventSubscription, 0, len(subs))
	for _, sub := range subs {
		result = append(result, cloneEventSubscription(sub))
	}
	slices.SortFunc(result, func(a, b EventSubscription) int {
		return strings.Compare(a.CustSubscriptionID, b.CustSubscriptionID)
	})

	return result, nil
}

// ModifyEventSubscription modifies a Neptune event subscription.
func (b *InMemoryBackend) ModifyEventSubscription(
	ctx context.Context,
	name, snsTopicARN, sourceType, enabled string,
	eventCategories []string,
) (*EventSubscription, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("ModifyEventSubscription")
	defer b.mu.Unlock()
	sub, exists := b.eventSubscriptionGet(region, name)
	if !exists {
		return nil, fmt.Errorf("%w: subscription %s not found", ErrSubscriptionNotFound, name)
	}
	if snsTopicARN != "" {
		sub.SnsTopicARN = snsTopicARN
	}
	if sourceType != "" {
		sub.SourceType = sourceType
	}
	switch enabled {
	case "true":
		sub.Enabled = true
	case "false":
		sub.Enabled = false
	}
	if len(eventCategories) > 0 {
		cats := make([]string, len(eventCategories))
		copy(cats, eventCategories)
		sub.EventCategoriesList = cats
	}
	cp := cloneEventSubscription(sub)

	return &cp, nil
}

// RemoveSourceIdentifierFromSubscription removes a source identifier from a Neptune event subscription.
func (b *InMemoryBackend) RemoveSourceIdentifierFromSubscription(
	ctx context.Context, name, sourceID string,
) (*EventSubscription, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: SubscriptionName is required", ErrInvalidParameter)
	}
	if sourceID == "" {
		return nil, fmt.Errorf("%w: SourceIdentifier is required", ErrInvalidParameter)
	}
	region := getRegion(ctx, b.region)
	b.mu.Lock("RemoveSourceIdentifierFromSubscription")
	defer b.mu.Unlock()
	sub, exists := b.eventSubscriptionGet(region, name)
	if !exists {
		return nil, fmt.Errorf("%w: subscription %s not found", ErrSubscriptionNotFound, name)
	}
	kept := make([]string, 0, len(sub.SourceIDs))
	for _, id := range sub.SourceIDs {
		if id != sourceID {
			kept = append(kept, id)
		}
	}
	sub.SourceIDs = kept
	cp := *sub
	cp.SourceIDs = make([]string, len(sub.SourceIDs))
	copy(cp.SourceIDs, sub.SourceIDs)

	return &cp, nil
}

// DeleteGlobalCluster deletes a Neptune global cluster (partition-scoped).
func (b *InMemoryBackend) DeleteGlobalCluster(
	_ context.Context,
	globalClusterID string,
) (*GlobalCluster, error) {
	b.mu.Lock("DeleteGlobalCluster")
	defer b.mu.Unlock()
	gc, exists := b.globalClusters.Get(globalClusterID)
	if !exists {
		return nil, fmt.Errorf(
			"%w: global cluster %s not found",
			ErrGlobalClusterNotFound,
			globalClusterID,
		)
	}
	cp := *gc
	cp.GlobalClusterMembers = make([]GlobalClusterMember, len(gc.GlobalClusterMembers))
	copy(cp.GlobalClusterMembers, gc.GlobalClusterMembers)
	b.globalClusters.Delete(globalClusterID)

	return &cp, nil
}

// FailoverGlobalCluster performs a failover for a Neptune global cluster (partition-scoped).
// targetDBClusterID is accepted for API compatibility but not used in the in-memory backend.
func (b *InMemoryBackend) FailoverGlobalCluster(
	_ context.Context, globalClusterID, _ string,
) (*GlobalCluster, error) {
	b.mu.Lock("FailoverGlobalCluster")
	defer b.mu.Unlock()
	gc, exists := b.globalClusters.Get(globalClusterID)
	if !exists {
		return nil, fmt.Errorf(
			"%w: global cluster %s not found",
			ErrGlobalClusterNotFound,
			globalClusterID,
		)
	}
	cp := *gc
	cp.GlobalClusterMembers = make([]GlobalClusterMember, len(gc.GlobalClusterMembers))
	copy(cp.GlobalClusterMembers, gc.GlobalClusterMembers)

	return &cp, nil
}

// ModifyGlobalCluster modifies a Neptune global cluster (partition-scoped).
func (b *InMemoryBackend) ModifyGlobalCluster(
	_ context.Context,
	globalClusterID string,
) (*GlobalCluster, error) {
	b.mu.Lock("ModifyGlobalCluster")
	defer b.mu.Unlock()
	gc, exists := b.globalClusters.Get(globalClusterID)
	if !exists {
		return nil, fmt.Errorf(
			"%w: global cluster %s not found",
			ErrGlobalClusterNotFound,
			globalClusterID,
		)
	}
	cp := *gc
	cp.GlobalClusterMembers = make([]GlobalClusterMember, len(gc.GlobalClusterMembers))
	copy(cp.GlobalClusterMembers, gc.GlobalClusterMembers)

	return &cp, nil
}

// RemoveFromGlobalCluster removes a DB cluster from a Neptune global cluster (partition-scoped).
func (b *InMemoryBackend) RemoveFromGlobalCluster(
	_ context.Context, globalClusterID, dbClusterARN string,
) (*GlobalCluster, error) {
	b.mu.Lock("RemoveFromGlobalCluster")
	defer b.mu.Unlock()
	gc, exists := b.globalClusters.Get(globalClusterID)
	if !exists {
		return nil, fmt.Errorf(
			"%w: global cluster %s not found",
			ErrGlobalClusterNotFound,
			globalClusterID,
		)
	}
	kept := make([]GlobalClusterMember, 0, len(gc.GlobalClusterMembers))
	for _, m := range gc.GlobalClusterMembers {
		if m.DBClusterARN != dbClusterARN {
			kept = append(kept, m)
		}
	}
	gc.GlobalClusterMembers = kept
	cp := *gc
	cp.GlobalClusterMembers = make([]GlobalClusterMember, len(gc.GlobalClusterMembers))
	copy(cp.GlobalClusterMembers, gc.GlobalClusterMembers)

	return &cp, nil
}

// SwitchoverGlobalCluster switches over a Neptune global cluster to a new primary (partition-scoped).
// targetDBClusterID is accepted for API compatibility but not used in the in-memory backend.
func (b *InMemoryBackend) SwitchoverGlobalCluster(
	_ context.Context, globalClusterID, _ string,
) (*GlobalCluster, error) {
	b.mu.Lock("SwitchoverGlobalCluster")
	defer b.mu.Unlock()
	gc, exists := b.globalClusters.Get(globalClusterID)
	if !exists {
		return nil, fmt.Errorf(
			"%w: global cluster %s not found",
			ErrGlobalClusterNotFound,
			globalClusterID,
		)
	}
	cp := *gc
	cp.GlobalClusterMembers = make([]GlobalClusterMember, len(gc.GlobalClusterMembers))
	copy(cp.GlobalClusterMembers, gc.GlobalClusterMembers)

	return &cp, nil
}

// RemoveRoleFromDBCluster removes an IAM role association from a Neptune DB cluster.
func (b *InMemoryBackend) RemoveRoleFromDBCluster(
	ctx context.Context,
	clusterID, roleARN string,
) error {
	if clusterID == "" {
		return fmt.Errorf("%w: DBClusterIdentifier is required", ErrInvalidParameter)
	}
	if roleARN == "" {
		return fmt.Errorf("%w: RoleArn is required", ErrInvalidParameter)
	}
	region := getRegion(ctx, b.region)
	b.mu.Lock("RemoveRoleFromDBCluster")
	defer b.mu.Unlock()
	cluster, exists := b.clusterGet(region, clusterID)
	if !exists {
		return fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, clusterID)
	}
	rolesStore := b.clusterRolesStore(region)
	roles := rolesStore[clusterID]
	kept := make([]string, 0, len(roles))
	for _, r := range roles {
		if r != roleARN {
			kept = append(kept, r)
		}
	}
	rolesStore[clusterID] = kept
	keptRoles := make([]string, 0, len(cluster.AssociatedRoles))
	for _, r := range cluster.AssociatedRoles {
		if r != roleARN {
			keptRoles = append(keptRoles, r)
		}
	}
	cluster.AssociatedRoles = keptRoles

	return nil
}

// RestoreDBClusterFromSnapshot restores a Neptune DB cluster from a snapshot.
func (b *InMemoryBackend) RestoreDBClusterFromSnapshot(
	ctx context.Context, snapshotID, clusterID string,
) (*DBCluster, error) {
	if snapshotID == "" {
		return nil, fmt.Errorf("%w: DBClusterSnapshotIdentifier is required", ErrInvalidParameter)
	}
	if clusterID == "" {
		return nil, fmt.Errorf("%w: DBClusterIdentifier is required", ErrInvalidParameter)
	}
	region := getRegion(ctx, b.region)
	b.mu.Lock("RestoreDBClusterFromSnapshot")
	defer b.mu.Unlock()
	snap, snapExists := b.clusterSnapshotGet(region, snapshotID)
	if !snapExists {
		return nil, fmt.Errorf(
			"%w: cluster snapshot %s not found",
			ErrClusterSnapshotNotFound,
			snapshotID,
		)
	}
	if b.clusterHas(region, clusterID) {
		return nil, fmt.Errorf("%w: cluster %s already exists", ErrClusterAlreadyExists, clusterID)
	}
	// Derive parameter group from the source cluster if available.
	paramGroupName := pgFamilyDefaultNeptune13
	if srcCluster, ok := b.clusterGet(region, snap.DBClusterIdentifier); ok {
		paramGroupName = srcCluster.DBClusterParameterGroupName
	}
	endpoint := fmt.Sprintf("%s.cluster.%s.neptune.amazonaws.com", clusterID, region)
	readerEndpoint := fmt.Sprintf("%s.cluster-ro.%s.neptune.amazonaws.com", clusterID, region)
	cluster := &DBCluster{
		region:                      region,
		DBClusterIdentifier:         clusterID,
		DBClusterArn:                b.clusterARN(region, clusterID),
		DBClusterResourceID:         fmt.Sprintf("cluster-%s", clusterID),
		ClusterCreateTime:           nowISO8601(),
		Engine:                      snap.Engine,
		EngineVersion:               snap.EngineVersion,
		EngineMode:                  engineModeProvisioned,
		Status:                      clusterStatusAvailable,
		DBClusterParameterGroupName: paramGroupName,
		Endpoint:                    endpoint,
		ReaderEndpoint:              readerEndpoint,
		Port:                        defaultNeptunePort,
		StorageEncrypted:            snap.StorageEncrypted,
		DBClusterMembers:            []DBClusterMember{},
		BackupRetentionPeriod:       defaultBackupRetentionPeriod,
	}
	b.clusterPut(cluster)
	cp := cloneCluster(cluster)

	return &cp, nil
}

// RestoreDBClusterToPointInTime restores a Neptune DB cluster to a point in time.
func (b *InMemoryBackend) RestoreDBClusterToPointInTime(
	ctx context.Context, srcClusterID, targetClusterID string,
) (*DBCluster, error) {
	if srcClusterID == "" {
		return nil, fmt.Errorf("%w: SourceDBClusterIdentifier is required", ErrInvalidParameter)
	}
	if targetClusterID == "" {
		return nil, fmt.Errorf("%w: DBClusterIdentifier is required", ErrInvalidParameter)
	}
	region := getRegion(ctx, b.region)
	b.mu.Lock("RestoreDBClusterToPointInTime")
	defer b.mu.Unlock()
	src, srcExists := b.clusterGet(region, srcClusterID)
	if !srcExists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, srcClusterID)
	}
	if b.clusterHas(region, targetClusterID) {
		return nil, fmt.Errorf(
			"%w: cluster %s already exists",
			ErrClusterAlreadyExists,
			targetClusterID,
		)
	}
	endpoint := fmt.Sprintf("%s.cluster.%s.neptune.amazonaws.com", targetClusterID, region)
	readerEndpoint := fmt.Sprintf("%s.cluster-ro.%s.neptune.amazonaws.com", targetClusterID, region)
	cluster := &DBCluster{
		region:                          region,
		DBClusterIdentifier:             targetClusterID,
		DBClusterArn:                    b.clusterARN(region, targetClusterID),
		DBClusterResourceID:             fmt.Sprintf("cluster-%s", targetClusterID),
		ClusterCreateTime:               nowISO8601(),
		Engine:                          src.Engine,
		EngineVersion:                   src.EngineVersion,
		EngineMode:                      src.EngineMode,
		Status:                          clusterStatusAvailable,
		DBClusterParameterGroupName:     src.DBClusterParameterGroupName,
		Endpoint:                        endpoint,
		ReaderEndpoint:                  readerEndpoint,
		Port:                            src.Port,
		StorageEncrypted:                src.StorageEncrypted,
		EnableIAMDatabaseAuthentication: src.EnableIAMDatabaseAuthentication,
		DeletionProtection:              src.DeletionProtection,
		DBClusterMembers:                []DBClusterMember{},
		BackupRetentionPeriod:           src.BackupRetentionPeriod,
	}
	b.clusterPut(cluster)
	cp := cloneCluster(cluster)

	return &cp, nil
}

// ModifyDBSubnetGroup modifies a Neptune DB subnet group.
func (b *InMemoryBackend) ModifyDBSubnetGroup(
	ctx context.Context,
	name, description string,
	subnetIDs []string,
) (*DBSubnetGroup, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("ModifyDBSubnetGroup")
	defer b.mu.Unlock()
	sg, exists := b.subnetGroupGet(region, name)
	if !exists {
		return nil, fmt.Errorf("%w: subnet group %s not found", ErrSubnetGroupNotFound, name)
	}
	if description != "" {
		sg.DBSubnetGroupDescription = description
	}
	if len(subnetIDs) > 0 {
		ids := make([]string, len(subnetIDs))
		copy(ids, subnetIDs)
		sg.SubnetIDs = ids
	}
	cp := cloneSubnetGroup(sg)

	return &cp, nil
}

// AccountID returns the backend's AWS account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Reset clears all backend state, returning it to a clean empty state.
//
// It calls b.registry.ResetAll() rather than re-registering tables:
// registerAllTables must run exactly once, at construction (store.Register
// panics on a duplicate name) -- see the doc comment on registerAllTables in
// store_setup.go.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()
	b.registry.ResetAll()
	b.clusterRoles = make(map[string]map[string][]string)
	b.tags = make(map[string]map[string][]Tag)
}

// AddClusterInternal creates a cluster directly, bypassing normal validation. Used for seeding tests.
func (b *InMemoryBackend) AddClusterInternal(id string) *DBCluster {
	b.mu.Lock("AddClusterInternal")
	defer b.mu.Unlock()
	endpoint := fmt.Sprintf("%s.cluster.%s.neptune.amazonaws.com", id, b.region)
	readerEndpoint := fmt.Sprintf("%s.cluster-ro.%s.neptune.amazonaws.com", id, b.region)
	c := &DBCluster{
		region:                      b.region,
		DBClusterIdentifier:         id,
		DBClusterArn:                b.clusterARN(b.region, id),
		Engine:                      neptuneEngine,
		EngineVersion:               defaultEngineVersion,
		EngineMode:                  engineModeProvisioned,
		Status:                      clusterStatusAvailable,
		DBClusterParameterGroupName: pgFamilyDefaultNeptune13,
		Endpoint:                    endpoint,
		ReaderEndpoint:              readerEndpoint,
		Port:                        defaultNeptunePort,
		BackupRetentionPeriod:       defaultBackupRetentionPeriod,
	}
	b.clusterPut(c)
	cp := cloneCluster(c)

	return &cp
}

// AddSnapshotInternal creates a snapshot directly, bypassing normal validation. Used for seeding tests.
func (b *InMemoryBackend) AddSnapshotInternal(snapshotID, clusterID string) *DBClusterSnapshot {
	b.mu.Lock("AddSnapshotInternal")
	defer b.mu.Unlock()
	snap := &DBClusterSnapshot{
		region:                      b.region,
		DBClusterSnapshotIdentifier: snapshotID,
		DBClusterSnapshotArn:        b.clusterSnapshotARN(b.region, snapshotID),
		DBClusterIdentifier:         clusterID,
		Engine:                      neptuneEngine,
		EngineVersion:               defaultEngineVersion,
		Status:                      clusterStatusAvailable,
		SnapshotType:                snapshotSourceManual,
	}
	b.clusterSnapshotPut(snap)
	cp := *snap

	return &cp
}

// AddClusterParameterGroupInternal creates a cluster parameter group directly. Used for seeding tests.
func (b *InMemoryBackend) AddClusterParameterGroupInternal(
	name, family string,
) *DBClusterParameterGroup {
	b.mu.Lock("AddClusterParameterGroupInternal")
	defer b.mu.Unlock()
	pg := &DBClusterParameterGroup{
		region:                      b.region,
		DBClusterParameterGroupName: name,
		DBParameterGroupFamily:      family,
		Description:                 "seeded for tests",
	}
	b.clusterParameterGroupPut(pg)
	cp := *pg

	return &cp
}

// AddParameterGroupInternal creates a DB parameter group directly. Used for seeding tests.
func (b *InMemoryBackend) AddParameterGroupInternal(name, family string) *DBParameterGroup {
	b.mu.Lock("AddParameterGroupInternal")
	defer b.mu.Unlock()
	pg := &DBParameterGroup{
		region:                 b.region,
		DBParameterGroupName:   name,
		DBParameterGroupFamily: family,
		Description:            "seeded for tests",
	}
	b.parameterGroupPut(pg)
	cp := *pg

	return &cp
}

// AddEventSubscriptionInternal creates an event subscription directly. Used for seeding tests.
func (b *InMemoryBackend) AddEventSubscriptionInternal(
	name, snsTopicARN string,
) *EventSubscription {
	b.mu.Lock("AddEventSubscriptionInternal")
	defer b.mu.Unlock()
	sub := &EventSubscription{
		region:             b.region,
		CustSubscriptionID: name,
		SnsTopicARN:        snsTopicARN,
		Status:             subscriptionStatusActive,
	}
	b.eventSubscriptionPut(sub)
	cp := *sub

	return &cp
}
