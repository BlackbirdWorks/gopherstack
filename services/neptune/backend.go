package neptune

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
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
)

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
	EngineVersion                   string
	EngineMode                      string
	KmsKeyID                        string
	PreferredBackupWindow           string
	PreferredMaintenanceWindow      string
	EnableIAMDatabaseAuthentication bool
	ManageMasterUserPassword        bool
	StorageEncrypted                bool
	DeletionProtection              bool
}

// DBClusterModifyOptions holds optional fields for ModifyDBCluster.
type DBClusterModifyOptions struct {
	ServerlessV2ScalingConfig       *ServerlessV2ScalingConfiguration
	EngineVersion                   string
	PreferredBackupWindow           string
	PreferredMaintenanceWindow      string
	EnableIAMDatabaseAuthentication bool
	IamAuthSet                      bool
	ManageMasterUserPassword        bool
	DeletionProtection              bool
	DeletionProtectionSet           bool
}

// DBClusterMember represents a single DB instance member of a Neptune cluster.
type DBClusterMember struct {
	DBInstanceIdentifier string `json:"DBInstanceIdentifier"`
	IsClusterWriter      bool   `json:"IsClusterWriter"`
}

// DBCluster represents an Amazon Neptune DB cluster.
type DBCluster struct {
	ServerlessV2ScalingConfig       *ServerlessV2ScalingConfiguration `json:"ServerlessV2ScalingConfiguration,omitempty"`
	MasterUserManagedSecret         *MasterUserManagedSecret          `json:"MasterUserManagedSecret,omitempty"`
	DBClusterIdentifier             string                            `json:"DBClusterIdentifier"`
	DBClusterArn                    string                            `json:"DBClusterArn"`
	Engine                          string                            `json:"Engine"`
	EngineVersion                   string                            `json:"EngineVersion"`
	EngineMode                      string                            `json:"EngineMode"`
	Status                          string                            `json:"Status"`
	DBClusterParameterGroupName     string                            `json:"DBClusterParameterGroupName"`
	DBSubnetGroupName               string                            `json:"DBSubnetGroupName"`
	Endpoint                        string                            `json:"Endpoint"`
	ReaderEndpoint                  string                            `json:"ReaderEndpoint"`
	PreferredBackupWindow           string                            `json:"PreferredBackupWindow"`
	PreferredMaintenanceWindow      string                            `json:"PreferredMaintenanceWindow"`
	KmsKeyID                        string                            `json:"KmsKeyID"`
	DBClusterMembers                []DBClusterMember                 `json:"DBClusterMembers"`
	Port                            int                               `json:"Port"`
	BackupRetentionPeriod           int                               `json:"BackupRetentionPeriod"`
	EnableIAMDatabaseAuthentication bool                              `json:"EnableIAMDatabaseAuthentication"`
	StorageEncrypted                bool                              `json:"StorageEncrypted"`
	MultiAZ                         bool                              `json:"MultiAZ"`
	DeletionProtection              bool                              `json:"DeletionProtection"`
}

// DBInstance represents an Amazon Neptune DB instance.
type DBInstance struct {
	DBInstanceIdentifier            string `json:"DBInstanceIdentifier"`
	DBInstanceArn                   string `json:"DBInstanceArn"`
	DBClusterIdentifier             string `json:"DBClusterIdentifier"`
	DBInstanceClass                 string `json:"DBInstanceClass"`
	Engine                          string `json:"Engine"`
	EngineVersion                   string `json:"EngineVersion"`
	DBInstanceStatus                string `json:"DBInstanceStatus"`
	Endpoint                        string `json:"Endpoint"`
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
	DBSubnetGroupName        string   `json:"DBSubnetGroupName"`
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
	DBClusterParameterGroupName string `json:"DBClusterParameterGroupName"`
	DBParameterGroupFamily      string `json:"DBParameterGroupFamily"`
	Description                 string `json:"Description"`
}

// DBClusterSnapshot represents a Neptune DB cluster snapshot.
type DBClusterSnapshot struct {
	DBClusterSnapshotIdentifier string `json:"DBClusterSnapshotIdentifier"`
	DBClusterSnapshotArn        string `json:"DBClusterSnapshotArn"`
	DBClusterIdentifier         string `json:"DBClusterIdentifier"`
	Engine                      string `json:"Engine"`
	EngineVersion               string `json:"EngineVersion"`
	Status                      string `json:"Status"`
	SnapshotType                string `json:"SnapshotType"`
	StorageEncrypted            bool   `json:"StorageEncrypted"`
}

// DBParameterGroup represents a Neptune DB parameter group.
type DBParameterGroup struct {
	DBParameterGroupName   string `json:"DBParameterGroupName"`
	DBParameterGroupFamily string `json:"DBParameterGroupFamily"`
	Description            string `json:"Description"`
}

// DBClusterEndpoint represents a Neptune DB cluster custom endpoint.
type DBClusterEndpoint struct {
	DBClusterEndpointIdentifier string `json:"DBClusterEndpointIdentifier"`
	DBClusterIdentifier         string `json:"DBClusterIdentifier"`
	EndpointType                string `json:"EndpointType"`
	Status                      string `json:"Status"`
	Endpoint                    string `json:"Endpoint"`
}

// EventSubscription represents a Neptune event subscription.
type EventSubscription struct {
	CustSubscriptionID string   `json:"CustSubscriptionID"`
	SnsTopicARN        string   `json:"SnsTopicARN"`
	Status             string   `json:"Status"`
	SourceIDs          []string `json:"SourceIDs"`
}

// GlobalCluster represents a Neptune global cluster.
type GlobalCluster struct {
	GlobalClusterIdentifier string                `json:"GlobalClusterIdentifier"`
	Status                  string                `json:"Status"`
	GlobalClusterMembers    []GlobalClusterMember `json:"GlobalClusterMembers"`
}

// GlobalClusterMember represents a member cluster in a global cluster.
type GlobalClusterMember struct {
	DBClusterARN string `json:"DBClusterARN"`
	IsWriter     bool   `json:"IsWriter"`
}

// InMemoryBackend is a thread-safe in-memory backend for Neptune.
//
// All regional resource maps are nested by region (outer key = region) so that
// same-named resources in different regions are fully isolated. GlobalClusters
// are global/partition-scoped (like AWS) and therefore are NOT region-nested.
type InMemoryBackend struct {
	clusters               map[string]map[string]*DBCluster
	instances              map[string]map[string]*DBInstance
	subnetGroups           map[string]map[string]*DBSubnetGroup
	clusterParameterGroups map[string]map[string]*DBClusterParameterGroup
	clusterSnapshots       map[string]map[string]*DBClusterSnapshot
	parameterGroups        map[string]map[string]*DBParameterGroup
	clusterEndpoints       map[string]map[string]*DBClusterEndpoint
	eventSubscriptions     map[string]map[string]*EventSubscription
	clusterRoles           map[string]map[string][]string
	tags                   map[string]map[string][]Tag
	globalClusters         map[string]*GlobalCluster // global/partition-scoped, not region-nested
	mu                     *lockmetrics.RWMutex
	accountID              string
	region                 string
}

// NewInMemoryBackend creates a new in-memory Neptune backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		clusters:               make(map[string]map[string]*DBCluster),
		instances:              make(map[string]map[string]*DBInstance),
		subnetGroups:           make(map[string]map[string]*DBSubnetGroup),
		clusterParameterGroups: make(map[string]map[string]*DBClusterParameterGroup),
		clusterSnapshots:       make(map[string]map[string]*DBClusterSnapshot),
		parameterGroups:        make(map[string]map[string]*DBParameterGroup),
		clusterEndpoints:       make(map[string]map[string]*DBClusterEndpoint),
		eventSubscriptions:     make(map[string]map[string]*EventSubscription),
		clusterRoles:           make(map[string]map[string][]string),
		tags:                   make(map[string]map[string][]Tag),
		globalClusters:         make(map[string]*GlobalCluster),
		accountID:              accountID,
		region:                 region,
		mu:                     lockmetrics.New("neptune"),
	}
}

// Region returns the backend's AWS region.
func (b *InMemoryBackend) Region() string { return b.region }

// The following lazy per-region store helpers return the resource map for the
// given region, creating it on first use. Callers must hold b.mu.

func (b *InMemoryBackend) clustersStore(region string) map[string]*DBCluster {
	if b.clusters[region] == nil {
		b.clusters[region] = make(map[string]*DBCluster)
	}

	return b.clusters[region]
}

func (b *InMemoryBackend) instancesStore(region string) map[string]*DBInstance {
	if b.instances[region] == nil {
		b.instances[region] = make(map[string]*DBInstance)
	}

	return b.instances[region]
}

func (b *InMemoryBackend) subnetGroupsStore(region string) map[string]*DBSubnetGroup {
	if b.subnetGroups[region] == nil {
		b.subnetGroups[region] = make(map[string]*DBSubnetGroup)
	}

	return b.subnetGroups[region]
}

func (b *InMemoryBackend) clusterParameterGroupsStore(region string) map[string]*DBClusterParameterGroup {
	if b.clusterParameterGroups[region] == nil {
		b.clusterParameterGroups[region] = make(map[string]*DBClusterParameterGroup)
	}

	return b.clusterParameterGroups[region]
}

func (b *InMemoryBackend) clusterSnapshotsStore(region string) map[string]*DBClusterSnapshot {
	if b.clusterSnapshots[region] == nil {
		b.clusterSnapshots[region] = make(map[string]*DBClusterSnapshot)
	}

	return b.clusterSnapshots[region]
}

func (b *InMemoryBackend) parameterGroupsStore(region string) map[string]*DBParameterGroup {
	if b.parameterGroups[region] == nil {
		b.parameterGroups[region] = make(map[string]*DBParameterGroup)
	}

	return b.parameterGroups[region]
}

func (b *InMemoryBackend) clusterEndpointsStore(region string) map[string]*DBClusterEndpoint {
	if b.clusterEndpoints[region] == nil {
		b.clusterEndpoints[region] = make(map[string]*DBClusterEndpoint)
	}

	return b.clusterEndpoints[region]
}

func (b *InMemoryBackend) eventSubscriptionsStore(region string) map[string]*EventSubscription {
	if b.eventSubscriptions[region] == nil {
		b.eventSubscriptions[region] = make(map[string]*EventSubscription)
	}

	return b.eventSubscriptions[region]
}

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

// cloneEventSubscription returns a deep copy of an event subscription (with its SourceIDs slice copied).
func cloneEventSubscription(sub *EventSubscription) EventSubscription {
	cp := *sub
	cp.SourceIDs = make([]string, len(sub.SourceIDs))
	copy(cp.SourceIDs, sub.SourceIDs)

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
// returns the source value from store. notFound is returned when the source is
// missing; alreadyExists when the target already exists.
func copyPreconditions[V any](
	store map[string]*V,
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

	src, exists := store[sourceName]
	if !exists {
		return nil, fmt.Errorf("%w: %s", notFound, sourceName)
	}

	if _, targetExists := store[targetName]; targetExists {
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

// CreateDBCluster creates a new Neptune DB cluster.
func (b *InMemoryBackend) CreateDBCluster(
	ctx context.Context,
	id, paramGroupName string,
	port int,
	opts DBClusterCreateOptions,
) (*DBCluster, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: DBClusterIdentifier is required", ErrInvalidParameter)
	}
	region := getRegion(ctx, b.region)
	b.mu.Lock("CreateDBCluster")
	defer b.mu.Unlock()
	clusters := b.clustersStore(region)
	if _, exists := clusters[id]; exists {
		return nil, fmt.Errorf("%w: cluster %s already exists", ErrClusterAlreadyExists, id)
	}
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
	endpoint := fmt.Sprintf("%s.cluster.%s.neptune.amazonaws.com", id, region)
	readerEndpoint := fmt.Sprintf("%s.cluster-ro.%s.neptune.amazonaws.com", id, region)
	cluster := &DBCluster{
		DBClusterIdentifier:             id,
		DBClusterArn:                    b.clusterARN(region, id),
		Engine:                          neptuneEngine,
		EngineVersion:                   engineVersion,
		EngineMode:                      engineMode,
		Status:                          clusterStatusAvailable,
		DBClusterParameterGroupName:     paramGroupName,
		Endpoint:                        endpoint,
		ReaderEndpoint:                  readerEndpoint,
		Port:                            port,
		DBClusterMembers:                []DBClusterMember{},
		BackupRetentionPeriod:           defaultBackupRetentionPeriod,
		StorageEncrypted:                opts.StorageEncrypted,
		EnableIAMDatabaseAuthentication: opts.EnableIAMDatabaseAuthentication,
		DeletionProtection:              opts.DeletionProtection,
		PreferredBackupWindow:           opts.PreferredBackupWindow,
		PreferredMaintenanceWindow:      opts.PreferredMaintenanceWindow,
		KmsKeyID:                        opts.KmsKeyID,
		ServerlessV2ScalingConfig:       opts.ServerlessV2ScalingConfig,
	}
	if opts.ManageMasterUserPassword {
		cluster.MasterUserManagedSecret = &MasterUserManagedSecret{
			SecretARN:    fmt.Sprintf("arn:aws:secretsmanager:%s:%s:secret:rds!cluster-%s", region, b.accountID, id),
			SecretStatus: "active",
		}
	}
	clusters[id] = cluster
	cp := cloneCluster(cluster)

	return &cp, nil
}

// DescribeDBClusters returns all Neptune DB clusters or a specific one.
func (b *InMemoryBackend) DescribeDBClusters(ctx context.Context, id string) ([]DBCluster, error) {
	region := getRegion(ctx, b.region)
	b.mu.RLock("DescribeDBClusters")
	defer b.mu.RUnlock()
	clusters := b.clustersStore(region)
	if id != "" {
		c, exists := clusters[id]
		if !exists {
			return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
		}

		return []DBCluster{cloneCluster(c)}, nil
	}
	result := make([]DBCluster, 0, len(clusters))
	for _, c := range clusters {
		result = append(result, cloneCluster(c))
	}

	return result, nil
}

// DeleteDBCluster deletes a Neptune DB cluster and all associated DB instances.
func (b *InMemoryBackend) DeleteDBCluster(ctx context.Context, id string) (*DBCluster, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("DeleteDBCluster")
	defer b.mu.Unlock()
	clusters := b.clustersStore(region)
	c, exists := clusters[id]
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
	delete(clusters, id)
	delete(b.tagsStore(region), b.clusterARN(region, id))
	delete(b.clusterRolesStore(region), id)

	// Clean up all instances associated with this cluster.
	instances := b.instancesStore(region)
	tagStore := b.tagsStore(region)
	for instID, inst := range instances {
		if inst.DBClusterIdentifier == id {
			delete(instances, instID)
			delete(tagStore, b.instanceARN(region, instID))
		}
	}

	// Clean up all custom endpoints associated with this cluster.
	endpoints := b.clusterEndpointsStore(region)
	for epID, ep := range endpoints {
		if ep.DBClusterIdentifier == id {
			delete(endpoints, epID)
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
	c, exists := b.clustersStore(region)[id]
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}
	if paramGroupName != "" {
		c.DBClusterParameterGroupName = paramGroupName
	}
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
	if opts.ManageMasterUserPassword {
		if c.MasterUserManagedSecret == nil {
			c.MasterUserManagedSecret = &MasterUserManagedSecret{
				SecretARN: fmt.Sprintf(
					"arn:aws:secretsmanager:%s:%s:secret:rds!cluster-%s",
					region,
					b.accountID,
					id,
				),
				SecretStatus: "active",
			}
		}
	}
	cp := cloneCluster(c)

	return &cp, nil
}

// StopDBCluster stops a Neptune DB cluster.
func (b *InMemoryBackend) StopDBCluster(ctx context.Context, id string) (*DBCluster, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("StopDBCluster")
	defer b.mu.Unlock()
	c, exists := b.clustersStore(region)[id]
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}
	if c.Status == clusterStatusStopped {
		return nil, fmt.Errorf("%w: cluster %s is already stopped", ErrInvalidDBClusterStateFault, id)
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
	c, exists := b.clustersStore(region)[id]
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}
	if c.Status != clusterStatusStopped {
		return nil, fmt.Errorf("%w: cluster %s is not in stopped state", ErrInvalidDBClusterStateFault, id)
	}
	c.Status = clusterStatusAvailable
	cp := cloneCluster(c)

	return &cp, nil
}

// FailoverDBCluster triggers a failover for a Neptune DB cluster.
func (b *InMemoryBackend) FailoverDBCluster(ctx context.Context, id string) (*DBCluster, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("FailoverDBCluster")
	defer b.mu.Unlock()
	c, exists := b.clustersStore(region)[id]
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}
	cp := cloneCluster(c)

	return &cp, nil
}

// CreateDBInstance creates a new Neptune DB instance.
func (b *InMemoryBackend) CreateDBInstance(
	ctx context.Context,
	id, clusterID, instanceClass string,
	opts DBInstanceCreateOptions,
) (*DBInstance, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: DBInstanceIdentifier is required", ErrInvalidParameter)
	}
	region := getRegion(ctx, b.region)
	b.mu.Lock("CreateDBInstance")
	defer b.mu.Unlock()
	instances := b.instancesStore(region)
	clusters := b.clustersStore(region)
	if _, exists := instances[id]; exists {
		return nil, fmt.Errorf("%w: instance %s already exists", ErrInstanceAlreadyExists, id)
	}
	if clusterID != "" {
		if _, exists := clusters[clusterID]; !exists {
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
	if clusterID != "" {
		if cl, ok := clusters[clusterID]; ok {
			engineVersion = cl.EngineVersion
		}
	}
	inst := &DBInstance{
		DBInstanceIdentifier:            id,
		DBInstanceArn:                   b.instanceARN(region, id),
		DBClusterIdentifier:             clusterID,
		DBInstanceClass:                 instanceClass,
		Engine:                          neptuneEngine,
		EngineVersion:                   engineVersion,
		DBInstanceStatus:                clusterStatusAvailable,
		Endpoint:                        endpoint,
		Port:                            defaultNeptunePort,
		AutoMinorVersionUpgrade:         true,
		PreferredMaintenanceWindow:      maintenanceWindow,
		DBParameterGroupName:            opts.DBParameterGroupName,
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
	instances[id] = inst
	if clusterID != "" {
		if cl, ok := clusters[clusterID]; ok {
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
func (b *InMemoryBackend) DescribeDBInstances(ctx context.Context, id string) ([]DBInstance, error) {
	region := getRegion(ctx, b.region)
	b.mu.RLock("DescribeDBInstances")
	defer b.mu.RUnlock()
	instances := b.instancesStore(region)
	if id != "" {
		inst, exists := instances[id]
		if !exists {
			return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, id)
		}
		cp := *inst

		return []DBInstance{cp}, nil
	}
	result := make([]DBInstance, 0, len(instances))
	for _, inst := range instances {
		result = append(result, *inst)
	}

	return result, nil
}

// DeleteDBInstance deletes a Neptune DB instance.
func (b *InMemoryBackend) DeleteDBInstance(ctx context.Context, id string) (*DBInstance, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("DeleteDBInstance")
	defer b.mu.Unlock()
	instances := b.instancesStore(region)
	inst, exists := instances[id]
	if !exists {
		return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, id)
	}
	cp := *inst
	delete(instances, id)
	delete(b.tagsStore(region), b.instanceARN(region, id))
	if cp.DBClusterIdentifier != "" {
		if cl, ok := b.clustersStore(region)[cp.DBClusterIdentifier]; ok {
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
	inst, exists := b.instancesStore(region)[id]
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
	inst, exists := b.instancesStore(region)[id]
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
	subnetGroups := b.subnetGroupsStore(region)
	if _, exists := subnetGroups[name]; exists {
		return nil, fmt.Errorf("%w: subnet group %s already exists", ErrSubnetGroupAlreadyExists, name)
	}
	ids := make([]string, len(subnetIDs))
	copy(ids, subnetIDs)
	sg := &DBSubnetGroup{
		DBSubnetGroupName:        name,
		DBSubnetGroupDescription: description,
		VpcID:                    vpcID,
		Status:                   "Complete",
		SubnetIDs:                ids,
	}
	subnetGroups[name] = sg
	cp := *sg
	cp.SubnetIDs = make([]string, len(ids))
	copy(cp.SubnetIDs, ids)

	return &cp, nil
}

// DescribeDBSubnetGroups returns all Neptune DB subnet groups or a specific one.
func (b *InMemoryBackend) DescribeDBSubnetGroups(ctx context.Context, name string) ([]DBSubnetGroup, error) {
	region := getRegion(ctx, b.region)
	b.mu.RLock("DescribeDBSubnetGroups")
	defer b.mu.RUnlock()
	subnetGroups := b.subnetGroupsStore(region)
	if name != "" {
		sg, exists := subnetGroups[name]
		if !exists {
			return nil, fmt.Errorf("%w: subnet group %s not found", ErrSubnetGroupNotFound, name)
		}

		return []DBSubnetGroup{cloneSubnetGroup(sg)}, nil
	}
	result := make([]DBSubnetGroup, 0, len(subnetGroups))
	for _, sg := range subnetGroups {
		result = append(result, cloneSubnetGroup(sg))
	}

	return result, nil
}

// DeleteDBSubnetGroup deletes a Neptune DB subnet group.
func (b *InMemoryBackend) DeleteDBSubnetGroup(ctx context.Context, name string) error {
	region := getRegion(ctx, b.region)
	b.mu.Lock("DeleteDBSubnetGroup")
	defer b.mu.Unlock()
	subnetGroups := b.subnetGroupsStore(region)
	if _, exists := subnetGroups[name]; !exists {
		return fmt.Errorf("%w: subnet group %s not found", ErrSubnetGroupNotFound, name)
	}
	delete(subnetGroups, name)
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
	groups := b.clusterParameterGroupsStore(region)
	if _, exists := groups[name]; exists {
		return nil, fmt.Errorf(
			"%w: cluster parameter group %s already exists",
			ErrClusterParameterGroupAlreadyExists,
			name,
		)
	}
	pg := &DBClusterParameterGroup{
		DBClusterParameterGroupName: name,
		DBParameterGroupFamily:      family,
		Description:                 description,
	}
	groups[name] = pg
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
	groups := b.clusterParameterGroupsStore(region)
	if name != "" {
		pg, exists := groups[name]
		if !exists {
			return nil, fmt.Errorf("%w: cluster parameter group %s not found", ErrClusterParameterGroupNotFound, name)
		}
		cp := *pg

		return []DBClusterParameterGroup{cp}, nil
	}
	result := make([]DBClusterParameterGroup, 0, len(groups))
	for _, pg := range groups {
		result = append(result, *pg)
	}

	return result, nil
}

// DeleteDBClusterParameterGroup deletes a Neptune DB cluster parameter group.
func (b *InMemoryBackend) DeleteDBClusterParameterGroup(ctx context.Context, name string) error {
	region := getRegion(ctx, b.region)
	b.mu.Lock("DeleteDBClusterParameterGroup")
	defer b.mu.Unlock()
	groups := b.clusterParameterGroupsStore(region)
	if _, exists := groups[name]; !exists {
		return fmt.Errorf("%w: cluster parameter group %s not found", ErrClusterParameterGroupNotFound, name)
	}
	delete(groups, name)
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
	pg, exists := b.clusterParameterGroupsStore(region)[name]
	if !exists {
		return nil, fmt.Errorf("%w: cluster parameter group %s not found", ErrClusterParameterGroupNotFound, name)
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
	snapshots := b.clusterSnapshotsStore(region)
	if _, exists := snapshots[snapshotID]; exists {
		return nil, fmt.Errorf("%w: cluster snapshot %s already exists", ErrClusterSnapshotAlreadyExists, snapshotID)
	}
	cl, exists := b.clustersStore(region)[clusterID]
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, clusterID)
	}
	snap := &DBClusterSnapshot{
		DBClusterSnapshotIdentifier: snapshotID,
		DBClusterSnapshotArn:        b.clusterSnapshotARN(region, snapshotID),
		DBClusterIdentifier:         clusterID,
		Engine:                      neptuneEngine,
		EngineVersion:               cl.EngineVersion,
		Status:                      clusterStatusAvailable,
		StorageEncrypted:            cl.StorageEncrypted,
		SnapshotType:                snapshotSourceManual,
	}
	snapshots[snapshotID] = snap
	cp := *snap

	return &cp, nil
}

// DescribeDBClusterSnapshots returns all Neptune cluster snapshots or a specific one.
// If clusterID is set, results are filtered to that cluster.
func (b *InMemoryBackend) DescribeDBClusterSnapshots(
	ctx context.Context, snapshotID, clusterID string,
) ([]DBClusterSnapshot, error) {
	region := getRegion(ctx, b.region)
	b.mu.RLock("DescribeDBClusterSnapshots")
	defer b.mu.RUnlock()
	snapshots := b.clusterSnapshotsStore(region)
	if snapshotID != "" {
		snap, exists := snapshots[snapshotID]
		if !exists {
			return nil, fmt.Errorf("%w: cluster snapshot %s not found", ErrClusterSnapshotNotFound, snapshotID)
		}
		cp := *snap

		return []DBClusterSnapshot{cp}, nil
	}
	result := make([]DBClusterSnapshot, 0, len(snapshots))
	for _, snap := range snapshots {
		if clusterID != "" && snap.DBClusterIdentifier != clusterID {
			continue
		}
		result = append(result, *snap)
	}

	return result, nil
}

// DeleteDBClusterSnapshot deletes a Neptune DB cluster snapshot.
func (b *InMemoryBackend) DeleteDBClusterSnapshot(ctx context.Context, snapshotID string) (*DBClusterSnapshot, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("DeleteDBClusterSnapshot")
	defer b.mu.Unlock()
	snapshots := b.clusterSnapshotsStore(region)
	snap, exists := snapshots[snapshotID]
	if !exists {
		return nil, fmt.Errorf("%w: cluster snapshot %s not found", ErrClusterSnapshotNotFound, snapshotID)
	}
	cp := *snap
	delete(snapshots, snapshotID)
	delete(b.tagsStore(region), b.clusterSnapshotARN(region, snapshotID))

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
		if _, ok := b.clustersStore(region)[resID]; !ok {
			return fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, resID)
		}
	case "db":
		if _, ok := b.instancesStore(region)[resID]; !ok {
			return fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, resID)
		}
	case "cluster-snapshot":
		if _, ok := b.clusterSnapshotsStore(region)[resID]; !ok {
			return fmt.Errorf("%w: cluster snapshot %s not found", ErrClusterSnapshotNotFound, resID)
		}
	case "subgrp":
		if _, ok := b.subnetGroupsStore(region)[resID]; !ok {
			return fmt.Errorf("%w: subnet group %s not found", ErrSubnetGroupNotFound, resID)
		}
	case "cluster-pg":
		if _, ok := b.clusterParameterGroupsStore(region)[resID]; !ok {
			return fmt.Errorf("%w: cluster parameter group %s not found", ErrClusterParameterGroupNotFound, resID)
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
			return fmt.Errorf("%w: tag key must be 1-%d characters", ErrInvalidParameter, maxTagKeyLen)
		}
		if len(t.Value) > maxTagValueLen {
			return fmt.Errorf("%w: tag value must be 0-%d characters", ErrInvalidParameter, maxTagValueLen)
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
		return fmt.Errorf("%w: resource cannot have more than %d tags", ErrInvalidParameter, maxTagsPerResource)
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
func (b *InMemoryBackend) RemoveTagsFromResource(ctx context.Context, arnStr string, keys []string) error {
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
	if _, exists := b.clustersStore(region)[clusterID]; !exists {
		return fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, clusterID)
	}
	roles := b.clusterRolesStore(region)
	if slices.Contains(roles[clusterID], roleARN) {
		return nil
	}
	roles[clusterID] = append(roles[clusterID], roleARN)

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
	sub, exists := b.eventSubscriptionsStore(region)[name]
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
	groups := b.clusterParameterGroupsStore(region)
	src, err := copyPreconditions(
		groups, sourceName, targetName,
		"SourceDBClusterParameterGroupIdentifier is required",
		"TargetDBClusterParameterGroupIdentifier is required",
		ErrClusterParameterGroupNotFound, ErrClusterParameterGroupAlreadyExists,
	)
	if err != nil {
		return nil, err
	}
	pg := &DBClusterParameterGroup{
		DBClusterParameterGroupName: targetName,
		DBParameterGroupFamily:      src.DBParameterGroupFamily,
		Description:                 resolveCopyDescription(targetDescription, src.Description),
	}
	groups[targetName] = pg
	cp := *pg

	return &cp, nil
}

// CopyDBClusterSnapshot copies a Neptune DB cluster snapshot.
func (b *InMemoryBackend) CopyDBClusterSnapshot(
	ctx context.Context, sourceSnapshotID, targetSnapshotID string,
) (*DBClusterSnapshot, error) {
	if sourceSnapshotID == "" {
		return nil, fmt.Errorf("%w: SourceDBClusterSnapshotIdentifier is required", ErrInvalidParameter)
	}
	if targetSnapshotID == "" {
		return nil, fmt.Errorf("%w: TargetDBClusterSnapshotIdentifier is required", ErrInvalidParameter)
	}
	region := getRegion(ctx, b.region)
	b.mu.Lock("CopyDBClusterSnapshot")
	defer b.mu.Unlock()
	snapshots := b.clusterSnapshotsStore(region)
	src, exists := snapshots[sourceSnapshotID]
	if !exists {
		return nil, fmt.Errorf("%w: cluster snapshot %s not found", ErrClusterSnapshotNotFound, sourceSnapshotID)
	}
	_, targetExists := snapshots[targetSnapshotID]
	if targetExists {
		return nil, fmt.Errorf(
			"%w: cluster snapshot %s already exists",
			ErrClusterSnapshotAlreadyExists,
			targetSnapshotID,
		)
	}
	snap := &DBClusterSnapshot{
		DBClusterSnapshotIdentifier: targetSnapshotID,
		DBClusterSnapshotArn:        b.clusterSnapshotARN(region, targetSnapshotID),
		DBClusterIdentifier:         src.DBClusterIdentifier,
		Engine:                      src.Engine,
		EngineVersion:               src.EngineVersion,
		Status:                      clusterStatusAvailable,
		StorageEncrypted:            src.StorageEncrypted,
		SnapshotType:                snapshotSourceManual,
	}
	snapshots[targetSnapshotID] = snap
	cp := *snap

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
	groups := b.parameterGroupsStore(region)
	src, err := copyPreconditions(
		groups, sourceName, targetName,
		"SourceDBParameterGroupIdentifier is required",
		"TargetDBParameterGroupIdentifier is required",
		ErrParameterGroupNotFound, ErrParameterGroupAlreadyExists,
	)
	if err != nil {
		return nil, err
	}
	pg := &DBParameterGroup{
		DBParameterGroupName:   targetName,
		DBParameterGroupFamily: src.DBParameterGroupFamily,
		Description:            resolveCopyDescription(targetDescription, src.Description),
	}
	groups[targetName] = pg
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
	endpoints := b.clusterEndpointsStore(region)
	if _, exists := endpoints[endpointID]; exists {
		return nil, fmt.Errorf("%w: cluster endpoint %s already exists", ErrClusterEndpointAlreadyExists, endpointID)
	}
	if _, exists := b.clustersStore(region)[clusterID]; !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, clusterID)
	}
	if endpointType == "" {
		endpointType = endpointTypeReader
	}
	switch endpointType {
	case endpointTypeReader, endpointTypeWriter, endpointTypeCustom, endpointTypeAny:
	default:
		return nil, fmt.Errorf("%w: EndpointType must be one of READER, WRITER, CUSTOM, ANY", ErrInvalidParameter)
	}
	ep := &DBClusterEndpoint{
		DBClusterEndpointIdentifier: endpointID,
		DBClusterIdentifier:         clusterID,
		EndpointType:                endpointType,
		Status:                      clusterStatusAvailable,
		Endpoint:                    fmt.Sprintf("%s.cluster-custom.neptune.%s.amazonaws.com", endpointID, region),
	}
	endpoints[endpointID] = ep
	cp := *ep

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
	pgs := b.parameterGroupsStore(region)
	if _, exists := pgs[name]; exists {
		return nil, fmt.Errorf("%w: parameter group %s already exists", ErrParameterGroupAlreadyExists, name)
	}
	pg := &DBParameterGroup{
		DBParameterGroupName:   name,
		DBParameterGroupFamily: family,
		Description:            description,
	}
	pgs[name] = pg
	cp := *pg

	return &cp, nil
}

// CreateEventSubscription creates a Neptune event notification subscription.
func (b *InMemoryBackend) CreateEventSubscription(
	ctx context.Context,
	name, snsTopicARN string,
	sourceIDs []string,
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
	subs := b.eventSubscriptionsStore(region)
	if _, exists := subs[name]; exists {
		return nil, fmt.Errorf("%w: subscription %s already exists", ErrSubscriptionAlreadyExists, name)
	}
	ids := make([]string, len(sourceIDs))
	copy(ids, sourceIDs)
	sub := &EventSubscription{
		CustSubscriptionID: name,
		SnsTopicARN:        snsTopicARN,
		Status:             subscriptionStatusActive,
		SourceIDs:          ids,
	}
	subs[name] = sub
	cp := *sub
	cp.SourceIDs = make([]string, len(ids))
	copy(cp.SourceIDs, ids)

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
	if _, exists := b.globalClusters[globalClusterID]; exists {
		return nil, fmt.Errorf("%w: global cluster %s already exists", ErrGlobalClusterAlreadyExists, globalClusterID)
	}
	gc := &GlobalCluster{
		GlobalClusterIdentifier: globalClusterID,
		Status:                  clusterStatusAvailable,
	}
	if sourceDBClusterID != "" {
		if cl, exists := b.clustersStore(region)[sourceDBClusterID]; exists {
			gc.GlobalClusterMembers = []GlobalClusterMember{
				{
					DBClusterARN: b.clusterARN(region, cl.DBClusterIdentifier),
					IsWriter:     true,
				},
			}
		}
	}
	b.globalClusters[globalClusterID] = gc
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
	result := make([]GlobalCluster, 0, len(b.globalClusters))
	for _, gc := range b.globalClusters {
		cp := *gc
		cp.GlobalClusterMembers = make([]GlobalClusterMember, len(gc.GlobalClusterMembers))
		copy(cp.GlobalClusterMembers, gc.GlobalClusterMembers)
		result = append(result, cp)
	}

	return result
}

// DeleteDBClusterEndpoint deletes a Neptune DB cluster custom endpoint.
func (b *InMemoryBackend) DeleteDBClusterEndpoint(ctx context.Context, endpointID string) error {
	region := getRegion(ctx, b.region)
	b.mu.Lock("DeleteDBClusterEndpoint")
	defer b.mu.Unlock()
	endpoints := b.clusterEndpointsStore(region)
	if _, exists := endpoints[endpointID]; !exists {
		return fmt.Errorf("%w: cluster endpoint %s not found", ErrClusterEndpointNotFound, endpointID)
	}
	delete(endpoints, endpointID)

	return nil
}

// DescribeDBClusterEndpoints returns all Neptune DB cluster endpoints or a specific one.
func (b *InMemoryBackend) DescribeDBClusterEndpoints(
	ctx context.Context, endpointID, clusterID string,
) ([]DBClusterEndpoint, error) {
	region := getRegion(ctx, b.region)
	b.mu.RLock("DescribeDBClusterEndpoints")
	defer b.mu.RUnlock()
	clusterEndpoints := b.clusterEndpointsStore(region)
	if endpointID != "" {
		ep, exists := clusterEndpoints[endpointID]
		if !exists {
			return nil, fmt.Errorf("%w: cluster endpoint %s not found", ErrClusterEndpointNotFound, endpointID)
		}
		cp := *ep

		return []DBClusterEndpoint{cp}, nil
	}
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
	ep, exists := b.clusterEndpointsStore(region)[endpointID]
	if !exists {
		return nil, fmt.Errorf("%w: cluster endpoint %s not found", ErrClusterEndpointNotFound, endpointID)
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
	groups := b.parameterGroupsStore(region)
	if _, exists := groups[name]; !exists {
		return fmt.Errorf("%w: parameter group %s not found", ErrParameterGroupNotFound, name)
	}
	delete(groups, name)

	return nil
}

// DescribeDBParameterGroups returns all Neptune DB parameter groups or a specific one.
func (b *InMemoryBackend) DescribeDBParameterGroups(ctx context.Context, name string) ([]DBParameterGroup, error) {
	region := getRegion(ctx, b.region)
	b.mu.RLock("DescribeDBParameterGroups")
	defer b.mu.RUnlock()
	groups := b.parameterGroupsStore(region)
	if name != "" {
		pg, exists := groups[name]
		if !exists {
			return nil, fmt.Errorf("%w: parameter group %s not found", ErrParameterGroupNotFound, name)
		}
		cp := *pg

		return []DBParameterGroup{cp}, nil
	}
	result := make([]DBParameterGroup, 0, len(groups))
	for _, pg := range groups {
		result = append(result, *pg)
	}

	return result, nil
}

// ModifyDBParameterGroup modifies a Neptune DB parameter group.
func (b *InMemoryBackend) ModifyDBParameterGroup(ctx context.Context, name string) (*DBParameterGroup, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("ModifyDBParameterGroup")
	defer b.mu.Unlock()
	pg, exists := b.parameterGroupsStore(region)[name]
	if !exists {
		return nil, fmt.Errorf("%w: parameter group %s not found", ErrParameterGroupNotFound, name)
	}
	cp := *pg

	return &cp, nil
}

// ResetDBParameterGroup resets a Neptune DB parameter group to its default values.
func (b *InMemoryBackend) ResetDBParameterGroup(ctx context.Context, name string) (*DBParameterGroup, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("ResetDBParameterGroup")
	defer b.mu.Unlock()
	pg, exists := b.parameterGroupsStore(region)[name]
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
	pg, exists := b.clusterParameterGroupsStore(region)[name]
	if !exists {
		return nil, fmt.Errorf("%w: cluster parameter group %s not found", ErrClusterParameterGroupNotFound, name)
	}
	cp := *pg

	return &cp, nil
}

// DeleteEventSubscription deletes a Neptune event subscription.
func (b *InMemoryBackend) DeleteEventSubscription(ctx context.Context, name string) (*EventSubscription, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("DeleteEventSubscription")
	defer b.mu.Unlock()
	subs := b.eventSubscriptionsStore(region)
	sub, exists := subs[name]
	if !exists {
		return nil, fmt.Errorf("%w: subscription %s not found", ErrSubscriptionNotFound, name)
	}
	cp := *sub
	cp.SourceIDs = make([]string, len(sub.SourceIDs))
	copy(cp.SourceIDs, sub.SourceIDs)
	delete(subs, name)

	return &cp, nil
}

// DescribeEventSubscriptions returns all event subscriptions or a specific one.
func (b *InMemoryBackend) DescribeEventSubscriptions(ctx context.Context, name string) ([]EventSubscription, error) {
	region := getRegion(ctx, b.region)
	b.mu.RLock("DescribeEventSubscriptions")
	defer b.mu.RUnlock()
	subs := b.eventSubscriptionsStore(region)
	if name != "" {
		sub, exists := subs[name]
		if !exists {
			return nil, fmt.Errorf("%w: subscription %s not found", ErrSubscriptionNotFound, name)
		}

		return []EventSubscription{cloneEventSubscription(sub)}, nil
	}
	result := make([]EventSubscription, 0, len(subs))
	for _, sub := range subs {
		result = append(result, cloneEventSubscription(sub))
	}

	return result, nil
}

// ModifyEventSubscription modifies a Neptune event subscription.
func (b *InMemoryBackend) ModifyEventSubscription(
	ctx context.Context, name, snsTopicARN string,
) (*EventSubscription, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("ModifyEventSubscription")
	defer b.mu.Unlock()
	sub, exists := b.eventSubscriptionsStore(region)[name]
	if !exists {
		return nil, fmt.Errorf("%w: subscription %s not found", ErrSubscriptionNotFound, name)
	}
	if snsTopicARN != "" {
		sub.SnsTopicARN = snsTopicARN
	}
	cp := *sub
	cp.SourceIDs = make([]string, len(sub.SourceIDs))
	copy(cp.SourceIDs, sub.SourceIDs)

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
	sub, exists := b.eventSubscriptionsStore(region)[name]
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
func (b *InMemoryBackend) DeleteGlobalCluster(_ context.Context, globalClusterID string) (*GlobalCluster, error) {
	b.mu.Lock("DeleteGlobalCluster")
	defer b.mu.Unlock()
	gc, exists := b.globalClusters[globalClusterID]
	if !exists {
		return nil, fmt.Errorf("%w: global cluster %s not found", ErrGlobalClusterNotFound, globalClusterID)
	}
	cp := *gc
	cp.GlobalClusterMembers = make([]GlobalClusterMember, len(gc.GlobalClusterMembers))
	copy(cp.GlobalClusterMembers, gc.GlobalClusterMembers)
	delete(b.globalClusters, globalClusterID)

	return &cp, nil
}

// FailoverGlobalCluster performs a failover for a Neptune global cluster (partition-scoped).
// targetDBClusterID is accepted for API compatibility but not used in the in-memory backend.
func (b *InMemoryBackend) FailoverGlobalCluster(
	_ context.Context, globalClusterID, _ string,
) (*GlobalCluster, error) {
	b.mu.Lock("FailoverGlobalCluster")
	defer b.mu.Unlock()
	gc, exists := b.globalClusters[globalClusterID]
	if !exists {
		return nil, fmt.Errorf("%w: global cluster %s not found", ErrGlobalClusterNotFound, globalClusterID)
	}
	cp := *gc
	cp.GlobalClusterMembers = make([]GlobalClusterMember, len(gc.GlobalClusterMembers))
	copy(cp.GlobalClusterMembers, gc.GlobalClusterMembers)

	return &cp, nil
}

// ModifyGlobalCluster modifies a Neptune global cluster (partition-scoped).
func (b *InMemoryBackend) ModifyGlobalCluster(_ context.Context, globalClusterID string) (*GlobalCluster, error) {
	b.mu.Lock("ModifyGlobalCluster")
	defer b.mu.Unlock()
	gc, exists := b.globalClusters[globalClusterID]
	if !exists {
		return nil, fmt.Errorf("%w: global cluster %s not found", ErrGlobalClusterNotFound, globalClusterID)
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
	gc, exists := b.globalClusters[globalClusterID]
	if !exists {
		return nil, fmt.Errorf("%w: global cluster %s not found", ErrGlobalClusterNotFound, globalClusterID)
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
	gc, exists := b.globalClusters[globalClusterID]
	if !exists {
		return nil, fmt.Errorf("%w: global cluster %s not found", ErrGlobalClusterNotFound, globalClusterID)
	}
	cp := *gc
	cp.GlobalClusterMembers = make([]GlobalClusterMember, len(gc.GlobalClusterMembers))
	copy(cp.GlobalClusterMembers, gc.GlobalClusterMembers)

	return &cp, nil
}

// RemoveRoleFromDBCluster removes an IAM role association from a Neptune DB cluster.
func (b *InMemoryBackend) RemoveRoleFromDBCluster(ctx context.Context, clusterID, roleARN string) error {
	if clusterID == "" {
		return fmt.Errorf("%w: DBClusterIdentifier is required", ErrInvalidParameter)
	}
	if roleARN == "" {
		return fmt.Errorf("%w: RoleArn is required", ErrInvalidParameter)
	}
	region := getRegion(ctx, b.region)
	b.mu.Lock("RemoveRoleFromDBCluster")
	defer b.mu.Unlock()
	if _, exists := b.clustersStore(region)[clusterID]; !exists {
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
	clusters := b.clustersStore(region)
	snap, snapExists := b.clusterSnapshotsStore(region)[snapshotID]
	if !snapExists {
		return nil, fmt.Errorf("%w: cluster snapshot %s not found", ErrClusterSnapshotNotFound, snapshotID)
	}
	if _, clExists := clusters[clusterID]; clExists {
		return nil, fmt.Errorf("%w: cluster %s already exists", ErrClusterAlreadyExists, clusterID)
	}
	// Derive parameter group from the source cluster if available.
	paramGroupName := pgFamilyDefaultNeptune13
	if srcCluster, ok := clusters[snap.DBClusterIdentifier]; ok {
		paramGroupName = srcCluster.DBClusterParameterGroupName
	}
	endpoint := fmt.Sprintf("%s.cluster.%s.neptune.amazonaws.com", clusterID, region)
	readerEndpoint := fmt.Sprintf("%s.cluster-ro.%s.neptune.amazonaws.com", clusterID, region)
	cluster := &DBCluster{
		DBClusterIdentifier:         clusterID,
		DBClusterArn:                b.clusterARN(region, clusterID),
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
	clusters[clusterID] = cluster
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
	clusters := b.clustersStore(region)
	src, srcExists := clusters[srcClusterID]
	if !srcExists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, srcClusterID)
	}
	if _, tgtExists := clusters[targetClusterID]; tgtExists {
		return nil, fmt.Errorf("%w: cluster %s already exists", ErrClusterAlreadyExists, targetClusterID)
	}
	endpoint := fmt.Sprintf("%s.cluster.%s.neptune.amazonaws.com", targetClusterID, region)
	readerEndpoint := fmt.Sprintf("%s.cluster-ro.%s.neptune.amazonaws.com", targetClusterID, region)
	cluster := &DBCluster{
		DBClusterIdentifier:             targetClusterID,
		DBClusterArn:                    b.clusterARN(region, targetClusterID),
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
	clusters[targetClusterID] = cluster
	cp := cloneCluster(cluster)

	return &cp, nil
}

// ModifyDBSubnetGroup modifies a Neptune DB subnet group.
func (b *InMemoryBackend) ModifyDBSubnetGroup(ctx context.Context, name, description string) (*DBSubnetGroup, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("ModifyDBSubnetGroup")
	defer b.mu.Unlock()
	sg, exists := b.subnetGroupsStore(region)[name]
	if !exists {
		return nil, fmt.Errorf("%w: subnet group %s not found", ErrSubnetGroupNotFound, name)
	}
	if description != "" {
		sg.DBSubnetGroupDescription = description
	}
	cp := *sg
	cp.SubnetIDs = make([]string, len(sg.SubnetIDs))
	copy(cp.SubnetIDs, sg.SubnetIDs)

	return &cp, nil
}

// AccountID returns the backend's AWS account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Reset clears all backend state, returning it to a clean empty state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()
	b.clusters = make(map[string]map[string]*DBCluster)
	b.instances = make(map[string]map[string]*DBInstance)
	b.subnetGroups = make(map[string]map[string]*DBSubnetGroup)
	b.clusterParameterGroups = make(map[string]map[string]*DBClusterParameterGroup)
	b.clusterSnapshots = make(map[string]map[string]*DBClusterSnapshot)
	b.parameterGroups = make(map[string]map[string]*DBParameterGroup)
	b.clusterEndpoints = make(map[string]map[string]*DBClusterEndpoint)
	b.eventSubscriptions = make(map[string]map[string]*EventSubscription)
	b.clusterRoles = make(map[string]map[string][]string)
	b.tags = make(map[string]map[string][]Tag)
	b.globalClusters = make(map[string]*GlobalCluster)
}

// AddClusterInternal creates a cluster directly, bypassing normal validation. Used for seeding tests.
func (b *InMemoryBackend) AddClusterInternal(id string) *DBCluster {
	b.mu.Lock("AddClusterInternal")
	defer b.mu.Unlock()
	endpoint := fmt.Sprintf("%s.cluster.%s.neptune.amazonaws.com", id, b.region)
	readerEndpoint := fmt.Sprintf("%s.cluster-ro.%s.neptune.amazonaws.com", id, b.region)
	c := &DBCluster{
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
	b.clustersStore(b.region)[id] = c
	cp := cloneCluster(c)

	return &cp
}

// AddSnapshotInternal creates a snapshot directly, bypassing normal validation. Used for seeding tests.
func (b *InMemoryBackend) AddSnapshotInternal(snapshotID, clusterID string) *DBClusterSnapshot {
	b.mu.Lock("AddSnapshotInternal")
	defer b.mu.Unlock()
	snap := &DBClusterSnapshot{
		DBClusterSnapshotIdentifier: snapshotID,
		DBClusterSnapshotArn:        b.clusterSnapshotARN(b.region, snapshotID),
		DBClusterIdentifier:         clusterID,
		Engine:                      neptuneEngine,
		EngineVersion:               defaultEngineVersion,
		Status:                      clusterStatusAvailable,
		SnapshotType:                snapshotSourceManual,
	}
	b.clusterSnapshotsStore(b.region)[snapshotID] = snap
	cp := *snap

	return &cp
}

// AddClusterParameterGroupInternal creates a cluster parameter group directly. Used for seeding tests.
func (b *InMemoryBackend) AddClusterParameterGroupInternal(name, family string) *DBClusterParameterGroup {
	b.mu.Lock("AddClusterParameterGroupInternal")
	defer b.mu.Unlock()
	pg := &DBClusterParameterGroup{
		DBClusterParameterGroupName: name,
		DBParameterGroupFamily:      family,
		Description:                 "seeded for tests",
	}
	b.clusterParameterGroupsStore(b.region)[name] = pg
	cp := *pg

	return &cp
}

// AddParameterGroupInternal creates a DB parameter group directly. Used for seeding tests.
func (b *InMemoryBackend) AddParameterGroupInternal(name, family string) *DBParameterGroup {
	b.mu.Lock("AddParameterGroupInternal")
	defer b.mu.Unlock()
	pg := &DBParameterGroup{
		DBParameterGroupName:   name,
		DBParameterGroupFamily: family,
		Description:            "seeded for tests",
	}
	b.parameterGroupsStore(b.region)[name] = pg
	cp := *pg

	return &cp
}

// AddEventSubscriptionInternal creates an event subscription directly. Used for seeding tests.
func (b *InMemoryBackend) AddEventSubscriptionInternal(name, snsTopicARN string) *EventSubscription {
	b.mu.Lock("AddEventSubscriptionInternal")
	defer b.mu.Unlock()
	sub := &EventSubscription{
		CustSubscriptionID: name,
		SnsTopicARN:        snsTopicARN,
		Status:             subscriptionStatusActive,
	}
	b.eventSubscriptionsStore(b.region)[name] = sub
	cp := *sub

	return &cp
}
