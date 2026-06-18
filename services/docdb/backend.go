package docdb

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// regionContextKey is the context key under which the per-request AWS region is stored.
type regionContextKey struct{}

// getRegion extracts the region from ctx, falling back to defaultRegion when unset.
// DocDB resources are isolated per region: every backend operation resolves the
// caller's region from the request context and operates only on that region's
// nested store.
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

var (
	ErrClusterNotFound                    = awserr.New("DBClusterNotFoundFault", awserr.ErrNotFound)
	ErrClusterAlreadyExists               = awserr.New("DBClusterAlreadyExistsFault", awserr.ErrAlreadyExists)
	ErrInstanceNotFound                   = awserr.New("DBInstanceNotFound", awserr.ErrNotFound)
	ErrInstanceAlreadyExists              = awserr.New("DBInstanceAlreadyExists", awserr.ErrAlreadyExists)
	ErrSubnetGroupNotFound                = awserr.New("DBSubnetGroupNotFoundFault", awserr.ErrNotFound)
	ErrSubnetGroupAlreadyExists           = awserr.New("DBSubnetGroupAlreadyExistsFault", awserr.ErrAlreadyExists)
	ErrSubnetGroupInUse                   = awserr.New("InvalidDBSubnetGroupStateFault", awserr.ErrInvalidParameter)
	ErrClusterParameterGroupNotFound      = awserr.New("DBClusterParameterGroupNotFoundFault", awserr.ErrNotFound)
	ErrClusterParameterGroupAlreadyExists = awserr.New(
		"DBClusterParameterGroupAlreadyExistsFault",
		awserr.ErrAlreadyExists,
	)
	ErrParameterGroupInUse            = awserr.New("InvalidDBParameterGroupStateFault", awserr.ErrInvalidParameter)
	ErrClusterSnapshotNotFound        = awserr.New("DBClusterSnapshotNotFoundFault", awserr.ErrNotFound)
	ErrClusterSnapshotAlreadyExists   = awserr.New("DBClusterSnapshotAlreadyExistsFault", awserr.ErrAlreadyExists)
	ErrEventSubscriptionNotFound      = awserr.New("SubscriptionNotFoundFault", awserr.ErrNotFound)
	ErrEventSubscriptionAlreadyExists = awserr.New("SubscriptionAlreadyExistFault", awserr.ErrAlreadyExists)
	ErrGlobalClusterNotFound          = awserr.New("GlobalClusterNotFoundFault", awserr.ErrNotFound)
	ErrGlobalClusterAlreadyExists     = awserr.New("GlobalClusterAlreadyExistsFault", awserr.ErrAlreadyExists)
	ErrInvalidParameter               = awserr.New("InvalidParameterValue", awserr.ErrInvalidParameter)
	ErrUnknownAction                  = awserr.New("InvalidAction", awserr.ErrInvalidParameter)
	ErrInvalidClusterState            = awserr.New("InvalidDBClusterStateFault", awserr.ErrInvalidParameter)
)

const (
	defaultDocDBPort           = 27017
	defaultInstanceClass       = "db.t3.medium"
	defaultEngineVersion       = "4.0.0"
	docDBEngineVersion36       = "3.6.0"
	docDBEngineVersion5        = "5.0.0"
	docDBEngine                = "docdb"
	snapshotPercentageComplete = 100

	paramEnabled   = "enabled"
	paramTypeStr   = "string"
	eventCatBackup = "backup"
	eventCatCreate = "creation"
	eventCatDelete = "deletion"
	eventCatNotify = "notification"

	defaultBackupWindow      = "00:00-01:00" //nolint:gosec // not a credential
	defaultMaintenanceWindow = "mon:05:00-mon:05:30"

	statusAvailable = "available"
	statusStopped   = "stopped"
	statusDeleting  = "deleting"

	// OptInType constants for ApplyPendingMaintenanceAction.
	optInTypeImmediate       = "immediate"
	optInTypeNextMaintenance = "next-maintenance"
	optInTypeUndoOptIn       = "undo-opt-in"

	maxTagCount    = 50
	maxTagKeyLen   = 128
	maxTagValueLen = 256

	maxPromotionTier         = 15
	maxBackupRetentionPeriod = 35
)

var validDocDBVersions = map[string]bool{ //nolint:gochecknoglobals // compile-time constant set
	docDBEngineVersion36: true,
	defaultEngineVersion: true,
	docDBEngineVersion5:  true,
}

// validateEngineVersion returns an error if engineVersion is non-empty and not a valid DocDB version.
func validateEngineVersion(engineVersion string) error {
	if engineVersion == "" {
		return nil
	}
	if !validDocDBVersions[engineVersion] {
		return fmt.Errorf(
			"%w: EngineVersion %q is not valid for engine %q; valid values: 3.6.0, 4.0.0, 5.0.0",
			ErrInvalidParameter, engineVersion, docDBEngine,
		)
	}

	return nil
}

// validateMasterUserPassword validates per AWS rules: 8-100 chars, no '/', '"', or '@'.
func validateMasterUserPassword(pw string) error {
	if len(pw) < 8 || len(pw) > 100 {
		return fmt.Errorf("%w: MasterUserPassword must be between 8 and 100 characters", ErrInvalidParameter)
	}
	if strings.ContainsAny(pw, "/\"@") {
		return fmt.Errorf("%w: MasterUserPassword must not contain '/', '\"', or '@'", ErrInvalidParameter)
	}

	return nil
}

// validateTags enforces AWS tag limits: key 1-128 chars, value 0-256 chars, max 50 tags.
func validateTags(tags map[string]string) error {
	if len(tags) > maxTagCount {
		return fmt.Errorf("%w: cannot specify more than %d tags per resource", ErrInvalidParameter, maxTagCount)
	}
	for k, v := range tags {
		if len(k) == 0 || len(k) > maxTagKeyLen {
			return fmt.Errorf("%w: tag key must be between 1 and %d characters", ErrInvalidParameter, maxTagKeyLen)
		}
		if len(v) > maxTagValueLen {
			return fmt.Errorf("%w: tag value must be at most %d characters", ErrInvalidParameter, maxTagValueLen)
		}
	}

	return nil
}

// validateTagList enforces AWS tag limits on a slice of Tag.
func validateTagList(tags []Tag) error {
	if len(tags) > maxTagCount {
		return fmt.Errorf("%w: cannot specify more than %d tags per resource", ErrInvalidParameter, maxTagCount)
	}
	for _, t := range tags {
		if len(t.Key) == 0 || len(t.Key) > maxTagKeyLen {
			return fmt.Errorf("%w: tag key must be between 1 and %d characters", ErrInvalidParameter, maxTagKeyLen)
		}
		if len(t.Value) > maxTagValueLen {
			return fmt.Errorf("%w: tag value must be at most %d characters", ErrInvalidParameter, maxTagValueLen)
		}
	}

	return nil
}

type DBCluster struct {
	Tags                             map[string]string `json:"tags"`
	DBClusterArn                     string            `json:"dbClusterArn"`
	EngineVersion                    string            `json:"engineVersion"`
	Engine                           string            `json:"engine"`
	PreferredMaintenanceWindow       string            `json:"preferredMaintenanceWindow"`
	MasterUsername                   string            `json:"masterUsername"`
	DatabaseName                     string            `json:"databaseName"`
	DBClusterParameterGroupName      string            `json:"dbClusterParameterGroupName"`
	Endpoint                         string            `json:"endpoint"`
	DBClusterIdentifier              string            `json:"dbClusterIdentifier"`
	ReaderEndpoint                   string            `json:"readerEndpoint"`
	Status                           string            `json:"status"`
	DBSubnetGroupName                string            `json:"dbSubnetGroupName"`
	PreferredBackupWindow            string            `json:"preferredBackupWindow"`
	ClusterCreateTime                string            `json:"clusterCreateTime"`
	HostedZoneID                     string            `json:"hostedZoneId"`
	KmsKeyID                         string            `json:"kmsKeyId"`
	ReplicationSourceIdentifier      string            `json:"replicationSourceIdentifier"`
	AvailabilityZones                []string          `json:"availabilityZones"`
	VpcSecurityGroupIDs              []string          `json:"vpcSecurityGroupIds"`
	EnabledCloudwatchLogsExports     []string          `json:"enabledCloudwatchLogsExports"`
	ReadReplicaIdentifiers           []string          `json:"readReplicaIdentifiers"`
	Port                             int               `json:"port"`
	BackupRetentionPeriod            int               `json:"backupRetentionPeriod"`
	StorageEncrypted                 bool              `json:"storageEncrypted"`
	MultiAZ                          bool              `json:"multiAZ"`
	DeletionProtection               bool              `json:"deletionProtection"`
	IAMDatabaseAuthenticationEnabled bool              `json:"iamDatabaseAuthenticationEnabled"`
}

type DBInstance struct {
	Tags                         map[string]string `json:"tags"`
	DBInstanceIdentifier         string            `json:"dbInstanceIdentifier"`
	DBClusterIdentifier          string            `json:"dbClusterIdentifier"`
	DBInstanceClass              string            `json:"dbInstanceClass"`
	Engine                       string            `json:"engine"`
	DBInstanceStatus             string            `json:"dbInstanceStatus"`
	Endpoint                     string            `json:"endpoint"`
	DBInstanceArn                string            `json:"dbInstanceArn"`
	EngineVersion                string            `json:"engineVersion"`
	AvailabilityZone             string            `json:"availabilityZone"`
	DBSubnetGroupName            string            `json:"dbSubnetGroupName"`
	PreferredMaintenanceWindow   string            `json:"preferredMaintenanceWindow"`
	CACertificateIdentifier      string            `json:"caCertificateIdentifier"`
	EnabledCloudwatchLogsExports []string          `json:"enabledCloudwatchLogsExports"`
	Port                         int               `json:"port"`
	PromotionTier                int               `json:"promotionTier"`
	StorageEncrypted             bool              `json:"storageEncrypted"`
	AutoMinorVersionUpgrade      bool              `json:"autoMinorVersionUpgrade"`
	PubliclyAccessible           bool              `json:"publiclyAccessible"`
	CopyTagsToSnapshot           bool              `json:"copyTagsToSnapshot"`
}

type DBSubnetGroup struct {
	Tags                     map[string]string `json:"tags"`
	DBSubnetGroupName        string            `json:"dbSubnetGroupName"`
	DBSubnetGroupDescription string            `json:"dbSubnetGroupDescription"`
	VpcID                    string            `json:"vpcID"`
	Status                   string            `json:"status"`
	DBSubnetGroupArn         string            `json:"dbSubnetGroupArn"`
	SubnetIDs                []string          `json:"subnetIDs"`
}

type Tag struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type DBClusterParameterGroup struct {
	Tags                        map[string]string `json:"tags"`
	DBClusterParameterGroupName string            `json:"dbClusterParameterGroupName"`
	DBParameterGroupFamily      string            `json:"dbParameterGroupFamily"`
	Description                 string            `json:"description"`
	DBClusterParameterGroupArn  string            `json:"dbClusterParameterGroupArn"`
}

type DBClusterSnapshot struct {
	Tags                        map[string]string `json:"tags"`
	DBClusterSnapshotIdentifier string            `json:"dbClusterSnapshotIdentifier"`
	DBClusterIdentifier         string            `json:"dbClusterIdentifier"`
	DBClusterArn                string            `json:"dbClusterArn"`
	Engine                      string            `json:"engine"`
	Status                      string            `json:"status"`
	EngineVersion               string            `json:"engineVersion"`
	SnapshotType                string            `json:"snapshotType"`
	SnapshotCreateTime          string            `json:"snapshotCreateTime"`
	PercentProgress             int               `json:"percentProgress"`
	StorageEncrypted            bool              `json:"storageEncrypted"`
}

type EventSubscription struct {
	SubscriptionName string   `json:"subscriptionName"`
	SnsTopicARN      string   `json:"snsTopicARN"`
	Status           string   `json:"status"`
	SourceType       string   `json:"sourceType"`
	SourceIDs        []string `json:"sourceIDs"`
	EventCategories  []string `json:"eventCategories"`
}

type GlobalCluster struct {
	GlobalClusterIdentifier string `json:"globalClusterIdentifier"`
	SourceDBClusterID       string `json:"sourceDBClusterID"`
	Status                  string `json:"status"`
	Engine                  string `json:"engine"`
	EngineVersion           string `json:"engineVersion"`
	GlobalClusterArn        string `json:"globalClusterArn"`
	StorageEncrypted        bool   `json:"storageEncrypted"`
	DeletionProtection      bool   `json:"deletionProtection"`
}

type Certificate struct {
	CertificateIdentifier string
	CertificateType       string
	Thumbprint            string
	ValidFrom             string
	ValidTill             string
}

type DBClusterParameter struct {
	ParameterName  string
	ParameterValue string
	Description    string
	Source         string
	ApplyType      string
	DataType       string
	IsModifiable   bool
}

// DBClusterSnapshotAttribute represents a single attribute of a cluster snapshot.
type DBClusterSnapshotAttribute struct {
	AttributeName   string   `json:"attributeName"`
	AttributeValues []string `json:"attributeValues"`
}

// DBClusterSnapshotAttributesResult holds the attributes for a cluster snapshot.
type DBClusterSnapshotAttributesResult struct {
	DBClusterSnapshotIdentifier string                       `json:"dbClusterSnapshotIdentifier"`
	Attributes                  []DBClusterSnapshotAttribute `json:"attributes"`
}

// ResourcePendingMaintenanceActions holds pending maintenance actions for a resource.
type ResourcePendingMaintenanceActions struct {
	ResourceIdentifier string
	Actions            []PendingMaintenanceAction
}

// PendingMaintenanceAction describes a pending maintenance action.
type PendingMaintenanceAction struct {
	Action      string
	OptInStatus string
}

// EventCategoryMap maps a source type to a list of event categories.
type EventCategoryMap struct {
	SourceType      string
	EventCategories []string
}

// InMemoryBackend is the in-memory store for DocDB resources.
//
// All resource maps except globalClusters are nested by region (outer key =
// region) so same-named resources are isolated across regions. Per-region inner
// maps are created lazily via the *Store helpers. Callers must hold b.mu while
// accessing the inner maps. GlobalClusters are partition-scoped and remain flat.
type InMemoryBackend struct {
	clusters               map[string]map[string]*DBCluster
	instances              map[string]map[string]*DBInstance
	subnetGroups           map[string]map[string]*DBSubnetGroup
	clusterParameterGroups map[string]map[string]*DBClusterParameterGroup
	clusterSnapshots       map[string]map[string]*DBClusterSnapshot
	eventSubscriptions     map[string]map[string]*EventSubscription
	globalClusters         map[string]*GlobalCluster
	snapshotAttributes     map[string]map[string]*DBClusterSnapshotAttributesResult
	tags                   map[string]map[string][]Tag
	mu                     *lockmetrics.RWMutex
	accountID              string
	region                 string
}

func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		clusters:               make(map[string]map[string]*DBCluster),
		instances:              make(map[string]map[string]*DBInstance),
		subnetGroups:           make(map[string]map[string]*DBSubnetGroup),
		clusterParameterGroups: make(map[string]map[string]*DBClusterParameterGroup),
		clusterSnapshots:       make(map[string]map[string]*DBClusterSnapshot),
		eventSubscriptions:     make(map[string]map[string]*EventSubscription),
		globalClusters:         make(map[string]*GlobalCluster),
		snapshotAttributes:     make(map[string]map[string]*DBClusterSnapshotAttributesResult),
		tags:                   make(map[string]map[string][]Tag),
		accountID:              accountID,
		region:                 region,
		mu:                     lockmetrics.New("docdb"),
	}
}

// Region returns the backend's configured default AWS region.
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

func (b *InMemoryBackend) eventSubscriptionsStore(region string) map[string]*EventSubscription {
	if b.eventSubscriptions[region] == nil {
		b.eventSubscriptions[region] = make(map[string]*EventSubscription)
	}

	return b.eventSubscriptions[region]
}

func (b *InMemoryBackend) snapshotAttributesStore(region string) map[string]*DBClusterSnapshotAttributesResult {
	if b.snapshotAttributes[region] == nil {
		b.snapshotAttributes[region] = make(map[string]*DBClusterSnapshotAttributesResult)
	}

	return b.snapshotAttributes[region]
}

func (b *InMemoryBackend) tagsStore(region string) map[string][]Tag {
	if b.tags[region] == nil {
		b.tags[region] = make(map[string][]Tag)
	}

	return b.tags[region]
}

// Reset clears all stored state, returning the backend to an empty state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.clusters = make(map[string]map[string]*DBCluster)
	b.instances = make(map[string]map[string]*DBInstance)
	b.subnetGroups = make(map[string]map[string]*DBSubnetGroup)
	b.clusterParameterGroups = make(map[string]map[string]*DBClusterParameterGroup)
	b.clusterSnapshots = make(map[string]map[string]*DBClusterSnapshot)
	b.eventSubscriptions = make(map[string]map[string]*EventSubscription)
	b.globalClusters = make(map[string]*GlobalCluster)
	b.snapshotAttributes = make(map[string]map[string]*DBClusterSnapshotAttributesResult)
	b.tags = make(map[string]map[string][]Tag)
}

// clusterARN returns the ARN for a DB cluster in the given region.
func (b *InMemoryBackend) clusterARN(region, id string) string {
	return arn.Build("rds", region, b.accountID, "cluster:"+id)
}

// instanceARN returns the ARN for a DB instance in the given region.
func (b *InMemoryBackend) instanceARN(region, id string) string {
	return arn.Build("rds", region, b.accountID, "db:"+id)
}

// subnetGroupARN returns the ARN for a DB subnet group in the given region.
func (b *InMemoryBackend) subnetGroupARN(region, name string) string {
	return arn.Build("rds", region, b.accountID, "subgrp:"+name)
}

// clusterParameterGroupARN returns the ARN for a DB cluster parameter group in the given region.
func (b *InMemoryBackend) clusterParameterGroupARN(region, name string) string {
	return arn.Build("rds", region, b.accountID, "cluster-pg:"+name)
}

// clusterSnapshotARN returns the ARN for a DB cluster snapshot in the given region.
func (b *InMemoryBackend) clusterSnapshotARN(region, id string) string {
	return arn.Build("rds", region, b.accountID, "cluster-snapshot:"+id)
}

// globalClusterARN returns the ARN for a global cluster.
func (b *InMemoryBackend) globalClusterARN(id string) string {
	return arn.Build("rds", b.region, b.accountID, "global-cluster:"+id)
}

func validateCreateDBClusterParams(
	id, engineVersion, masterUserPassword string,
	backupRetentionPeriod int,
	tags map[string]string,
) error {
	if id == "" {
		return fmt.Errorf("%w: DBClusterIdentifier is required", ErrInvalidParameter)
	}
	if err := validateEngineVersion(engineVersion); err != nil {
		return err
	}
	if masterUserPassword != "" {
		if err := validateMasterUserPassword(masterUserPassword); err != nil {
			return err
		}
	}
	if err := validateTags(tags); err != nil {
		return err
	}
	if backupRetentionPeriod != 0 && (backupRetentionPeriod < 1 || backupRetentionPeriod > maxBackupRetentionPeriod) {
		return fmt.Errorf(
			"%w: BackupRetentionPeriod must be between 1 and %d",
			ErrInvalidParameter, maxBackupRetentionPeriod,
		)
	}

	return nil
}

func (b *InMemoryBackend) CreateDBCluster(
	ctx context.Context,
	id, engine, engineVersion, masterUser, masterUserPassword, dbName, paramGroupName, subnetGroupName string,
	port int,
	storageEncrypted, deletionProtection bool,
	backupRetentionPeriod int,
	preferredBackupWindow, preferredMaintenanceWindow string,
	availabilityZones []string,
	tags map[string]string,
	opts *CreateDBClusterOptions,
) (*DBCluster, error) {
	if err := validateCreateDBClusterParams(
		id, engineVersion, masterUserPassword, backupRetentionPeriod, tags,
	); err != nil {
		return nil, err
	}
	region := getRegion(ctx, b.region)
	b.mu.Lock("CreateDBCluster")
	defer b.mu.Unlock()
	clusters := b.clustersStore(region)
	if _, exists := clusters[id]; exists {
		return nil, fmt.Errorf("%w: cluster %s already exists", ErrClusterAlreadyExists, id)
	}
	if engine == "" {
		engine = docDBEngine
	}
	if engineVersion == "" {
		engineVersion = defaultEngineVersion
	}
	if paramGroupName == "" {
		paramGroupName = "default.docdb4.0"
	}
	if port <= 0 {
		port = defaultDocDBPort
	}
	if backupRetentionPeriod == 0 {
		backupRetentionPeriod = 1
	}
	if preferredBackupWindow == "" {
		preferredBackupWindow = defaultBackupWindow
	}
	if preferredMaintenanceWindow == "" {
		preferredMaintenanceWindow = defaultMaintenanceWindow
	}
	clusterArn := b.clusterARN(region, id)
	endpoint := fmt.Sprintf("%s.cluster.docdb.%s.amazonaws.com", id, region)
	readerEndpoint := fmt.Sprintf("%s.cluster-ro.docdb.%s.amazonaws.com", id, region)
	azs := make([]string, len(availabilityZones))
	copy(azs, availabilityZones)

	var (
		kmsKeyID                         string
		vpcSecurityGroupIDs              []string
		enabledCloudwatchLogsExports     []string
		iamDatabaseAuthenticationEnabled bool
	)
	if opts != nil {
		kmsKeyID = opts.KmsKeyID
		iamDatabaseAuthenticationEnabled = opts.IAMDatabaseAuthenticationEnabled
		if len(opts.VpcSecurityGroupIDs) > 0 {
			vpcSecurityGroupIDs = make([]string, len(opts.VpcSecurityGroupIDs))
			copy(vpcSecurityGroupIDs, opts.VpcSecurityGroupIDs)
		}
		if len(opts.EnabledCloudwatchLogsExports) > 0 {
			enabledCloudwatchLogsExports = make([]string, len(opts.EnabledCloudwatchLogsExports))
			copy(enabledCloudwatchLogsExports, opts.EnabledCloudwatchLogsExports)
		}
	}

	cluster := &DBCluster{
		DBClusterIdentifier:              id,
		Engine:                           engine,
		Status:                           statusAvailable,
		MasterUsername:                   masterUser,
		DatabaseName:                     dbName,
		DBClusterParameterGroupName:      paramGroupName,
		DBSubnetGroupName:                subnetGroupName,
		Endpoint:                         endpoint,
		ReaderEndpoint:                   readerEndpoint,
		Port:                             port,
		DBClusterArn:                     clusterArn,
		EngineVersion:                    engineVersion,
		StorageEncrypted:                 storageEncrypted,
		DeletionProtection:               deletionProtection,
		BackupRetentionPeriod:            backupRetentionPeriod,
		PreferredBackupWindow:            preferredBackupWindow,
		PreferredMaintenanceWindow:       preferredMaintenanceWindow,
		AvailabilityZones:                azs,
		ClusterCreateTime:                time.Now().UTC().Format(time.RFC3339),
		Tags:                             copyTags(tags),
		KmsKeyID:                         kmsKeyID,
		VpcSecurityGroupIDs:              vpcSecurityGroupIDs,
		EnabledCloudwatchLogsExports:     enabledCloudwatchLogsExports,
		IAMDatabaseAuthenticationEnabled: iamDatabaseAuthenticationEnabled,
	}
	clusters[id] = cluster
	if len(tags) > 0 {
		b.tagsStore(region)[clusterArn] = tagsFromMap(tags)
	}

	return copyCluster(cluster), nil
}

// CreateDBClusterOptions holds optional parameters for CreateDBCluster.
type CreateDBClusterOptions struct {
	KmsKeyID                         string
	VpcSecurityGroupIDs              []string
	EnabledCloudwatchLogsExports     []string
	IAMDatabaseAuthenticationEnabled bool
}

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

		return []DBCluster{*copyCluster(c)}, nil
	}
	result := make([]DBCluster, 0, len(clusters))
	for _, c := range clusters {
		result = append(result, *copyCluster(c))
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].DBClusterIdentifier < result[j].DBClusterIdentifier
	})

	return result, nil
}

func (b *InMemoryBackend) DeleteDBCluster(
	ctx context.Context,
	id string,
	opts *DeleteDBClusterOptions,
) (*DBCluster, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: DBClusterIdentifier is required", ErrInvalidParameter)
	}
	region := getRegion(ctx, b.region)
	b.mu.Lock("DeleteDBCluster")
	defer b.mu.Unlock()
	clusters := b.clustersStore(region)
	c, exists := clusters[id]
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}
	if c.DeletionProtection {
		return nil, fmt.Errorf("%w: cluster %s has deletion protection enabled", ErrInvalidClusterState, id)
	}
	instances := b.instancesStore(region)
	for _, inst := range instances {
		if inst.DBClusterIdentifier == id {
			return nil, fmt.Errorf("%w: cluster %s still has instances, delete them first", ErrInvalidClusterState, id)
		}
	}
	cp := copyCluster(c)

	// Create a final snapshot if requested.
	if opts != nil && !opts.SkipFinalSnapshot && opts.FinalDBClusterSnapshotIdentifier != "" {
		snapID := opts.FinalDBClusterSnapshotIdentifier
		snapshots := b.clusterSnapshotsStore(region)
		if _, snapExists := snapshots[snapID]; snapExists {
			return nil, fmt.Errorf(
				"%w: cluster snapshot %s already exists",
				ErrClusterSnapshotAlreadyExists,
				snapID,
			)
		}
		snap := &DBClusterSnapshot{
			DBClusterSnapshotIdentifier: snapID,
			DBClusterIdentifier:         id,
			Engine:                      c.Engine,
			Status:                      statusAvailable,
			EngineVersion:               c.EngineVersion,
			StorageEncrypted:            c.StorageEncrypted,
			SnapshotType:                "manual",
			PercentProgress:             snapshotPercentageComplete,
			SnapshotCreateTime:          time.Now().UTC().Format(time.RFC3339),
			DBClusterArn:                b.clusterARN(region, id),
		}
		snapshots[snapID] = snap
	}

	delete(clusters, id)
	delete(b.tagsStore(region), b.clusterARN(region, id))

	return cp, nil
}

// DeleteDBClusterOptions holds optional parameters for DeleteDBCluster.
type DeleteDBClusterOptions struct {
	FinalDBClusterSnapshotIdentifier string
	SkipFinalSnapshot                bool
}

func (b *InMemoryBackend) ModifyDBCluster(
	ctx context.Context,
	id, paramGroupName string,
	deletionProtection *bool,
	backupRetentionPeriod int,
	preferredBackupWindow, preferredMaintenanceWindow string,
	opts *ModifyDBClusterOptions,
) (*DBCluster, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("ModifyDBCluster")
	defer b.mu.Unlock()
	c, exists := b.clustersStore(region)[id]
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}
	if c.Status == statusDeleting {
		return nil, fmt.Errorf("%w: cluster %s is in deleting state", ErrInvalidClusterState, id)
	}
	if paramGroupName != "" {
		c.DBClusterParameterGroupName = paramGroupName
	}
	if deletionProtection != nil {
		c.DeletionProtection = *deletionProtection
	}
	if backupRetentionPeriod > 0 {
		c.BackupRetentionPeriod = backupRetentionPeriod
	}
	if preferredBackupWindow != "" {
		c.PreferredBackupWindow = preferredBackupWindow
	}
	if preferredMaintenanceWindow != "" {
		c.PreferredMaintenanceWindow = preferredMaintenanceWindow
	}
	if opts != nil {
		applyModifyDBClusterOpts(c, opts)
	}

	return copyCluster(c), nil
}

// applyModifyDBClusterOpts applies optional ModifyDBCluster parameters to an existing cluster.
func applyModifyDBClusterOpts(c *DBCluster, opts *ModifyDBClusterOptions) {
	if opts.EngineVersion != "" {
		c.EngineVersion = opts.EngineVersion
	}
	if opts.Port > 0 {
		c.Port = opts.Port
	}
	if len(opts.VpcSecurityGroupIDs) > 0 {
		vpcSGs := make([]string, len(opts.VpcSecurityGroupIDs))
		copy(vpcSGs, opts.VpcSecurityGroupIDs)
		c.VpcSecurityGroupIDs = vpcSGs
	}
	if len(opts.EnableLogsTypes) > 0 {
		existing := make(map[string]bool, len(c.EnabledCloudwatchLogsExports))
		for _, t := range c.EnabledCloudwatchLogsExports {
			existing[t] = true
		}
		for _, t := range opts.EnableLogsTypes {
			if !existing[t] {
				c.EnabledCloudwatchLogsExports = append(c.EnabledCloudwatchLogsExports, t)
				existing[t] = true
			}
		}
	}
	if len(opts.DisableLogsTypes) > 0 {
		disableSet := make(map[string]bool, len(opts.DisableLogsTypes))
		for _, t := range opts.DisableLogsTypes {
			disableSet[t] = true
		}
		kept := c.EnabledCloudwatchLogsExports[:0]
		for _, t := range c.EnabledCloudwatchLogsExports {
			if !disableSet[t] {
				kept = append(kept, t)
			}
		}
		c.EnabledCloudwatchLogsExports = kept
	}
}

// ModifyDBClusterOptions holds optional extra parameters for ModifyDBCluster.
type ModifyDBClusterOptions struct {
	EngineVersion       string
	VpcSecurityGroupIDs []string
	EnableLogsTypes     []string
	DisableLogsTypes    []string
	Port                int
}

func (b *InMemoryBackend) StopDBCluster(ctx context.Context, id string) (*DBCluster, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("StopDBCluster")
	defer b.mu.Unlock()
	c, exists := b.clustersStore(region)[id]
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}
	if c.Status != statusAvailable {
		return nil, fmt.Errorf("%w: cluster %s is not in available state", ErrInvalidClusterState, id)
	}
	c.Status = statusStopped

	return copyCluster(c), nil
}

func (b *InMemoryBackend) StartDBCluster(ctx context.Context, id string) (*DBCluster, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("StartDBCluster")
	defer b.mu.Unlock()
	c, exists := b.clustersStore(region)[id]
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}
	if c.Status != statusStopped {
		return nil, fmt.Errorf("%w: cluster %s is not in stopped state", ErrInvalidClusterState, id)
	}
	c.Status = statusAvailable

	return copyCluster(c), nil
}

func (b *InMemoryBackend) FailoverDBCluster(ctx context.Context, id string) (*DBCluster, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("FailoverDBCluster")
	defer b.mu.Unlock()
	c, exists := b.clustersStore(region)[id]
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}
	if c.Status != statusAvailable {
		return nil, fmt.Errorf("%w: cluster %s is not in available state for failover", ErrInvalidClusterState, id)
	}

	return copyCluster(c), nil
}

func (b *InMemoryBackend) CreateDBInstance(
	ctx context.Context,
	id, clusterID, instanceClass, engine string,
	promotionTier int,
	tags map[string]string,
	opts *CreateDBInstanceOptions,
) (*DBInstance, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: DBInstanceIdentifier is required", ErrInvalidParameter)
	}
	if promotionTier < 0 || promotionTier > maxPromotionTier {
		return nil, fmt.Errorf(
			"%w: PromotionTier must be between 0 and %d",
			ErrInvalidParameter, maxPromotionTier,
		)
	}
	if err := validateTags(tags); err != nil {
		return nil, err
	}
	region := getRegion(ctx, b.region)
	b.mu.Lock("CreateDBInstance")
	defer b.mu.Unlock()
	instances := b.instancesStore(region)
	if _, exists := instances[id]; exists {
		return nil, fmt.Errorf("%w: instance %s already exists", ErrInstanceAlreadyExists, id)
	}
	clusters := b.clustersStore(region)
	if clusterID != "" {
		if _, exists := clusters[clusterID]; !exists {
			return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, clusterID)
		}
	}
	if engine == "" {
		engine = docDBEngine
	}
	if instanceClass == "" {
		instanceClass = defaultInstanceClass
	}
	var clusterEngineVersion string
	var clusterStorageEncrypted bool
	var clusterAZ string
	var clusterSubnetGroupName string
	if clusterID != "" {
		if parentCluster, exists := clusters[clusterID]; exists {
			clusterEngineVersion = parentCluster.EngineVersion
			clusterStorageEncrypted = parentCluster.StorageEncrypted
			clusterAZ = firstAZ(parentCluster.AvailabilityZones)
			clusterSubnetGroupName = parentCluster.DBSubnetGroupName
		}
	}
	instanceArn := b.instanceARN(region, id)
	endpoint := fmt.Sprintf("%s.docdb.%s.amazonaws.com", id, region)

	var (
		caCertID           string
		copyTagsToSnapshot bool
	)
	if opts != nil {
		caCertID = opts.CACertificateIdentifier
		copyTagsToSnapshot = opts.CopyTagsToSnapshot
	}

	inst := &DBInstance{
		DBInstanceIdentifier:    id,
		DBClusterIdentifier:     clusterID,
		DBInstanceClass:         instanceClass,
		Engine:                  engine,
		DBInstanceStatus:        statusAvailable,
		Endpoint:                endpoint,
		Port:                    defaultDocDBPort,
		DBInstanceArn:           instanceArn,
		EngineVersion:           valueOrDefault(clusterEngineVersion, defaultEngineVersion),
		StorageEncrypted:        clusterStorageEncrypted,
		AvailabilityZone:        clusterAZ,
		DBSubnetGroupName:       clusterSubnetGroupName,
		PromotionTier:           promotionTier,
		Tags:                    copyTags(tags),
		CACertificateIdentifier: caCertID,
		CopyTagsToSnapshot:      copyTagsToSnapshot,
	}
	instances[id] = inst
	if len(tags) > 0 {
		b.tagsStore(region)[instanceArn] = tagsFromMap(tags)
	}

	return copyInstance(inst), nil
}

// CreateDBInstanceOptions holds optional parameters for CreateDBInstance.
type CreateDBInstanceOptions struct {
	CACertificateIdentifier string
	CopyTagsToSnapshot      bool
}

func (b *InMemoryBackend) DescribeDBInstances(ctx context.Context, id, clusterID string) ([]DBInstance, error) {
	region := getRegion(ctx, b.region)
	b.mu.RLock("DescribeDBInstances")
	defer b.mu.RUnlock()
	instances := b.instancesStore(region)
	if id != "" {
		inst, exists := instances[id]
		if !exists {
			return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, id)
		}

		return []DBInstance{*copyInstance(inst)}, nil
	}
	result := make([]DBInstance, 0, len(instances))
	for _, inst := range instances {
		if clusterID != "" && inst.DBClusterIdentifier != clusterID {
			continue
		}
		result = append(result, *copyInstance(inst))
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].DBInstanceIdentifier < result[j].DBInstanceIdentifier
	})

	return result, nil
}

// DBClusterMemberEntry represents an instance that is a member of a DB cluster.
type DBClusterMemberEntry struct {
	DBInstanceIdentifier string
	PromotionTier        int
	IsClusterWriter      bool
}

// GetClusterMembers returns the instances that belong to a given cluster, ordered by identifier.
func (b *InMemoryBackend) GetClusterMembers(ctx context.Context, clusterID string) []DBClusterMemberEntry {
	region := getRegion(ctx, b.region)
	b.mu.RLock("GetClusterMembers")
	defer b.mu.RUnlock()
	var members []DBClusterMemberEntry
	for _, inst := range b.instancesStore(region) {
		if inst.DBClusterIdentifier == clusterID {
			members = append(members, DBClusterMemberEntry{
				DBInstanceIdentifier: inst.DBInstanceIdentifier,
				PromotionTier:        inst.PromotionTier,
			})
		}
	}
	sort.Slice(members, func(i, j int) bool {
		return members[i].DBInstanceIdentifier < members[j].DBInstanceIdentifier
	})
	// Mark the first member (lowest PromotionTier or alphabetically first) as writer.
	if len(members) > 0 {
		members[0].IsClusterWriter = true
	}

	return members
}

func (b *InMemoryBackend) DeleteDBInstance(ctx context.Context, id string) (*DBInstance, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: DBInstanceIdentifier is required", ErrInvalidParameter)
	}
	region := getRegion(ctx, b.region)
	b.mu.Lock("DeleteDBInstance")
	defer b.mu.Unlock()
	instances := b.instancesStore(region)
	inst, exists := instances[id]
	if !exists {
		return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, id)
	}
	cp := copyInstance(inst)
	delete(instances, id)
	delete(b.tagsStore(region), b.instanceARN(region, id))

	return cp, nil
}

func (b *InMemoryBackend) ModifyDBInstance(
	ctx context.Context,
	id, instanceClass string,
	autoMinorVersionUpgrade *bool,
	preferredMaintenanceWindow string,
	opts *ModifyDBInstanceOptions,
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
	if autoMinorVersionUpgrade != nil {
		inst.AutoMinorVersionUpgrade = *autoMinorVersionUpgrade
	}
	if preferredMaintenanceWindow != "" {
		inst.PreferredMaintenanceWindow = preferredMaintenanceWindow
	}
	if opts == nil {
		return copyInstance(inst), nil
	}
	if opts.CACertificateIdentifier != "" {
		inst.CACertificateIdentifier = opts.CACertificateIdentifier
	}
	if opts.CopyTagsToSnapshot != nil {
		inst.CopyTagsToSnapshot = *opts.CopyTagsToSnapshot
	}
	if opts.PromotionTier != nil {
		if *opts.PromotionTier < 0 || *opts.PromotionTier > maxPromotionTier {
			return nil, fmt.Errorf(
				"%w: PromotionTier must be between 0 and %d",
				ErrInvalidParameter, maxPromotionTier,
			)
		}
		inst.PromotionTier = *opts.PromotionTier
	}

	return copyInstance(inst), nil
}

// ModifyDBInstanceOptions holds optional extra parameters for ModifyDBInstance.
type ModifyDBInstanceOptions struct {
	CopyTagsToSnapshot      *bool
	PromotionTier           *int
	CACertificateIdentifier string
}

func (b *InMemoryBackend) RebootDBInstance(ctx context.Context, id string) (*DBInstance, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("RebootDBInstance")
	defer b.mu.Unlock()
	inst, exists := b.instancesStore(region)[id]
	if !exists {
		return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, id)
	}

	return copyInstance(inst), nil
}

func (b *InMemoryBackend) CreateDBSubnetGroup(
	ctx context.Context,
	name, description, vpcID string,
	subnetIDs []string,
	tags map[string]string,
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
	sgArn := b.subnetGroupARN(region, name)
	sg := &DBSubnetGroup{
		DBSubnetGroupName:        name,
		DBSubnetGroupDescription: description,
		VpcID:                    vpcID,
		Status:                   "Complete",
		SubnetIDs:                ids,
		DBSubnetGroupArn:         sgArn,
		Tags:                     copyTags(tags),
	}
	subnetGroups[name] = sg
	if len(tags) > 0 {
		b.tagsStore(region)[sgArn] = tagsFromMap(tags)
	}
	cp := *sg
	cp.SubnetIDs = make([]string, len(ids))
	copy(cp.SubnetIDs, ids)
	cp.Tags = copyTags(sg.Tags)

	return &cp, nil
}

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
		cp := *sg
		cp.SubnetIDs = make([]string, len(sg.SubnetIDs))
		copy(cp.SubnetIDs, sg.SubnetIDs)
		cp.Tags = copyTags(sg.Tags)

		return []DBSubnetGroup{cp}, nil
	}
	result := make([]DBSubnetGroup, 0, len(subnetGroups))
	for _, sg := range subnetGroups {
		cp := *sg
		cp.SubnetIDs = make([]string, len(sg.SubnetIDs))
		copy(cp.SubnetIDs, sg.SubnetIDs)
		cp.Tags = copyTags(sg.Tags)
		result = append(result, cp)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].DBSubnetGroupName < result[j].DBSubnetGroupName
	})

	return result, nil
}

func (b *InMemoryBackend) DeleteDBSubnetGroup(ctx context.Context, name string) error {
	region := getRegion(ctx, b.region)
	b.mu.Lock("DeleteDBSubnetGroup")
	defer b.mu.Unlock()
	subnetGroups := b.subnetGroupsStore(region)
	if _, exists := subnetGroups[name]; !exists {
		return fmt.Errorf("%w: subnet group %s not found", ErrSubnetGroupNotFound, name)
	}
	for _, c := range b.clustersStore(region) {
		if c.DBSubnetGroupName == name {
			return fmt.Errorf(
				"%w: subnet group %s is used by cluster %s",
				ErrSubnetGroupInUse,
				name,
				c.DBClusterIdentifier,
			)
		}
	}
	delete(subnetGroups, name)
	delete(b.tagsStore(region), b.subnetGroupARN(region, name))

	return nil
}

func (b *InMemoryBackend) CreateDBClusterParameterGroup(
	ctx context.Context,
	name, family, description string,
	tags map[string]string,
) (*DBClusterParameterGroup, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: DBClusterParameterGroupName is required", ErrInvalidParameter)
	}
	region := getRegion(ctx, b.region)
	b.mu.Lock("CreateDBClusterParameterGroup")
	defer b.mu.Unlock()
	pgStore := b.clusterParameterGroupsStore(region)
	if _, exists := pgStore[name]; exists {
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
		DBClusterParameterGroupArn:  b.clusterParameterGroupARN(region, name),
		Tags:                        copyTags(tags),
	}
	pgStore[name] = pg
	pgArn := b.clusterParameterGroupARN(region, name)
	if len(tags) > 0 {
		b.tagsStore(region)[pgArn] = tagsFromMap(tags)
	}
	cp := *pg
	cp.Tags = copyTags(pg.Tags)

	return &cp, nil
}

func (b *InMemoryBackend) DescribeDBClusterParameterGroups(
	ctx context.Context,
	name string,
) ([]DBClusterParameterGroup, error) {
	region := getRegion(ctx, b.region)
	b.mu.RLock("DescribeDBClusterParameterGroups")
	defer b.mu.RUnlock()
	pgStore := b.clusterParameterGroupsStore(region)
	if name != "" {
		pg, exists := pgStore[name]
		if !exists {
			return nil, fmt.Errorf("%w: cluster parameter group %s not found", ErrClusterParameterGroupNotFound, name)
		}
		cp := *pg
		cp.Tags = copyTags(pg.Tags)

		return []DBClusterParameterGroup{cp}, nil
	}
	result := make([]DBClusterParameterGroup, 0, len(pgStore))
	for _, pg := range pgStore {
		cp := *pg
		cp.Tags = copyTags(pg.Tags)
		result = append(result, cp)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].DBClusterParameterGroupName < result[j].DBClusterParameterGroupName
	})

	return result, nil
}

func (b *InMemoryBackend) DeleteDBClusterParameterGroup(ctx context.Context, name string) error {
	region := getRegion(ctx, b.region)
	b.mu.Lock("DeleteDBClusterParameterGroup")
	defer b.mu.Unlock()
	pgStore := b.clusterParameterGroupsStore(region)
	if _, exists := pgStore[name]; !exists {
		return fmt.Errorf("%w: cluster parameter group %s not found", ErrClusterParameterGroupNotFound, name)
	}
	for _, c := range b.clustersStore(region) {
		if c.DBClusterParameterGroupName == name {
			return fmt.Errorf(
				"%w: parameter group %s is used by cluster %s",
				ErrParameterGroupInUse,
				name,
				c.DBClusterIdentifier,
			)
		}
	}
	delete(pgStore, name)
	delete(b.tagsStore(region), b.clusterParameterGroupARN(region, name))

	return nil
}

func (b *InMemoryBackend) ModifyDBClusterParameterGroup(
	ctx context.Context,
	name string,
) (*DBClusterParameterGroup, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("ModifyDBClusterParameterGroup")
	defer b.mu.Unlock()
	pg, exists := b.clusterParameterGroupsStore(region)[name]
	if !exists {
		return nil, fmt.Errorf("%w: cluster parameter group %s not found", ErrClusterParameterGroupNotFound, name)
	}
	cp := *pg
	cp.Tags = copyTags(pg.Tags)

	return &cp, nil
}

func (b *InMemoryBackend) CreateDBClusterSnapshot(
	ctx context.Context,
	snapshotID, clusterID string,
	tags map[string]string,
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
	c, exists := b.clustersStore(region)[clusterID]
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, clusterID)
	}
	snap := &DBClusterSnapshot{
		DBClusterSnapshotIdentifier: snapshotID,
		DBClusterIdentifier:         clusterID,
		Engine:                      c.Engine,
		Status:                      statusAvailable,
		EngineVersion:               c.EngineVersion,
		StorageEncrypted:            c.StorageEncrypted,
		SnapshotType:                "manual",
		PercentProgress:             snapshotPercentageComplete,
		SnapshotCreateTime:          time.Now().UTC().Format(time.RFC3339),
		DBClusterArn:                b.clusterARN(region, clusterID),
		Tags:                        copyTags(tags),
	}
	snapshots[snapshotID] = snap
	snapArn := b.clusterSnapshotARN(region, snapshotID)
	if len(tags) > 0 {
		b.tagsStore(region)[snapArn] = tagsFromMap(tags)
	}
	cp := *snap
	cp.Tags = copyTags(snap.Tags)

	return &cp, nil
}

func (b *InMemoryBackend) DescribeDBClusterSnapshots(
	ctx context.Context,
	snapshotID, clusterID, snapshotType string,
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
		cp.Tags = copyTags(snap.Tags)

		return []DBClusterSnapshot{cp}, nil
	}
	result := make([]DBClusterSnapshot, 0, len(snapshots))
	for _, snap := range snapshots {
		if clusterID != "" && snap.DBClusterIdentifier != clusterID {
			continue
		}
		if snapshotType != "" && snap.SnapshotType != snapshotType {
			continue
		}
		cp := *snap
		cp.Tags = copyTags(snap.Tags)
		result = append(result, cp)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].DBClusterSnapshotIdentifier < result[j].DBClusterSnapshotIdentifier
	})

	return result, nil
}

func (b *InMemoryBackend) DeleteDBClusterSnapshot(ctx context.Context, snapshotID string) (*DBClusterSnapshot, error) {
	if snapshotID == "" {
		return nil, fmt.Errorf("%w: DBClusterSnapshotIdentifier is required", ErrInvalidParameter)
	}
	region := getRegion(ctx, b.region)
	b.mu.Lock("DeleteDBClusterSnapshot")
	defer b.mu.Unlock()
	snapshots := b.clusterSnapshotsStore(region)
	snap, exists := snapshots[snapshotID]
	if !exists {
		return nil, fmt.Errorf("%w: cluster snapshot %s not found", ErrClusterSnapshotNotFound, snapshotID)
	}
	cp := *snap
	cp.Tags = copyTags(snap.Tags)
	delete(snapshots, snapshotID)
	delete(b.tagsStore(region), b.clusterSnapshotARN(region, snapshotID))

	return &cp, nil
}

func (b *InMemoryBackend) AddTagsToResource(ctx context.Context, arnStr string, tags []Tag) error {
	if err := validateTagList(tags); err != nil {
		return err
	}
	region := regionFromARN(arnStr, getRegion(ctx, b.region))
	b.mu.Lock("AddTagsToResource")
	defer b.mu.Unlock()
	tagStore := b.tagsStore(region)
	current := tagStore[arnStr]
	idx := make(map[string]int, len(current))
	for i, t := range current {
		idx[t.Key] = i
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

func (b *InMemoryBackend) RemoveTagsFromResource(ctx context.Context, arnStr string, keys []string) {
	region := regionFromARN(arnStr, getRegion(ctx, b.region))
	b.mu.Lock("RemoveTagsFromResource")
	defer b.mu.Unlock()
	tagStore := b.tagsStore(region)
	remove := make(map[string]bool, len(keys))
	for _, k := range keys {
		remove[k] = true
	}
	current := tagStore[arnStr]
	kept := make([]Tag, 0, len(current))
	for _, t := range current {
		if !remove[t.Key] {
			kept = append(kept, t)
		}
	}
	tagStore[arnStr] = kept
}

func (b *InMemoryBackend) ListTagsForResource(ctx context.Context, arnStr string) []Tag {
	region := regionFromARN(arnStr, getRegion(ctx, b.region))
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()
	src := b.tagsStore(region)[arnStr]
	cp := make([]Tag, len(src))
	copy(cp, src)
	sort.Slice(cp, func(i, j int) bool {
		return cp[i].Key < cp[j].Key
	})

	return cp
}

// AddSourceIdentifierToSubscription adds a source identifier to an event subscription.
func (b *InMemoryBackend) AddSourceIdentifierToSubscription(
	ctx context.Context,
	subscriptionName, sourceID string,
) (*EventSubscription, error) {
	if subscriptionName == "" {
		return nil, fmt.Errorf("%w: SubscriptionName is required", ErrInvalidParameter)
	}
	if sourceID == "" {
		return nil, fmt.Errorf("%w: SourceIdentifier is required", ErrInvalidParameter)
	}
	region := getRegion(ctx, b.region)
	b.mu.Lock("AddSourceIdentifierToSubscription")
	defer b.mu.Unlock()
	sub, exists := b.eventSubscriptionsStore(region)[subscriptionName]
	if !exists {
		return nil, fmt.Errorf("%w: subscription %s not found", ErrEventSubscriptionNotFound, subscriptionName)
	}
	if slices.Contains(sub.SourceIDs, sourceID) {
		return copyEventSubscription(sub), nil
	}
	sub.SourceIDs = append(sub.SourceIDs, sourceID)

	return copyEventSubscription(sub), nil
}

// ApplyPendingMaintenanceAction applies a pending maintenance action to a resource.
func (b *InMemoryBackend) ApplyPendingMaintenanceAction(
	_ context.Context,
	resourceARN, action, optInType string,
) error {
	if resourceARN == "" {
		return fmt.Errorf("%w: ResourceIdentifier is required", ErrInvalidParameter)
	}
	if action == "" {
		return fmt.Errorf("%w: ApplyAction is required", ErrInvalidParameter)
	}
	if optInType == "" {
		return fmt.Errorf("%w: OptInType is required", ErrInvalidParameter)
	}
	switch optInType {
	case optInTypeImmediate, optInTypeNextMaintenance, optInTypeUndoOptIn:
		// valid
	default:
		return fmt.Errorf(
			"%w: OptInType must be one of %s, %s, %s",
			ErrInvalidParameter,
			optInTypeImmediate, optInTypeNextMaintenance, optInTypeUndoOptIn,
		)
	}

	return nil
}

// CopyDBClusterParameterGroup copies a DB cluster parameter group.
func (b *InMemoryBackend) CopyDBClusterParameterGroup(
	ctx context.Context,
	sourceGroupName, targetName, targetDescription string,
) (*DBClusterParameterGroup, error) {
	if sourceGroupName == "" {
		return nil, fmt.Errorf("%w: SourceDBClusterParameterGroupIdentifier is required", ErrInvalidParameter)
	}
	if targetName == "" {
		return nil, fmt.Errorf("%w: TargetDBClusterParameterGroupIdentifier is required", ErrInvalidParameter)
	}
	region := getRegion(ctx, b.region)
	b.mu.Lock("CopyDBClusterParameterGroup")
	defer b.mu.Unlock()
	pgStore := b.clusterParameterGroupsStore(region)
	src, exists := pgStore[sourceGroupName]
	if !exists {
		return nil, fmt.Errorf(
			"%w: cluster parameter group %s not found",
			ErrClusterParameterGroupNotFound,
			sourceGroupName,
		)
	}
	if _, ok := pgStore[targetName]; ok {
		return nil, fmt.Errorf(
			"%w: cluster parameter group %s already exists",
			ErrClusterParameterGroupAlreadyExists,
			targetName,
		)
	}
	desc := targetDescription
	if desc == "" {
		desc = src.Description
	}
	pg := &DBClusterParameterGroup{
		DBClusterParameterGroupName: targetName,
		DBParameterGroupFamily:      src.DBParameterGroupFamily,
		Description:                 desc,
		DBClusterParameterGroupArn:  b.clusterParameterGroupARN(region, targetName),
	}
	pgStore[targetName] = pg
	cp := *pg
	cp.Tags = copyTags(pg.Tags)

	return &cp, nil
}

// CopyDBClusterSnapshot copies a DB cluster snapshot.
func (b *InMemoryBackend) CopyDBClusterSnapshot(
	ctx context.Context,
	sourceSnapshotID, targetSnapshotID string,
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
	if _, ok := snapshots[targetSnapshotID]; ok {
		return nil, fmt.Errorf(
			"%w: cluster snapshot %s already exists",
			ErrClusterSnapshotAlreadyExists,
			targetSnapshotID,
		)
	}
	snap := &DBClusterSnapshot{
		DBClusterSnapshotIdentifier: targetSnapshotID,
		DBClusterIdentifier:         src.DBClusterIdentifier,
		DBClusterArn:                src.DBClusterArn,
		Engine:                      src.Engine,
		Status:                      statusAvailable,
		EngineVersion:               src.EngineVersion,
		StorageEncrypted:            src.StorageEncrypted,
		SnapshotType:                src.SnapshotType,
		PercentProgress:             src.PercentProgress,
	}
	snapshots[targetSnapshotID] = snap
	cp := *snap
	cp.Tags = copyTags(snap.Tags)

	return &cp, nil
}

// CreateEventSubscription creates an event subscription.
func (b *InMemoryBackend) CreateEventSubscription(
	ctx context.Context,
	name, snsTopicARN, sourceType string,
	eventCategories, sourceIDs []string,
) (*EventSubscription, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: SubscriptionName is required", ErrInvalidParameter)
	}
	region := getRegion(ctx, b.region)
	b.mu.Lock("CreateEventSubscription")
	defer b.mu.Unlock()
	subStore := b.eventSubscriptionsStore(region)
	if _, exists := subStore[name]; exists {
		return nil, fmt.Errorf("%w: subscription %s already exists", ErrEventSubscriptionAlreadyExists, name)
	}
	cats := make([]string, len(eventCategories))
	copy(cats, eventCategories)
	ids := make([]string, len(sourceIDs))
	copy(ids, sourceIDs)
	sub := &EventSubscription{
		SubscriptionName: name,
		SnsTopicARN:      snsTopicARN,
		Status:           "active",
		SourceType:       sourceType,
		EventCategories:  cats,
		SourceIDs:        ids,
	}
	subStore[name] = sub

	return copyEventSubscription(sub), nil
}

// CreateGlobalCluster creates a global cluster.
func (b *InMemoryBackend) CreateGlobalCluster(
	_ context.Context,
	id, sourceDBClusterID, engine, engineVersion string,
) (*GlobalCluster, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: GlobalClusterIdentifier is required", ErrInvalidParameter)
	}
	b.mu.Lock("CreateGlobalCluster")
	defer b.mu.Unlock()
	if _, exists := b.globalClusters[id]; exists {
		return nil, fmt.Errorf("%w: global cluster %s already exists", ErrGlobalClusterAlreadyExists, id)
	}
	if engine == "" {
		engine = docDBEngine
	}
	if engineVersion == "" {
		engineVersion = defaultEngineVersion
	}
	gc := &GlobalCluster{
		GlobalClusterIdentifier: id,
		SourceDBClusterID:       sourceDBClusterID,
		Status:                  statusAvailable,
		Engine:                  engine,
		EngineVersion:           engineVersion,
		GlobalClusterArn:        b.globalClusterARN(id),
	}
	b.globalClusters[id] = gc
	cp := *gc

	return &cp, nil
}

// DeleteEventSubscription deletes an event subscription.
func (b *InMemoryBackend) DeleteEventSubscription(ctx context.Context, name string) (*EventSubscription, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("DeleteEventSubscription")
	defer b.mu.Unlock()
	subStore := b.eventSubscriptionsStore(region)
	sub, exists := subStore[name]
	if !exists {
		return nil, fmt.Errorf("%w: subscription %s not found", ErrEventSubscriptionNotFound, name)
	}
	cp := copyEventSubscription(sub)
	delete(subStore, name)

	return cp, nil
}

// DeleteGlobalCluster deletes a global cluster.
func (b *InMemoryBackend) DeleteGlobalCluster(_ context.Context, id string) (*GlobalCluster, error) {
	b.mu.Lock("DeleteGlobalCluster")
	defer b.mu.Unlock()
	gc, exists := b.globalClusters[id]
	if !exists {
		return nil, fmt.Errorf("%w: global cluster %s not found", ErrGlobalClusterNotFound, id)
	}
	cp := *gc
	delete(b.globalClusters, id)

	return &cp, nil
}

// DescribeCertificates returns certificate information.
func (b *InMemoryBackend) DescribeCertificates(_ context.Context, certificateID string) []Certificate {
	certs := []Certificate{
		{
			CertificateIdentifier: "rds-ca-2019",
			CertificateType:       "CA",
			Thumbprint:            "d404926ab3b1c6f0ad61f8d95dadf6c3eea47dbf",
			ValidFrom:             "2019-09-19T00:00:00Z",
			ValidTill:             "2024-08-22T00:00:00Z",
		},
		{
			CertificateIdentifier: "rds-ca-rsa2048-g1",
			CertificateType:       "CA",
			Thumbprint:            "cf5c7c1cf32cae39012fc84c8d9e76c25bce55fb",
			ValidFrom:             "2021-05-25T00:00:00Z",
			ValidTill:             "2061-05-25T00:00:00Z",
		},
	}
	if certificateID == "" {
		return certs
	}
	for _, c := range certs {
		if c.CertificateIdentifier == certificateID {
			return []Certificate{c}
		}
	}

	return []Certificate{}
}

// DescribeDBClusterParameters returns the parameters for a DB cluster parameter group.
func (b *InMemoryBackend) DescribeDBClusterParameters(
	ctx context.Context,
	groupName string,
) ([]DBClusterParameter, error) {
	region := getRegion(ctx, b.region)
	b.mu.RLock("DescribeDBClusterParameters")
	defer b.mu.RUnlock()
	if groupName == "" {
		return nil, fmt.Errorf("%w: DBClusterParameterGroupName is required", ErrInvalidParameter)
	}
	if _, exists := b.clusterParameterGroupsStore(region)[groupName]; !exists {
		return nil, fmt.Errorf("%w: cluster parameter group %s not found", ErrClusterParameterGroupNotFound, groupName)
	}
	params := []DBClusterParameter{
		{
			ParameterName:  "tls",
			ParameterValue: paramEnabled,
			Description:    "Specifies the TLS setting",
			Source:         "system",
			ApplyType:      "static",
			DataType:       paramTypeStr,
			IsModifiable:   true,
		},
		{
			ParameterName:  "ttl_monitor",
			ParameterValue: paramEnabled,
			Description:    "Specifies the TTL monitor setting",
			Source:         "system",
			ApplyType:      "dynamic",
			DataType:       paramTypeStr,
			IsModifiable:   true,
		},
	}

	return params, nil
}

// DescribeGlobalClusters returns global clusters, optionally filtered by ID, sorted by identifier.
func (b *InMemoryBackend) DescribeGlobalClusters(_ context.Context, id string) []GlobalCluster {
	b.mu.RLock("DescribeGlobalClusters")
	defer b.mu.RUnlock()
	if id != "" {
		gc, exists := b.globalClusters[id]
		if !exists {
			return []GlobalCluster{}
		}
		cp := *gc

		return []GlobalCluster{cp}
	}
	result := make([]GlobalCluster, 0, len(b.globalClusters))
	for _, gc := range b.globalClusters {
		result = append(result, *gc)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].GlobalClusterIdentifier < result[j].GlobalClusterIdentifier
	})

	return result
}

// DescribeEventSubscriptions returns event subscriptions, optionally filtered by name.
func (b *InMemoryBackend) DescribeEventSubscriptions(ctx context.Context, name string) []EventSubscription {
	region := getRegion(ctx, b.region)
	b.mu.RLock("DescribeEventSubscriptions")
	defer b.mu.RUnlock()
	subStore := b.eventSubscriptionsStore(region)
	if name != "" {
		sub, exists := subStore[name]
		if !exists {
			return []EventSubscription{}
		}

		return []EventSubscription{*copyEventSubscription(sub)}
	}
	result := make([]EventSubscription, 0, len(subStore))
	for _, sub := range subStore {
		result = append(result, *copyEventSubscription(sub))
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].SubscriptionName < result[j].SubscriptionName
	})

	return result
}

// ModifyEventSubscription modifies an event subscription.
func (b *InMemoryBackend) ModifyEventSubscription(
	ctx context.Context,
	name, snsTopicARN, sourceType string,
	eventCategories []string,
) (*EventSubscription, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: SubscriptionName is required", ErrInvalidParameter)
	}
	region := getRegion(ctx, b.region)
	b.mu.Lock("ModifyEventSubscription")
	defer b.mu.Unlock()
	sub, exists := b.eventSubscriptionsStore(region)[name]
	if !exists {
		return nil, fmt.Errorf("%w: subscription %s not found", ErrEventSubscriptionNotFound, name)
	}
	if snsTopicARN != "" {
		sub.SnsTopicARN = snsTopicARN
	}
	if sourceType != "" {
		sub.SourceType = sourceType
	}
	if len(eventCategories) > 0 {
		cats := make([]string, len(eventCategories))
		copy(cats, eventCategories)
		sub.EventCategories = cats
	}

	return copyEventSubscription(sub), nil
}

// RemoveSourceIdentifierFromSubscription removes a source identifier from an event subscription.
func (b *InMemoryBackend) RemoveSourceIdentifierFromSubscription(
	ctx context.Context,
	subscriptionName, sourceID string,
) (*EventSubscription, error) {
	if subscriptionName == "" {
		return nil, fmt.Errorf("%w: SubscriptionName is required", ErrInvalidParameter)
	}
	if sourceID == "" {
		return nil, fmt.Errorf("%w: SourceIdentifier is required", ErrInvalidParameter)
	}
	region := getRegion(ctx, b.region)
	b.mu.Lock("RemoveSourceIdentifierFromSubscription")
	defer b.mu.Unlock()
	sub, exists := b.eventSubscriptionsStore(region)[subscriptionName]
	if !exists {
		return nil, fmt.Errorf("%w: subscription %s not found", ErrEventSubscriptionNotFound, subscriptionName)
	}
	ids := make([]string, 0, len(sub.SourceIDs))
	for _, id := range sub.SourceIDs {
		if id != sourceID {
			ids = append(ids, id)
		}
	}
	sub.SourceIDs = ids

	return copyEventSubscription(sub), nil
}

// DescribePendingMaintenanceActions returns pending maintenance actions for resources.
// This implementation returns an empty list (in-memory emulation has no real pending actions).
func (b *InMemoryBackend) DescribePendingMaintenanceActions(
	_ context.Context,
	_ string,
) []ResourcePendingMaintenanceActions {
	return []ResourcePendingMaintenanceActions{}
}

// DescribeDBClusterSnapshotAttributes returns attributes for a cluster snapshot.
func (b *InMemoryBackend) DescribeDBClusterSnapshotAttributes(
	ctx context.Context,
	snapshotID string,
) (*DBClusterSnapshotAttributesResult, error) {
	if snapshotID == "" {
		return nil, fmt.Errorf("%w: DBClusterSnapshotIdentifier is required", ErrInvalidParameter)
	}
	region := getRegion(ctx, b.region)
	b.mu.RLock("DescribeDBClusterSnapshotAttributes")
	defer b.mu.RUnlock()
	if _, exists := b.clusterSnapshotsStore(region)[snapshotID]; !exists {
		return nil, fmt.Errorf("%w: cluster snapshot %s not found", ErrClusterSnapshotNotFound, snapshotID)
	}
	result, ok := b.snapshotAttributesStore(region)[snapshotID]
	if !ok {
		return &DBClusterSnapshotAttributesResult{
			DBClusterSnapshotIdentifier: snapshotID,
			Attributes:                  []DBClusterSnapshotAttribute{},
		}, nil
	}
	cp := &DBClusterSnapshotAttributesResult{
		DBClusterSnapshotIdentifier: result.DBClusterSnapshotIdentifier,
		Attributes:                  make([]DBClusterSnapshotAttribute, len(result.Attributes)),
	}
	for i, a := range result.Attributes {
		vals := make([]string, len(a.AttributeValues))
		copy(vals, a.AttributeValues)
		cp.Attributes[i] = DBClusterSnapshotAttribute{
			AttributeName:   a.AttributeName,
			AttributeValues: vals,
		}
	}

	return cp, nil
}

// ModifyDBClusterSnapshotAttribute modifies an attribute on a cluster snapshot.
// findOrCreateAttribute finds an existing attribute by name in the result, or creates and appends a new one.
func findOrCreateAttribute(
	result *DBClusterSnapshotAttributesResult,
	attributeName string,
) *DBClusterSnapshotAttribute {
	for i := range result.Attributes {
		if result.Attributes[i].AttributeName == attributeName {
			return &result.Attributes[i]
		}
	}
	result.Attributes = append(result.Attributes, DBClusterSnapshotAttribute{
		AttributeName:   attributeName,
		AttributeValues: []string{},
	})

	return &result.Attributes[len(result.Attributes)-1]
}

// applySnapshotAttributeChanges adds and removes values from an attribute in place.
func applySnapshotAttributeChanges(attr *DBClusterSnapshotAttribute, valuesToAdd, valuesToRemove []string) {
	existing := make(map[string]struct{}, len(attr.AttributeValues))
	for _, v := range attr.AttributeValues {
		existing[v] = struct{}{}
	}
	for _, v := range valuesToAdd {
		if _, ok := existing[v]; !ok {
			attr.AttributeValues = append(attr.AttributeValues, v)
			existing[v] = struct{}{}
		}
	}
	if len(valuesToRemove) == 0 {
		return
	}
	removeSet := make(map[string]bool, len(valuesToRemove))
	for _, v := range valuesToRemove {
		removeSet[v] = true
	}
	kept := attr.AttributeValues[:0]
	for _, v := range attr.AttributeValues {
		if !removeSet[v] {
			kept = append(kept, v)
		}
	}
	attr.AttributeValues = kept
}

// copySnapshotAttributesResult returns a deep copy of a DBClusterSnapshotAttributesResult.
func copySnapshotAttributesResult(result *DBClusterSnapshotAttributesResult) *DBClusterSnapshotAttributesResult {
	cp := &DBClusterSnapshotAttributesResult{
		DBClusterSnapshotIdentifier: result.DBClusterSnapshotIdentifier,
		Attributes:                  make([]DBClusterSnapshotAttribute, len(result.Attributes)),
	}
	for i, a := range result.Attributes {
		vals := make([]string, len(a.AttributeValues))
		copy(vals, a.AttributeValues)
		cp.Attributes[i] = DBClusterSnapshotAttribute{
			AttributeName:   a.AttributeName,
			AttributeValues: vals,
		}
	}

	return cp
}

func (b *InMemoryBackend) ModifyDBClusterSnapshotAttribute(
	ctx context.Context,
	snapshotID, attributeName string,
	valuesToAdd, valuesToRemove []string,
) (*DBClusterSnapshotAttributesResult, error) {
	if snapshotID == "" {
		return nil, fmt.Errorf("%w: DBClusterSnapshotIdentifier is required", ErrInvalidParameter)
	}
	if attributeName == "" {
		return nil, fmt.Errorf("%w: AttributeName is required", ErrInvalidParameter)
	}
	region := getRegion(ctx, b.region)
	b.mu.Lock("ModifyDBClusterSnapshotAttribute")
	defer b.mu.Unlock()
	if _, exists := b.clusterSnapshotsStore(region)[snapshotID]; !exists {
		return nil, fmt.Errorf("%w: cluster snapshot %s not found", ErrClusterSnapshotNotFound, snapshotID)
	}
	attrStore := b.snapshotAttributesStore(region)
	result, ok := attrStore[snapshotID]
	if !ok {
		result = &DBClusterSnapshotAttributesResult{
			DBClusterSnapshotIdentifier: snapshotID,
			Attributes:                  []DBClusterSnapshotAttribute{},
		}
	}
	attr := findOrCreateAttribute(result, attributeName)
	applySnapshotAttributeChanges(attr, valuesToAdd, valuesToRemove)
	attrStore[snapshotID] = result

	return copySnapshotAttributesResult(result), nil
}

// DescribeEngineDefaultClusterParameters returns the default parameters for an engine family.
func (b *InMemoryBackend) DescribeEngineDefaultClusterParameters(
	_ context.Context,
	_ string,
) []DBClusterParameter {
	return []DBClusterParameter{
		{
			ParameterName:  "tls",
			ParameterValue: paramEnabled,
			Description:    "Specifies the TLS setting",
			Source:         "engine-default",
			ApplyType:      "static",
			DataType:       paramTypeStr,
			IsModifiable:   true,
		},
		{
			ParameterName:  "ttl_monitor",
			ParameterValue: paramEnabled,
			Description:    "Specifies the TTL monitor setting",
			Source:         "engine-default",
			ApplyType:      "dynamic",
			DataType:       paramTypeStr,
			IsModifiable:   true,
		},
	}
}

// ResetDBClusterParameterGroup resets a parameter group to its default values.
func (b *InMemoryBackend) ResetDBClusterParameterGroup(
	ctx context.Context,
	name string,
) (*DBClusterParameterGroup, error) {
	region := getRegion(ctx, b.region)
	b.mu.Lock("ResetDBClusterParameterGroup")
	defer b.mu.Unlock()
	pg, exists := b.clusterParameterGroupsStore(region)[name]
	if !exists {
		return nil, fmt.Errorf("%w: cluster parameter group %s not found", ErrClusterParameterGroupNotFound, name)
	}
	cp := *pg
	cp.Tags = copyTags(pg.Tags)

	return &cp, nil
}

// ModifyDBSubnetGroup modifies a DB subnet group.
func (b *InMemoryBackend) ModifyDBSubnetGroup(
	ctx context.Context,
	name, description string,
	subnetIDs []string,
) (*DBSubnetGroup, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: DBSubnetGroupName is required", ErrInvalidParameter)
	}
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
	if len(subnetIDs) > 0 {
		ids := make([]string, len(subnetIDs))
		copy(ids, subnetIDs)
		sg.SubnetIDs = ids
	}
	cp := *sg
	cp.SubnetIDs = make([]string, len(sg.SubnetIDs))
	copy(cp.SubnetIDs, sg.SubnetIDs)
	cp.Tags = copyTags(sg.Tags)

	return &cp, nil
}

// ModifyGlobalCluster modifies a global cluster.
func (b *InMemoryBackend) ModifyGlobalCluster(
	_ context.Context,
	id, newID string,
	deletionProtection *bool,
) (*GlobalCluster, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: GlobalClusterIdentifier is required", ErrInvalidParameter)
	}
	b.mu.Lock("ModifyGlobalCluster")
	defer b.mu.Unlock()
	gc, exists := b.globalClusters[id]
	if !exists {
		return nil, fmt.Errorf("%w: global cluster %s not found", ErrGlobalClusterNotFound, id)
	}
	if deletionProtection != nil {
		gc.DeletionProtection = *deletionProtection
	}
	if newID != "" && newID != id {
		delete(b.globalClusters, id)
		gc.GlobalClusterIdentifier = newID
		gc.GlobalClusterArn = b.globalClusterARN(newID)
		b.globalClusters[newID] = gc
	}
	cp := *gc

	return &cp, nil
}

// FailoverGlobalCluster initiates a failover for a global cluster.
func (b *InMemoryBackend) FailoverGlobalCluster(_ context.Context, id, _ string) (*GlobalCluster, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: GlobalClusterIdentifier is required", ErrInvalidParameter)
	}
	b.mu.Lock("FailoverGlobalCluster")
	defer b.mu.Unlock()
	gc, exists := b.globalClusters[id]
	if !exists {
		return nil, fmt.Errorf("%w: global cluster %s not found", ErrGlobalClusterNotFound, id)
	}
	gc.Status = "failing-over"
	cp := *gc

	return &cp, nil
}

// RemoveFromGlobalCluster removes a DB cluster from a global cluster.
func (b *InMemoryBackend) RemoveFromGlobalCluster(
	_ context.Context,
	globalClusterID, _ string,
) (*GlobalCluster, error) {
	if globalClusterID == "" {
		return nil, fmt.Errorf("%w: GlobalClusterIdentifier is required", ErrInvalidParameter)
	}
	b.mu.Lock("RemoveFromGlobalCluster")
	defer b.mu.Unlock()
	gc, exists := b.globalClusters[globalClusterID]
	if !exists {
		return nil, fmt.Errorf("%w: global cluster %s not found", ErrGlobalClusterNotFound, globalClusterID)
	}
	cp := *gc

	return &cp, nil
}

// SwitchoverGlobalCluster initiates a switchover for a global cluster.
func (b *InMemoryBackend) SwitchoverGlobalCluster(_ context.Context, id, _ string) (*GlobalCluster, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: GlobalClusterIdentifier is required", ErrInvalidParameter)
	}
	b.mu.Lock("SwitchoverGlobalCluster")
	defer b.mu.Unlock()
	gc, exists := b.globalClusters[id]
	if !exists {
		return nil, fmt.Errorf("%w: global cluster %s not found", ErrGlobalClusterNotFound, id)
	}
	gc.Status = "switching-over"
	cp := *gc

	return &cp, nil
}

// RestoreDBClusterFromSnapshot restores a new cluster from a snapshot.
func (b *InMemoryBackend) RestoreDBClusterFromSnapshot(
	ctx context.Context,
	snapshotID, clusterID, engine string,
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
	snapshots := b.clusterSnapshotsStore(region)
	snap, snapExists := snapshots[snapshotID]
	if !snapExists {
		return nil, fmt.Errorf("%w: cluster snapshot %s not found", ErrClusterSnapshotNotFound, snapshotID)
	}
	clusters := b.clustersStore(region)
	if _, clusterExists := clusters[clusterID]; clusterExists {
		return nil, fmt.Errorf("%w: cluster %s already exists", ErrClusterAlreadyExists, clusterID)
	}
	if engine == "" {
		engine = snap.Engine
	}
	engineVersion := snap.EngineVersion
	if engineVersion == "" {
		engineVersion = defaultEngineVersion
	}
	var paramGroupName, subnetGroupName string
	if src, exists := clusters[snap.DBClusterIdentifier]; exists {
		paramGroupName = src.DBClusterParameterGroupName
		subnetGroupName = src.DBSubnetGroupName
	}
	if paramGroupName == "" {
		paramGroupName = "default.docdb4.0"
	}
	clusterArn := b.clusterARN(region, clusterID)
	endpoint := fmt.Sprintf("%s.cluster.docdb.%s.amazonaws.com", clusterID, region)
	readerEndpoint := fmt.Sprintf("%s.cluster-ro.docdb.%s.amazonaws.com", clusterID, region)
	cluster := &DBCluster{
		DBClusterIdentifier:         clusterID,
		Engine:                      engine,
		Status:                      statusAvailable,
		DBClusterParameterGroupName: paramGroupName,
		DBSubnetGroupName:           subnetGroupName,
		Endpoint:                    endpoint,
		ReaderEndpoint:              readerEndpoint,
		Port:                        defaultDocDBPort,
		DBClusterArn:                clusterArn,
		EngineVersion:               engineVersion,
		StorageEncrypted:            snap.StorageEncrypted,
		ClusterCreateTime:           time.Now().UTC().Format(time.RFC3339),
	}
	clusters[clusterID] = cluster

	return copyCluster(cluster), nil
}

// RestoreDBClusterToPointInTime restores a new cluster to a point in time from a source cluster.
func (b *InMemoryBackend) RestoreDBClusterToPointInTime(
	ctx context.Context,
	sourceClusterID, targetClusterID string,
) (*DBCluster, error) {
	if sourceClusterID == "" {
		return nil, fmt.Errorf("%w: SourceDBClusterIdentifier is required", ErrInvalidParameter)
	}
	if targetClusterID == "" {
		return nil, fmt.Errorf("%w: DBClusterIdentifier is required", ErrInvalidParameter)
	}
	region := getRegion(ctx, b.region)
	b.mu.Lock("RestoreDBClusterToPointInTime")
	defer b.mu.Unlock()
	clusters := b.clustersStore(region)
	src, srcExists := clusters[sourceClusterID]
	if !srcExists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, sourceClusterID)
	}
	if _, targetExists := clusters[targetClusterID]; targetExists {
		return nil, fmt.Errorf("%w: cluster %s already exists", ErrClusterAlreadyExists, targetClusterID)
	}
	clusterArn := b.clusterARN(region, targetClusterID)
	endpoint := fmt.Sprintf("%s.cluster.docdb.%s.amazonaws.com", targetClusterID, region)
	readerEndpoint := fmt.Sprintf("%s.cluster-ro.docdb.%s.amazonaws.com", targetClusterID, region)
	cluster := &DBCluster{
		DBClusterIdentifier:         targetClusterID,
		Engine:                      src.Engine,
		Status:                      statusAvailable,
		MasterUsername:              src.MasterUsername,
		DatabaseName:                src.DatabaseName,
		DBClusterParameterGroupName: src.DBClusterParameterGroupName,
		DBSubnetGroupName:           src.DBSubnetGroupName,
		Endpoint:                    endpoint,
		ReaderEndpoint:              readerEndpoint,
		Port:                        src.Port,
		DBClusterArn:                clusterArn,
		EngineVersion:               src.EngineVersion,
		StorageEncrypted:            src.StorageEncrypted,
		PreferredBackupWindow:       src.PreferredBackupWindow,
		PreferredMaintenanceWindow:  src.PreferredMaintenanceWindow,
		ClusterCreateTime:           time.Now().UTC().Format(time.RFC3339),
	}
	clusters[targetClusterID] = cluster

	return copyCluster(cluster), nil
}

// DBEngineVersion represents a supported DocDB engine version.
type DBEngineVersion struct {
	Engine              string
	EngineVersion       string
	DBEngineDescription string
}

// DescribeDBEngineVersions returns available engine versions, optionally filtered.
func (b *InMemoryBackend) DescribeDBEngineVersions(_ context.Context, engine, engineVersion string) []DBEngineVersion {
	all := []DBEngineVersion{
		{Engine: docDBEngine, EngineVersion: defaultEngineVersion, DBEngineDescription: "Amazon DocumentDB"},
		{Engine: docDBEngine, EngineVersion: docDBEngineVersion5, DBEngineDescription: "Amazon DocumentDB"},
	}
	result := make([]DBEngineVersion, 0, len(all))
	for _, v := range all {
		if engine != "" && v.Engine != engine {
			continue
		}
		if engineVersion != "" && v.EngineVersion != engineVersion {
			continue
		}
		result = append(result, v)
	}

	return result
}

// DescribeEventCategories returns the event categories for DocDB.
func (b *InMemoryBackend) DescribeEventCategories(_ context.Context, sourceType string) []EventCategoryMap {
	clusterCategories := []string{
		"availability", eventCatBackup, "configuration change",
		eventCatCreate, eventCatDelete, "failover", "maintenance", eventCatNotify,
	}
	instanceCategories := []string{
		"availability", eventCatBackup, "configuration change",
		eventCatCreate, eventCatDelete, "failover", "maintenance",
		eventCatNotify, "recovery", "restoration",
	}
	snapshotCategories := []string{
		eventCatBackup, eventCatCreate, eventCatDelete, eventCatNotify, "restoration",
	}
	all := []EventCategoryMap{
		{SourceType: "db-cluster", EventCategories: clusterCategories},
		{SourceType: "db-instance", EventCategories: instanceCategories},
		{SourceType: "db-cluster-snapshot", EventCategories: snapshotCategories},
	}
	if sourceType == "" {
		return all
	}
	for _, m := range all {
		if m.SourceType == sourceType {
			return []EventCategoryMap{m}
		}
	}

	return []EventCategoryMap{}
}

// AddDBClusterInternal seeds a cluster directly for testing.
func (b *InMemoryBackend) AddDBClusterInternal(cluster *DBCluster) {
	b.mu.Lock("AddDBClusterInternal")
	defer b.mu.Unlock()
	b.clustersStore(b.region)[cluster.DBClusterIdentifier] = cluster
}

// AddDBInstanceInternal seeds an instance directly for testing.
func (b *InMemoryBackend) AddDBInstanceInternal(inst *DBInstance) {
	b.mu.Lock("AddDBInstanceInternal")
	defer b.mu.Unlock()
	b.instancesStore(b.region)[inst.DBInstanceIdentifier] = inst
}

// AddDBSubnetGroupInternal seeds a subnet group directly for testing.
func (b *InMemoryBackend) AddDBSubnetGroupInternal(sg *DBSubnetGroup) {
	b.mu.Lock("AddDBSubnetGroupInternal")
	defer b.mu.Unlock()
	b.subnetGroupsStore(b.region)[sg.DBSubnetGroupName] = sg
}

// AddDBClusterParameterGroupInternal seeds a parameter group directly for testing.
func (b *InMemoryBackend) AddDBClusterParameterGroupInternal(pg *DBClusterParameterGroup) {
	b.mu.Lock("AddDBClusterParameterGroupInternal")
	defer b.mu.Unlock()
	b.clusterParameterGroupsStore(b.region)[pg.DBClusterParameterGroupName] = pg
}

// AddDBClusterSnapshotInternal seeds a snapshot directly for testing.
func (b *InMemoryBackend) AddDBClusterSnapshotInternal(snap *DBClusterSnapshot) {
	b.mu.Lock("AddDBClusterSnapshotInternal")
	defer b.mu.Unlock()
	b.clusterSnapshotsStore(b.region)[snap.DBClusterSnapshotIdentifier] = snap
}

// AddEventSubscriptionInternal seeds an event subscription directly for testing.
func (b *InMemoryBackend) AddEventSubscriptionInternal(sub *EventSubscription) {
	b.mu.Lock("AddEventSubscriptionInternal")
	defer b.mu.Unlock()
	b.eventSubscriptionsStore(b.region)[sub.SubscriptionName] = sub
}

// AddGlobalClusterInternal seeds a global cluster directly for testing.
func (b *InMemoryBackend) AddGlobalClusterInternal(gc *GlobalCluster) {
	b.mu.Lock("AddGlobalClusterInternal")
	defer b.mu.Unlock()
	b.globalClusters[gc.GlobalClusterIdentifier] = gc
}

// copyCluster returns a deep copy of a DBCluster.
func copyCluster(c *DBCluster) *DBCluster {
	cp := *c
	cp.Tags = copyTags(c.Tags)
	if len(c.AvailabilityZones) > 0 {
		cp.AvailabilityZones = make([]string, len(c.AvailabilityZones))
		copy(cp.AvailabilityZones, c.AvailabilityZones)
	}
	if len(c.VpcSecurityGroupIDs) > 0 {
		cp.VpcSecurityGroupIDs = make([]string, len(c.VpcSecurityGroupIDs))
		copy(cp.VpcSecurityGroupIDs, c.VpcSecurityGroupIDs)
	}
	if len(c.EnabledCloudwatchLogsExports) > 0 {
		cp.EnabledCloudwatchLogsExports = make([]string, len(c.EnabledCloudwatchLogsExports))
		copy(cp.EnabledCloudwatchLogsExports, c.EnabledCloudwatchLogsExports)
	}
	if len(c.ReadReplicaIdentifiers) > 0 {
		cp.ReadReplicaIdentifiers = make([]string, len(c.ReadReplicaIdentifiers))
		copy(cp.ReadReplicaIdentifiers, c.ReadReplicaIdentifiers)
	}

	return &cp
}

// copyInstance returns a deep copy of a DBInstance.
func copyInstance(inst *DBInstance) *DBInstance {
	cp := *inst
	cp.Tags = copyTags(inst.Tags)

	return &cp
}

// copyEventSubscription returns a deep copy of an EventSubscription.
func copyEventSubscription(sub *EventSubscription) *EventSubscription {
	cp := *sub
	cp.SourceIDs = make([]string, len(sub.SourceIDs))
	copy(cp.SourceIDs, sub.SourceIDs)
	cp.EventCategories = make([]string, len(sub.EventCategories))
	copy(cp.EventCategories, sub.EventCategories)

	return &cp
}

// copyTags returns a deep copy of a string map (tags).
func copyTags(src map[string]string) map[string]string {
	if src == nil {
		return nil
	}
	dst := make(map[string]string, len(src))
	maps.Copy(dst, src)

	return dst
}

// tagsFromMap converts a map[string]string to []Tag.
func tagsFromMap(m map[string]string) []Tag {
	tags := make([]Tag, 0, len(m))
	for k, v := range m {
		tags = append(tags, Tag{Key: k, Value: v})
	}

	return tags
}

// firstAZ returns the first availability zone from a slice, or empty string.
func firstAZ(azs []string) string {
	if len(azs) == 0 {
		return ""
	}

	return azs[0]
}

// valueOrDefault returns s if non-empty, otherwise returns def.
func valueOrDefault(s, def string) string {
	if s == "" {
		return def
	}

	return s
}
