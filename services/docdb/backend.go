package docdb

import (
	"fmt"
	"maps"
	"slices"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

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
)

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
	HostedZoneId                     string            `json:"hostedZoneId"`
	KmsKeyId                         string            `json:"kmsKeyId"`
	ReplicationSourceIdentifier      string            `json:"replicationSourceIdentifier"`
	AvailabilityZones                []string          `json:"availabilityZones"`
	VpcSecurityGroupIds              []string          `json:"vpcSecurityGroupIds"`
	EnabledCloudwatchLogsExports     []string          `json:"enabledCloudwatchLogsExports"`
	ReadReplicaIdentifiers           []string          `json:"readReplicaIdentifiers"`
	Port                             int               `json:"port"`
	BackupRetentionPeriod            int               `json:"backupRetentionPeriod"`
	StorageEncrypted                 bool              `json:"storageEncrypted"`
	MultiAZ                         bool              `json:"multiAZ"`
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

type InMemoryBackend struct {
	clusters               map[string]*DBCluster
	instances              map[string]*DBInstance
	subnetGroups           map[string]*DBSubnetGroup
	clusterParameterGroups map[string]*DBClusterParameterGroup
	clusterSnapshots       map[string]*DBClusterSnapshot
	eventSubscriptions     map[string]*EventSubscription
	globalClusters         map[string]*GlobalCluster
	snapshotAttributes     map[string]*DBClusterSnapshotAttributesResult
	tags                   map[string][]Tag
	mu                     *lockmetrics.RWMutex
	accountID              string
	region                 string
}

func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		clusters:               make(map[string]*DBCluster),
		instances:              make(map[string]*DBInstance),
		subnetGroups:           make(map[string]*DBSubnetGroup),
		clusterParameterGroups: make(map[string]*DBClusterParameterGroup),
		clusterSnapshots:       make(map[string]*DBClusterSnapshot),
		eventSubscriptions:     make(map[string]*EventSubscription),
		globalClusters:         make(map[string]*GlobalCluster),
		snapshotAttributes:     make(map[string]*DBClusterSnapshotAttributesResult),
		tags:                   make(map[string][]Tag),
		accountID:              accountID,
		region:                 region,
		mu:                     lockmetrics.New("docdb"),
	}
}

// Reset clears all stored state, returning the backend to an empty state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.clusters = make(map[string]*DBCluster)
	b.instances = make(map[string]*DBInstance)
	b.subnetGroups = make(map[string]*DBSubnetGroup)
	b.clusterParameterGroups = make(map[string]*DBClusterParameterGroup)
	b.clusterSnapshots = make(map[string]*DBClusterSnapshot)
	b.eventSubscriptions = make(map[string]*EventSubscription)
	b.globalClusters = make(map[string]*GlobalCluster)
	b.snapshotAttributes = make(map[string]*DBClusterSnapshotAttributesResult)
	b.tags = make(map[string][]Tag)
}

func (b *InMemoryBackend) Region() string { return b.region }

// clusterARN returns the ARN for a DB cluster.
func (b *InMemoryBackend) clusterARN(id string) string {
	return arn.Build("rds", b.region, b.accountID, "cluster:"+id)
}

// instanceARN returns the ARN for a DB instance.
func (b *InMemoryBackend) instanceARN(id string) string {
	return arn.Build("rds", b.region, b.accountID, "db:"+id)
}

// subnetGroupARN returns the ARN for a DB subnet group.
func (b *InMemoryBackend) subnetGroupARN(name string) string {
	return arn.Build("rds", b.region, b.accountID, "subgrp:"+name)
}

// clusterParameterGroupARN returns the ARN for a DB cluster parameter group.
func (b *InMemoryBackend) clusterParameterGroupARN(name string) string {
	return arn.Build("rds", b.region, b.accountID, "cluster-pg:"+name)
}

// clusterSnapshotARN returns the ARN for a DB cluster snapshot.
func (b *InMemoryBackend) clusterSnapshotARN(id string) string {
	return arn.Build("rds", b.region, b.accountID, "cluster-snapshot:"+id)
}

// globalClusterARN returns the ARN for a global cluster.
func (b *InMemoryBackend) globalClusterARN(id string) string {
	return arn.Build("rds", b.region, b.accountID, "global-cluster:"+id)
}

func (b *InMemoryBackend) CreateDBCluster(
	id, engine, engineVersion, masterUser, dbName, paramGroupName, subnetGroupName string,
	port int,
	storageEncrypted, deletionProtection bool,
	backupRetentionPeriod int,
	preferredBackupWindow, preferredMaintenanceWindow string,
	availabilityZones []string,
	tags map[string]string,
	opts *CreateDBClusterOptions,
) (*DBCluster, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: DBClusterIdentifier is required", ErrInvalidParameter)
	}
	b.mu.Lock("CreateDBCluster")
	defer b.mu.Unlock()
	if _, exists := b.clusters[id]; exists {
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
	clusterArn := b.clusterARN(id)
	endpoint := fmt.Sprintf("%s.cluster.docdb.%s.amazonaws.com", id, b.region)
	readerEndpoint := fmt.Sprintf("%s.cluster-ro.docdb.%s.amazonaws.com", id, b.region)
	azs := make([]string, len(availabilityZones))
	copy(azs, availabilityZones)

	var (
		kmsKeyId                         string
		vpcSecurityGroupIds              []string
		enabledCloudwatchLogsExports     []string
		iamDatabaseAuthenticationEnabled bool
	)
	if opts != nil {
		kmsKeyId = opts.KmsKeyId
		iamDatabaseAuthenticationEnabled = opts.IAMDatabaseAuthenticationEnabled
		if len(opts.VpcSecurityGroupIds) > 0 {
			vpcSecurityGroupIds = make([]string, len(opts.VpcSecurityGroupIds))
			copy(vpcSecurityGroupIds, opts.VpcSecurityGroupIds)
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
		KmsKeyId:                         kmsKeyId,
		VpcSecurityGroupIds:              vpcSecurityGroupIds,
		EnabledCloudwatchLogsExports:     enabledCloudwatchLogsExports,
		IAMDatabaseAuthenticationEnabled: iamDatabaseAuthenticationEnabled,
	}
	b.clusters[id] = cluster
	if len(tags) > 0 {
		b.tags[clusterArn] = tagsFromMap(tags)
	}

	return copyCluster(cluster), nil
}

// CreateDBClusterOptions holds optional parameters for CreateDBCluster.
type CreateDBClusterOptions struct {
	KmsKeyId                         string
	VpcSecurityGroupIds              []string
	EnabledCloudwatchLogsExports     []string
	IAMDatabaseAuthenticationEnabled bool
}

func (b *InMemoryBackend) DescribeDBClusters(id string) ([]DBCluster, error) {
	b.mu.RLock("DescribeDBClusters")
	defer b.mu.RUnlock()
	if id != "" {
		c, exists := b.clusters[id]
		if !exists {
			return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
		}

		return []DBCluster{*copyCluster(c)}, nil
	}
	result := make([]DBCluster, 0, len(b.clusters))
	for _, c := range b.clusters {
		result = append(result, *copyCluster(c))
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].DBClusterIdentifier < result[j].DBClusterIdentifier
	})

	return result, nil
}

func (b *InMemoryBackend) DeleteDBCluster(id string) (*DBCluster, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: DBClusterIdentifier is required", ErrInvalidParameter)
	}
	b.mu.Lock("DeleteDBCluster")
	defer b.mu.Unlock()
	c, exists := b.clusters[id]
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}
	if c.DeletionProtection {
		return nil, fmt.Errorf("%w: cluster %s has deletion protection enabled", ErrInvalidClusterState, id)
	}
	for _, inst := range b.instances {
		if inst.DBClusterIdentifier == id {
			return nil, fmt.Errorf("%w: cluster %s still has instances, delete them first", ErrInvalidClusterState, id)
		}
	}
	cp := copyCluster(c)
	delete(b.clusters, id)
	delete(b.tags, b.clusterARN(id))

	return cp, nil
}

func (b *InMemoryBackend) ModifyDBCluster(
	id, paramGroupName string,
	deletionProtection *bool,
	backupRetentionPeriod int,
	preferredBackupWindow, preferredMaintenanceWindow string,
) (*DBCluster, error) {
	b.mu.Lock("ModifyDBCluster")
	defer b.mu.Unlock()
	c, exists := b.clusters[id]
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

	return copyCluster(c), nil
}

func (b *InMemoryBackend) StopDBCluster(id string) (*DBCluster, error) {
	b.mu.Lock("StopDBCluster")
	defer b.mu.Unlock()
	c, exists := b.clusters[id]
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}
	if c.Status != statusAvailable {
		return nil, fmt.Errorf("%w: cluster %s is not in available state", ErrInvalidClusterState, id)
	}
	c.Status = statusStopped

	return copyCluster(c), nil
}

func (b *InMemoryBackend) StartDBCluster(id string) (*DBCluster, error) {
	b.mu.Lock("StartDBCluster")
	defer b.mu.Unlock()
	c, exists := b.clusters[id]
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}
	if c.Status != statusStopped {
		return nil, fmt.Errorf("%w: cluster %s is not in stopped state", ErrInvalidClusterState, id)
	}
	c.Status = statusAvailable

	return copyCluster(c), nil
}

func (b *InMemoryBackend) FailoverDBCluster(id string) (*DBCluster, error) {
	b.mu.Lock("FailoverDBCluster")
	defer b.mu.Unlock()
	c, exists := b.clusters[id]
	if !exists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, id)
	}
	if c.Status != statusAvailable {
		return nil, fmt.Errorf("%w: cluster %s is not in available state for failover", ErrInvalidClusterState, id)
	}

	return copyCluster(c), nil
}

func (b *InMemoryBackend) CreateDBInstance(
	id, clusterID, instanceClass, engine string,
	promotionTier int,
	tags map[string]string,
) (*DBInstance, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: DBInstanceIdentifier is required", ErrInvalidParameter)
	}
	b.mu.Lock("CreateDBInstance")
	defer b.mu.Unlock()
	if _, exists := b.instances[id]; exists {
		return nil, fmt.Errorf("%w: instance %s already exists", ErrInstanceAlreadyExists, id)
	}
	if engine == "" {
		engine = docDBEngine
	}
	if instanceClass == "" {
		instanceClass = defaultInstanceClass
	}
	if promotionTier <= 0 {
		promotionTier = 1
	}
	var clusterEngineVersion string
	var clusterStorageEncrypted bool
	var clusterAZ string
	var clusterSubnetGroupName string
	if clusterID != "" {
		if parentCluster, exists := b.clusters[clusterID]; exists {
			clusterEngineVersion = parentCluster.EngineVersion
			clusterStorageEncrypted = parentCluster.StorageEncrypted
			clusterAZ = firstAZ(parentCluster.AvailabilityZones)
			clusterSubnetGroupName = parentCluster.DBSubnetGroupName
		}
	}
	instanceArn := b.instanceARN(id)
	endpoint := fmt.Sprintf("%s.docdb.%s.amazonaws.com", id, b.region)
	inst := &DBInstance{
		DBInstanceIdentifier: id,
		DBClusterIdentifier:  clusterID,
		DBInstanceClass:      instanceClass,
		Engine:               engine,
		DBInstanceStatus:     statusAvailable,
		Endpoint:             endpoint,
		Port:                 defaultDocDBPort,
		DBInstanceArn:        instanceArn,
		EngineVersion:        valueOrDefault(clusterEngineVersion, defaultEngineVersion),
		StorageEncrypted:     clusterStorageEncrypted,
		AvailabilityZone:     clusterAZ,
		DBSubnetGroupName:    clusterSubnetGroupName,
		PromotionTier:        promotionTier,
		Tags:                 copyTags(tags),
	}
	b.instances[id] = inst
	if len(tags) > 0 {
		b.tags[instanceArn] = tagsFromMap(tags)
	}

	return copyInstance(inst), nil
}

func (b *InMemoryBackend) DescribeDBInstances(id, clusterID string) ([]DBInstance, error) {
	b.mu.RLock("DescribeDBInstances")
	defer b.mu.RUnlock()
	if id != "" {
		inst, exists := b.instances[id]
		if !exists {
			return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, id)
		}

		return []DBInstance{*copyInstance(inst)}, nil
	}
	result := make([]DBInstance, 0, len(b.instances))
	for _, inst := range b.instances {
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

func (b *InMemoryBackend) DeleteDBInstance(id string) (*DBInstance, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: DBInstanceIdentifier is required", ErrInvalidParameter)
	}
	b.mu.Lock("DeleteDBInstance")
	defer b.mu.Unlock()
	inst, exists := b.instances[id]
	if !exists {
		return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, id)
	}
	cp := copyInstance(inst)
	delete(b.instances, id)
	delete(b.tags, b.instanceARN(id))

	return cp, nil
}

func (b *InMemoryBackend) ModifyDBInstance(
	id, instanceClass string,
	autoMinorVersionUpgrade *bool,
	preferredMaintenanceWindow string,
) (*DBInstance, error) {
	b.mu.Lock("ModifyDBInstance")
	defer b.mu.Unlock()
	inst, exists := b.instances[id]
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

	return copyInstance(inst), nil
}

func (b *InMemoryBackend) RebootDBInstance(id string) (*DBInstance, error) {
	b.mu.Lock("RebootDBInstance")
	defer b.mu.Unlock()
	inst, exists := b.instances[id]
	if !exists {
		return nil, fmt.Errorf("%w: instance %s not found", ErrInstanceNotFound, id)
	}

	return copyInstance(inst), nil
}

func (b *InMemoryBackend) CreateDBSubnetGroup(
	name, description, vpcID string,
	subnetIDs []string,
	tags map[string]string,
) (*DBSubnetGroup, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: DBSubnetGroupName is required", ErrInvalidParameter)
	}
	b.mu.Lock("CreateDBSubnetGroup")
	defer b.mu.Unlock()
	if _, exists := b.subnetGroups[name]; exists {
		return nil, fmt.Errorf("%w: subnet group %s already exists", ErrSubnetGroupAlreadyExists, name)
	}
	ids := make([]string, len(subnetIDs))
	copy(ids, subnetIDs)
	sgArn := b.subnetGroupARN(name)
	sg := &DBSubnetGroup{
		DBSubnetGroupName:        name,
		DBSubnetGroupDescription: description,
		VpcID:                    vpcID,
		Status:                   "Complete",
		SubnetIDs:                ids,
		DBSubnetGroupArn:         sgArn,
		Tags:                     copyTags(tags),
	}
	b.subnetGroups[name] = sg
	if len(tags) > 0 {
		b.tags[sgArn] = tagsFromMap(tags)
	}
	cp := *sg
	cp.SubnetIDs = make([]string, len(ids))
	copy(cp.SubnetIDs, ids)
	cp.Tags = copyTags(sg.Tags)

	return &cp, nil
}

func (b *InMemoryBackend) DescribeDBSubnetGroups(name string) ([]DBSubnetGroup, error) {
	b.mu.RLock("DescribeDBSubnetGroups")
	defer b.mu.RUnlock()
	if name != "" {
		sg, exists := b.subnetGroups[name]
		if !exists {
			return nil, fmt.Errorf("%w: subnet group %s not found", ErrSubnetGroupNotFound, name)
		}
		cp := *sg
		cp.SubnetIDs = make([]string, len(sg.SubnetIDs))
		copy(cp.SubnetIDs, sg.SubnetIDs)
		cp.Tags = copyTags(sg.Tags)

		return []DBSubnetGroup{cp}, nil
	}
	result := make([]DBSubnetGroup, 0, len(b.subnetGroups))
	for _, sg := range b.subnetGroups {
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

func (b *InMemoryBackend) DeleteDBSubnetGroup(name string) error {
	b.mu.Lock("DeleteDBSubnetGroup")
	defer b.mu.Unlock()
	if _, exists := b.subnetGroups[name]; !exists {
		return fmt.Errorf("%w: subnet group %s not found", ErrSubnetGroupNotFound, name)
	}
	for _, c := range b.clusters {
		if c.DBSubnetGroupName == name {
			return fmt.Errorf(
				"%w: subnet group %s is used by cluster %s",
				ErrSubnetGroupInUse,
				name,
				c.DBClusterIdentifier,
			)
		}
	}
	delete(b.subnetGroups, name)
	delete(b.tags, b.subnetGroupARN(name))

	return nil
}

func (b *InMemoryBackend) CreateDBClusterParameterGroup(
	name, family, description string,
	tags map[string]string,
) (*DBClusterParameterGroup, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: DBClusterParameterGroupName is required", ErrInvalidParameter)
	}
	b.mu.Lock("CreateDBClusterParameterGroup")
	defer b.mu.Unlock()
	if _, exists := b.clusterParameterGroups[name]; exists {
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
		DBClusterParameterGroupArn:  b.clusterParameterGroupARN(name),
		Tags:                        copyTags(tags),
	}
	b.clusterParameterGroups[name] = pg
	pgArn := b.clusterParameterGroupARN(name)
	if len(tags) > 0 {
		b.tags[pgArn] = tagsFromMap(tags)
	}
	cp := *pg
	cp.Tags = copyTags(pg.Tags)

	return &cp, nil
}

func (b *InMemoryBackend) DescribeDBClusterParameterGroups(name string) ([]DBClusterParameterGroup, error) {
	b.mu.RLock("DescribeDBClusterParameterGroups")
	defer b.mu.RUnlock()
	if name != "" {
		pg, exists := b.clusterParameterGroups[name]
		if !exists {
			return nil, fmt.Errorf("%w: cluster parameter group %s not found", ErrClusterParameterGroupNotFound, name)
		}
		cp := *pg
		cp.Tags = copyTags(pg.Tags)

		return []DBClusterParameterGroup{cp}, nil
	}
	result := make([]DBClusterParameterGroup, 0, len(b.clusterParameterGroups))
	for _, pg := range b.clusterParameterGroups {
		cp := *pg
		cp.Tags = copyTags(pg.Tags)
		result = append(result, cp)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].DBClusterParameterGroupName < result[j].DBClusterParameterGroupName
	})

	return result, nil
}

func (b *InMemoryBackend) DeleteDBClusterParameterGroup(name string) error {
	b.mu.Lock("DeleteDBClusterParameterGroup")
	defer b.mu.Unlock()
	if _, exists := b.clusterParameterGroups[name]; !exists {
		return fmt.Errorf("%w: cluster parameter group %s not found", ErrClusterParameterGroupNotFound, name)
	}
	for _, c := range b.clusters {
		if c.DBClusterParameterGroupName == name {
			return fmt.Errorf(
				"%w: parameter group %s is used by cluster %s",
				ErrParameterGroupInUse,
				name,
				c.DBClusterIdentifier,
			)
		}
	}
	delete(b.clusterParameterGroups, name)
	delete(b.tags, b.clusterParameterGroupARN(name))

	return nil
}

func (b *InMemoryBackend) ModifyDBClusterParameterGroup(name string) (*DBClusterParameterGroup, error) {
	b.mu.Lock("ModifyDBClusterParameterGroup")
	defer b.mu.Unlock()
	pg, exists := b.clusterParameterGroups[name]
	if !exists {
		return nil, fmt.Errorf("%w: cluster parameter group %s not found", ErrClusterParameterGroupNotFound, name)
	}
	cp := *pg
	cp.Tags = copyTags(pg.Tags)

	return &cp, nil
}

func (b *InMemoryBackend) CreateDBClusterSnapshot(
	snapshotID, clusterID string,
	tags map[string]string,
) (*DBClusterSnapshot, error) {
	if snapshotID == "" {
		return nil, fmt.Errorf("%w: DBClusterSnapshotIdentifier is required", ErrInvalidParameter)
	}
	if clusterID == "" {
		return nil, fmt.Errorf("%w: DBClusterIdentifier is required", ErrInvalidParameter)
	}
	b.mu.Lock("CreateDBClusterSnapshot")
	defer b.mu.Unlock()
	if _, exists := b.clusterSnapshots[snapshotID]; exists {
		return nil, fmt.Errorf("%w: cluster snapshot %s already exists", ErrClusterSnapshotAlreadyExists, snapshotID)
	}
	c, exists := b.clusters[clusterID]
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
		DBClusterArn:                b.clusterARN(clusterID),
		Tags:                        copyTags(tags),
	}
	b.clusterSnapshots[snapshotID] = snap
	snapArn := b.clusterSnapshotARN(snapshotID)
	if len(tags) > 0 {
		b.tags[snapArn] = tagsFromMap(tags)
	}
	cp := *snap
	cp.Tags = copyTags(snap.Tags)

	return &cp, nil
}

func (b *InMemoryBackend) DescribeDBClusterSnapshots(
	snapshotID, clusterID, snapshotType string,
) ([]DBClusterSnapshot, error) {
	b.mu.RLock("DescribeDBClusterSnapshots")
	defer b.mu.RUnlock()
	if snapshotID != "" {
		snap, exists := b.clusterSnapshots[snapshotID]
		if !exists {
			return nil, fmt.Errorf("%w: cluster snapshot %s not found", ErrClusterSnapshotNotFound, snapshotID)
		}
		cp := *snap
		cp.Tags = copyTags(snap.Tags)

		return []DBClusterSnapshot{cp}, nil
	}
	result := make([]DBClusterSnapshot, 0, len(b.clusterSnapshots))
	for _, snap := range b.clusterSnapshots {
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

func (b *InMemoryBackend) DeleteDBClusterSnapshot(snapshotID string) (*DBClusterSnapshot, error) {
	if snapshotID == "" {
		return nil, fmt.Errorf("%w: DBClusterSnapshotIdentifier is required", ErrInvalidParameter)
	}
	b.mu.Lock("DeleteDBClusterSnapshot")
	defer b.mu.Unlock()
	snap, exists := b.clusterSnapshots[snapshotID]
	if !exists {
		return nil, fmt.Errorf("%w: cluster snapshot %s not found", ErrClusterSnapshotNotFound, snapshotID)
	}
	cp := *snap
	cp.Tags = copyTags(snap.Tags)
	delete(b.clusterSnapshots, snapshotID)
	delete(b.tags, b.clusterSnapshotARN(snapshotID))

	return &cp, nil
}

func (b *InMemoryBackend) AddTagsToResource(arn string, tags []Tag) {
	b.mu.Lock("AddTagsToResource")
	defer b.mu.Unlock()
	current := b.tags[arn]
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
	b.tags[arn] = current
}

func (b *InMemoryBackend) RemoveTagsFromResource(arn string, keys []string) {
	b.mu.Lock("RemoveTagsFromResource")
	defer b.mu.Unlock()
	remove := make(map[string]bool, len(keys))
	for _, k := range keys {
		remove[k] = true
	}
	current := b.tags[arn]
	kept := make([]Tag, 0, len(current))
	for _, t := range current {
		if !remove[t.Key] {
			kept = append(kept, t)
		}
	}
	b.tags[arn] = kept
}

func (b *InMemoryBackend) ListTagsForResource(arn string) []Tag {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()
	src := b.tags[arn]
	cp := make([]Tag, len(src))
	copy(cp, src)
	sort.Slice(cp, func(i, j int) bool {
		return cp[i].Key < cp[j].Key
	})

	return cp
}

// AddSourceIdentifierToSubscription adds a source identifier to an event subscription.
func (b *InMemoryBackend) AddSourceIdentifierToSubscription(
	subscriptionName, sourceID string,
) (*EventSubscription, error) {
	if subscriptionName == "" {
		return nil, fmt.Errorf("%w: SubscriptionName is required", ErrInvalidParameter)
	}
	if sourceID == "" {
		return nil, fmt.Errorf("%w: SourceIdentifier is required", ErrInvalidParameter)
	}
	b.mu.Lock("AddSourceIdentifierToSubscription")
	defer b.mu.Unlock()
	sub, exists := b.eventSubscriptions[subscriptionName]
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
	sourceGroupName, targetName, targetDescription string,
) (*DBClusterParameterGroup, error) {
	if sourceGroupName == "" {
		return nil, fmt.Errorf("%w: SourceDBClusterParameterGroupIdentifier is required", ErrInvalidParameter)
	}
	if targetName == "" {
		return nil, fmt.Errorf("%w: TargetDBClusterParameterGroupIdentifier is required", ErrInvalidParameter)
	}
	b.mu.Lock("CopyDBClusterParameterGroup")
	defer b.mu.Unlock()
	src, exists := b.clusterParameterGroups[sourceGroupName]
	if !exists {
		return nil, fmt.Errorf(
			"%w: cluster parameter group %s not found",
			ErrClusterParameterGroupNotFound,
			sourceGroupName,
		)
	}
	if _, ok := b.clusterParameterGroups[targetName]; ok {
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
		DBClusterParameterGroupArn:  b.clusterParameterGroupARN(targetName),
	}
	b.clusterParameterGroups[targetName] = pg
	cp := *pg
	cp.Tags = copyTags(pg.Tags)

	return &cp, nil
}

// CopyDBClusterSnapshot copies a DB cluster snapshot.
func (b *InMemoryBackend) CopyDBClusterSnapshot(
	sourceSnapshotID, targetSnapshotID string,
) (*DBClusterSnapshot, error) {
	if sourceSnapshotID == "" {
		return nil, fmt.Errorf("%w: SourceDBClusterSnapshotIdentifier is required", ErrInvalidParameter)
	}
	if targetSnapshotID == "" {
		return nil, fmt.Errorf("%w: TargetDBClusterSnapshotIdentifier is required", ErrInvalidParameter)
	}
	b.mu.Lock("CopyDBClusterSnapshot")
	defer b.mu.Unlock()
	src, exists := b.clusterSnapshots[sourceSnapshotID]
	if !exists {
		return nil, fmt.Errorf("%w: cluster snapshot %s not found", ErrClusterSnapshotNotFound, sourceSnapshotID)
	}
	if _, ok := b.clusterSnapshots[targetSnapshotID]; ok {
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
	b.clusterSnapshots[targetSnapshotID] = snap
	cp := *snap
	cp.Tags = copyTags(snap.Tags)

	return &cp, nil
}

// CreateEventSubscription creates an event subscription.
func (b *InMemoryBackend) CreateEventSubscription(
	name, snsTopicARN, sourceType string,
	eventCategories, sourceIDs []string,
) (*EventSubscription, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: SubscriptionName is required", ErrInvalidParameter)
	}
	b.mu.Lock("CreateEventSubscription")
	defer b.mu.Unlock()
	if _, exists := b.eventSubscriptions[name]; exists {
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
	b.eventSubscriptions[name] = sub

	return copyEventSubscription(sub), nil
}

// CreateGlobalCluster creates a global cluster.
func (b *InMemoryBackend) CreateGlobalCluster(
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
func (b *InMemoryBackend) DeleteEventSubscription(name string) (*EventSubscription, error) {
	b.mu.Lock("DeleteEventSubscription")
	defer b.mu.Unlock()
	sub, exists := b.eventSubscriptions[name]
	if !exists {
		return nil, fmt.Errorf("%w: subscription %s not found", ErrEventSubscriptionNotFound, name)
	}
	cp := copyEventSubscription(sub)
	delete(b.eventSubscriptions, name)

	return cp, nil
}

// DeleteGlobalCluster deletes a global cluster.
func (b *InMemoryBackend) DeleteGlobalCluster(id string) (*GlobalCluster, error) {
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
func (b *InMemoryBackend) DescribeCertificates(certificateID string) []Certificate {
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
func (b *InMemoryBackend) DescribeDBClusterParameters(groupName string) ([]DBClusterParameter, error) {
	b.mu.RLock("DescribeDBClusterParameters")
	defer b.mu.RUnlock()
	if groupName == "" {
		return nil, fmt.Errorf("%w: DBClusterParameterGroupName is required", ErrInvalidParameter)
	}
	if _, exists := b.clusterParameterGroups[groupName]; !exists {
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
func (b *InMemoryBackend) DescribeGlobalClusters(id string) []GlobalCluster {
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
func (b *InMemoryBackend) DescribeEventSubscriptions(name string) []EventSubscription {
	b.mu.RLock("DescribeEventSubscriptions")
	defer b.mu.RUnlock()
	if name != "" {
		sub, exists := b.eventSubscriptions[name]
		if !exists {
			return []EventSubscription{}
		}

		return []EventSubscription{*copyEventSubscription(sub)}
	}
	result := make([]EventSubscription, 0, len(b.eventSubscriptions))
	for _, sub := range b.eventSubscriptions {
		result = append(result, *copyEventSubscription(sub))
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].SubscriptionName < result[j].SubscriptionName
	})

	return result
}

// ModifyEventSubscription modifies an event subscription.
func (b *InMemoryBackend) ModifyEventSubscription(
	name, snsTopicARN, sourceType string,
	eventCategories []string,
) (*EventSubscription, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: SubscriptionName is required", ErrInvalidParameter)
	}
	b.mu.Lock("ModifyEventSubscription")
	defer b.mu.Unlock()
	sub, exists := b.eventSubscriptions[name]
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
	subscriptionName, sourceID string,
) (*EventSubscription, error) {
	if subscriptionName == "" {
		return nil, fmt.Errorf("%w: SubscriptionName is required", ErrInvalidParameter)
	}
	if sourceID == "" {
		return nil, fmt.Errorf("%w: SourceIdentifier is required", ErrInvalidParameter)
	}
	b.mu.Lock("RemoveSourceIdentifierFromSubscription")
	defer b.mu.Unlock()
	sub, exists := b.eventSubscriptions[subscriptionName]
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
func (b *InMemoryBackend) DescribePendingMaintenanceActions(_ string) []ResourcePendingMaintenanceActions {
	return []ResourcePendingMaintenanceActions{}
}

// DescribeDBClusterSnapshotAttributes returns attributes for a cluster snapshot.
func (b *InMemoryBackend) DescribeDBClusterSnapshotAttributes(
	snapshotID string,
) (*DBClusterSnapshotAttributesResult, error) {
	if snapshotID == "" {
		return nil, fmt.Errorf("%w: DBClusterSnapshotIdentifier is required", ErrInvalidParameter)
	}
	b.mu.RLock("DescribeDBClusterSnapshotAttributes")
	defer b.mu.RUnlock()
	if _, exists := b.clusterSnapshots[snapshotID]; !exists {
		return nil, fmt.Errorf("%w: cluster snapshot %s not found", ErrClusterSnapshotNotFound, snapshotID)
	}
	result, ok := b.snapshotAttributes[snapshotID]
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
	snapshotID, attributeName string,
	valuesToAdd, valuesToRemove []string,
) (*DBClusterSnapshotAttributesResult, error) {
	if snapshotID == "" {
		return nil, fmt.Errorf("%w: DBClusterSnapshotIdentifier is required", ErrInvalidParameter)
	}
	if attributeName == "" {
		return nil, fmt.Errorf("%w: AttributeName is required", ErrInvalidParameter)
	}
	b.mu.Lock("ModifyDBClusterSnapshotAttribute")
	defer b.mu.Unlock()
	if _, exists := b.clusterSnapshots[snapshotID]; !exists {
		return nil, fmt.Errorf("%w: cluster snapshot %s not found", ErrClusterSnapshotNotFound, snapshotID)
	}
	result, ok := b.snapshotAttributes[snapshotID]
	if !ok {
		result = &DBClusterSnapshotAttributesResult{
			DBClusterSnapshotIdentifier: snapshotID,
			Attributes:                  []DBClusterSnapshotAttribute{},
		}
	}
	attr := findOrCreateAttribute(result, attributeName)
	applySnapshotAttributeChanges(attr, valuesToAdd, valuesToRemove)
	b.snapshotAttributes[snapshotID] = result

	return copySnapshotAttributesResult(result), nil
}

// DescribeEngineDefaultClusterParameters returns the default parameters for an engine family.
func (b *InMemoryBackend) DescribeEngineDefaultClusterParameters(
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
func (b *InMemoryBackend) ResetDBClusterParameterGroup(name string) (*DBClusterParameterGroup, error) {
	b.mu.Lock("ResetDBClusterParameterGroup")
	defer b.mu.Unlock()
	pg, exists := b.clusterParameterGroups[name]
	if !exists {
		return nil, fmt.Errorf("%w: cluster parameter group %s not found", ErrClusterParameterGroupNotFound, name)
	}
	cp := *pg
	cp.Tags = copyTags(pg.Tags)

	return &cp, nil
}

// ModifyDBSubnetGroup modifies a DB subnet group.
func (b *InMemoryBackend) ModifyDBSubnetGroup(
	name, description string,
	subnetIDs []string,
) (*DBSubnetGroup, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: DBSubnetGroupName is required", ErrInvalidParameter)
	}
	b.mu.Lock("ModifyDBSubnetGroup")
	defer b.mu.Unlock()
	sg, exists := b.subnetGroups[name]
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
func (b *InMemoryBackend) ModifyGlobalCluster(id, newID string, deletionProtection *bool) (*GlobalCluster, error) {
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
func (b *InMemoryBackend) FailoverGlobalCluster(id, _ string) (*GlobalCluster, error) {
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
func (b *InMemoryBackend) RemoveFromGlobalCluster(globalClusterID, _ string) (*GlobalCluster, error) {
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
func (b *InMemoryBackend) SwitchoverGlobalCluster(id, _ string) (*GlobalCluster, error) {
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
	snapshotID, clusterID, engine string,
) (*DBCluster, error) {
	if snapshotID == "" {
		return nil, fmt.Errorf("%w: DBClusterSnapshotIdentifier is required", ErrInvalidParameter)
	}
	if clusterID == "" {
		return nil, fmt.Errorf("%w: DBClusterIdentifier is required", ErrInvalidParameter)
	}
	b.mu.Lock("RestoreDBClusterFromSnapshot")
	defer b.mu.Unlock()
	snap, snapExists := b.clusterSnapshots[snapshotID]
	if !snapExists {
		return nil, fmt.Errorf("%w: cluster snapshot %s not found", ErrClusterSnapshotNotFound, snapshotID)
	}
	if _, clusterExists := b.clusters[clusterID]; clusterExists {
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
	if src, exists := b.clusters[snap.DBClusterIdentifier]; exists {
		paramGroupName = src.DBClusterParameterGroupName
		subnetGroupName = src.DBSubnetGroupName
	}
	if paramGroupName == "" {
		paramGroupName = "default.docdb4.0"
	}
	clusterArn := b.clusterARN(clusterID)
	endpoint := fmt.Sprintf("%s.cluster.docdb.%s.amazonaws.com", clusterID, b.region)
	readerEndpoint := fmt.Sprintf("%s.cluster-ro.docdb.%s.amazonaws.com", clusterID, b.region)
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
	b.clusters[clusterID] = cluster

	return copyCluster(cluster), nil
}

// RestoreDBClusterToPointInTime restores a new cluster to a point in time from a source cluster.
func (b *InMemoryBackend) RestoreDBClusterToPointInTime(
	sourceClusterID, targetClusterID string,
) (*DBCluster, error) {
	if sourceClusterID == "" {
		return nil, fmt.Errorf("%w: SourceDBClusterIdentifier is required", ErrInvalidParameter)
	}
	if targetClusterID == "" {
		return nil, fmt.Errorf("%w: DBClusterIdentifier is required", ErrInvalidParameter)
	}
	b.mu.Lock("RestoreDBClusterToPointInTime")
	defer b.mu.Unlock()
	src, srcExists := b.clusters[sourceClusterID]
	if !srcExists {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrClusterNotFound, sourceClusterID)
	}
	if _, targetExists := b.clusters[targetClusterID]; targetExists {
		return nil, fmt.Errorf("%w: cluster %s already exists", ErrClusterAlreadyExists, targetClusterID)
	}
	clusterArn := b.clusterARN(targetClusterID)
	endpoint := fmt.Sprintf("%s.cluster.docdb.%s.amazonaws.com", targetClusterID, b.region)
	readerEndpoint := fmt.Sprintf("%s.cluster-ro.docdb.%s.amazonaws.com", targetClusterID, b.region)
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
	b.clusters[targetClusterID] = cluster

	return copyCluster(cluster), nil
}

// DBEngineVersion represents a supported DocDB engine version.
type DBEngineVersion struct {
	Engine              string
	EngineVersion       string
	DBEngineDescription string
}

// DescribeDBEngineVersions returns available engine versions, optionally filtered.
func (b *InMemoryBackend) DescribeDBEngineVersions(engine, engineVersion string) []DBEngineVersion {
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
func (b *InMemoryBackend) DescribeEventCategories(sourceType string) []EventCategoryMap {
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
	b.clusters[cluster.DBClusterIdentifier] = cluster
}

// AddDBInstanceInternal seeds an instance directly for testing.
func (b *InMemoryBackend) AddDBInstanceInternal(inst *DBInstance) {
	b.mu.Lock("AddDBInstanceInternal")
	defer b.mu.Unlock()
	b.instances[inst.DBInstanceIdentifier] = inst
}

// AddDBSubnetGroupInternal seeds a subnet group directly for testing.
func (b *InMemoryBackend) AddDBSubnetGroupInternal(sg *DBSubnetGroup) {
	b.mu.Lock("AddDBSubnetGroupInternal")
	defer b.mu.Unlock()
	b.subnetGroups[sg.DBSubnetGroupName] = sg
}

// AddDBClusterParameterGroupInternal seeds a parameter group directly for testing.
func (b *InMemoryBackend) AddDBClusterParameterGroupInternal(pg *DBClusterParameterGroup) {
	b.mu.Lock("AddDBClusterParameterGroupInternal")
	defer b.mu.Unlock()
	b.clusterParameterGroups[pg.DBClusterParameterGroupName] = pg
}

// AddDBClusterSnapshotInternal seeds a snapshot directly for testing.
func (b *InMemoryBackend) AddDBClusterSnapshotInternal(snap *DBClusterSnapshot) {
	b.mu.Lock("AddDBClusterSnapshotInternal")
	defer b.mu.Unlock()
	b.clusterSnapshots[snap.DBClusterSnapshotIdentifier] = snap
}

// AddEventSubscriptionInternal seeds an event subscription directly for testing.
func (b *InMemoryBackend) AddEventSubscriptionInternal(sub *EventSubscription) {
	b.mu.Lock("AddEventSubscriptionInternal")
	defer b.mu.Unlock()
	b.eventSubscriptions[sub.SubscriptionName] = sub
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
