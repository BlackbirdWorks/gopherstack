package rds

import (
	"context"
	"time"
)

// StorageBackend defines the interface for RDS backend implementations.
// All mutating methods must be safe for concurrent use.
type StorageBackend interface {
	// Lifecycle
	Region() string
	AccountID() string
	Reset()
	Snapshot(ctx context.Context) []byte
	Restore(ctx context.Context, data []byte) error

	// DB instance operations
	CreateDBInstance(
		id, engine, instanceClass, dbName, masterUser, paramGroupName string,
		allocatedStorage int,
		opts DBInstanceOptions,
	) (*DBInstance, error)
	DeleteDBInstance(id string) (*DBInstance, error)
	DeleteDBInstanceWithOptions(
		id string,
		skipFinalSnapshot bool,
		finalSnapshotID string,
		deleteAutomatedBackups bool,
	) (*DBInstance, error)
	DescribeDBInstances(id string) ([]DBInstance, error)
	ModifyDBInstance(
		id, instanceClass string,
		allocatedStorage int,
		opts DBInstanceOptions,
	) (*DBInstance, error)
	StartDBInstance(id string) (*DBInstance, error)
	StopDBInstance(id string) (*DBInstance, error)
	RebootDBInstance(id string) (*DBInstance, error)
	CreateDBInstanceReadReplica(id, sourceID, sourceRegion string) (*DBInstance, error)
	PromoteReadReplica(id string) (*DBInstance, error)
	DescribeDBInstanceAutomatedBackups(instanceID string) []DBInstanceAutomatedBackup
	DescribeValidDBInstanceModifications(id string) (*DBInstance, error)

	// DB snapshot operations
	CreateDBSnapshot(snapshotID, instanceID string) (*DBSnapshot, error)
	DescribeDBSnapshots(snapshotID, instanceID string) ([]DBSnapshot, error)
	DeleteDBSnapshot(snapshotID string) (*DBSnapshot, error)
	CopyDBSnapshot(
		sourceSnapshotID, targetSnapshotID string,
		opts CopyDBSnapshotOptions,
	) (*DBSnapshot, error)
	RestoreDBInstanceFromDBSnapshot(
		id, snapshotID string,
		opts DBInstanceOptions,
	) (*DBInstance, error)
	RestoreDBInstanceToPointInTime(id, sourceID string, opts DBInstanceOptions) (*DBInstance, error)

	// DB subnet group operations
	CreateDBSubnetGroup(name, description, vpcID string, subnetIDs []string) (*DBSubnetGroup, error)
	DescribeDBSubnetGroups(name string) ([]DBSubnetGroup, error)
	DeleteDBSubnetGroup(name string) error

	// DB parameter group operations
	CreateDBParameterGroup(name, family, description string) (*DBParameterGroup, error)
	DescribeDBParameterGroups(name string) ([]DBParameterGroup, error)
	DeleteDBParameterGroup(name string) error
	ModifyDBParameterGroup(name string, params []DBParameter) (*DBParameterGroup, error)
	DescribeDBParameters(groupName string) ([]DBParameter, error)
	ResetDBParameterGroup(name string, resetAll bool, params []string) (*DBParameterGroup, error)
	CopyDBParameterGroup(
		sourceGroupName, targetGroupName, targetDescription string,
	) (*DBParameterGroup, error)

	// Option group operations
	CreateOptionGroup(name, engine, majorVersion, description string) (*OptionGroup, error)
	DescribeOptionGroups(name string) ([]OptionGroup, error)
	DeleteOptionGroup(name string) error
	ModifyOptionGroup(
		name string,
		optionsToAdd []OptionGroupOption,
		optionsToRemove []string,
	) (*OptionGroup, error)
	CopyOptionGroup(
		sourceGroupName, targetGroupName, targetDescription string,
	) (*OptionGroup, error)

	// DB cluster operations
	CreateDBCluster(
		id, engine, masterUser, dbName, paramGroupName string,
		port int,
		serverlessV2Cfg *ServerlessV2ScalingConfiguration,
		opts DBClusterOptions,
	) (*DBCluster, error)
	DescribeDBClusters(id string) ([]DBCluster, error)
	DeleteDBCluster(id string) (*DBCluster, error)
	DeleteDBClusterWithOptions(id string, skipFinalSnapshot bool, finalSnapshotID string) (*DBCluster, error)
	ModifyDBCluster(id, paramGroupName string, opts DBClusterOptions) (*DBCluster, error)
	StartDBCluster(id string) (*DBCluster, error)
	StopDBCluster(id string) (*DBCluster, error)
	RestoreDBClusterFromSnapshot(clusterID, snapshotID, engine string) (*DBCluster, error)
	RestoreDBClusterToPointInTime(clusterID, sourceClusterID string) (*DBCluster, error)

	// DB cluster parameter group operations
	CreateDBClusterParameterGroup(name, family, description string) (*DBParameterGroup, error)
	DescribeDBClusterParameterGroups(name string) ([]DBParameterGroup, error)
	CopyDBClusterParameterGroup(
		sourceGroupName, targetGroupName, targetDescription string,
	) (*DBParameterGroup, error)

	// DB cluster snapshot operations
	CreateDBClusterSnapshot(snapshotID, clusterID string) (*DBClusterSnapshot, error)
	DescribeDBClusterSnapshots(snapshotID, clusterID string) ([]DBClusterSnapshot, error)
	DeleteDBClusterSnapshot(snapshotID string) (*DBClusterSnapshot, error)
	CopyDBClusterSnapshot(sourceSnapshotID, targetSnapshotID string) (*DBClusterSnapshot, error)

	// DB cluster endpoint operations
	CreateDBClusterEndpoint(endpointID, clusterID, endpointType string) (*DBClusterEndpoint, error)
	DescribeDBClusterEndpoints(clusterID, endpointID string) ([]DBClusterEndpoint, error)
	DeleteDBClusterEndpoint(endpointID string) (*DBClusterEndpoint, error)

	// Global cluster operations
	CreateGlobalCluster(
		id, engine, engineVersion string,
		storageEncrypted, deletionProtection bool,
	) (*GlobalCluster, error)
	DescribeGlobalClusters(id string) ([]GlobalCluster, error)
	DeleteGlobalCluster(id string) (*GlobalCluster, error)
	ModifyGlobalCluster(
		id, newID, engineVersion string,
		deletionProtection *bool,
	) (*GlobalCluster, error)

	// Export task operations
	StartExportTask(taskID, sourceARN, s3Bucket, iamRoleARN, kmsKeyID string) (*ExportTask, error)
	DescribeExportTasks(taskID string) ([]ExportTask, error)
	CancelExportTask(taskID string) (*ExportTask, error)

	// Tag operations
	AddTagsToResource(arn string, tags []Tag)
	RemoveTagsFromResource(arn string, keys []string)
	ListTagsForResource(arn string) []Tag

	// Engine and instance metadata
	DescribeDBEngineVersions(engine, engineVersion string) []DBEngineVersion
	CreateCustomDBEngineVersion(
		engine, engineVersion, description string,
	) (*CustomDBEngineVersion, error)
	DeleteCustomDBEngineVersion(engine, engineVersion string) (*CustomDBEngineVersion, error)
	ModifyCustomDBEngineVersion(
		engine, engineVersion, description, status string,
	) (*CustomDBEngineVersion, error)
	DescribeCustomDBEngineVersions(engine, engineVersion string) []CustomDBEngineVersion
	DescribeOrderableDBInstanceOptions(engine, engineVersion string) []OrderableDBInstanceOption
	DescribeDBLogFiles(instanceID string, filter LogFileFilter) ([]DBLogFile, error)
	DownloadDBLogFilePortion(instanceID, logFileName, marker string, numberOfLines int) (LogFilePortion, error)

	// IAM role operations
	AddRoleToDBCluster(clusterID, roleARN, featureName string) error
	RemoveRoleFromDBCluster(clusterID, roleARN, featureName string) error
	AddRoleToDBInstance(instanceID, roleARN, featureName string) error
	RemoveRoleFromDBInstance(instanceID, roleARN, featureName string) error

	// Event subscription operations
	AddSourceIdentifierToSubscription(
		subscriptionName, sourceIdentifier string,
	) (*EventSubscription, error)
	RemoveSourceIdentifierFromSubscription(
		subscriptionName, sourceIdentifier string,
	) (*EventSubscription, error)

	// Maintenance operations
	ApplyPendingMaintenanceAction(resourceID, applyAction, optInType string) (string, error)
	BacktrackDBCluster(clusterID, backtrackTo string) (*DBClusterBacktrack, error)

	// Security group operations
	AuthorizeDBSecurityGroupIngress(groupName, cidrIP string) (*DBSecurityGroup, error)
	CreateDBSecurityGroup(name, description string) (*DBSecurityGroup, error)

	// Blue/Green Deployment operations
	CreateBlueGreenDeployment(name, source string) (*BlueGreenDeployment, error)

	// Global cluster membership operations
	RemoveFromGlobalCluster(globalClusterID, dbClusterARN string) (*GlobalCluster, error)
	FailoverGlobalCluster(globalClusterID, targetDBClusterIdentifier string) (*GlobalCluster, error)
	SwitchoverGlobalCluster(
		globalClusterID, targetDBClusterIdentifier string,
	) (*GlobalCluster, error)

	// Read replica promotion operations
	SwitchoverReadReplica(instanceID string) (*DBInstance, error)
	PromoteReadReplicaDBCluster(clusterID string) (*DBCluster, error)

	// Account and certificate operations
	DescribeAccountAttributes() []AccountAttribute
	DescribeCertificates(certID string) ([]Certificate, error)
	ModifyCertificates(certID string) (*Certificate, error)
	DescribePendingMaintenanceActions(resourceARN string) []PendingMaintenanceAction
	DescribeSourceRegions(regionName string) []SourceRegion
	DescribeDBMajorEngineVersions(engine string) []DBMajorEngineVersion
	DescribeEngineDefaultParameters(dbParameterGroupFamily string) []DBParameter
	DescribeEngineDefaultClusterParameters(dbParameterGroupFamily string) []DBParameter

	// Snapshot attribute operations
	DescribeDBSnapshotAttributes(snapshotID string) (*DBSnapshotAttributesResult, error)
	ModifyDBSnapshot(snapshotID, optionGroupName, engineVersion string) (*DBSnapshot, error)
	ModifyDBSnapshotAttribute(
		snapshotID, attributeName string,
		valuesToAdd, valuesToRemove []string,
	) (*DBSnapshotAttributesResult, error)
	DescribeDBClusterSnapshotAttributes(
		snapshotID string,
	) (*DBClusterSnapshotAttributesResult, error)
	ModifyDBClusterSnapshotAttribute(
		snapshotID, attributeName string,
		valuesToAdd, valuesToRemove []string,
	) (*DBClusterSnapshotAttributesResult, error)
	DescribeDBClusterBacktracks(clusterID string) ([]DBClusterBacktrack, error)

	// HTTP endpoint operations
	EnableHTTPEndpoint(resourceARN string) error
	DisableHTTPEndpoint(resourceARN string) error
	ModifyCurrentDBClusterCapacity(clusterID string, capacity int) (*DBCluster, error)

	// S3 restore operations
	RestoreDBInstanceFromS3(
		id, engine, dbInstanceClass, s3Bucket, s3IngestionRoleArn, sourceEngine, sourceEngineVersion string,
	) (*DBInstance, error)
	RestoreDBClusterFromS3(
		id, engine, masterUsername, s3Bucket, s3IngestionRoleArn, sourceEngine, sourceEngineVersion string,
	) (*DBCluster, error)

	// Recommendation operations
	ModifyDBRecommendation(recID, status string) (*DBRecommendation, error)
	DescribeDBRecommendations(recID, status string) []DBRecommendation

	// Reserved instance operations
	PurchaseReservedDBInstancesOffering(
		offeringID, reservedDBInstanceID string,
		dbInstanceCount int,
	) (*ReservedDBInstance, error)
	DescribeReservedDBInstances(reservedDBInstanceID, dbInstanceClass string) []ReservedDBInstance
	DescribeReservedDBInstancesOfferings(
		offeringID, dbInstanceClass string,
	) []ReservedDBInstancesOffering

	// DB Proxy operations
	CreateDBProxy(name, engineFamily, roleARN string, auth []UserAuthConfig) (*DBProxy, error)
	DeleteDBProxy(name string) (*DBProxy, error)
	DescribeDBProxies(name string) ([]DBProxy, error)
	ModifyDBProxy(
		name string,
		requireTLS *bool,
		idleClientTimeout *int,
		auth []UserAuthConfig,
	) (*DBProxy, error)

	// DB Proxy target operations
	RegisterDBProxyTargets(
		proxyName, targetGroupName string,
		dbInstanceIDs, dbClusterIDs []string,
	) ([]DBProxyTarget, error)
	DeregisterDBProxyTargets(
		proxyName, targetGroupName string,
		dbInstanceIDs, dbClusterIDs []string,
	) error
	DescribeDBProxyTargets(proxyName, targetGroupName string) ([]DBProxyTarget, error)
	DescribeDBProxyTargetGroups(proxyName, targetGroupName string) ([]DBProxyTargetGroup, error)
	ModifyDBProxyTargetGroup(
		proxyName, targetGroupName string,
		cfg ConnectionPoolConfig,
	) (*DBProxyTargetGroup, error)

	// DB Proxy endpoint operations
	CreateDBProxyEndpoint(
		proxyName, endpointName, targetRole string,
		vpcSubnetIDs, vpcSGIDs []string,
	) (*DBProxyEndpoint, error)
	DeleteDBProxyEndpoint(endpointName string) (*DBProxyEndpoint, error)
	DescribeDBProxyEndpoints(proxyName, endpointName string) ([]DBProxyEndpoint, error)
	ModifyDBProxyEndpoint(endpointName string, vpcSGIDs []string) (*DBProxyEndpoint, error)

	// Activity stream operations
	StartActivityStream(clusterID, kmsKeyID, mode string) (*DBCluster, error)
	StopActivityStream(clusterID string) (*DBCluster, error)
	ModifyActivityStream(clusterID string, auditPolicy string) (*DBCluster, error)

	// DB Shard Group operations
	CreateDBShardGroup(
		id, clusterID string,
		maxACU, minACU float64,
		computeRedundancy int,
		publiclyAccessible bool,
	) (*DBShardGroup, error)
	DeleteDBShardGroup(id string) (*DBShardGroup, error)
	DescribeDBShardGroups(id string) ([]DBShardGroup, error)
	ModifyDBShardGroup(id string, maxACU float64, computeRedundancy int) (*DBShardGroup, error)
	RebootDBShardGroup(id string) (*DBShardGroup, error)

	// Integration operations
	CreateIntegration(name, sourceARN, targetARN, kmsKeyID, dataFilter, description string) (*Integration, error)
	DeleteIntegration(identifier string) (*Integration, error)
	DescribeIntegrations(identifier string) ([]Integration, error)
	ModifyIntegration(identifier, dataFilter, description string) (*Integration, error)

	// Tenant Database operations
	CreateTenantDatabase(instanceID, tenantDBName, masterUsername string) (*TenantDatabase, error)
	DeleteTenantDatabase(instanceID, tenantDBName string) (*TenantDatabase, error)
	DescribeTenantDatabases(instanceID, tenantDBName string) ([]TenantDatabase, error)
	ModifyTenantDatabase(instanceID, tenantDBName string) (*TenantDatabase, error)

	// DB Cluster Automated Backup operations
	DeleteDBClusterAutomatedBackup(resourceID string) (*DBClusterAutomatedBackup, error)
	DescribeDBClusterAutomatedBackups(clusterID string) []DBClusterAutomatedBackup

	// DB Instance Automated Backup enhanced operations
	DeleteDBInstanceAutomatedBackup(resourceID string) (*DBInstanceAutomatedBackup, error)
	StartDBInstanceAutomatedBackupsReplication(
		sourceInstanceARN string,
		backupRetentionPeriod int,
	) (*DBInstanceAutomatedBackup, error)
	StopDBInstanceAutomatedBackupsReplication(
		sourceInstanceARN string,
	) (*DBInstanceAutomatedBackup, error)

	// DB Snapshot Tenant Database operations
	DescribeDBSnapshotTenantDatabases(snapshotID, instanceID string) []DBSnapshotTenantDatabase

	// Performance Insights operations
	GetPerformanceInsightsData(
		resourceID, metric string,
		startTime, endTime time.Time,
		periodInSeconds int,
	) ([]PIDataPoint, error)
}

// Ensure InMemoryBackend satisfies the StorageBackend interface at compile time.
var _ StorageBackend = (*InMemoryBackend)(nil)
