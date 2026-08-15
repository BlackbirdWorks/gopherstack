package neptune

import "context"

// StorageBackend defines the interface for Neptune backend implementations.
// All mutating methods must be safe for concurrent use.
//
// Regional operations take a context.Context from which the target AWS region is
// resolved (see getRegion); same-named resources are isolated per region. Global
// cluster operations are partition-scoped and ignore the region.
type StorageBackend interface {
	// Cluster operations
	CreateDBCluster(
		ctx context.Context,
		id, paramGroupName string,
		port int,
		opts DBClusterCreateOptions,
	) (*DBCluster, error)
	DescribeDBClusters(
		ctx context.Context,
		id string,
		filters DBClusterFilters,
	) ([]DBCluster, error)
	DeleteDBCluster(ctx context.Context, id string, opts DBClusterDeleteOptions) (*DBCluster, error)
	ModifyDBCluster(
		ctx context.Context,
		id, paramGroupName string,
		opts DBClusterModifyOptions,
	) (*DBCluster, error)
	StopDBCluster(ctx context.Context, id string) (*DBCluster, error)
	StartDBCluster(ctx context.Context, id string) (*DBCluster, error)
	FailoverDBCluster(ctx context.Context, id, targetInstanceID string) (*DBCluster, error)

	// Instance operations
	CreateDBInstance(
		ctx context.Context,
		id, clusterID, instanceClass string,
		opts DBInstanceCreateOptions,
	) (*DBInstance, error)
	DescribeDBInstances(ctx context.Context, id, clusterFilter string) ([]DBInstance, error)
	DeleteDBInstance(ctx context.Context, id string) (*DBInstance, error)
	ModifyDBInstance(
		ctx context.Context,
		id, instanceClass string,
		opts DBInstanceModifyOptions,
	) (*DBInstance, error)
	RebootDBInstance(ctx context.Context, id string) (*DBInstance, error)

	// Subnet group operations
	CreateDBSubnetGroup(
		ctx context.Context,
		name, description, vpcID string,
		subnetIDs []string,
	) (*DBSubnetGroup, error)
	DescribeDBSubnetGroups(ctx context.Context, name string) ([]DBSubnetGroup, error)
	DeleteDBSubnetGroup(ctx context.Context, name string) error

	// Cluster parameter group operations
	CreateDBClusterParameterGroup(
		ctx context.Context,
		name, family, description string,
	) (*DBClusterParameterGroup, error)
	DescribeDBClusterParameterGroups(
		ctx context.Context,
		name string,
	) ([]DBClusterParameterGroup, error)
	DeleteDBClusterParameterGroup(ctx context.Context, name string) error
	ModifyDBClusterParameterGroup(
		ctx context.Context,
		name string,
		params []ParameterInput,
	) (*DBClusterParameterGroup, error)

	// Cluster snapshot operations
	CreateDBClusterSnapshot(
		ctx context.Context,
		snapshotID, clusterID string,
	) (*DBClusterSnapshot, error)
	DescribeDBClusterSnapshots(
		ctx context.Context,
		snapshotID, clusterID, snapshotTypeFilter string,
	) ([]DBClusterSnapshot, error)
	DeleteDBClusterSnapshot(ctx context.Context, snapshotID string) (*DBClusterSnapshot, error)
	ModifyDBClusterSnapshotAttribute(
		ctx context.Context,
		snapshotID, attributeName string,
		valuesToAdd, valuesToRemove []string,
	) (*DBClusterSnapshot, error)

	// Tag operations
	AddTagsToResource(ctx context.Context, arn string, tags []Tag) error
	RemoveTagsFromResource(ctx context.Context, arn string, keys []string) error
	ListTagsForResource(ctx context.Context, arn string) ([]Tag, error)

	// New operations (Issue #902)
	AddRoleToDBCluster(ctx context.Context, clusterID, roleARN string) error
	AddSourceIdentifierToSubscription(
		ctx context.Context,
		name, sourceID string,
	) (*EventSubscription, error)
	ApplyPendingMaintenanceAction(
		ctx context.Context,
		resourceID, applyAction, optInType string,
	) (*ResourcePendingMaintenanceActions, error)
	DescribePendingMaintenanceActions(ctx context.Context, resourceFilter string) []ResourcePendingMaintenanceActions
	DescribeEvents(ctx context.Context, filter EventsFilter) []Event
	CopyDBClusterParameterGroup(
		ctx context.Context,
		sourceName, targetName, targetDescription string,
	) (*DBClusterParameterGroup, error)
	CopyDBClusterSnapshot(
		ctx context.Context,
		sourceSnapshotID, targetSnapshotID string,
	) (*DBClusterSnapshot, error)
	CopyDBParameterGroup(
		ctx context.Context,
		sourceName, targetName, targetDescription string,
	) (*DBParameterGroup, error)
	CreateDBClusterEndpoint(
		ctx context.Context,
		endpointID, clusterID, endpointType string,
	) (*DBClusterEndpoint, error)
	CreateDBParameterGroup(
		ctx context.Context,
		name, family, description string,
	) (*DBParameterGroup, error)
	CreateEventSubscription(
		ctx context.Context,
		name, snsTopicARN, sourceType string,
		sourceIDs []string,
		enabled bool,
	) (*EventSubscription, error)
	CreateGlobalCluster(
		ctx context.Context,
		globalClusterID, sourceDBClusterID, databaseName string,
	) (*GlobalCluster, error)
	DescribeGlobalClusters(ctx context.Context) []GlobalCluster

	// Cluster endpoint operations
	DeleteDBClusterEndpoint(ctx context.Context, endpointID string) (*DBClusterEndpoint, error)
	DescribeDBClusterEndpoints(
		ctx context.Context,
		endpointID, clusterID string,
	) ([]DBClusterEndpoint, error)
	ModifyDBClusterEndpoint(
		ctx context.Context,
		endpointID, endpointType string,
		staticMembers, excludedMembers []string,
	) (*DBClusterEndpoint, error)

	// DB parameter group operations
	DeleteDBParameterGroup(ctx context.Context, name string) error
	DescribeDBParameterGroups(ctx context.Context, name string) ([]DBParameterGroup, error)
	ModifyDBParameterGroup(
		ctx context.Context, name string, params []ParameterInput,
	) (*DBParameterGroup, error)
	ResetDBParameterGroup(
		ctx context.Context, name string, resetAll bool, params []ParameterInput,
	) (*DBParameterGroup, error)
	DescribeDBParameters(ctx context.Context, name string) ([]EngineParameter, error)

	// Cluster parameter group extended operations
	ResetDBClusterParameterGroup(
		ctx context.Context, name string, resetAll bool, params []ParameterInput,
	) (*DBClusterParameterGroup, error)
	DescribeDBClusterParameters(ctx context.Context, name string) ([]EngineParameter, error)

	// Event subscription extended operations
	DeleteEventSubscription(ctx context.Context, name string) (*EventSubscription, error)
	DescribeEventSubscriptions(ctx context.Context, name string) ([]EventSubscription, error)
	ModifyEventSubscription(
		ctx context.Context,
		name, snsTopicARN, sourceType, enabled string,
		eventCategories []string,
	) (*EventSubscription, error)
	RemoveSourceIdentifierFromSubscription(
		ctx context.Context,
		name, sourceID string,
	) (*EventSubscription, error)

	// Global cluster extended operations
	DeleteGlobalCluster(ctx context.Context, globalClusterID string) (*GlobalCluster, error)
	FailoverGlobalCluster(
		ctx context.Context,
		globalClusterID, targetDBClusterID string,
	) (*GlobalCluster, error)
	ModifyGlobalCluster(
		ctx context.Context, globalClusterID string, opts GlobalClusterModifyOptions,
	) (*GlobalCluster, error)
	RemoveFromGlobalCluster(
		ctx context.Context,
		globalClusterID, dbClusterID string,
	) (*GlobalCluster, error)
	SwitchoverGlobalCluster(
		ctx context.Context,
		globalClusterID, targetDBClusterID string,
	) (*GlobalCluster, error)

	// Role operations
	RemoveRoleFromDBCluster(ctx context.Context, clusterID, roleARN string) error

	// Restore operations
	RestoreDBClusterFromSnapshot(
		ctx context.Context,
		snapshotID, clusterID string,
	) (*DBCluster, error)
	RestoreDBClusterToPointInTime(
		ctx context.Context,
		srcClusterID, targetClusterID string,
	) (*DBCluster, error)

	// Subnet group extended operations
	ModifyDBSubnetGroup(ctx context.Context, name, description string, subnetIDs []string) (*DBSubnetGroup, error)

	// Lifecycle
	Reset()
	Region() string
	AccountID() string
	Snapshot(ctx context.Context) []byte
	Restore(ctx context.Context, data []byte) error
}

// compile-time assertion that InMemoryBackend satisfies StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)
