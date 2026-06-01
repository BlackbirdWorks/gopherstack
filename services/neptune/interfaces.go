package neptune

// StorageBackend defines the interface for Neptune backend implementations.
// All mutating methods must be safe for concurrent use.
type StorageBackend interface {
	// Cluster operations
	CreateDBCluster(id, paramGroupName string, port int, opts DBClusterCreateOptions) (*DBCluster, error)
	DescribeDBClusters(id string) ([]DBCluster, error)
	DeleteDBCluster(id string) (*DBCluster, error)
	ModifyDBCluster(id, paramGroupName string, opts DBClusterModifyOptions) (*DBCluster, error)
	StopDBCluster(id string) (*DBCluster, error)
	StartDBCluster(id string) (*DBCluster, error)
	FailoverDBCluster(id string) (*DBCluster, error)

	// Instance operations
	CreateDBInstance(id, clusterID, instanceClass string, opts DBInstanceCreateOptions) (*DBInstance, error)
	DescribeDBInstances(id string) ([]DBInstance, error)
	DeleteDBInstance(id string) (*DBInstance, error)
	ModifyDBInstance(id, instanceClass string, opts DBInstanceModifyOptions) (*DBInstance, error)
	RebootDBInstance(id string) (*DBInstance, error)

	// Subnet group operations
	CreateDBSubnetGroup(name, description, vpcID string, subnetIDs []string) (*DBSubnetGroup, error)
	DescribeDBSubnetGroups(name string) ([]DBSubnetGroup, error)
	DeleteDBSubnetGroup(name string) error

	// Cluster parameter group operations
	CreateDBClusterParameterGroup(name, family, description string) (*DBClusterParameterGroup, error)
	DescribeDBClusterParameterGroups(name string) ([]DBClusterParameterGroup, error)
	DeleteDBClusterParameterGroup(name string) error
	ModifyDBClusterParameterGroup(name string) (*DBClusterParameterGroup, error)

	// Cluster snapshot operations
	CreateDBClusterSnapshot(snapshotID, clusterID string) (*DBClusterSnapshot, error)
	DescribeDBClusterSnapshots(snapshotID, clusterID string) ([]DBClusterSnapshot, error)
	DeleteDBClusterSnapshot(snapshotID string) (*DBClusterSnapshot, error)

	// Tag operations
	AddTagsToResource(arn string, tags []Tag) error
	RemoveTagsFromResource(arn string, keys []string) error
	ListTagsForResource(arn string) ([]Tag, error)

	// New operations (Issue #902)
	AddRoleToDBCluster(clusterID, roleARN string) error
	AddSourceIdentifierToSubscription(name, sourceID string) (*EventSubscription, error)
	ApplyPendingMaintenanceAction(resourceID, applyAction, optInType string) error
	CopyDBClusterParameterGroup(sourceName, targetName, targetDescription string) (*DBClusterParameterGroup, error)
	CopyDBClusterSnapshot(sourceSnapshotID, targetSnapshotID string) (*DBClusterSnapshot, error)
	CopyDBParameterGroup(sourceName, targetName, targetDescription string) (*DBParameterGroup, error)
	CreateDBClusterEndpoint(endpointID, clusterID, endpointType string) (*DBClusterEndpoint, error)
	CreateDBParameterGroup(name, family, description string) (*DBParameterGroup, error)
	CreateEventSubscription(name, snsTopicARN string, sourceIDs []string) (*EventSubscription, error)
	CreateGlobalCluster(globalClusterID, sourceDBClusterID string) (*GlobalCluster, error)
	DescribeGlobalClusters() []GlobalCluster

	// Cluster endpoint operations
	DeleteDBClusterEndpoint(endpointID string) error
	DescribeDBClusterEndpoints(endpointID, clusterID string) ([]DBClusterEndpoint, error)
	ModifyDBClusterEndpoint(endpointID, endpointType string) (*DBClusterEndpoint, error)

	// DB parameter group operations
	DeleteDBParameterGroup(name string) error
	DescribeDBParameterGroups(name string) ([]DBParameterGroup, error)
	ModifyDBParameterGroup(name string) (*DBParameterGroup, error)
	ResetDBParameterGroup(name string) (*DBParameterGroup, error)

	// Cluster parameter group extended operations
	ResetDBClusterParameterGroup(name string) (*DBClusterParameterGroup, error)

	// Event subscription extended operations
	DeleteEventSubscription(name string) (*EventSubscription, error)
	DescribeEventSubscriptions(name string) ([]EventSubscription, error)
	ModifyEventSubscription(name, snsTopicARN string) (*EventSubscription, error)
	RemoveSourceIdentifierFromSubscription(name, sourceID string) (*EventSubscription, error)

	// Global cluster extended operations
	DeleteGlobalCluster(globalClusterID string) (*GlobalCluster, error)
	FailoverGlobalCluster(globalClusterID, targetDBClusterID string) (*GlobalCluster, error)
	ModifyGlobalCluster(globalClusterID string) (*GlobalCluster, error)
	RemoveFromGlobalCluster(globalClusterID, dbClusterID string) (*GlobalCluster, error)
	SwitchoverGlobalCluster(globalClusterID, targetDBClusterID string) (*GlobalCluster, error)

	// Role operations
	RemoveRoleFromDBCluster(clusterID, roleARN string) error

	// Restore operations
	RestoreDBClusterFromSnapshot(snapshotID, clusterID string) (*DBCluster, error)
	RestoreDBClusterToPointInTime(srcClusterID, targetClusterID string) (*DBCluster, error)

	// Subnet group extended operations
	ModifyDBSubnetGroup(name, description string) (*DBSubnetGroup, error)

	// Lifecycle
	Reset()
	Region() string
	AccountID() string
	Snapshot() []byte
	Restore(data []byte) error
}

// compile-time assertion that InMemoryBackend satisfies StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)
