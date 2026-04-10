package neptune

// StorageBackend defines the interface for Neptune backend implementations.
// All mutating methods must be safe for concurrent use.
type StorageBackend interface {
	// Cluster operations
	CreateDBCluster(id, paramGroupName string, port int) (*DBCluster, error)
	DescribeDBClusters(id string) ([]DBCluster, error)
	DeleteDBCluster(id string) (*DBCluster, error)
	ModifyDBCluster(id, paramGroupName string) (*DBCluster, error)
	StopDBCluster(id string) (*DBCluster, error)
	StartDBCluster(id string) (*DBCluster, error)
	FailoverDBCluster(id string) (*DBCluster, error)

	// Instance operations
	CreateDBInstance(id, clusterID, instanceClass string) (*DBInstance, error)
	DescribeDBInstances(id string) ([]DBInstance, error)
	DeleteDBInstance(id string) (*DBInstance, error)
	ModifyDBInstance(id, instanceClass string) (*DBInstance, error)
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
	DescribeDBClusterSnapshots(snapshotID string) ([]DBClusterSnapshot, error)
	DeleteDBClusterSnapshot(snapshotID string) (*DBClusterSnapshot, error)

	// Tag operations
	AddTagsToResource(arn string, tags []Tag)
	RemoveTagsFromResource(arn string, keys []string)
	ListTagsForResource(arn string) []Tag

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

	// Lifecycle
	Reset()
	Region() string
	AccountID() string
	Snapshot() []byte
	Restore(data []byte) error
}

// compile-time assertion that InMemoryBackend satisfies StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)
