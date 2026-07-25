package memorydb

import (
	"context"
)

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
	DescribeSnapshots(ctx context.Context, name, clusterName, source string) ([]*Snapshot, error)
	CopySnapshot(ctx context.Context, req *copySnapshotRequest) (*Snapshot, error)
	DeleteSnapshot(ctx context.Context, name string) (*Snapshot, error)
	ExportSnapshot(ctx context.Context, req *exportSnapshotRequest) (*Snapshot, error)
	DescribeEngineVersions(ctx context.Context, req *describeEngineVersionsRequest) ([]*EngineVersion, error)
	DescribeEvents(ctx context.Context, req *describeEventsRequest) ([]*Event, error)
	CreateMultiRegionCluster(ctx context.Context, req *createMultiRegionClusterRequest) (*MultiRegionCluster, error)
	DeleteMultiRegionCluster(ctx context.Context, name string) (*MultiRegionCluster, error)
	DescribeMultiRegionClusters(ctx context.Context, name string) ([]*MultiRegionCluster, error)
	UpdateMultiRegionCluster(ctx context.Context, req *updateMultiRegionClusterRequest) (*MultiRegionCluster, error)
	RegionalClustersFor(multiRegionClusterName string) []*Cluster
	DescribeMultiRegionParameterGroups(ctx context.Context, name string) ([]*MultiRegionParameterGroup, error)
	DescribeParameters(ctx context.Context, parameterGroupName string) (map[string]string, error)
	ResetParameterGroup(
		ctx context.Context,
		name string,
		parameterNames []string,
		allParameters bool,
	) (*ParameterGroup, error)
	FailoverShard(ctx context.Context, clusterName, shardConfiguration string) (*Cluster, error)
	ListAllowedNodeTypeUpdates(ctx context.Context, clusterName string) ([]string, error)
	ListAllowedMultiRegionClusterUpdates(ctx context.Context, clusterName string) ([]string, error)
	BatchUpdateCluster(ctx context.Context, clusterNames []string) map[string]*Cluster
	DescribeReservedNodes(ctx context.Context, req *describeReservedNodesRequest) ([]*ReservedNode, error)
	DescribeReservedNodesOfferings(
		ctx context.Context,
		req *describeReservedNodesOfferingsRequest,
	) ([]*ReservedNodesOffering, error)
	PurchaseReservedNodesOffering(ctx context.Context, req *purchaseReservedNodesOfferingRequest) (*ReservedNode, error)
	DescribeMultiRegionParameters(ctx context.Context, parameterGroupName string) (map[string]string, error)
	DescribeServiceUpdates(ctx context.Context, req *describeServiceUpdatesRequest) ([]*ServiceUpdate, error)
	Region() string
	Reset()
	Snapshot(ctx context.Context) []byte
	Restore(ctx context.Context, data []byte) error
}
