package dax

// StorageBackend is the interface for DAX storage operations.
type StorageBackend interface {
	// Cluster operations.
	CreateCluster(input CreateClusterInput) (*Cluster, error)
	DescribeClusters(
		clusterNames []string,
		maxResults int,
		nextToken string,
	) ([]*Cluster, string, error)
	UpdateCluster(input UpdateClusterInput) (*Cluster, error)
	DeleteCluster(clusterName string) (*Cluster, error)

	// Tag operations.
	TagResource(resourceArn string, tags map[string]string) error
	UntagResource(resourceArn string, tagKeys []string) error
	ListTags(resourceArn string, nextToken string) (map[string]string, string, error)

	// Parameter group operations.
	CreateParameterGroup(name, description string) (*ParameterGroup, error)
	DescribeParameterGroups(
		names []string,
		maxResults int,
		nextToken string,
	) ([]*ParameterGroup, string, error)
	DeleteParameterGroup(name string) error

	// Subnet group operations.
	CreateSubnetGroup(name, description string, subnetIDs []string) (*SubnetGroup, error)
	DescribeSubnetGroups(
		names []string,
		maxResults int,
		nextToken string,
	) ([]*SubnetGroup, string, error)
	DeleteSubnetGroup(name string) error

	// Reset / persistence.
	Reset()
	Snapshot() []byte
	Restore(data []byte) error
}

var _ StorageBackend = (*InMemoryBackend)(nil)
