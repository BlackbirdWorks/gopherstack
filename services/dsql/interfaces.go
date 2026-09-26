package dsql

// StorageBackend is the interface for the Aurora DSQL backend.
type StorageBackend interface {
	CreateCluster(accountID, region string, in CreateClusterInput) (*Cluster, error)
	GetCluster(identifier string) (*Cluster, error)
	ListClusters(nextToken string, maxResults int) ([]*Cluster, string, error)
	UpdateCluster(identifier string, in UpdateClusterInput) (*Cluster, error)
	DeleteCluster(identifier string) (*Cluster, error)

	GetClusterPolicy(identifier string) (*ClusterPolicy, error)
	PutClusterPolicy(identifier, policy, expectedVersion string) (*ClusterPolicy, error)
	DeleteClusterPolicy(identifier, expectedVersion string) (*ClusterPolicy, error)

	GetVpcEndpointServiceName(identifier, region string) (serviceName, clusterVpcEndpoint string, err error)

	CreateStream(clusterIdentifier string, in CreateStreamInput) (*Stream, error)
	GetStream(clusterIdentifier, streamIdentifier string) (*Stream, error)
	DeleteStream(clusterIdentifier, streamIdentifier string) (*Stream, error)
	ListStreams(clusterIdentifier, nextToken string, maxResults int) ([]*Stream, string, error)

	TagResource(resourceARN string, tags map[string]string) error
	UntagResource(resourceARN string, tagKeys []string) error
	ListTagsForResource(resourceARN string) (map[string]string, error)

	Reset()
}

// CreateClusterInput carries CreateCluster's optional fields.
type CreateClusterInput struct {
	MultiRegion               *MultiRegionProperties
	Tags                      map[string]string
	Policy                    string
	KmsEncryptionKey          string
	DeletionProtectionEnabled bool
}

// UpdateClusterInput carries UpdateCluster's optional fields. A nil pointer
// means "leave unchanged"; KmsEncryptionKey uses the empty string to mean
// "unchanged" and the reserved value "AWS_OWNED_KMS_KEY" to mean "revert to
// the AWS owned key", matching the real API's documented semantics.
type UpdateClusterInput struct {
	MultiRegion               *MultiRegionProperties
	DeletionProtectionEnabled *bool
	KmsEncryptionKey          string
}

// CreateStreamInput carries CreateStream's fields.
type CreateStreamInput struct {
	Target   *StreamTarget
	Tags     map[string]string
	Format   string
	Ordering string
}

// Compile-time assertion that InMemoryBackend implements StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)
