package redshift

// StorageBackend defines the interface for Redshift backend implementations.
// All mutating methods must be safe for concurrent use.
type StorageBackend interface {
	// Cluster operations
	CreateCluster(id, nodeType, dbName, masterUser string) (*Cluster, error)
	DeleteCluster(id string) (*Cluster, error)
	DescribeClusters(id string) ([]Cluster, error)

	// Tag operations
	DescribeTags() map[string]map[string]string
	CreateTags(clusterID string, kv map[string]string) error
	DeleteTags(clusterID string, keys []string) error

	// New operations (issue #910)
	AcceptReservedNodeExchange(reservedNodeID, targetOfferingID string) (*ReservedNode, error)
	AddPartner(accountID, clusterID, databaseName, partnerName string) (*Partner, error)
	AssociateDataShareConsumer(
		dataShareArn, consumerArn, consumerRegion string,
		associateEntireAccount bool,
	) (*DataShare, error)
	AuthorizeClusterSecurityGroupIngress(
		groupName, cidrIP, ec2GroupName, ec2GroupOwnerID string,
	) (*ClusterSecurityGroup, error)
	AuthorizeDataShare(dataShareArn, consumerIdentifier string) (*DataShare, error)
	AuthorizeEndpointAccess(clusterID, grantee string, vpcIDs []string) (*EndpointAuthorization, error)
	AuthorizeSnapshotAccess(snapshotID, accountWithRestoreAccess string) (*Snapshot, error)
	BatchDeleteClusterSnapshots(identifiers []string) ([]SnapshotBatchError, []string)
	BatchModifyClusterSnapshots(identifiers []string, retentionPeriod int, force bool) ([]SnapshotBatchError, []string)
	CancelResize(clusterID string) (*ResizeProgress, error)

	// Seed helpers for tests
	AddReservedNodeInternal(node *ReservedNode)
	AddDataShareInternal(ds *DataShare)
	AddSecurityGroupInternal(sg *ClusterSecurityGroup)
	AddSnapshotInternal(snap *Snapshot)
	AddActiveResizeInternal(clusterID string, resize *ResizeProgress)

	// Lifecycle
	Reset()
	Region() string
	AccountID() string
	Snapshot() []byte
	Restore(data []byte) error
	SetDNSRegistrar(dns DNSRegistrar)
}

// compile-time assertion that InMemoryBackend satisfies StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)
