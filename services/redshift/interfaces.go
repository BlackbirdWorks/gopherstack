package redshift

// StorageBackend defines the interface for Redshift backend implementations.
// All mutating methods must be safe for concurrent use.
type StorageBackend interface {
	// Cluster operations
	CreateCluster(id, nodeType, dbName, masterUser string) (*Cluster, error)
	DeleteCluster(id string) (*Cluster, error)
	DescribeClusters(id string) ([]Cluster, error)
	ModifyCluster(
		id, nodeType string,
		numberOfNodes int,
		masterUserPassword string,
		encrypted, enhancedVpcRouting bool,
	) (*Cluster, error)
	RebootCluster(id string) (*Cluster, error)
	PauseCluster(id string) (*Cluster, error)
	ResumeCluster(id string) (*Cluster, error)
	ResizeCluster(id, nodeType, clusterType string, numberOfNodes int, classic bool) (*Cluster, error)
	RotateEncryptionKey(id string) (*Cluster, error)
	ModifyClusterIamRoles(id string, addRoles, removeRoles []string) (*Cluster, error)
	ModifyClusterMaintenance(id, maintenanceTrack string, deferMaintenance bool) (*Cluster, error)

	// Tag operations
	DescribeTags() map[string]map[string]string
	CreateTags(clusterID string, kv map[string]string) error
	DeleteTags(clusterID string, keys []string) error

	// Parameter group operations
	CreateClusterParameterGroup(name, family, description string) (*ClusterParameterGroup, error)
	DeleteClusterParameterGroup(name string) error
	DescribeClusterParameterGroups(name string) ([]ClusterParameterGroup, error)
	DescribeClusterParameters(groupName string) ([]ClusterParameter, error)
	ModifyClusterParameterGroup(groupName string, params []ClusterParameter) (*ClusterParameterGroup, error)
	ResetClusterParameterGroup(
		groupName string,
		resetAllParameters bool,
		params []ClusterParameter,
	) (*ClusterParameterGroup, error)
	DescribeDefaultClusterParameters(family string) ([]ClusterParameter, error)

	// Reserved node operations
	AcceptReservedNodeExchange(reservedNodeID, targetOfferingID string) (*ReservedNode, error)
	DescribeReservedNodes(reservedNodeID string) ([]ReservedNode, error)
	DescribeReservedNodeOfferings(offeringID string) ([]ReservedNodeOffering, error)
	PurchaseReservedNodeOffering(offeringID, reservedNodeID string, nodeCount int) (*ReservedNode, error)
	DescribeReservedNodeExchangeStatus(reservedNodeID string) (string, error)
	GetReservedNodeExchangeOfferings(reservedNodeID string) ([]ReservedNodeOffering, error)

	// Security group operations
	AuthorizeClusterSecurityGroupIngress(
		groupName, cidrIP, ec2GroupName, ec2GroupOwnerID string,
	) (*ClusterSecurityGroup, error)
	CreateClusterSecurityGroup(name, description string) (*ClusterSecurityGroup, error)
	DeleteClusterSecurityGroup(name string) error
	DescribeClusterSecurityGroups(name string) ([]ClusterSecurityGroup, error)
	RevokeClusterSecurityGroupIngress(
		groupName, cidrIP, ec2GroupName, ec2GroupOwnerID string,
	) (*ClusterSecurityGroup, error)

	// Snapshot operations
	CreateClusterSnapshot(snapshotID, clusterID string) (*Snapshot, error)
	DeleteClusterSnapshot(snapshotID string) (*Snapshot, error)
	DescribeClusterSnapshots(snapshotID, clusterID string) ([]Snapshot, error)
	CopyClusterSnapshot(sourceSnapshotID, destinationSnapshotID string) (*Snapshot, error)
	RestoreFromClusterSnapshot(clusterID, snapshotID string) (*Cluster, error)
	AuthorizeSnapshotAccess(snapshotID, accountWithRestoreAccess string) (*Snapshot, error)
	BatchDeleteClusterSnapshots(identifiers []string) ([]SnapshotBatchError, []string)
	BatchModifyClusterSnapshots(identifiers []string, retentionPeriod int, force bool) ([]SnapshotBatchError, []string)

	// Subnet group operations
	CreateClusterSubnetGroup(name, description, vpcID string, subnetIDs []string) (*ClusterSubnetGroup, error)
	DeleteClusterSubnetGroup(name string) error
	DescribeClusterSubnetGroups(name string) ([]ClusterSubnetGroup, error)
	ModifyClusterSubnetGroup(name, description string, subnetIDs []string) (*ClusterSubnetGroup, error)

	// Endpoint operations
	AuthorizeEndpointAccess(clusterID, grantee string, vpcIDs []string) (*EndpointAuthorization, error)

	// Data share operations
	AddPartner(accountID, clusterID, databaseName, partnerName string) (*Partner, error)
	AssociateDataShareConsumer(
		dataShareArn, consumerArn, consumerRegion string,
		associateEntireAccount bool,
	) (*DataShare, error)
	AuthorizeDataShare(dataShareArn, consumerIdentifier string) (*DataShare, error)

	// Resize operations
	CancelResize(clusterID string) (*ResizeProgress, error)

	// Logging operations
	EnableLogging(clusterID, bucketName, s3KeyPrefix string) (*LoggingStatus, error)
	DisableLogging(clusterID string) (*LoggingStatus, error)

	// Event operations
	DescribeEvents(sourceIdentifier, sourceType string) ([]Event, error)
	CreateEventSubscription(
		subscriptionName, snsTopicArn, sourceType, severity string,
		sourceIDs, eventCategories []string,
		enabled bool,
	) (*EventSubscription, error)
	DeleteEventSubscription(subscriptionName string) error
	DescribeEventSubscriptions(subscriptionName string) ([]EventSubscription, error)
	ModifyEventSubscription(
		subscriptionName, snsTopicArn, sourceType, severity string,
		sourceIDs, eventCategories []string,
		enabled *bool,
	) (*EventSubscription, error)

	// Seed helpers for tests
	AddReservedNodeInternal(node *ReservedNode)
	AddDataShareInternal(ds *DataShare)
	AddSecurityGroupInternal(sg *ClusterSecurityGroup)
	AddSnapshotInternal(snap *Snapshot)
	AddActiveResizeInternal(clusterID string, resize *ResizeProgress)
	AddParameterGroupInternal(pg *ClusterParameterGroup)
	AddSubnetGroupInternal(sg *ClusterSubnetGroup)

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
