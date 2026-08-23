package memorydb

import (
	"time"
)

// Cluster represents an in-memory MemoryDB cluster.
//
// PendingStatus/AvailableAt implement the goroutine-free "creating" ->
// "available" lifecycle overlay (see lifecycle.go): PendingStatus holds the
// transient status observed until the backend clock passes AvailableAt, at
// which point Status (the terminal value) takes over. Both are zero-valued
// (no-op) unless SetLifecycleDelay has been configured, preserving the
// pre-existing instant-available behavior by default.
type Cluster struct {
	CreatedAt   time.Time         `json:"createdAt"`
	AvailableAt time.Time         `json:"availableAt"`
	Tags        map[string]string `json:"tags"`
	// AppliedServiceUpdates tracks which service updates BatchUpdateCluster has
	// applied to this cluster, keyed by ServiceUpdateName. Drives the per-cluster
	// "complete" vs "available" status DescribeServiceUpdates reports.
	AppliedServiceUpdates   map[string]bool `json:"appliedServiceUpdates,omitempty"`
	KmsKeyID                string          `json:"kmsKeyID"`
	SnsTopicArn             string          `json:"snsTopicArn"`
	SnsTopicStatus          string          `json:"snsTopicStatus"`
	Description             string          `json:"description"`
	NodeType                string          `json:"nodeType"`
	EngineVersion           string          `json:"engineVersion"`
	ACLName                 string          `json:"aclName"`
	SubnetGroupName         string          `json:"subnetGroupName"`
	ParameterGroupName      string          `json:"parameterGroupName"`
	ParameterGroupStatus    string          `json:"parameterGroupStatus"`
	MultiRegionClusterName  string          `json:"multiRegionClusterName"`
	Status                  string          `json:"status"`
	PendingStatus           string          `json:"pendingStatus"`
	MaintenanceWindow       string          `json:"maintenanceWindow"`
	Name                    string          `json:"name"`
	ARN                     string          `json:"arn"`
	Region                  string          `json:"region"`
	SnapshotWindow          string          `json:"snapshotWindow"`
	Endpoint                string          `json:"endpoint"`
	AvailabilityMode        string          `json:"availabilityMode"`
	Engine                  string          `json:"engine"`
	DataTiering             string          `json:"dataTiering"`
	NetworkType             string          `json:"networkType"`
	IPDiscovery             string          `json:"ipDiscovery"`
	SecurityGroupIDs        []string        `json:"securityGroupIDs"`
	NumReplicasPerShard     int32           `json:"numReplicasPerShard"`
	SnapshotRetentionLimit  int32           `json:"snapshotRetentionLimit"`
	Port                    int32           `json:"port"`
	NumShards               int32           `json:"numShards"`
	TLSEnabled              bool            `json:"tlsEnabled"`
	AutoMinorVersionUpgrade bool            `json:"autoMinorVersionUpgrade"`
}

type createClusterRequest struct {
	NumShards               *int32     `json:"NumShards,omitempty"`
	TLSEnabled              *bool      `json:"TLSEnabled,omitempty"`
	Port                    *int32     `json:"Port,omitempty"`
	SnapshotRetentionLimit  *int32     `json:"SnapshotRetentionLimit,omitempty"`
	NumReplicasPerShard     *int32     `json:"NumReplicasPerShard,omitempty"`
	DataTiering             *bool      `json:"DataTiering,omitempty"`
	AutoMinorVersionUpgrade *bool      `json:"AutoMinorVersionUpgrade,omitempty"`
	ParameterGroupName      string     `json:"ParameterGroupName,omitempty"`
	Description             string     `json:"Description,omitempty"`
	SubnetGroupName         string     `json:"SubnetGroupName,omitempty"`
	KmsKeyID                string     `json:"KmsKeyId,omitempty"`
	SnsTopicArn             string     `json:"SnsTopicArn,omitempty"`
	MaintenanceWindow       string     `json:"MaintenanceWindow,omitempty"`
	SnapshotWindow          string     `json:"SnapshotWindow,omitempty"`
	EngineVersion           string     `json:"EngineVersion,omitempty"`
	Engine                  string     `json:"Engine,omitempty"`
	NetworkType             string     `json:"NetworkType,omitempty"`
	IPDiscovery             string     `json:"IpDiscovery,omitempty"`
	SnapshotName            string     `json:"SnapshotName,omitempty"`
	NodeType                string     `json:"NodeType"`
	ClusterName             string     `json:"ClusterName"`
	ACLName                 string     `json:"ACLName"`
	MultiRegionClusterName  string     `json:"MultiRegionClusterName,omitempty"`
	Tags                    []tagEntry `json:"Tags,omitempty"`
	SecurityGroupIDs        []string   `json:"SecurityGroupIds,omitempty"`
	SnapshotArns            []string   `json:"SnapshotArns,omitempty"`
}

type describeClusterRequest struct {
	MaxResults       *int32 `json:"MaxResults,omitempty"`
	ShowShardDetails *bool  `json:"ShowShardDetails,omitempty"`
	ClusterName      string `json:"ClusterName,omitempty"`
	NextToken        string `json:"NextToken,omitempty"`
}

type deleteClusterRequest struct {
	ClusterName       string `json:"ClusterName"`
	FinalSnapshotName string `json:"FinalSnapshotName,omitempty"`
}

type updateClusterRequest struct {
	SnapshotRetentionLimit  *int32                       `json:"SnapshotRetentionLimit,omitempty"`
	ShardConfiguration      *shardConfigurationRequest   `json:"ShardConfiguration,omitempty"`
	ReplicaConfiguration    *replicaConfigurationRequest `json:"ReplicaConfiguration,omitempty"`
	AutoMinorVersionUpgrade *bool                        `json:"AutoMinorVersionUpgrade,omitempty"`
	MaintenanceWindow       string                       `json:"MaintenanceWindow,omitempty"`
	NodeType                string                       `json:"NodeType,omitempty"`
	EngineVersion           string                       `json:"EngineVersion,omitempty"`
	SnapshotWindow          string                       `json:"SnapshotWindow,omitempty"`
	SnsTopicArn             string                       `json:"SnsTopicArn,omitempty"`
	SnsTopicStatus          string                       `json:"SnsTopicStatus,omitempty"`
	ACLName                 string                       `json:"ACLName,omitempty"`
	Description             string                       `json:"Description,omitempty"`
	ClusterName             string                       `json:"ClusterName"`
	NetworkType             string                       `json:"NetworkType,omitempty"`
	IPDiscovery             string                       `json:"IpDiscovery,omitempty"`
	Engine                  string                       `json:"Engine,omitempty"`
	ParameterGroupName      string                       `json:"ParameterGroupName,omitempty"`
	SecurityGroupIDs        []string                     `json:"SecurityGroupIds,omitempty"`
	Tags                    []tagEntry                   `json:"Tags,omitempty"`
}

type replicaConfigurationRequest struct {
	ReplicaCount *int32 `json:"ReplicaCount,omitempty"`
}

type shardConfigurationRequest struct {
	ShardCount *int32 `json:"ShardCount,omitempty"`
}

// -- ACL request types -----------------------------------------------------------

type securityGroupMembership struct {
	SecurityGroupID string `json:"SecurityGroupId,omitempty"`
	Status          string `json:"Status,omitempty"`
}

// clusterObject is the wire shape of the real SDK's types.Cluster, field-diffed
// against deserializers.go's awsAwsjson11_deserializeDocumentCluster (the
// authoritative list of 29 recognized keys). Three fields from a prior pass
// were fabricated and have been removed: "Tags" (Cluster carries no inline
// tags -- confirmed absent from the deserializer; a real client fetches tags
// via the separate ListTags(ResourceArn) operation, matching this service's
// ListTags/TagResource/UntagResource op family), "MultiRegionParameterGroupName"
// (that field exists on the distinct ClusterConfiguration shape used inside
// Snapshot, not on Cluster itself), and "NumberOfReplicasPerShard" (not part
// of the wire Cluster shape at all -- a real client derives replica count per
// shard from len(Shards[i].Nodes)-1).
type clusterObject struct {
	ClusterEndpoint         *endpointObject           `json:"ClusterEndpoint,omitempty"`
	PendingUpdates          *pendingUpdatesObject     `json:"PendingUpdates,omitempty"`
	SubnetGroupName         string                    `json:"SubnetGroupName,omitempty"`
	SnsTopicArn             string                    `json:"SnsTopicArn,omitempty"`
	SnsTopicStatus          string                    `json:"SnsTopicStatus,omitempty"`
	Description             string                    `json:"Description,omitempty"`
	Status                  string                    `json:"Status,omitempty"`
	NodeType                string                    `json:"NodeType,omitempty"`
	EngineVersion           string                    `json:"EngineVersion,omitempty"`
	EnginePatchVersion      string                    `json:"EnginePatchVersion,omitempty"`
	ARN                     string                    `json:"ARN,omitempty"`
	Name                    string                    `json:"Name,omitempty"`
	ACLName                 string                    `json:"ACLName,omitempty"`
	KmsKeyID                string                    `json:"KmsKeyId,omitempty"`
	MaintenanceWindow       string                    `json:"MaintenanceWindow,omitempty"`
	ParameterGroupName      string                    `json:"ParameterGroupName,omitempty"`
	ParameterGroupStatus    string                    `json:"ParameterGroupStatus,omitempty"`
	MultiRegionClusterName  string                    `json:"MultiRegionClusterName"`
	SnapshotWindow          string                    `json:"SnapshotWindow,omitempty"`
	AvailabilityMode        string                    `json:"AvailabilityMode,omitempty"`
	Engine                  string                    `json:"Engine,omitempty"`
	DataTiering             string                    `json:"DataTiering,omitempty"`
	NetworkType             string                    `json:"NetworkType,omitempty"`
	IPDiscovery             string                    `json:"IpDiscovery,omitempty"`
	Shards                  []shardObject             `json:"Shards,omitempty"`
	SecurityGroups          []securityGroupMembership `json:"SecurityGroups,omitempty"`
	NumberOfShards          int32                     `json:"NumberOfShards,omitempty"`
	SnapshotRetentionLimit  int32                     `json:"SnapshotRetentionLimit,omitempty"`
	TLSEnabled              bool                      `json:"TLSEnabled"`
	AutoMinorVersionUpgrade bool                      `json:"AutoMinorVersionUpgrade"`
}

// shardObject represents a single shard in a MemoryDB cluster.
type shardObject struct {
	Name          string       `json:"Name,omitempty"`
	Status        string       `json:"Status,omitempty"`
	Slots         string       `json:"Slots,omitempty"`
	Nodes         []nodeObject `json:"Nodes,omitempty"`
	NumberOfNodes int32        `json:"NumberOfNodes,omitempty"`
}

// nodeObject represents a node within a shard.
type nodeObject struct {
	Endpoint         *endpointObject `json:"Endpoint,omitempty"`
	Name             string          `json:"Name,omitempty"`
	Status           string          `json:"Status,omitempty"`
	AvailabilityZone string          `json:"AvailabilityZone,omitempty"`
	CreateTime       float64         `json:"CreateTime,omitempty"`
}

// pendingUpdatesObject represents pending changes to a cluster.
type pendingUpdatesObject struct {
	ACLs           *pendingACLsUpdate     `json:"ACLs,omitempty"`
	ServiceUpdates []pendingServiceUpdate `json:"ServiceUpdates,omitempty"`
}

// pendingACLsUpdate represents a pending ACL change.
type pendingACLsUpdate struct {
	ACLToApply string `json:"ACLToApply,omitempty"`
}

// pendingServiceUpdate represents a pending service update.
type pendingServiceUpdate struct {
	ServiceUpdateName string `json:"ServiceUpdateName,omitempty"`
	Status            string `json:"Status,omitempty"`
}

type endpointObject struct {
	Address string `json:"Address"`
	Port    int32  `json:"Port"`
}

// createClusterResponse is the response for CreateCluster.
type createClusterResponse struct {
	Cluster clusterObject `json:"Cluster"`
}

// describeClusterResponse is the response for DescribeClusters.
type describeClusterResponse struct {
	NextToken string          `json:"NextToken,omitempty"`
	Clusters  []clusterObject `json:"Clusters"`
}

// updateClusterResponse is the response for UpdateCluster.
type updateClusterResponse struct {
	Cluster clusterObject `json:"Cluster"`
}

// deleteClusterResponse is the response for DeleteCluster.
type deleteClusterResponse struct {
	Cluster clusterObject `json:"Cluster"`
}

type batchUpdateClusterServiceUpdate struct {
	ServiceUpdateNameToApply string `json:"ServiceUpdateNameToApply,omitempty"`
}

type batchUpdateClusterRequest struct {
	ServiceUpdate *batchUpdateClusterServiceUpdate `json:"ServiceUpdate,omitempty"`
	ClusterNames  []string                         `json:"ClusterNames"`
}

type unprocessedCluster struct {
	ClusterName  string `json:"ClusterName,omitempty"`
	ErrorType    string `json:"ErrorType,omitempty"`
	ErrorMessage string `json:"ErrorMessage,omitempty"`
}

type batchUpdateClusterResponse struct {
	ProcessedClusters   []clusterObject      `json:"ProcessedClusters"`
	UnprocessedClusters []unprocessedCluster `json:"UnprocessedClusters"`
}

type failoverShardRequest struct {
	ClusterName string `json:"ClusterName"`
	ShardName   string `json:"ShardName,omitempty"`
}

type failoverShardResponse struct {
	Cluster clusterObject `json:"Cluster"`
}

// -- ListAllowedNodeTypeUpdates request/response types -----------------------

type listAllowedNodeTypeUpdatesRequest struct {
	ClusterName string `json:"ClusterName"`
}

type listAllowedNodeTypeUpdatesResponse struct {
	ScaleUpNodeTypes   []string `json:"ScaleUpNodeTypes"`
	ScaleDownNodeTypes []string `json:"ScaleDownNodeTypes"`
}

// -- ListAllowedMultiRegionClusterUpdates request/response types -------------
