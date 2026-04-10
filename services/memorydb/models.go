package memorydb

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Cluster represents an in-memory MemoryDB cluster.
type Cluster struct {
	CreatedAt              time.Time         `json:"createdAt"`
	Tags                   map[string]string `json:"tags"`
	ARN                    string            `json:"arn"`
	Name                   string            `json:"name"`
	Description            string            `json:"description"`
	NodeType               string            `json:"nodeType"`
	EngineVersion          string            `json:"engineVersion"`
	ACLName                string            `json:"aclName"`
	SubnetGroupName        string            `json:"subnetGroupName"`
	ParameterGroupName     string            `json:"parameterGroupName"`
	Status                 string            `json:"status"`
	Region                 string            `json:"region"`
	KmsKeyID               string            `json:"kmsKeyID"`
	SnsTopicArn            string            `json:"snsTopicArn"`
	MaintenanceWindow      string            `json:"maintenanceWindow"`
	SnapshotWindow         string            `json:"snapshotWindow"`
	SecurityGroupIDs       []string          `json:"securityGroupIDs"`
	NumShards              int32             `json:"numShards"`
	NumReplicasPerShard    int32             `json:"numReplicasPerShard"`
	SnapshotRetentionLimit int32             `json:"snapshotRetentionLimit"`
	Port                   int32             `json:"port"`
	TLSEnabled             bool              `json:"tlsEnabled"`
}

// ACL represents an in-memory MemoryDB Access Control List.
type ACL struct {
	CreatedAt time.Time         `json:"createdAt"`
	Tags      map[string]string `json:"tags"`
	ARN       string            `json:"arn"`
	Name      string            `json:"name"`
	Status    string            `json:"status"`
	UserNames []string          `json:"userNames"`
}

// SubnetGroup represents an in-memory MemoryDB subnet group.
type SubnetGroup struct {
	CreatedAt   time.Time         `json:"createdAt"`
	Tags        map[string]string `json:"tags"`
	ARN         string            `json:"arn"`
	Name        string            `json:"name"`
	Description string            `json:"description"`
	VPCID       string            `json:"vpcID"`
	SubnetIDs   []string          `json:"subnetIDs"`
}

// User represents an in-memory MemoryDB user.
type User struct {
	CreatedAt    time.Time         `json:"createdAt"`
	Tags         map[string]string `json:"tags"`
	ARN          string            `json:"arn"`
	Name         string            `json:"name"`
	AccessString string            `json:"accessString"`
	Status       string            `json:"status"`
	AuthType     string            `json:"authType"`
	Passwords    []string          `json:"passwords"`
}

// ParameterGroup represents an in-memory MemoryDB parameter group.
type ParameterGroup struct {
	CreatedAt   time.Time         `json:"createdAt"`
	Tags        map[string]string `json:"tags"`
	Parameters  map[string]string `json:"parameters"`
	ARN         string            `json:"arn"`
	Name        string            `json:"name"`
	Description string            `json:"description"`
	Family      string            `json:"family"`
}

// -- Request types ----------------------------------------------------------------

type createClusterRequest struct {
	NumShards              *int32     `json:"NumShards,omitempty"`
	TLSEnabled             *bool      `json:"TLSEnabled,omitempty"`
	Port                   *int32     `json:"Port,omitempty"`
	SnapshotRetentionLimit *int32     `json:"SnapshotRetentionLimit,omitempty"`
	NumReplicasPerShard    *int32     `json:"NumReplicasPerShard,omitempty"`
	ParameterGroupName     string     `json:"ParameterGroupName,omitempty"`
	Description            string     `json:"Description,omitempty"`
	SubnetGroupName        string     `json:"SubnetGroupName,omitempty"`
	KmsKeyID               string     `json:"KmsKeyId,omitempty"`
	SnsTopicArn            string     `json:"SnsTopicArn,omitempty"`
	MaintenanceWindow      string     `json:"MaintenanceWindow,omitempty"`
	SnapshotWindow         string     `json:"SnapshotWindow,omitempty"`
	EngineVersion          string     `json:"EngineVersion,omitempty"`
	NodeType               string     `json:"NodeType"`
	ClusterName            string     `json:"ClusterName"`
	ACLName                string     `json:"ACLName"`
	Tags                   []tagEntry `json:"Tags,omitempty"`
	SecurityGroupIDs       []string   `json:"SecurityGroupIds,omitempty"`
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
	SnapshotRetentionLimit *int32                       `json:"SnapshotRetentionLimit,omitempty"`
	ShardConfiguration     *shardConfigurationRequest   `json:"ShardConfiguration,omitempty"`
	ReplicaConfiguration   *replicaConfigurationRequest `json:"ReplicaConfiguration,omitempty"`
	MaintenanceWindow      string                       `json:"MaintenanceWindow,omitempty"`
	NodeType               string                       `json:"NodeType,omitempty"`
	EngineVersion          string                       `json:"EngineVersion,omitempty"`
	SnapshotWindow         string                       `json:"SnapshotWindow,omitempty"`
	SnsTopicArn            string                       `json:"SnsTopicArn,omitempty"`
	SnsTopicStatus         string                       `json:"SnsTopicStatus,omitempty"`
	ACLName                string                       `json:"ACLName,omitempty"`
	Description            string                       `json:"Description,omitempty"`
	ClusterName            string                       `json:"ClusterName"`
	Tags                   []tagEntry                   `json:"Tags,omitempty"`
}

type replicaConfigurationRequest struct {
	ReplicaCount *int32 `json:"ReplicaCount,omitempty"`
}

type shardConfigurationRequest struct {
	ShardCount *int32 `json:"ShardCount,omitempty"`
}

// -- ACL request types -----------------------------------------------------------

type createACLRequest struct {
	ACLName   string     `json:"ACLName"`
	Tags      []tagEntry `json:"Tags,omitempty"`
	UserNames []string   `json:"UserNames,omitempty"`
}

type describeACLRequest struct {
	MaxResults *int32 `json:"MaxResults,omitempty"`
	ACLName    string `json:"ACLName,omitempty"`
	NextToken  string `json:"NextToken,omitempty"`
}

type deleteACLRequest struct {
	ACLName string `json:"ACLName"`
}

type updateACLRequest struct {
	ACLName           string   `json:"ACLName"`
	UserNamesToAdd    []string `json:"UserNamesToAdd,omitempty"`
	UserNamesToRemove []string `json:"UserNamesToRemove,omitempty"`
}

// -- Subnet group request types --------------------------------------------------

type createSubnetGroupRequest struct {
	SubnetGroupName string     `json:"SubnetGroupName"`
	Description     string     `json:"Description,omitempty"`
	Tags            []tagEntry `json:"Tags,omitempty"`
	SubnetIDs       []string   `json:"SubnetIds,omitempty"`
}

type describeSubnetGroupRequest struct {
	MaxResults      *int32 `json:"MaxResults,omitempty"`
	SubnetGroupName string `json:"SubnetGroupName,omitempty"`
	NextToken       string `json:"NextToken,omitempty"`
}

type deleteSubnetGroupRequest struct {
	SubnetGroupName string `json:"SubnetGroupName"`
}

type updateSubnetGroupRequest struct {
	SubnetGroupName string   `json:"SubnetGroupName"`
	Description     string   `json:"Description,omitempty"`
	SubnetIDs       []string `json:"SubnetIds,omitempty"`
}

// -- User request types ----------------------------------------------------------

type createUserRequest struct {
	AuthenticationMode authenticationModeReq `json:"AuthenticationMode"`
	UserName           string                `json:"UserName"`
	AccessString       string                `json:"AccessString"`
	Tags               []tagEntry            `json:"Tags,omitempty"`
}

type authenticationModeReq struct {
	Type      string   `json:"Type"`
	Passwords []string `json:"Passwords,omitempty"`
}

type describeUserRequest struct {
	MaxResults *int32 `json:"MaxResults,omitempty"`
	UserName   string `json:"UserName,omitempty"`
	NextToken  string `json:"NextToken,omitempty"`
}

type deleteUserRequest struct {
	UserName string `json:"UserName"`
}

type updateUserRequest struct {
	AuthenticationMode *authenticationModeReq `json:"AuthenticationMode,omitempty"`
	UserName           string                 `json:"UserName"`
	AccessString       string                 `json:"AccessString,omitempty"`
}

// -- Parameter group request types -----------------------------------------------

type createParameterGroupRequest struct {
	ParameterGroupName string     `json:"ParameterGroupName"`
	Family             string     `json:"Family"`
	Description        string     `json:"Description,omitempty"`
	Tags               []tagEntry `json:"Tags,omitempty"`
}

type describeParameterGroupRequest struct {
	MaxResults         *int32 `json:"MaxResults,omitempty"`
	ParameterGroupName string `json:"ParameterGroupName,omitempty"`
	NextToken          string `json:"NextToken,omitempty"`
}

type deleteParameterGroupRequest struct {
	ParameterGroupName string `json:"ParameterGroupName"`
}

type updateParameterGroupRequest struct {
	ParameterGroupName  string                    `json:"ParameterGroupName"`
	ParameterNameValues []parameterNameValueEntry `json:"ParameterNameValues"`
}

type parameterNameValueEntry struct {
	ParameterName  string `json:"ParameterName"`
	ParameterValue string `json:"ParameterValue"`
}

// -- Tags request types ----------------------------------------------------------

type listTagsRequest struct {
	ResourceArn string `json:"ResourceArn"`
}

type tagResourceRequest struct {
	ResourceArn string     `json:"ResourceArn"`
	Tags        []tagEntry `json:"Tags"`
}

type untagResourceRequest struct {
	ResourceArn string   `json:"ResourceArn"`
	TagKeys     []string `json:"TagKeys"`
}

// -- Response types ---------------------------------------------------------------

type clusterObject struct {
	ClusterEndpoint        *endpointObject `json:"ClusterEndpoint,omitempty"`
	ACLName                string          `json:"ACLName,omitempty"`
	KmsKeyID               string          `json:"KmsKeyId,omitempty"`
	Description            string          `json:"Description,omitempty"`
	Status                 string          `json:"Status,omitempty"`
	NodeType               string          `json:"NodeType,omitempty"`
	EngineVersion          string          `json:"EngineVersion,omitempty"`
	EnginePatchVersion     string          `json:"EnginePatchVersion,omitempty"`
	ARN                    string          `json:"ARN,omitempty"`
	Name                   string          `json:"Name,omitempty"`
	SubnetGroupName        string          `json:"SubnetGroupName,omitempty"`
	ParameterGroupName     string          `json:"ParameterGroupName,omitempty"`
	SnsTopicArn            string          `json:"SnsTopicArn,omitempty"`
	MaintenanceWindow      string          `json:"MaintenanceWindow,omitempty"`
	SnapshotWindow         string          `json:"SnapshotWindow,omitempty"`
	Tags                   []tagEntry      `json:"Tags,omitempty"`
	Shards                 []shardObject   `json:"Shards,omitempty"`
	NumberOfShards         int32           `json:"NumberOfShards,omitempty"`
	SnapshotRetentionLimit int32           `json:"SnapshotRetentionLimit,omitempty"`
	TLSEnabled             bool            `json:"TLSEnabled"`
}

// shardObject represents a single shard in a MemoryDB cluster.
type shardObject struct {
	Name          string `json:"Name,omitempty"`
	Status        string `json:"Status,omitempty"`
	Slots         string `json:"Slots,omitempty"`
	NumberOfNodes int32  `json:"NumberOfNodes,omitempty"`
}

type endpointObject struct {
	Address string `json:"Address"`
	Port    int32  `json:"Port"`
}

type aclObject struct {
	ARN       string   `json:"ARN,omitempty"`
	Name      string   `json:"Name,omitempty"`
	Status    string   `json:"Status,omitempty"`
	UserNames []string `json:"UserNames,omitempty"`
}

type subnetGroupObject struct {
	ARN         string        `json:"ARN,omitempty"`
	Name        string        `json:"Name,omitempty"`
	Description string        `json:"Description,omitempty"`
	VPCID       string        `json:"VpcId,omitempty"`
	Subnets     []subnetEntry `json:"Subnets,omitempty"`
}

type subnetEntry struct {
	Identifier string `json:"Identifier,omitempty"`
}

type userObject struct {
	ARN          string `json:"ARN,omitempty"`
	Name         string `json:"Name,omitempty"`
	AccessString string `json:"AccessString,omitempty"`
	Status       string `json:"Status,omitempty"`
}

type parameterGroupObject struct {
	ARN         string `json:"ARN,omitempty"`
	Name        string `json:"Name,omitempty"`
	Description string `json:"Description,omitempty"`
	Family      string `json:"Family,omitempty"`
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

// createACLResponse is the response for CreateACL.
type createACLResponse struct {
	ACL aclObject `json:"ACL"`
}

// describeACLResponse is the response for DescribeACLs.
type describeACLResponse struct {
	NextToken string      `json:"NextToken,omitempty"`
	ACLs      []aclObject `json:"ACLs"`
}

// updateACLResponse is the response for UpdateACL.
type updateACLResponse struct {
	ACL aclObject `json:"ACL"`
}

// deleteACLResponse is the response for DeleteACL.
type deleteACLResponse struct {
	ACL aclObject `json:"ACL"`
}

// createSubnetGroupResponse is the response for CreateSubnetGroup.
type createSubnetGroupResponse struct {
	SubnetGroup subnetGroupObject `json:"SubnetGroup"`
}

// describeSubnetGroupResponse is the response for DescribeSubnetGroups.
type describeSubnetGroupResponse struct {
	NextToken    string              `json:"NextToken,omitempty"`
	SubnetGroups []subnetGroupObject `json:"SubnetGroups"`
}

// updateSubnetGroupResponse is the response for UpdateSubnetGroup.
type updateSubnetGroupResponse struct {
	SubnetGroup subnetGroupObject `json:"SubnetGroup"`
}

// deleteSubnetGroupResponse is the response for DeleteSubnetGroup.
type deleteSubnetGroupResponse struct {
	SubnetGroup subnetGroupObject `json:"SubnetGroup"`
}

// createUserResponse is the response for CreateUser.
type createUserResponse struct {
	User userObject `json:"User"`
}

// describeUserResponse is the response for DescribeUsers.
type describeUserResponse struct {
	NextToken string       `json:"NextToken,omitempty"`
	Users     []userObject `json:"Users"`
}

// updateUserResponse is the response for UpdateUser.
type updateUserResponse struct {
	User userObject `json:"User"`
}

// deleteUserResponse is the response for DeleteUser.
type deleteUserResponse struct {
	User userObject `json:"User"`
}

// createParameterGroupResponse is the response for CreateParameterGroup.
type createParameterGroupResponse struct {
	ParameterGroup parameterGroupObject `json:"ParameterGroup"`
}

// describeParameterGroupResponse is the response for DescribeParameterGroups.
type describeParameterGroupResponse struct {
	NextToken       string                 `json:"NextToken,omitempty"`
	ParameterGroups []parameterGroupObject `json:"ParameterGroups"`
}

// deleteParameterGroupResponse is the response for DeleteParameterGroup.
type deleteParameterGroupResponse struct {
	ParameterGroup parameterGroupObject `json:"ParameterGroup"`
}

// updateParameterGroupResponse is the response for UpdateParameterGroup.
type updateParameterGroupResponse struct {
	ParameterGroup parameterGroupObject `json:"ParameterGroup"`
}

// listTagsResponse is the response for ListTags.
type listTagsResponse struct {
	TagList []tagEntry `json:"TagList"`
}

// tagEntry is a key/value tag pair.
type tagEntry struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// Snapshot represents an in-memory MemoryDB snapshot.
type Snapshot struct {
	CreatedAt   time.Time         `json:"createdAt"`
	Tags        map[string]string `json:"tags"`
	ARN         string            `json:"arn"`
	Name        string            `json:"name"`
	ClusterName string            `json:"clusterName"`
	Status      string            `json:"status"`
	KmsKeyID    string            `json:"kmsKeyID"`
}

// EngineVersion describes a supported MemoryDB engine version.
type EngineVersion struct {
	EngineVersion        string `json:"engineVersion"`
	EnginePatchVersion   string `json:"enginePatchVersion"`
	ParameterGroupFamily string `json:"parameterGroupFamily"`
	Description          string `json:"description"`
}

// Event represents a MemoryDB event.
type Event struct {
	Date       time.Time `json:"date"`
	SourceName string    `json:"sourceName"`
	SourceType string    `json:"sourceType"`
	Message    string    `json:"message"`
}

// MultiRegionCluster represents an in-memory MemoryDB multi-region cluster.
type MultiRegionCluster struct {
	CreatedAt                     time.Time         `json:"createdAt"`
	Tags                          map[string]string `json:"tags"`
	ARN                           string            `json:"arn"`
	MultiRegionClusterName        string            `json:"multiRegionClusterName"`
	Description                   string            `json:"description"`
	NodeType                      string            `json:"nodeType"`
	Engine                        string            `json:"engine"`
	EngineVersion                 string            `json:"engineVersion"`
	MultiRegionParameterGroupName string            `json:"multiRegionParameterGroupName"`
	Status                        string            `json:"status"`
}

// MultiRegionParameterGroup represents an in-memory MemoryDB multi-region parameter group.
type MultiRegionParameterGroup struct {
	CreatedAt   time.Time         `json:"createdAt"`
	Tags        map[string]string `json:"tags"`
	ARN         string            `json:"arn"`
	Name        string            `json:"name"`
	Description string            `json:"description"`
	Family      string            `json:"family"`
}

// -- Snapshot request/response types ------------------------------------------

type createSnapshotRequest struct {
	ClusterName  string     `json:"ClusterName"`
	SnapshotName string     `json:"SnapshotName"`
	KmsKeyID     string     `json:"KmsKeyId,omitempty"`
	Tags         []tagEntry `json:"Tags,omitempty"`
}

type describeSnapshotRequest struct {
	MaxResults   *int32 `json:"MaxResults,omitempty"`
	SnapshotName string `json:"SnapshotName,omitempty"`
	ClusterName  string `json:"ClusterName,omitempty"`
	SnapshotType string `json:"SnapshotType,omitempty"`
	NextToken    string `json:"NextToken,omitempty"`
}

type copySnapshotRequest struct {
	SourceSnapshotName string     `json:"SourceSnapshotName"`
	TargetSnapshotName string     `json:"TargetSnapshotName"`
	KmsKeyID           string     `json:"KmsKeyId,omitempty"`
	TargetBucket       string     `json:"TargetBucket,omitempty"`
	Tags               []tagEntry `json:"Tags,omitempty"`
}

type deleteSnapshotRequest struct {
	SnapshotName string `json:"SnapshotName"`
}

type snapshotObject struct {
	ARN         string `json:"ARN,omitempty"`
	Name        string `json:"Name,omitempty"`
	ClusterName string `json:"ClusterConfiguration,omitempty"`
	Status      string `json:"Status,omitempty"`
	KmsKeyID    string `json:"KmsKeyId,omitempty"`
	CreatedAt   string `json:"SnapshotCreationTime,omitempty"`
}

type createSnapshotResponse struct {
	Snapshot snapshotObject `json:"Snapshot"`
}

type describeSnapshotResponse struct {
	NextToken string           `json:"NextToken,omitempty"`
	Snapshots []snapshotObject `json:"Snapshots"`
}

type copySnapshotResponse struct {
	Snapshot snapshotObject `json:"Snapshot"`
}

type deleteSnapshotResponse struct {
	Snapshot snapshotObject `json:"Snapshot"`
}

// -- EngineVersion request/response types ------------------------------------

type describeEngineVersionsRequest struct {
	MaxResults           *int32 `json:"MaxResults,omitempty"`
	ParameterGroupFamily string `json:"ParameterGroupFamily,omitempty"`
	NextToken            string `json:"NextToken,omitempty"`
	DefaultOnly          bool   `json:"DefaultOnly,omitempty"`
}

type engineVersionObject struct {
	EngineVersion        string `json:"EngineVersion,omitempty"`
	EnginePatchVersion   string `json:"EnginePatchVersion,omitempty"`
	ParameterGroupFamily string `json:"ParameterGroupFamily,omitempty"`
	Description          string `json:"Description,omitempty"`
}

type describeEngineVersionsResponse struct {
	NextToken      string                `json:"NextToken,omitempty"`
	EngineVersions []engineVersionObject `json:"EngineVersions"`
}

// -- Event request/response types --------------------------------------------

type describeEventsRequest struct {
	MaxResults *int32 `json:"MaxResults,omitempty"`
	Duration   *int32 `json:"Duration,omitempty"`
	SourceName string `json:"SourceName,omitempty"`
	SourceType string `json:"SourceType,omitempty"`
	NextToken  string `json:"NextToken,omitempty"`
}

type eventObject struct {
	Date       string `json:"Date,omitempty"`
	SourceName string `json:"SourceName,omitempty"`
	SourceType string `json:"SourceType,omitempty"`
	Message    string `json:"Message,omitempty"`
}

type describeEventsResponse struct {
	NextToken string        `json:"NextToken,omitempty"`
	Events    []eventObject `json:"Events"`
}

// -- MultiRegionCluster request/response types --------------------------------

type createMultiRegionClusterRequest struct {
	TLSEnabled                    *bool      `json:"TLSEnabled,omitempty"`
	MultiRegionClusterNameSuffix  string     `json:"MultiRegionClusterNameSuffix"`
	Description                   string     `json:"Description,omitempty"`
	NodeType                      string     `json:"NodeType"`
	Engine                        string     `json:"Engine,omitempty"`
	EngineVersion                 string     `json:"EngineVersion,omitempty"`
	MultiRegionParameterGroupName string     `json:"MultiRegionParameterGroupName,omitempty"`
	Tags                          []tagEntry `json:"Tags,omitempty"`
}

type deleteMultiRegionClusterRequest struct {
	MultiRegionClusterName string `json:"MultiRegionClusterName"`
}

type describeMultiRegionClustersRequest struct {
	MaxResults             *int32 `json:"MaxResults,omitempty"`
	ShowClusterDetails     *bool  `json:"ShowClusterDetails,omitempty"`
	MultiRegionClusterName string `json:"MultiRegionClusterName,omitempty"`
	NextToken              string `json:"NextToken,omitempty"`
}

type multiRegionClusterObject struct {
	ARN                           string `json:"ARN,omitempty"`
	MultiRegionClusterName        string `json:"MultiRegionClusterName,omitempty"`
	Description                   string `json:"Description,omitempty"`
	NodeType                      string `json:"NodeType,omitempty"`
	Engine                        string `json:"Engine,omitempty"`
	EngineVersion                 string `json:"EngineVersion,omitempty"`
	MultiRegionParameterGroupName string `json:"MultiRegionParameterGroupName,omitempty"`
	Status                        string `json:"Status,omitempty"`
}

type createMultiRegionClusterResponse struct {
	MultiRegionCluster multiRegionClusterObject `json:"MultiRegionCluster"`
}

type deleteMultiRegionClusterResponse struct {
	MultiRegionCluster multiRegionClusterObject `json:"MultiRegionCluster"`
}

type describeMultiRegionClustersResponse struct {
	NextToken           string                     `json:"NextToken,omitempty"`
	MultiRegionClusters []multiRegionClusterObject `json:"MultiRegionClusters"`
}

// -- MultiRegionParameterGroup request/response types -------------------------

type describeMultiRegionParameterGroupsRequest struct {
	MaxResults         *int32 `json:"MaxResults,omitempty"`
	ParameterGroupName string `json:"ParameterGroupName,omitempty"`
	NextToken          string `json:"NextToken,omitempty"`
}

type multiRegionParameterGroupObject struct {
	ARN         string `json:"ARN,omitempty"`
	Name        string `json:"Name,omitempty"`
	Description string `json:"Description,omitempty"`
	Family      string `json:"Family,omitempty"`
}

type describeMultiRegionParameterGroupsResponse struct {
	NextToken                  string                            `json:"NextToken,omitempty"`
	MultiRegionParameterGroups []multiRegionParameterGroupObject `json:"MultiRegionParameterGroups"`
}

// -- BatchUpdateCluster request/response types --------------------------------

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

// errorResponse is the standard JSON error response body.
type errorResponse = service.JSONErrorResponse

// -- DescribeParameters request/response types --------------------------------

type describeParametersRequest struct {
	ParameterGroupName string `json:"ParameterGroupName"`
	MaxResults         *int32 `json:"MaxResults,omitempty"`
	NextToken          string `json:"NextToken,omitempty"`
}

type parameterObject struct {
	Name          string `json:"Name,omitempty"`
	Value         string `json:"Value,omitempty"`
	Description   string `json:"Description,omitempty"`
	DataType      string `json:"DataType,omitempty"`
	AllowedValues string `json:"AllowedValues,omitempty"`
}

type describeParametersResponse struct {
	NextToken  string            `json:"NextToken,omitempty"`
	Parameters []parameterObject `json:"Parameters"`
}

// -- ResetParameterGroup request/response types ------------------------------

type resetParameterGroupRequest struct {
	ParameterGroupName string   `json:"ParameterGroupName"`
	ParameterNames     []string `json:"ParameterNames,omitempty"`
	AllParameters      bool     `json:"AllParameters,omitempty"`
}

type resetParameterGroupResponse struct {
	ParameterGroup parameterGroupObject `json:"ParameterGroup"`
}

// -- FailoverShard request/response types ------------------------------------

type failoverShardRequest struct {
	ClusterName        string `json:"ClusterName"`
	ShardConfiguration string `json:"ShardConfiguration,omitempty"`
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

type listAllowedMultiRegionClusterUpdatesRequest struct {
	MultiRegionClusterName string `json:"MultiRegionClusterName"`
}

type listAllowedMultiRegionClusterUpdatesResponse struct {
	ScaleUpNodeTypes   []string `json:"ScaleUpNodeTypes"`
	ScaleDownNodeTypes []string `json:"ScaleDownNodeTypes"`
}

// -- UpdateMultiRegionCluster request/response types -------------------------

type updateMultiRegionClusterRequest struct {
	MultiRegionClusterName        string `json:"MultiRegionClusterName"`
	Description                   string `json:"Description,omitempty"`
	NodeType                      string `json:"NodeType,omitempty"`
	EngineVersion                 string `json:"EngineVersion,omitempty"`
	MultiRegionParameterGroupName string `json:"MultiRegionParameterGroupName,omitempty"`
}

type updateMultiRegionClusterResponse struct {
	MultiRegionCluster multiRegionClusterObject `json:"MultiRegionCluster"`
}

// -- DescribeServiceUpdates request/response types ---------------------------

type serviceUpdateObject struct {
	ServiceUpdateName   string `json:"ServiceUpdateName,omitempty"`
	ReleaseDate         string `json:"ReleaseDate,omitempty"`
	Description         string `json:"Description,omitempty"`
	Status              string `json:"Status,omitempty"`
	Type                string `json:"Type,omitempty"`
	AutoUpdateStartDate string `json:"AutoUpdateStartDate,omitempty"`
}

type describeServiceUpdatesResponse struct {
	NextToken      string                `json:"NextToken,omitempty"`
	ServiceUpdates []serviceUpdateObject `json:"ServiceUpdates"`
}
