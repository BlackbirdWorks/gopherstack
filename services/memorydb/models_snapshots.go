package memorydb

import (
	"time"
)

// Snapshot represents an in-memory MemoryDB snapshot.
//
// NOTE: no "SnapshotType" field -- deleted (see snapshotObject's doc comment).
// It duplicated Source (every call site set both to the same value); Source
// is now the single source of truth internally too.
type Snapshot struct {
	CreatedAt            time.Time             `json:"createdAt"`
	Tags                 map[string]string     `json:"tags"`
	ARN                  string                `json:"arn"`
	Name                 string                `json:"name"`
	ClusterName          string                `json:"clusterName"`
	Status               string                `json:"status"`
	KmsKeyID             string                `json:"kmsKeyID"`
	Source               string                `json:"source"`
	DataTiering          string                `json:"dataTiering"`
	ClusterConfiguration snapshotClusterConfig `json:"clusterConfiguration"`
}

// snapshotClusterConfig holds the cluster configuration recorded at snapshot time.
type snapshotClusterConfig struct {
	Engine                 string `json:"Engine,omitempty"`
	VpcID                  string `json:"VpcId,omitempty"`
	EngineVersion          string `json:"EngineVersion,omitempty"`
	Description            string `json:"Description,omitempty"`
	Name                   string `json:"Name,omitempty"`
	SnapshotWindow         string `json:"SnapshotWindow,omitempty"`
	TopicArn               string `json:"TopicArn,omitempty"`
	MaintenanceWindow      string `json:"MaintenanceWindow,omitempty"`
	NodeType               string `json:"NodeType,omitempty"`
	ParameterGroupName     string `json:"ParameterGroupName,omitempty"`
	SubnetGroupName        string `json:"SubnetGroupName,omitempty"`
	Port                   int32  `json:"Port,omitempty"`
	SnapshotRetentionLimit int32  `json:"SnapshotRetentionLimit,omitempty"`
	NumShards              int32  `json:"NumShards,omitempty"`
}

type createSnapshotRequest struct {
	ClusterName  string     `json:"ClusterName"`
	SnapshotName string     `json:"SnapshotName"`
	KmsKeyID     string     `json:"KmsKeyId,omitempty"`
	Tags         []tagEntry `json:"Tags,omitempty"`
}

// describeSnapshotRequest mirrors DescribeSnapshotsInput, which has no
// "SnapshotType" field -- only ClusterName, MaxResults, NextToken, ShowDetail,
// SnapshotName, Source (confirmed via api_op_DescribeSnapshots.go). A prior
// pass invented SnapshotType as a filter, redundant with Source; removed.
// ShowDetail (gating ClusterConfiguration in the response, mirroring
// ShowShardDetails/ShowClusterDetails elsewhere in this service) is not yet
// implemented -- see PARITY.md.
type describeSnapshotRequest struct {
	MaxResults   *int32 `json:"MaxResults,omitempty"`
	SnapshotName string `json:"SnapshotName,omitempty"`
	ClusterName  string `json:"ClusterName,omitempty"`
	Source       string `json:"Source,omitempty"`
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

// snapshotObject is field-diffed against the real SDK's types.Snapshot
// (deserializers.go's awsAwsjson11_deserializeDocumentSnapshot: exactly ARN,
// ClusterConfiguration, DataTiering, KmsKeyId, Name, Source, Status). A prior
// pass fabricated two fields that don't exist at this level -- "SnapshotType"
// (a redundant duplicate of Source) and "SnapshotCreationTime" (real
// SnapshotCreationTime belongs to types.ShardDetail, nested inside
// ClusterConfiguration.Shards, not top-level Snapshot; not modeled here, see
// PARITY.md) -- and omitted the real "DataTiering" field; fixed.
type snapshotObject struct {
	ClusterConfiguration *snapshotClusterConfig `json:"ClusterConfiguration,omitempty"`
	ARN                  string                 `json:"ARN,omitempty"`
	Name                 string                 `json:"Name,omitempty"`
	Status               string                 `json:"Status,omitempty"`
	KmsKeyID             string                 `json:"KmsKeyId,omitempty"`
	Source               string                 `json:"Source,omitempty"`
	DataTiering          string                 `json:"DataTiering,omitempty"`
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

type exportSnapshotRequest struct {
	SnapshotName string     `json:"SnapshotName"`
	S3BucketName string     `json:"S3BucketName,omitempty"`
	KmsKeyID     string     `json:"KmsKeyId,omitempty"`
	Tags         []tagEntry `json:"Tags,omitempty"`
}

type exportSnapshotResponse struct {
	Snapshot snapshotObject `json:"Snapshot"`
}

// -- EngineVersion request/response types ------------------------------------
