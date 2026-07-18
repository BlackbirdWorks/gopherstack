package memorydb

import (
	"time"
)

// Snapshot represents an in-memory MemoryDB snapshot.
type Snapshot struct {
	CreatedAt            time.Time             `json:"createdAt"`
	Tags                 map[string]string     `json:"tags"`
	ARN                  string                `json:"arn"`
	Name                 string                `json:"name"`
	ClusterName          string                `json:"clusterName"`
	Status               string                `json:"status"`
	KmsKeyID             string                `json:"kmsKeyID"`
	SnapshotType         string                `json:"snapshotType"`
	Source               string                `json:"source"`
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

type describeSnapshotRequest struct {
	MaxResults   *int32 `json:"MaxResults,omitempty"`
	SnapshotName string `json:"SnapshotName,omitempty"`
	ClusterName  string `json:"ClusterName,omitempty"`
	SnapshotType string `json:"SnapshotType,omitempty"`
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

// snapshotObject.CreatedAt is epoch seconds (float64), matching the real
// Snapshot.SnapshotCreationTime TStamp shape.
type snapshotObject struct {
	ClusterConfiguration *snapshotClusterConfig `json:"ClusterConfiguration,omitempty"`
	ARN                  string                 `json:"ARN,omitempty"`
	Name                 string                 `json:"Name,omitempty"`
	Status               string                 `json:"Status,omitempty"`
	KmsKeyID             string                 `json:"KmsKeyId,omitempty"`
	SnapshotType         string                 `json:"SnapshotType,omitempty"`
	Source               string                 `json:"Source,omitempty"`
	CreatedAt            float64                `json:"SnapshotCreationTime,omitempty"`
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
