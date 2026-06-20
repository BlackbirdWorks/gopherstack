// Package dax provides an in-memory simulation of the Amazon DAX (DynamoDB Accelerator) API.
package dax

import "time"

// Cluster status values.
const (
	StatusAvailable             = "available"
	StatusCreating              = "creating"
	StatusDeleting              = "deleting"
	StatusModifying             = "modifying"
	StatusRebooting             = "rebooting"
	StatusFailed                = "failed"
	StatusIncompatibleNetwork   = "incompatible-network"
	StatusIncompatibleParameter = "incompatible-parameters"
)

// ClusterEndpointEncryptionType values.
const (
	EncryptionTypeNone = "NONE"
	EncryptionTypeTLS  = "TLS"
)

// EventSourceType values.
const (
	EventSourceTypeCluster        = "CLUSTER"
	EventSourceTypeParameterGroup = "PARAMETER_GROUP"
	EventSourceTypeSubnetGroup    = "SUBNET_GROUP"
	EventSourceTypeNode           = "NODE"
)

// Default values.
const (
	DefaultParameterGroupName = "default.dax1.0"
	DefaultSubnetGroupName    = "default"

	// maxReplicationFactor is the maximum allowed nodes per cluster.
	maxReplicationFactor = 10
	// minReplicationFactor is the minimum allowed nodes per cluster.
	minReplicationFactor = 1

	// maxEventsPerBuffer is the maximum number of events retained in the ring buffer.
	maxEventsPerBuffer = 1000

	// maxTagsPerResource is the maximum number of tags allowed per DAX resource.
	maxTagsPerResource = 50
	// maxTagKeyLength is the maximum allowed length of a tag key.
	maxTagKeyLength = 128
	// maxTagValueLength is the maximum allowed length of a tag value.
	maxTagValueLength = 256
)

// validNodeTypes is the set of valid DAX node type identifiers.
var validNodeTypes = map[string]bool{ //nolint:gochecknoglobals // package-level lookup table
	"dax.r4.large":    true,
	"dax.r4.xlarge":   true,
	"dax.r4.2xlarge":  true,
	"dax.r4.4xlarge":  true,
	"dax.r4.8xlarge":  true,
	"dax.r4.16xlarge": true,
	"dax.r5.large":    true,
	"dax.r5.xlarge":   true,
	"dax.r5.2xlarge":  true,
	"dax.r5.4xlarge":  true,
	"dax.r5.8xlarge":  true,
	"dax.r5.12xlarge": true,
	"dax.r5.16xlarge": true,
	"dax.r5.24xlarge": true,
	"dax.t3.small":    true,
	"dax.t3.medium":   true,
}

// defaultParameterValues are the canonical DAX 1.0 parameter defaults.
var defaultParameterValues = map[string]string{ //nolint:gochecknoglobals // package-level lookup table
	paramQueryTTL:  "300000",
	paramRecordTTL: "300000",
}

// defaultParameterDescriptions provides human-readable descriptions for default parameters.
var defaultParameterDescriptions = map[string]string{ //nolint:gochecknoglobals // package-level lookup table
	paramQueryTTL:  "The number of milliseconds for which query results are cached.",
	paramRecordTTL: "The number of milliseconds for which individual item results are cached.",
}

// Endpoint represents a DAX cluster endpoint.
type Endpoint struct {
	Address string `json:"address"`
	URL     string `json:"url"`
	Port    int    `json:"port"`
}

// Node represents a single DAX node in a cluster.
type Node struct {
	CreateTime           time.Time `json:"createTime"`
	Endpoint             *Endpoint `json:"endpoint,omitempty"`
	NodeID               string    `json:"nodeId"`
	NodeStatus           string    `json:"nodeStatus"`
	AvailabilityZone     string    `json:"availabilityZone"`
	ParameterGroupStatus string    `json:"parameterGroupStatus"`
}

// ParameterGroupStatus holds parameter group status for a cluster.
type ParameterGroupStatus struct {
	ParameterGroupName   string   `json:"parameterGroupName"`
	ParameterApplyStatus string   `json:"parameterApplyStatus"`
	NodeIDsToReboot      []string `json:"nodeIdsToReboot,omitempty"`
}

// SSEDescription holds encryption at rest details.
type SSEDescription struct {
	Status string `json:"status"`
}

// NotificationConfiguration holds SNS notification topic configuration.
type NotificationConfiguration struct {
	TopicArn    string `json:"topicArn"`
	TopicStatus string `json:"topicStatus"`
}

// SubnetEntry represents a subnet with its availability zone.
type SubnetEntry struct {
	SubnetID         string `json:"subnetId"`
	AvailabilityZone string `json:"availabilityZone"`
}

// SubnetGroup represents a DAX subnet group.
type SubnetGroup struct {
	SubnetGroupName string        `json:"subnetGroupName"`
	Description     string        `json:"description"`
	VpcID           string        `json:"vpcId"`
	Subnets         []SubnetEntry `json:"subnets"`
}

// ParameterType values distinguish individual versus per-node-type parameters.
const (
	ParameterTypeDefault          = "DEFAULT"
	ParameterTypeNodeTypeSpecific = "NODE_TYPE_SPECIFIC"
)

// paramQueryTTL is the canonical DAX parameter name for query result TTL.
const paramQueryTTL = "query-ttl-millis"

// paramRecordTTL is the canonical DAX parameter name for individual item TTL.
const paramRecordTTL = "record-ttl-millis"

// defaultParameterAllowedValues are the allowed value ranges for each default parameter.
var defaultParameterAllowedValues = map[string]string{ //nolint:gochecknoglobals // package-level lookup table
	paramQueryTTL:  "0-2147483647",
	paramRecordTTL: "0-2147483647",
}

// Parameter represents a DAX parameter with metadata.
type Parameter struct {
	ParameterName  string `json:"parameterName"`
	ParameterValue string `json:"parameterValue"`
	Description    string `json:"description"`
	Source         string `json:"source"`
	DataType       string `json:"dataType"`
	IsModifiable   string `json:"isModifiable"`
	ChangeType     string `json:"changeType"`
	AllowedValues  string `json:"allowedValues,omitempty"`
	ParameterType  string `json:"parameterType,omitempty"`
}

// ParameterNameValue is a name-value pair for parameter updates.
type ParameterNameValue struct {
	ParameterName  string `json:"parameterName"`
	ParameterValue string `json:"parameterValue"`
}

// Event represents a DAX service lifecycle event.
type Event struct {
	Date       time.Time `json:"date"`
	SourceName string    `json:"sourceName"`
	SourceType string    `json:"sourceType"`
	Message    string    `json:"message"`
}

// Cluster represents an Amazon DAX cluster.
type Cluster struct {
	CreateTime                    time.Time                  `json:"createTime"`
	Tags                          map[string]string          `json:"tags"`
	Endpoint                      *Endpoint                  `json:"endpoint,omitempty"`
	NotificationConfiguration     *NotificationConfiguration `json:"notificationConfiguration,omitempty"`
	Status                        string                     `json:"status"`
	Description                   string                     `json:"description"`
	IamRoleArn                    string                     `json:"iamRoleArn"`
	SubnetGroupName               string                     `json:"subnetGroupName"`
	SSEDescription                SSEDescription             `json:"sseDescription"`
	ClusterName                   string                     `json:"clusterName"`
	ClusterArn                    string                     `json:"clusterArn"`
	PreferredMaintenanceWindow    string                     `json:"preferredMaintenanceWindow"`
	NodeType                      string                     `json:"nodeType"`
	ClusterEndpointEncryptionType string                     `json:"clusterEndpointEncryptionType"`
	ParameterGroup                ParameterGroupStatus       `json:"parameterGroup"`
	Nodes                         []Node                     `json:"nodes"`
	SecurityGroupIDs              []string                   `json:"securityGroupIds"`
	ActiveNodes                   int                        `json:"activeNodes"`
	TotalNodes                    int                        `json:"totalNodes"`
}

// ParameterGroup represents a DAX parameter group.
type ParameterGroup struct {
	Parameters         map[string]string `json:"parameters"`
	ParameterGroupName string            `json:"parameterGroupName"`
	Description        string            `json:"description"`
}

// CreateClusterInput holds parameters for creating a DAX cluster.
type CreateClusterInput struct {
	Tags                          map[string]string
	NodeType                      string
	ClusterName                   string
	Description                   string
	IamRoleArn                    string
	SubnetGroupName               string
	PreferredMaintenanceWindow    string
	ParameterGroupName            string
	NotificationTopicArn          string
	ClusterEndpointEncryptionType string
	AvailabilityZones             []string
	SecurityGroupIDs              []string
	ReplicationFactor             int
	SSESpecificationEnabled       bool
}

// UpdateClusterInput holds parameters for updating a DAX cluster.
// Description uses a pointer so callers can explicitly clear it (empty string vs absent).
type UpdateClusterInput struct {
	Description                *string
	ClusterName                string
	PreferredMaintenanceWindow string
	ParameterGroupName         string
	NotificationTopicArn       string
	NotificationTopicStatus    string
	SecurityGroupIDs           []string
}

// IncreaseReplicationFactorInput holds parameters for increasing cluster replication.
type IncreaseReplicationFactorInput struct {
	ClusterName          string
	AvailabilityZones    []string
	NewReplicationFactor int
}

// DecreaseReplicationFactorInput holds parameters for decreasing cluster replication.
type DecreaseReplicationFactorInput struct {
	ClusterName          string
	NodeIDsToRemove      []string
	NewReplicationFactor int
}

// UpdateParameterGroupInput holds parameters for updating a parameter group.
type UpdateParameterGroupInput struct {
	ParameterGroupName  string
	ParameterNameValues []ParameterNameValue
}

// UpdateSubnetGroupInput holds parameters for updating a subnet group.
type UpdateSubnetGroupInput struct {
	SubnetGroupName string
	Description     string
	SubnetIDs       []string
}
