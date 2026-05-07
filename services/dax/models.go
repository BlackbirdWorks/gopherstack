// Package dax provides an in-memory simulation of the Amazon DAX (DynamoDB Accelerator) API.
package dax

import "time"

// Cluster status values.
const (
	StatusAvailable  = "available"
	StatusCreating   = "creating"
	StatusDeleting   = "deleting"
	StatusModifying  = "modifying"
	StatusRestarting = "restarting"
)

// Default values.
const (
	DefaultParameterGroupName = "default.dax1.0"
	DefaultSubnetGroupName    = "default"
)

// Endpoint represents a DAX cluster endpoint.
type Endpoint struct {
	Address string `json:"address"`
	Port    int    `json:"port"`
	URL     string `json:"url"`
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

// SubnetGroup represents a DAX subnet group.
type SubnetGroup struct {
	SubnetGroupName string   `json:"subnetGroupName"`
	Description     string   `json:"description"`
	VpcID           string   `json:"vpcId"`
	SubnetIDs       []string `json:"subnetIds"`
}

// Cluster represents an Amazon DAX cluster.
type Cluster struct {
	CreateTime                 time.Time            `json:"createTime"`
	Endpoint                   *Endpoint            `json:"endpoint,omitempty"`
	ParameterGroup             ParameterGroupStatus `json:"parameterGroup"`
	SSEDescription             SSEDescription       `json:"sseDescription"`
	Nodes                      []Node               `json:"nodes"`
	SecurityGroupIDs           []string             `json:"securityGroupIds"`
	SubnetGroupName            string               `json:"subnetGroupName"`
	Tags                       map[string]string    `json:"tags"`
	ClusterName                string               `json:"clusterName"`
	ClusterArn                 string               `json:"clusterArn"`
	Description                string               `json:"description"`
	NodeType                   string               `json:"nodeType"`
	Status                     string               `json:"status"`
	PreferredMaintenanceWindow string               `json:"preferredMaintenanceWindow"`
	IamRoleArn                 string               `json:"iamRoleArn"`
	ActiveNodes                int                  `json:"activeNodes"`
	TotalNodes                 int                  `json:"totalNodes"`
}

// ParameterGroup represents a DAX parameter group.
type ParameterGroup struct {
	ParameterGroupName string            `json:"parameterGroupName"`
	Description        string            `json:"description"`
	Parameters         map[string]string `json:"parameters"`
}

// CreateClusterInput holds parameters for creating a DAX cluster.
type CreateClusterInput struct {
	Tags                       map[string]string
	NodeType                   string
	ClusterName                string
	Description                string
	IamRoleArn                 string
	ReplicationFactor          int
	AvailabilityZones          []string
	SubnetGroupName            string
	SecurityGroupIDs           []string
	PreferredMaintenanceWindow string
	ParameterGroupName         string
	SSESpecificationEnabled    bool
}

// UpdateClusterInput holds parameters for updating a DAX cluster.
type UpdateClusterInput struct {
	SecurityGroupIDs           []string
	ClusterName                string
	Description                string
	PreferredMaintenanceWindow string
	ParameterGroupName         string
	NotificationTopicArn       string
	NotificationTopicStatus    string
}
