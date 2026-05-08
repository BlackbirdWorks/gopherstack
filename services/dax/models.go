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
	Tags                       map[string]string    `json:"tags"`
	Endpoint                   *Endpoint            `json:"endpoint,omitempty"`
	Status                     string               `json:"status"`
	Description                string               `json:"description"`
	IamRoleArn                 string               `json:"iamRoleArn"`
	SubnetGroupName            string               `json:"subnetGroupName"`
	SSEDescription             SSEDescription       `json:"sseDescription"`
	ClusterName                string               `json:"clusterName"`
	ClusterArn                 string               `json:"clusterArn"`
	PreferredMaintenanceWindow string               `json:"preferredMaintenanceWindow"`
	NodeType                   string               `json:"nodeType"`
	ParameterGroup             ParameterGroupStatus `json:"parameterGroup"`
	Nodes                      []Node               `json:"nodes"`
	SecurityGroupIDs           []string             `json:"securityGroupIds"`
	ActiveNodes                int                  `json:"activeNodes"`
	TotalNodes                 int                  `json:"totalNodes"`
}

// ParameterGroup represents a DAX parameter group.
type ParameterGroup struct {
	Parameters         map[string]string `json:"parameters"`
	ParameterGroupName string            `json:"parameterGroupName"`
	Description        string            `json:"description"`
}

// CreateClusterInput holds parameters for creating a DAX cluster.
type CreateClusterInput struct {
	Tags                       map[string]string
	NodeType                   string
	ClusterName                string
	Description                string
	IamRoleArn                 string
	SubnetGroupName            string
	PreferredMaintenanceWindow string
	ParameterGroupName         string
	AvailabilityZones          []string
	SecurityGroupIDs           []string
	ReplicationFactor          int
	SSESpecificationEnabled    bool
}

// UpdateClusterInput holds parameters for updating a DAX cluster.
type UpdateClusterInput struct {
	ClusterName                string
	Description                string
	PreferredMaintenanceWindow string
	ParameterGroupName         string
	NotificationTopicArn       string
	NotificationTopicStatus    string
	SecurityGroupIDs           []string
}
