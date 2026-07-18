package memorydb

import (
	"time"
)

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
