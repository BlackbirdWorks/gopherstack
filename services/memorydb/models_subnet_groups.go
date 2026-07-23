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

// subnetGroupObject is field-diffed against the real SDK's types.SubnetGroup
// (deserializers.go's awsAwsjson11_deserializeDocumentSubnetGroup: ARN,
// Description, Name, Subnets, SupportedNetworkTypes, VpcId).
type subnetGroupObject struct {
	ARN                   string        `json:"ARN,omitempty"`
	Name                  string        `json:"Name,omitempty"`
	Description           string        `json:"Description,omitempty"`
	VPCID                 string        `json:"VpcId,omitempty"`
	Subnets               []subnetEntry `json:"Subnets,omitempty"`
	SupportedNetworkTypes []string      `json:"SupportedNetworkTypes,omitempty"`
}

// subnetEntry is field-diffed against the real SDK's types.Subnet
// (deserializers.go's awsAwsjson11_deserializeDocumentSubnet: AvailabilityZone,
// Identifier, SupportedNetworkTypes).
type subnetEntry struct {
	AvailabilityZone      *availabilityZoneObject `json:"AvailabilityZone,omitempty"`
	Identifier            string                  `json:"Identifier,omitempty"`
	SupportedNetworkTypes []string                `json:"SupportedNetworkTypes,omitempty"`
}

// availabilityZoneObject mirrors the real types.AvailabilityZone (just Name).
type availabilityZoneObject struct {
	Name string `json:"Name,omitempty"`
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
