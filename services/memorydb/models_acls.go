package memorydb

import (
	"time"
)

// ACL represents an in-memory MemoryDB Access Control List.
type ACL struct {
	CreatedAt time.Time         `json:"createdAt"`
	Tags      map[string]string `json:"tags"`
	ARN       string            `json:"arn"`
	Name      string            `json:"name"`
	Status    string            `json:"status"`
	UserNames []string          `json:"userNames"`
}

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

// aclPendingChangesObject represents pending user membership changes on an ACL.
type aclPendingChangesObject struct {
	UserNamesToAdd    []string `json:"UserNamesToAdd,omitempty"`
	UserNamesToRemove []string `json:"UserNamesToRemove,omitempty"`
}

type aclObject struct {
	PendingChanges       *aclPendingChangesObject `json:"PendingChanges,omitempty"`
	ARN                  string                   `json:"ARN,omitempty"`
	Name                 string                   `json:"Name,omitempty"`
	Status               string                   `json:"Status,omitempty"`
	MinimumEngineVersion string                   `json:"MinimumEngineVersion,omitempty"`
	UserNames            []string                 `json:"UserNames,omitempty"`
	Clusters             []string                 `json:"Clusters,omitempty"`
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
