package memorydb

import (
	"time"
)

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

type parameterGroupObject struct {
	ARN         string `json:"ARN,omitempty"`
	Name        string `json:"Name,omitempty"`
	Description string `json:"Description,omitempty"`
	Family      string `json:"Family,omitempty"`
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

type describeParametersRequest struct {
	ParameterGroupName string `json:"ParameterGroupName"`
	MaxResults         *int32 `json:"MaxResults,omitempty"`
	NextToken          string `json:"NextToken,omitempty"`
}

// parameterObject is field-diffed against the real SDK's types.Parameter
// (deserializers.go's awsAwsjson11_deserializeDocumentParameter: exactly
// AllowedValues, DataType, Description, MinimumEngineVersion, Name, Value).
// A prior pass added fabricated "ChangeType"/"Source" fields; removed.
type parameterObject struct {
	Name                 string `json:"Name,omitempty"`
	Value                string `json:"Value,omitempty"`
	Description          string `json:"Description,omitempty"`
	DataType             string `json:"DataType,omitempty"`
	AllowedValues        string `json:"AllowedValues,omitempty"`
	MinimumEngineVersion string `json:"MinimumEngineVersion,omitempty"`
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
