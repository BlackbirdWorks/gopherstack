package memorydb

import (
	"time"
)

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
	Parameters  map[string]string `json:"parameters"`
	ARN         string            `json:"arn"`
	Name        string            `json:"name"`
	Description string            `json:"description"`
	Family      string            `json:"family"`
}

// -- Snapshot request/response types ------------------------------------------

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

type describeMultiRegionParametersRequest struct {
	MaxResults         *int32 `json:"MaxResults,omitempty"`
	ParameterGroupName string `json:"ParameterGroupName"`
	NextToken          string `json:"NextToken,omitempty"`
}

type describeMultiRegionParametersResponse struct {
	NextToken  string            `json:"NextToken,omitempty"`
	Parameters []parameterObject `json:"Parameters"`
}
