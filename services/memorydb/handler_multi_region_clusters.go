package memorydb

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleCreateMultiRegionCluster(ctx context.Context, c *echo.Context, body []byte) error {
	var req createMultiRegionClusterRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.MultiRegionClusterNameSuffix == "" {
		return writeError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValueException",
			"MultiRegionClusterNameSuffix is required",
		)
	}

	if req.NodeType == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "NodeType is required")
	}

	mrc, err := h.Backend.CreateMultiRegionCluster(ctx, &req)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	obj := toMultiRegionClusterObject(mrc, h.Backend.RegionalClustersFor(mrc.MultiRegionClusterName))

	return c.JSON(http.StatusOK, createMultiRegionClusterResponse{MultiRegionCluster: obj})
}

func (h *Handler) handleDeleteMultiRegionCluster(ctx context.Context, c *echo.Context, body []byte) error {
	var req deleteMultiRegionClusterRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.MultiRegionClusterName == "" {
		return writeError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValueException",
			"MultiRegionClusterName is required",
		)
	}

	mrc, err := h.Backend.DeleteMultiRegionCluster(ctx, req.MultiRegionClusterName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	// The multi-Region cluster itself is gone, but any regional clusters that
	// still reference it (deletion order isn't enforced by this mock) should
	// still be reflected on the deletion response, matching a real client's view.
	obj := toMultiRegionClusterObject(mrc, h.Backend.RegionalClustersFor(mrc.MultiRegionClusterName))

	return c.JSON(http.StatusOK, deleteMultiRegionClusterResponse{MultiRegionCluster: obj})
}

func (h *Handler) handleDescribeMultiRegionClusters(ctx context.Context, c *echo.Context, body []byte) error {
	var req describeMultiRegionClustersRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	mrcs, err := h.Backend.DescribeMultiRegionClusters(ctx, req.MultiRegionClusterName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	mrcs, nextToken := paginateItems(
		mrcs, req.NextToken, req.MaxResults, func(mrc *MultiRegionCluster) string { return mrc.MultiRegionClusterName },
	)

	showClusters := req.ShowClusterDetails != nil && *req.ShowClusterDetails

	objs := make([]multiRegionClusterObject, 0, len(mrcs))

	for _, mrc := range mrcs {
		var clusters []*Cluster
		if showClusters {
			clusters = h.Backend.RegionalClustersFor(mrc.MultiRegionClusterName)
		}

		objs = append(objs, toMultiRegionClusterObject(mrc, clusters))
	}

	return c.JSON(
		http.StatusOK,
		describeMultiRegionClustersResponse{MultiRegionClusters: objs, NextToken: nextToken},
	)
}

// -- MultiRegionParameterGroup handlers ------------------------------------------

func (h *Handler) handleDescribeMultiRegionParameterGroups(ctx context.Context, c *echo.Context, body []byte) error {
	var req describeMultiRegionParameterGroupsRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	mrpgs, err := h.Backend.DescribeMultiRegionParameterGroups(ctx, req.MultiRegionParameterGroupName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	mrpgs, nextToken := paginateItems(
		mrpgs, req.NextToken, req.MaxResults, func(mrpg *MultiRegionParameterGroup) string { return mrpg.Name },
	)

	objs := make([]multiRegionParameterGroupObject, 0, len(mrpgs))

	for _, mrpg := range mrpgs {
		objs = append(objs, multiRegionParameterGroupObject{
			ARN:         mrpg.ARN,
			Name:        mrpg.Name,
			Description: mrpg.Description,
			Family:      mrpg.Family,
		})
	}

	return c.JSON(
		http.StatusOK,
		describeMultiRegionParameterGroupsResponse{MultiRegionParameterGroups: objs, NextToken: nextToken},
	)
}

// -- BatchUpdateCluster handler --------------------------------------------------

func (h *Handler) handleListAllowedMultiRegionClusterUpdates(ctx context.Context, c *echo.Context, body []byte) error {
	var req listAllowedMultiRegionClusterUpdatesRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.MultiRegionClusterName == "" {
		return writeError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValueException",
			"MultiRegionClusterName is required",
		)
	}

	nodeTypes, err := h.Backend.ListAllowedMultiRegionClusterUpdates(ctx, req.MultiRegionClusterName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, listAllowedMultiRegionClusterUpdatesResponse{
		ScaleUpNodeTypes:   nodeTypes,
		ScaleDownNodeTypes: nodeTypes,
	})
}

func (h *Handler) handleUpdateMultiRegionCluster(ctx context.Context, c *echo.Context, body []byte) error {
	var req updateMultiRegionClusterRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.MultiRegionClusterName == "" {
		return writeError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValueException",
			"MultiRegionClusterName is required",
		)
	}

	mrc, err := h.Backend.UpdateMultiRegionCluster(ctx, &req)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	obj := toMultiRegionClusterObject(mrc, h.Backend.RegionalClustersFor(mrc.MultiRegionClusterName))

	return c.JSON(http.StatusOK, updateMultiRegionClusterResponse{MultiRegionCluster: obj})
}

func (h *Handler) handleDescribeMultiRegionParameters(ctx context.Context, c *echo.Context, body []byte) error {
	var req describeMultiRegionParametersRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.MultiRegionParameterGroupName == "" {
		return writeError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValueException",
			"MultiRegionParameterGroupName is required",
		)
	}

	params, err := h.Backend.DescribeMultiRegionParameters(ctx, req.MultiRegionParameterGroupName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	objs := make([]multiRegionParameterObject, 0, len(params))

	for k, v := range params {
		objs = append(objs, multiRegionParameterObject{
			Name:     k,
			Value:    v,
			DataType: "string",
			Source:   "system",
		})
	}

	sort.Slice(objs, func(i, j int) bool { return objs[i].Name < objs[j].Name })

	objs, nextToken := paginateItems(
		objs, req.NextToken, req.MaxResults, func(p multiRegionParameterObject) string { return p.Name },
	)

	return c.JSON(
		http.StatusOK,
		describeMultiRegionParametersResponse{MultiRegionParameters: objs, NextToken: nextToken},
	)
}

// -- helpers ---------------------------------------------------------------------

// toMultiRegionClusterObject converts a MultiRegionCluster to its JSON representation.
// clusters, when non-nil, populates the Clusters field (the real SDK's
// MultiRegionCluster.Clusters []RegionalCluster) with the per-Region clusters
// that reference this multi-Region cluster.
func toMultiRegionClusterObject(mrc *MultiRegionCluster, clusters []*Cluster) multiRegionClusterObject {
	obj := multiRegionClusterObject{
		MultiRegionClusterName:        mrc.MultiRegionClusterName,
		ARN:                           mrc.ARN,
		Description:                   mrc.Description,
		NodeType:                      mrc.NodeType,
		Engine:                        mrc.Engine,
		EngineVersion:                 mrc.EngineVersion,
		MultiRegionParameterGroupName: mrc.MultiRegionParameterGroupName,
		Status:                        mrc.Status,
		NumberOfShards:                mrc.NumShards,
		TLSEnabled:                    mrc.TLSEnabled,
	}

	if len(clusters) > 0 {
		obj.Clusters = make([]regionalClusterObject, 0, len(clusters))
		for _, c := range clusters {
			obj.Clusters = append(obj.Clusters, regionalClusterObject{
				ARN:         c.ARN,
				ClusterName: c.Name,
				Region:      c.Region,
				Status:      c.Status,
			})
		}
	}

	return obj
}
