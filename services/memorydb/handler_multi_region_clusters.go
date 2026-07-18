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

	return c.JSON(http.StatusOK, createMultiRegionClusterResponse{MultiRegionCluster: toMultiRegionClusterObject(mrc)})
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

	return c.JSON(http.StatusOK, deleteMultiRegionClusterResponse{MultiRegionCluster: toMultiRegionClusterObject(mrc)})
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

	objs := make([]multiRegionClusterObject, 0, len(mrcs))

	for _, mrc := range mrcs {
		objs = append(objs, toMultiRegionClusterObject(mrc))
	}

	return c.JSON(http.StatusOK, describeMultiRegionClustersResponse{MultiRegionClusters: objs})
}

// -- MultiRegionParameterGroup handlers ------------------------------------------

func (h *Handler) handleDescribeMultiRegionParameterGroups(ctx context.Context, c *echo.Context, body []byte) error {
	var req describeMultiRegionParameterGroupsRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	mrpgs, err := h.Backend.DescribeMultiRegionParameterGroups(ctx, req.ParameterGroupName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	objs := make([]multiRegionParameterGroupObject, 0, len(mrpgs))

	for _, mrpg := range mrpgs {
		objs = append(objs, multiRegionParameterGroupObject{
			ARN:         mrpg.ARN,
			Name:        mrpg.Name,
			Description: mrpg.Description,
			Family:      mrpg.Family,
		})
	}

	return c.JSON(http.StatusOK, describeMultiRegionParameterGroupsResponse{MultiRegionParameterGroups: objs})
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

	return c.JSON(http.StatusOK, updateMultiRegionClusterResponse{MultiRegionCluster: toMultiRegionClusterObject(mrc)})
}

func (h *Handler) handleDescribeMultiRegionParameters(ctx context.Context, c *echo.Context, body []byte) error {
	var req describeMultiRegionParametersRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.ParameterGroupName == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "ParameterGroupName is required")
	}

	params, err := h.Backend.DescribeMultiRegionParameters(ctx, req.ParameterGroupName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	objs := make([]parameterObject, 0, len(params))

	for k, v := range params {
		objs = append(objs, parameterObject{
			Name:     k,
			Value:    v,
			DataType: "string",
		})
	}

	sort.Slice(objs, func(i, j int) bool { return objs[i].Name < objs[j].Name })

	return c.JSON(http.StatusOK, describeMultiRegionParametersResponse{Parameters: objs})
}

// -- helpers ---------------------------------------------------------------------

// toMultiRegionClusterObject converts a MultiRegionCluster to its JSON representation.
func toMultiRegionClusterObject(mrc *MultiRegionCluster) multiRegionClusterObject {
	return multiRegionClusterObject{
		MultiRegionClusterName:        mrc.MultiRegionClusterName,
		ARN:                           mrc.ARN,
		Description:                   mrc.Description,
		NodeType:                      mrc.NodeType,
		Engine:                        mrc.Engine,
		EngineVersion:                 mrc.EngineVersion,
		MultiRegionParameterGroupName: mrc.MultiRegionParameterGroupName,
		Status:                        mrc.Status,
	}
}
