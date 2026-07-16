package memorydb

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleCreateParameterGroup(ctx context.Context, c *echo.Context, body []byte) error {
	var req createParameterGroupRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.ParameterGroupName == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "ParameterGroupName is required")
	}

	if err := validateTagEntries(req.Tags); err != nil {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", err.Error())
	}

	pg, err := h.Backend.CreateParameterGroup(ctx, &req)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createParameterGroupResponse{ParameterGroup: toParameterGroupObject(pg)})
}

func (h *Handler) handleDescribeParameterGroups(ctx context.Context, c *echo.Context, body []byte) error {
	var req describeParameterGroupRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	pgs, err := h.Backend.DescribeParameterGroups(ctx, req.ParameterGroupName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	pgs, nextToken := paginateItems(
		pgs,
		req.NextToken,
		req.MaxResults,
		func(pg *ParameterGroup) string { return pg.Name },
	)

	objs := make([]parameterGroupObject, 0, len(pgs))

	for _, pg := range pgs {
		objs = append(objs, toParameterGroupObject(pg))
	}

	return c.JSON(http.StatusOK, describeParameterGroupResponse{ParameterGroups: objs, NextToken: nextToken})
}

func (h *Handler) handleDeleteParameterGroup(ctx context.Context, c *echo.Context, body []byte) error {
	var req deleteParameterGroupRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.ParameterGroupName == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "ParameterGroupName is required")
	}

	pg, err := h.Backend.DeleteParameterGroup(ctx, req.ParameterGroupName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, deleteParameterGroupResponse{ParameterGroup: toParameterGroupObject(pg)})
}

func (h *Handler) handleUpdateParameterGroup(ctx context.Context, c *echo.Context, body []byte) error {
	var req updateParameterGroupRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.ParameterGroupName == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "ParameterGroupName is required")
	}

	pg, err := h.Backend.UpdateParameterGroup(ctx, &req)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, updateParameterGroupResponse{ParameterGroup: toParameterGroupObject(pg)})
}

// -- Tag handlers ----------------------------------------------------------------

func (h *Handler) handleDescribeParameters(ctx context.Context, c *echo.Context, body []byte) error {
	var req describeParametersRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	params, err := h.Backend.DescribeParameters(ctx, req.ParameterGroupName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	objs := make([]parameterObject, 0, len(params))

	for k, v := range params {
		objs = append(objs, parameterObject{
			Name:                 k,
			Value:                v,
			DataType:             "string",
			ChangeType:           "immediate",
			Source:               "system",
			MinimumEngineVersion: engineVersion62,
		})
	}

	sort.Slice(objs, func(i, j int) bool { return objs[i].Name < objs[j].Name })

	return c.JSON(http.StatusOK, describeParametersResponse{Parameters: objs})
}

func (h *Handler) handleResetParameterGroup(ctx context.Context, c *echo.Context, body []byte) error {
	var req resetParameterGroupRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	pg, err := h.Backend.ResetParameterGroup(ctx, req.ParameterGroupName, req.ParameterNames, req.AllParameters)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, resetParameterGroupResponse{ParameterGroup: toParameterGroupObject(pg)})
}

// toParameterGroupObject converts a ParameterGroup to its JSON representation.
func toParameterGroupObject(pg *ParameterGroup) parameterGroupObject {
	return parameterGroupObject{
		Name:        pg.Name,
		ARN:         pg.ARN,
		Description: pg.Description,
		Family:      pg.Family,
	}
}
