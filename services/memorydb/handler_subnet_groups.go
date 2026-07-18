package memorydb

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleCreateSubnetGroup(ctx context.Context, c *echo.Context, body []byte) error {
	var req createSubnetGroupRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.SubnetGroupName == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "SubnetGroupName is required")
	}

	if err := validateTagEntries(req.Tags); err != nil {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", err.Error())
	}

	sg, err := h.Backend.CreateSubnetGroup(ctx, &req)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createSubnetGroupResponse{SubnetGroup: toSubnetGroupObject(sg)})
}

func (h *Handler) handleDescribeSubnetGroups(ctx context.Context, c *echo.Context, body []byte) error {
	var req describeSubnetGroupRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	sgs, err := h.Backend.DescribeSubnetGroups(ctx, req.SubnetGroupName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	sgs, nextToken := paginateItems(sgs, req.NextToken, req.MaxResults, func(sg *SubnetGroup) string { return sg.Name })

	objs := make([]subnetGroupObject, 0, len(sgs))

	for _, sg := range sgs {
		objs = append(objs, toSubnetGroupObject(sg))
	}

	return c.JSON(http.StatusOK, describeSubnetGroupResponse{SubnetGroups: objs, NextToken: nextToken})
}

func (h *Handler) handleDeleteSubnetGroup(ctx context.Context, c *echo.Context, body []byte) error {
	var req deleteSubnetGroupRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.SubnetGroupName == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "SubnetGroupName is required")
	}

	sg, err := h.Backend.DeleteSubnetGroup(ctx, req.SubnetGroupName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, deleteSubnetGroupResponse{SubnetGroup: toSubnetGroupObject(sg)})
}

func (h *Handler) handleUpdateSubnetGroup(ctx context.Context, c *echo.Context, body []byte) error {
	var req updateSubnetGroupRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.SubnetGroupName == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "SubnetGroupName is required")
	}

	sg, err := h.Backend.UpdateSubnetGroup(ctx, &req)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, updateSubnetGroupResponse{SubnetGroup: toSubnetGroupObject(sg)})
}

// -- User handlers ---------------------------------------------------------------

// toSubnetGroupObject converts a SubnetGroup to its JSON representation.
func toSubnetGroupObject(sg *SubnetGroup) subnetGroupObject {
	subnets := make([]subnetEntry, 0, len(sg.SubnetIDs))

	for _, id := range sg.SubnetIDs {
		subnets = append(subnets, subnetEntry{Identifier: id})
	}

	return subnetGroupObject{
		Name:        sg.Name,
		ARN:         sg.ARN,
		Description: sg.Description,
		VPCID:       sg.VPCID,
		Subnets:     subnets,
	}
}
