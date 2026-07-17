package efs

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

type createMountTargetBody struct {
	FileSystemID   string   `json:"FileSystemId"`
	SubnetID       string   `json:"SubnetId"`
	IPAddress      string   `json:"IpAddress"`
	SecurityGroups []string `json:"SecurityGroups"`
}

func (h *Handler) handleCreateMountTarget(c *echo.Context, body []byte) error {
	var in createMountTargetBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("BadRequest", "invalid request body"))
	}

	if in.FileSystemID == "" {
		return c.JSON(http.StatusBadRequest, errResp("BadRequest", "FileSystemId is required"))
	}

	if in.SubnetID == "" {
		return c.JSON(http.StatusBadRequest, errResp("BadRequest", "SubnetId is required"))
	}

	req := CreateMountTargetRequest(in)

	mt, err := h.Backend.CreateMountTarget(h.contextWithRegion(c), req)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, mtToResponse(mt))
}

func (h *Handler) handleDescribeMountTargets(c *echo.Context, mountTargetID string) error {
	// AccessPointId is a mutually exclusive filter: resolve it to the file system
	// the access point belongs to, then list mount targets for that file system.
	if apID := c.Request().URL.Query().Get("AccessPointId"); apID != "" {
		ctx := h.contextWithRegion(c)
		aps, _, err := h.Backend.DescribeAccessPoints(ctx, "", apID, "", 1)
		if err != nil {
			return h.handleError(c, err)
		}
		if len(aps) == 0 {
			return h.handleError(c, ErrAccessPointNotFound)
		}
		fsID := aps[0].FileSystemID
		marker := c.Request().URL.Query().Get("Marker")
		maxItems := queryInt(c, "MaxItems", defaultMaxItems)
		results, nextMarker, err := h.Backend.DescribeMountTargets(ctx, fsID, "", marker, maxItems)
		if err != nil {
			return h.handleError(c, err)
		}
		items := make([]map[string]any, 0, len(results))
		for _, mt := range results {
			items = append(items, mtToResponse(mt))
		}
		resp := map[string]any{"MountTargets": items}
		if nextMarker != "" {
			resp["NextMarker"] = nextMarker
		}

		return c.JSON(http.StatusOK, resp)
	}

	return describeListResponse(
		c, h,
		h.Backend.DescribeMountTargets, mtToResponse,
		mountTargetID, "MountTargetId", "Marker", "MaxItems", "MountTargets", "NextMarker",
	)
}

func (h *Handler) handleDeleteMountTarget(c *echo.Context, mountTargetID string) error {
	if err := h.Backend.DeleteMountTarget(h.contextWithRegion(c), mountTargetID); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func mtToResponse(mt *MountTarget) map[string]any {
	resp := map[string]any{
		"MountTargetId":        mt.MountTargetID,
		"MountTargetArn":       mt.MountTargetArn,
		keyFileSystemID:        mt.FileSystemID,
		"SubnetId":             mt.SubnetID,
		keyLifeCycleState:      mt.LifeCycleState,
		"IpAddress":            mt.IPAddress,
		keyOwnerID:             mt.OwnerID,
		"NetworkInterfaceId":   mt.NetworkInterfaceID,
		"VpcId":                mt.VpcID,
		"AvailabilityZoneName": mt.AvailabilityZoneName,
		"AvailabilityZoneId":   mt.AvailabilityZoneID,
	}
	if len(mt.SecurityGroups) > 0 {
		resp["SecurityGroups"] = mt.SecurityGroups
	}

	return resp
}
