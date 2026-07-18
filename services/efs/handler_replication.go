package efs

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

type createReplicationConfigBody struct {
	Destinations []ReplicationDestination `json:"Destinations"`
}

func (h *Handler) handleCreateReplicationConfiguration(
	c *echo.Context,
	fileSystemID string,
	body []byte,
) error {
	var in createReplicationConfigBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("BadRequest", "invalid request body"))
	}

	if fileSystemID == "" {
		return c.JSON(http.StatusBadRequest, errResp("BadRequest", "FileSystemId is required"))
	}

	rc, err := h.Backend.CreateReplicationConfiguration(
		h.contextWithRegion(c),
		fileSystemID,
		in.Destinations,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, rcToResponse(rc))
}

func (h *Handler) handleDeleteReplicationConfiguration(c *echo.Context, fileSystemID string) error {
	if err := h.Backend.DeleteReplicationConfiguration(h.contextWithRegion(c), fileSystemID); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleDescribeReplicationConfigurations(c *echo.Context) error {
	fsID := c.Request().URL.Query().Get(keyFileSystemID)

	rcs, err := h.Backend.DescribeReplicationConfigurations(h.contextWithRegion(c), fsID)
	if err != nil {
		return h.handleError(c, err)
	}

	items := make([]map[string]any, 0, len(rcs))
	for _, rc := range rcs {
		items = append(items, rcToResponse(rc))
	}

	return c.JSON(http.StatusOK, map[string]any{
		"Replications": items,
	})
}

func rcToResponse(rc *ReplicationConfiguration) map[string]any {
	return map[string]any{
		"OriginalSourceFileSystemArn": rc.OriginalSourceFileSystemARN,
		"SourceFileSystemArn":         rc.SourceFileSystemARN,
		"SourceFileSystemId":          rc.SourceFileSystemID,
		"SourceFileSystemOwnerId":     rc.SourceFileSystemOwnerID,
		"SourceFileSystemRegion":      rc.SourceFileSystemRegion,
		"CreationTime":                rc.CreationTime,
		"Destinations":                rc.Destinations,
	}
}

type updateFileSystemProtectionBody struct {
	ReplicationOverwriteProtection string `json:"ReplicationOverwriteProtection"`
}

func (h *Handler) handleUpdateFileSystemProtection(
	c *echo.Context,
	fileSystemID string,
	body []byte,
) error {
	var in updateFileSystemProtectionBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("BadRequest", "invalid request body"))
	}

	if err := h.Backend.UpdateFileSystemProtection(
		h.contextWithRegion(c), fileSystemID, in.ReplicationOverwriteProtection,
	); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"ReplicationOverwriteProtection": in.ReplicationOverwriteProtection,
	})
}
