package efs

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

type createAccessPointBody struct {
	PosixUser     *PosixUser     `json:"PosixUser"`
	RootDirectory *RootDirectory `json:"RootDirectory"`
	FileSystemID  string         `json:"FileSystemId"`
	ClientToken   string         `json:"ClientToken"`
	Tags          []tagEntry     `json:"Tags"`
}

func (h *Handler) handleCreateAccessPoint(c *echo.Context, body []byte) error {
	var in createAccessPointBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("BadRequest", "invalid request body"))
	}

	if in.FileSystemID == "" {
		return c.JSON(http.StatusBadRequest, errResp("BadRequest", "FileSystemId is required"))
	}

	req := CreateAccessPointRequest{
		FileSystemID:  in.FileSystemID,
		ClientToken:   in.ClientToken,
		Tags:          tagsFromEntries(in.Tags),
		PosixUser:     in.PosixUser,
		RootDirectory: in.RootDirectory,
	}

	ap, err := h.Backend.CreateAccessPoint(h.contextWithRegion(c), req)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, apToResponse(ap))
}

func (h *Handler) handleDescribeAccessPoints(c *echo.Context, accessPointID string) error {
	return describeListResponse(
		c, h,
		h.Backend.DescribeAccessPoints, apToResponse,
		accessPointID, "AccessPointId", "NextToken", "MaxResults", "AccessPoints", "NextToken",
	)
}

func (h *Handler) handleDeleteAccessPoint(c *echo.Context, accessPointID string) error {
	if err := h.Backend.DeleteAccessPoint(h.contextWithRegion(c), accessPointID); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func apToResponse(ap *AccessPoint) map[string]any {
	resp := map[string]any{
		"AccessPointId":   ap.AccessPointID,
		"AccessPointArn":  ap.AccessPointArn,
		keyFileSystemID:   ap.FileSystemID,
		keyLifeCycleState: ap.LifeCycleState,
		keyOwnerID:        ap.OwnerID,
		keyTags:           tagsToEntries(ap.Tags.Clone()),
	}
	if ap.Name != "" {
		resp["Name"] = ap.Name
	}
	if ap.ClientToken != "" {
		resp["ClientToken"] = ap.ClientToken
	}
	if ap.PosixUser != nil {
		resp["PosixUser"] = ap.PosixUser
	}
	if ap.RootDirectory != nil {
		resp["RootDirectory"] = ap.RootDirectory
	}

	return resp
}
