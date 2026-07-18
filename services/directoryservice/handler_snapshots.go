package directoryservice

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

func (h *Handler) handleCreateSnapshot(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string `json:"DirectoryId"`
		Name        string `json:"Name"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "DirectoryId is required"))
	}

	snap, snapErr := h.Backend.CreateSnapshot(h.contextWithRegion(c), req.DirectoryID, req.Name)
	if snapErr != nil {
		return h.mapError(c, snapErr)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keySnapshotID: snap.SnapshotID,
	})
}

func (h *Handler) handleDeleteSnapshot(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		SnapshotID string `json:"SnapshotId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.SnapshotID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "SnapshotId is required"))
	}

	if delErr := h.Backend.DeleteSnapshot(h.contextWithRegion(c), req.SnapshotID); delErr != nil {
		return h.mapError(c, delErr)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keySnapshotID: req.SnapshotID,
	})
}

func (h *Handler) handleDescribeSnapshots(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string   `json:"DirectoryId"`
		NextToken   string   `json:"NextToken"`
		SnapshotIDs []string `json:"SnapshotIds"`
		Limit       int32    `json:"Limit"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
		}
	}

	snaps, nextToken, listErr := h.Backend.DescribeSnapshots(
		h.contextWithRegion(c),
		req.DirectoryID,
		req.SnapshotIDs,
		req.Limit,
		req.NextToken,
	)
	if listErr != nil {
		return h.mapError(c, listErr)
	}

	snapList := make([]map[string]any, 0, len(snaps))
	for _, s := range snaps {
		snapList = append(snapList, snapshotToJSON(s))
	}

	resp := map[string]any{
		"Snapshots": snapList,
	}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleGetSnapshotLimits(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string `json:"DirectoryId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "DirectoryId is required"))
	}

	limits, limErr := h.Backend.GetSnapshotLimits(h.contextWithRegion(c), req.DirectoryID)
	if limErr != nil {
		return h.mapError(c, limErr)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"SnapshotLimits": map[string]any{
			"ManualSnapshotsCurrentCount": limits.ManualSnapshotsCurrentCount,
			"ManualSnapshotsLimit":        limits.ManualSnapshotsLimit,
			"ManualSnapshotsLimitReached": limits.ManualSnapshotsLimitReached,
		},
	})
}

func (h *Handler) handleRestoreFromSnapshot(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		SnapshotID string `json:"SnapshotId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.SnapshotID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "SnapshotId is required"))
	}

	if restoreErr := h.Backend.RestoreFromSnapshot(h.contextWithRegion(c), req.SnapshotID); restoreErr != nil {
		return h.mapError(c, restoreErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}
