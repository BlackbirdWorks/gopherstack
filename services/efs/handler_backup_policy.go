package efs

import (
	"encoding/json/v2"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

func (h *Handler) handleDescribeBackupPolicy(c *echo.Context, fileSystemID string) error {
	status, err := h.Backend.DescribeBackupPolicy(h.contextWithRegion(c), fileSystemID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"BackupPolicy": map[string]any{
			keyStatus: status,
		},
	})
}

type putBackupPolicyBody struct {
	BackupPolicy struct {
		Status string `json:"Status"`
	} `json:"BackupPolicy"`
}

func (h *Handler) handlePutBackupPolicy(c *echo.Context, fileSystemID string, body []byte) error {
	var in putBackupPolicyBody
	if err := json.Unmarshal(body, &in, jsonx.V1()); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("BadRequest", "invalid request body"))
	}

	if err := h.Backend.PutBackupPolicy(h.contextWithRegion(c), fileSystemID, in.BackupPolicy.Status); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"BackupPolicy": map[string]any{
			keyStatus: in.BackupPolicy.Status,
		},
	})
}
