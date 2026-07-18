package efs

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleDescribeBackupPolicy(c *echo.Context, fileSystemID string) error {
	status, err := h.Backend.DescribeBackupPolicy(h.contextWithRegion(c), fileSystemID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"BackupPolicy": map[string]any{
			"Status": status,
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
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("BadRequest", "invalid request body"))
	}

	if err := h.Backend.PutBackupPolicy(h.contextWithRegion(c), fileSystemID, in.BackupPolicy.Status); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"BackupPolicy": map[string]any{
			"Status": in.BackupPolicy.Status,
		},
	})
}
