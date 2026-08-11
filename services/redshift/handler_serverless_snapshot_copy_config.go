package redshift

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *ServerlessHandler) handleCreateSnapshotCopyConfigurationSL(c *echo.Context, body []byte) error {
	var req struct {
		NamespaceName           string `json:"namespaceName"`
		DestinationRegion       string `json:"destinationRegion"`
		DestinationKmsKeyID     string `json:"destinationKmsKeyId"`
		SnapshotRetentionPeriod int    `json:"snapshotRetentionPeriod"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return slBadRequest(c, "invalid request body")
	}

	if req.NamespaceName == "" || req.DestinationRegion == "" {
		return slBadRequest(c, "namespaceName and destinationRegion are required")
	}

	cfg, err := h.Backend.CreateSnapshotCopyConfigurationSL(
		req.NamespaceName, req.DestinationRegion, req.DestinationKmsKeyID, req.SnapshotRetentionPeriod,
	)
	if err != nil {
		return slHandleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{slRespSnapshotCopyConfig: cfg})
}

func (h *ServerlessHandler) handleUpdateSnapshotCopyConfigurationSL(c *echo.Context, body []byte) error {
	var req struct {
		SnapshotCopyConfigurationID string `json:"snapshotCopyConfigurationId"`
		SnapshotRetentionPeriod     int    `json:"snapshotRetentionPeriod"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return slBadRequest(c, "invalid request body")
	}

	if req.SnapshotCopyConfigurationID == "" {
		return slBadRequest(c, "snapshotCopyConfigurationId is required")
	}

	cfg, err := h.Backend.UpdateSnapshotCopyConfigurationSL(
		req.SnapshotCopyConfigurationID, req.SnapshotRetentionPeriod,
	)
	if err != nil {
		return slHandleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{slRespSnapshotCopyConfig: cfg})
}

func (h *ServerlessHandler) handleDeleteSnapshotCopyConfigurationSL(c *echo.Context, body []byte) error {
	var req struct {
		SnapshotCopyConfigurationID string `json:"snapshotCopyConfigurationId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return slBadRequest(c, "invalid request body")
	}

	if req.SnapshotCopyConfigurationID == "" {
		return slBadRequest(c, "snapshotCopyConfigurationId is required")
	}

	cfg, err := h.Backend.DeleteSnapshotCopyConfigurationSL(req.SnapshotCopyConfigurationID)
	if err != nil {
		return slHandleErr(c, err)
	}

	// DeleteSnapshotCopyConfigurationResponse echoes the deleted object
	// (confirmed against service-2.json's required "snapshotCopyConfiguration"
	// member), unlike most other serverless Delete* responses.
	return c.JSON(http.StatusOK, map[string]any{slRespSnapshotCopyConfig: cfg})
}

func (h *ServerlessHandler) handleListSnapshotCopyConfigurationsSL(c *echo.Context, body []byte) error {
	var req struct {
		NamespaceName string `json:"namespaceName"`
		NextToken     string `json:"nextToken"`
		MaxResults    int    `json:"maxResults"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return slBadRequest(c, "invalid request body")
		}
	}

	list, outToken := h.Backend.ListSnapshotCopyConfigurationsSL(req.NamespaceName, req.MaxResults, req.NextToken)
	resp := map[string]any{"snapshotCopyConfigurations": list}

	if outToken != "" {
		resp["nextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}
