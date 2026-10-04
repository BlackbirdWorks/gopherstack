package kinesisvideo

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *Handler) buildEdgeOps() map[string]handlerFunc {
	return map[string]handlerFunc{
		pathStartEdgeConfigurationUpdate: h.handleStartEdgeConfigurationUpdate,
		pathDescribeEdgeConfiguration:    h.handleDescribeEdgeConfiguration,
		pathDeleteEdgeConfiguration:      h.handleDeleteEdgeConfiguration,
		pathListEdgeAgentConfigurations:  h.handleListEdgeAgentConfigurations,
	}
}

func (h *Handler) handleStartEdgeConfigurationUpdate(c *echo.Context, body []byte) error {
	var req edgeStreamRequest
	if err := json.Unmarshal(body, &req); err != nil || req.EdgeConfig == nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidArgumentException", "invalid request body")
	}

	st, err := h.Backend.StartEdgeConfigurationUpdate(req.StreamName, req.StreamARN, edgeConfigFromDTO(req.EdgeConfig))
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return h.writeJSON(c, edgeStateToResponse(st))
}

func (h *Handler) handleDescribeEdgeConfiguration(c *echo.Context, body []byte) error {
	var req edgeStreamRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidArgumentException", "invalid request body")
	}

	st, err := h.Backend.DescribeEdgeConfiguration(req.StreamName, req.StreamARN)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return h.writeJSON(c, edgeStateToResponse(st))
}

func (h *Handler) handleDeleteEdgeConfiguration(c *echo.Context, body []byte) error {
	var req edgeStreamRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidArgumentException", "invalid request body")
	}

	if err := h.Backend.DeleteEdgeConfiguration(req.StreamName, req.StreamARN); err != nil {
		return h.writeBackendError(c, err)
	}

	return h.writeJSON(c, struct{}{})
}

func (h *Handler) handleListEdgeAgentConfigurations(c *echo.Context, body []byte) error {
	var req listEdgeAgentConfigurationsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidArgumentException", "invalid request body")
	}

	items, next, err := h.Backend.ListEdgeAgentConfigurations(req.HubDeviceArn, req.NextToken, int(req.MaxResults))
	if err != nil {
		return h.writeBackendError(c, err)
	}

	out := make([]edgeStateResponse, 0, len(items))
	for _, it := range items {
		out = append(out, edgeStateToResponse(it))
	}

	return h.writeJSON(c, listEdgeAgentConfigurationsResponse{EdgeConfigs: out, NextToken: next})
}
