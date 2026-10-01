package kinesisvideo

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *Handler) buildStorageOps() map[string]handlerFunc {
	return map[string]handlerFunc{
		pathDescribeStreamStorageConfiguration: h.handleDescribeStreamStorageConfiguration,
		pathUpdateStreamStorageConfiguration:   h.handleUpdateStreamStorageConfiguration,
		pathDescribeMediaStorageConfiguration:  h.handleDescribeMediaStorageConfiguration,
		pathUpdateMediaStorageConfiguration:    h.handleUpdateMediaStorageConfiguration,
		pathGetSignalingChannelEndpoint:        h.handleGetSignalingChannelEndpoint,
	}
}

func (h *Handler) handleDescribeStreamStorageConfiguration(c *echo.Context, body []byte) error {
	var req streamStorageConfigRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidArgumentException", "invalid request body")
	}

	s, err := h.Backend.DescribeStreamStorageConfiguration(req.StreamName, req.StreamARN)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return h.writeJSON(c, describeStreamStorageConfigurationResponse{
		StreamARN:                  s.ARN,
		StreamName:                 s.Name,
		StreamStorageConfiguration: &streamStorageConfigurationDTO{DefaultStorageTier: s.DefaultStorageTier},
	})
}

func (h *Handler) handleUpdateStreamStorageConfiguration(c *echo.Context, body []byte) error {
	var req streamStorageConfigRequest
	if err := json.Unmarshal(body, &req); err != nil || req.StreamStorageConfiguration == nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidArgumentException", "invalid request body")
	}

	err := h.Backend.UpdateStreamStorageConfiguration(
		req.StreamName, req.StreamARN, req.CurrentVersion, req.StreamStorageConfiguration.DefaultStorageTier)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return h.writeJSON(c, struct{}{})
}

func (h *Handler) handleDescribeMediaStorageConfiguration(c *echo.Context, body []byte) error {
	var req mediaStorageConfigRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidArgumentException", "invalid request body")
	}

	cfg, err := h.Backend.DescribeMediaStorageConfiguration(req.ChannelName, req.ChannelARN)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	var dto *mediaStorageConfigurationDTO
	if cfg != nil {
		dto = &mediaStorageConfigurationDTO{Status: cfg.Status, StreamARN: cfg.StreamARN}
	}

	return h.writeJSON(c, describeMediaStorageConfigurationResponse{MediaStorageConfiguration: dto})
}

func (h *Handler) handleUpdateMediaStorageConfiguration(c *echo.Context, body []byte) error {
	var req mediaStorageConfigRequest
	if err := json.Unmarshal(body, &req); err != nil || req.MediaStorageConfiguration == nil || req.ChannelARN == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidArgumentException", "invalid request body")
	}

	cfg := MediaStorage{
		Status:    req.MediaStorageConfiguration.Status,
		StreamARN: req.MediaStorageConfiguration.StreamARN,
	}

	if err := h.Backend.UpdateMediaStorageConfiguration(req.ChannelARN, cfg); err != nil {
		return h.writeBackendError(c, err)
	}

	return h.writeJSON(c, struct{}{})
}

func (h *Handler) handleGetSignalingChannelEndpoint(c *echo.Context, body []byte) error {
	var req getSignalingChannelEndpointRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidArgumentException", "invalid request body")
	}

	var role string

	var protocols []string
	if sm := req.EndpointConfig; sm != nil {
		role = sm.Role
		protocols = sm.Protocols
	}

	eps, err := h.Backend.GetSignalingChannelEndpoint(
		req.ChannelARN, role, regionFromRequest(c, h.DefaultRegion), protocols)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	list := make([]resourceEndpointListItemDTO, 0, len(eps))
	for _, ep := range eps {
		list = append(list, resourceEndpointListItemDTO{Protocol: ep.Protocol, ResourceEndpoint: ep.Endpoint})
	}

	return h.writeJSON(c, getSignalingChannelEndpointResponse{ResourceEndpointList: list})
}
