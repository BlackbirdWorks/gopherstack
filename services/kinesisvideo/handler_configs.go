package kinesisvideo

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *Handler) buildConfigOps() map[string]handlerFunc {
	return map[string]handlerFunc{
		pathDescribeImageGenerationConfiguration: h.handleDescribeImageGenerationConfiguration,
		pathUpdateImageGenerationConfiguration:   h.handleUpdateImageGenerationConfiguration,
		pathDescribeNotificationConfiguration:    h.handleDescribeNotificationConfiguration,
		pathUpdateNotificationConfiguration:      h.handleUpdateNotificationConfiguration,
	}
}

func (h *Handler) handleDescribeImageGenerationConfiguration(c *echo.Context, body []byte) error {
	var req describeImageGenerationConfigurationRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidArgumentException", "invalid request body")
	}

	cfg, err := h.Backend.DescribeImageGenerationConfiguration(req.StreamName, req.StreamARN)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return h.writeJSON(c, describeImageGenerationConfigurationResponse{
		ImageGenerationConfiguration: imageGenerationConfigToDTO(cfg),
	})
}

func (h *Handler) handleUpdateImageGenerationConfiguration(c *echo.Context, body []byte) error {
	var req updateImageGenerationConfigurationRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidArgumentException", "invalid request body")
	}

	cfg := imageGenerationConfigFromDTO(req.ImageGenerationConfiguration)

	if err := h.Backend.UpdateImageGenerationConfiguration(req.StreamName, req.StreamARN, cfg); err != nil {
		return h.writeBackendError(c, err)
	}

	return h.writeJSON(c, struct{}{})
}

func (h *Handler) handleDescribeNotificationConfiguration(c *echo.Context, body []byte) error {
	var req describeNotificationConfigurationRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidArgumentException", "invalid request body")
	}

	cfg, err := h.Backend.DescribeNotificationConfiguration(req.StreamName, req.StreamARN)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return h.writeJSON(c, describeNotificationConfigurationResponse{
		NotificationConfiguration: notificationConfigToDTO(cfg),
	})
}

func (h *Handler) handleUpdateNotificationConfiguration(c *echo.Context, body []byte) error {
	var req updateNotificationConfigurationRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidArgumentException", "invalid request body")
	}

	cfg := notificationConfigFromDTO(req.NotificationConfiguration)

	if err := h.Backend.UpdateNotificationConfiguration(req.StreamName, req.StreamARN, cfg); err != nil {
		return h.writeBackendError(c, err)
	}

	return h.writeJSON(c, struct{}{})
}
