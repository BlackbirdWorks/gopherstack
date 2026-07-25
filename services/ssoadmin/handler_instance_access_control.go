package ssoadmin

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleCreateInstanceAccessControlAttributeConfiguration(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn                                 string `json:"InstanceArn"`
		InstanceAccessControlAttributeConfiguration struct {
			AccessControlAttributes []struct {
				Key   string `json:"Key"`
				Value struct {
					Source []string `json:"Source"`
				} `json:"Value"`
			} `json:"AccessControlAttributes"`
		} `json:"InstanceAccessControlAttributeConfiguration"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	attrs := make(
		[]AccessControlAttribute,
		0,
		len(req.InstanceAccessControlAttributeConfiguration.AccessControlAttributes),
	)
	for _, a := range req.InstanceAccessControlAttributeConfiguration.AccessControlAttributes {
		attrs = append(attrs, AccessControlAttribute{
			Key:   a.Key,
			Value: AccessControlAttributeValue{Source: a.Value.Source},
		})
	}

	if err := h.Backend.CreateInstanceAccessControlAttributeConfiguration(req.InstanceArn, attrs); err != nil {
		if errors.Is(err, ErrACAAlreadyExists) {
			return writeError(c, http.StatusBadRequest, "ConflictException",
				"instance access control attribute configuration already exists for: "+req.InstanceArn)
		}

		return handleBackendError(c, err, "instance not found: "+req.InstanceArn)
	}

	return writeJSON(c, http.StatusOK, map[string]any{})
}

func (h *Handler) handleDeleteInstanceAccessControlAttributeConfiguration(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn string `json:"InstanceArn"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	if err := h.Backend.DeleteInstanceAccessControlAttributeConfiguration(req.InstanceArn); err != nil {
		return handleBackendError(c, err, "instance access control attribute configuration not found")
	}

	return writeJSON(c, http.StatusOK, map[string]any{})
}

func (h *Handler) handleDescribeInstanceAccessControlAttributeConfiguration(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn string `json:"InstanceArn"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	cfg, err := h.Backend.DescribeInstanceAccessControlAttributeConfiguration(req.InstanceArn)
	if err != nil {
		return handleBackendError(c, err, "instance access control attribute configuration not found")
	}

	attrs := make([]map[string]any, 0, len(cfg.AccessControlAttributes))
	for _, attr := range cfg.AccessControlAttributes {
		attrs = append(attrs, map[string]any{
			"Key": attr.Key,
			"Value": map[string]any{
				"Source": attr.Value.Source,
			},
		})
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"InstanceAccessControlAttributeConfiguration": map[string]any{
			"AccessControlAttributes": attrs,
		},
		keyStatus:      cfg.Status,
		"StatusReason": cfg.StatusReason,
	})
}

func (h *Handler) handleUpdateInstanceAccessControlAttributeConfiguration(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn                                 string `json:"InstanceArn"`
		InstanceAccessControlAttributeConfiguration struct {
			AccessControlAttributes []struct {
				Key   string `json:"Key"`
				Value struct {
					Source []string `json:"Source"`
				} `json:"Value"`
			} `json:"AccessControlAttributes"`
		} `json:"InstanceAccessControlAttributeConfiguration"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	if req.InstanceArn == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", "InstanceArn is required")
	}

	attrs := make(
		[]AccessControlAttribute,
		0,
		len(req.InstanceAccessControlAttributeConfiguration.AccessControlAttributes),
	)
	for _, a := range req.InstanceAccessControlAttributeConfiguration.AccessControlAttributes {
		attrs = append(attrs, AccessControlAttribute{
			Key:   a.Key,
			Value: AccessControlAttributeValue{Source: a.Value.Source},
		})
	}

	if err := h.Backend.UpdateInstanceAccessControlAttributeConfiguration(req.InstanceArn, attrs); err != nil {
		return handleBackendError(c, err, "instance not found: "+req.InstanceArn)
	}

	return writeJSON(c, http.StatusOK, map[string]any{})
}
