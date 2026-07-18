package ssoadmin

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handlePutInlinePolicyToPermissionSet(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn      string `json:"InstanceArn"`
		PermissionSetArn string `json:"PermissionSetArn"`
		InlinePolicy     string `json:"InlinePolicy"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	if err := h.Backend.PutInlinePolicyToPermissionSet(
		req.InstanceArn,
		req.PermissionSetArn,
		req.InlinePolicy,
	); err != nil {
		return handleBackendError(c, err, "permission set not found: "+req.PermissionSetArn)
	}

	return writeJSON(c, http.StatusOK, map[string]any{})
}

func (h *Handler) handleGetInlinePolicyForPermissionSet(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn      string `json:"InstanceArn"`
		PermissionSetArn string `json:"PermissionSetArn"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	policy, err := h.Backend.GetInlinePolicyForPermissionSet(req.InstanceArn, req.PermissionSetArn)
	if err != nil {
		return handleBackendError(c, err, "permission set not found: "+req.PermissionSetArn)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"InlinePolicy": policy,
	})
}

func (h *Handler) handleDeleteInlinePolicyFromPermissionSet(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn      string `json:"InstanceArn"`
		PermissionSetArn string `json:"PermissionSetArn"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	if err := h.Backend.DeleteInlinePolicyFromPermissionSet(req.InstanceArn, req.PermissionSetArn); err != nil {
		return handleBackendError(c, err, "permission set not found: "+req.PermissionSetArn)
	}

	return writeJSON(c, http.StatusOK, map[string]any{})
}
