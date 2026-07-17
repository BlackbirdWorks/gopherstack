package ssoadmin

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handlePutPermissionsBoundaryToPermissionSet(c *echo.Context, body []byte) error {
	var req struct {
		PermissionsBoundary struct {
			CustomerManagedPolicyReference *struct {
				Name string `json:"Name"`
				Path string `json:"Path"`
			} `json:"CustomerManagedPolicyReference"`
			ManagedPolicyArn string `json:"ManagedPolicyArn"`
		} `json:"PermissionsBoundary"`
		InstanceArn      string `json:"InstanceArn"`
		PermissionSetArn string `json:"PermissionSetArn"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	boundary := &PermissionsBoundary{
		ManagedPolicyArn: req.PermissionsBoundary.ManagedPolicyArn,
	}
	if req.PermissionsBoundary.CustomerManagedPolicyReference != nil {
		boundary.CustomerManagedPolicyReference = &CustomerManagedPolicyReference{
			Name: req.PermissionsBoundary.CustomerManagedPolicyReference.Name,
			Path: req.PermissionsBoundary.CustomerManagedPolicyReference.Path,
		}
	}

	if err := h.Backend.PutPermissionsBoundaryToPermissionSet(
		req.InstanceArn,
		req.PermissionSetArn,
		boundary,
	); err != nil {
		return handleBackendError(c, err, "permission set not found: "+req.PermissionSetArn)
	}

	return writeJSON(c, http.StatusOK, map[string]any{})
}

func (h *Handler) handleGetPermissionsBoundaryForPermissionSet(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn      string `json:"InstanceArn"`
		PermissionSetArn string `json:"PermissionSetArn"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	boundary, err := h.Backend.GetPermissionsBoundaryForPermissionSet(req.InstanceArn, req.PermissionSetArn)
	if err != nil {
		return handleBackendError(c, err, "permissions boundary not found")
	}

	bView := map[string]any{}
	if boundary.ManagedPolicyArn != "" {
		bView["ManagedPolicyArn"] = boundary.ManagedPolicyArn
	}
	if boundary.CustomerManagedPolicyReference != nil {
		bView["CustomerManagedPolicyReference"] = map[string]any{
			"Name": boundary.CustomerManagedPolicyReference.Name,
			"Path": boundary.CustomerManagedPolicyReference.Path,
		}
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"PermissionsBoundary": bView,
	})
}

func (h *Handler) handleDeletePermissionsBoundaryFromPermissionSet(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn      string `json:"InstanceArn"`
		PermissionSetArn string `json:"PermissionSetArn"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	if req.InstanceArn == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", "InstanceArn is required")
	}
	if req.PermissionSetArn == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", "PermissionSetArn is required")
	}
	if err := h.Backend.DeletePermissionsBoundaryFromPermissionSet(req.InstanceArn, req.PermissionSetArn); err != nil {
		return handleBackendError(c, err, "permissions boundary not found for: "+req.PermissionSetArn)
	}

	return writeJSON(c, http.StatusOK, map[string]any{})
}
