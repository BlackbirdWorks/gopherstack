package ssoadmin

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

type managedPolicyView struct {
	Arn  string `json:"Arn"`
	Name string `json:"Name"`
}

func (h *Handler) handleAttachManagedPolicyToPermissionSet(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn      string `json:"InstanceArn"`
		PermissionSetArn string `json:"PermissionSetArn"`
		ManagedPolicyArn string `json:"ManagedPolicyArn"`
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

	if req.ManagedPolicyArn == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", "ManagedPolicyArn is required")
	}

	name := req.ManagedPolicyArn
	parts := strings.Split(req.ManagedPolicyArn, "/")
	if len(parts) > 0 {
		name = parts[len(parts)-1]
	}

	if err := h.Backend.AttachManagedPolicyToPermissionSet(
		req.InstanceArn,
		req.PermissionSetArn,
		req.ManagedPolicyArn,
		name,
	); err != nil {
		return handleBackendError(c, err, "permission set not found: "+req.PermissionSetArn)
	}

	return writeJSON(c, http.StatusOK, map[string]any{})
}

func (h *Handler) handleDetachManagedPolicyFromPermissionSet(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn      string `json:"InstanceArn"`
		PermissionSetArn string `json:"PermissionSetArn"`
		ManagedPolicyArn string `json:"ManagedPolicyArn"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	if err := h.Backend.DetachManagedPolicyFromPermissionSet(
		req.InstanceArn,
		req.PermissionSetArn,
		req.ManagedPolicyArn,
	); err != nil {
		return handleBackendError(c, err, "permission set not found: "+req.PermissionSetArn)
	}

	return writeJSON(c, http.StatusOK, map[string]any{})
}

func (h *Handler) handleListManagedPoliciesInPermissionSet(c *echo.Context, body []byte) error {
	return listPermissionSetSubItems(
		c, body,
		func(instanceArn, permissionSetArn string) ([]managedPolicyView, error) {
			policies, err := h.Backend.ListManagedPoliciesInPermissionSet(instanceArn, permissionSetArn)
			if err != nil {
				return nil, err
			}

			views := make([]managedPolicyView, 0, len(policies))
			for _, mp := range policies {
				views = append(views, managedPolicyView(mp))
			}

			return views, nil
		},
		func(v managedPolicyView) string { return v.Arn },
		"AttachedManagedPolicies",
	)
}

type customerManagedPolicyReferenceView struct {
	Name string `json:"Name"`
	Path string `json:"Path,omitempty"`
}

func (h *Handler) handleAttachCustomerManagedPolicyReferenceToPermissionSet(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn                    string                             `json:"InstanceArn"`
		PermissionSetArn               string                             `json:"PermissionSetArn"`
		CustomerManagedPolicyReference customerManagedPolicyReferenceView `json:"CustomerManagedPolicyReference"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	if err := h.Backend.AttachCustomerManagedPolicyReferenceToPermissionSet(
		req.InstanceArn,
		req.PermissionSetArn,
		req.CustomerManagedPolicyReference.Name,
		req.CustomerManagedPolicyReference.Path,
	); err != nil {
		return handleBackendError(c, err, "permission set not found: "+req.PermissionSetArn)
	}

	return writeJSON(c, http.StatusOK, map[string]any{})
}

func (h *Handler) handleListCustomerManagedPolicyReferencesInPermissionSet(c *echo.Context, body []byte) error {
	return listPermissionSetSubItems(
		c, body,
		func(instanceArn, permissionSetArn string) ([]customerManagedPolicyReferenceView, error) {
			refs, err := h.Backend.ListCustomerManagedPolicyReferencesInPermissionSet(instanceArn, permissionSetArn)
			if err != nil {
				return nil, err
			}

			out := make([]customerManagedPolicyReferenceView, 0, len(refs))
			for _, ref := range refs {
				out = append(out, customerManagedPolicyReferenceView(ref))
			}

			return out, nil
		},
		func(v customerManagedPolicyReferenceView) string { return v.Name },
		"CustomerManagedPolicyReferences",
	)
}

func (h *Handler) handleDetachCustomerManagedPolicyReferenceFromPermissionSet(
	c *echo.Context,
	body []byte,
) error {
	var req struct {
		InstanceArn                    string                             `json:"InstanceArn"`
		PermissionSetArn               string                             `json:"PermissionSetArn"`
		CustomerManagedPolicyReference customerManagedPolicyReferenceView `json:"CustomerManagedPolicyReference"`
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
	if err := h.Backend.DetachCustomerManagedPolicyReferenceFromPermissionSet(
		req.InstanceArn,
		req.PermissionSetArn,
		req.CustomerManagedPolicyReference.Name,
		req.CustomerManagedPolicyReference.Path,
	); err != nil {
		return handleBackendError(c, err, "customer managed policy reference not found")
	}

	return writeJSON(c, http.StatusOK, map[string]any{})
}
