package ssoadmin

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleDeleteApplicationGrant(c *echo.Context, body []byte) error {
	var req struct {
		ApplicationArn string `json:"ApplicationArn"`
		GrantType      string `json:"GrantType"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	if err := h.Backend.DeleteApplicationGrant(req.ApplicationArn, req.GrantType); err != nil {
		return handleBackendError(c, err, "application grant not found")
	}

	return writeJSON(c, http.StatusOK, map[string]any{})
}

func (h *Handler) handleListApplicationGrants(c *echo.Context, body []byte) error {
	var req struct {
		ApplicationArn string `json:"ApplicationArn"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	grants, err := h.Backend.ListApplicationGrants(req.ApplicationArn)
	if err != nil {
		return handleBackendError(c, err, "application not found: "+req.ApplicationArn)
	}

	out := make([]map[string]any, 0, len(grants))
	for _, grant := range grants {
		out = append(out, map[string]any{
			"GrantType": grant.GrantType,
			keyGrant:    grant.Grant,
		})
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"Grants":     out,
		keyNextToken: nil,
	})
}

func (h *Handler) handlePutApplicationGrant(c *echo.Context, body []byte) error {
	var req struct {
		ApplicationArn string          `json:"ApplicationArn"`
		GrantType      string          `json:"GrantType"`
		Grant          json.RawMessage `json:"Grant"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	if err := h.Backend.PutApplicationGrant(req.ApplicationArn, req.GrantType, req.Grant); err != nil {
		return handleBackendError(c, err, "application not found: "+req.ApplicationArn)
	}

	return writeJSON(c, http.StatusOK, map[string]any{})
}

func (h *Handler) handleGetApplicationGrant(c *echo.Context, body []byte) error {
	var req struct {
		ApplicationArn string `json:"ApplicationArn"`
		GrantType      string `json:"GrantType"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	if req.ApplicationArn == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", "ApplicationArn is required")
	}
	if req.GrantType == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", "GrantType is required")
	}

	grantBody, err := h.Backend.GetApplicationGrant(req.ApplicationArn, req.GrantType)
	if err != nil {
		return handleBackendError(c, err, "grant not found: "+req.GrantType)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyGrant: map[string]any{
			"GrantType": req.GrantType,
			keyGrant:    grantBody,
		},
	})
}
