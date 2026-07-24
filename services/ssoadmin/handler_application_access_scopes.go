package ssoadmin

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleDeleteApplicationAccessScope(c *echo.Context, body []byte) error {
	var req struct {
		ApplicationArn string `json:"ApplicationArn"`
		Scope          string `json:"Scope"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	if err := h.Backend.DeleteApplicationAccessScope(req.ApplicationArn, req.Scope); err != nil {
		return handleBackendError(c, err, "scope not found: "+req.Scope)
	}

	return writeJSON(c, http.StatusOK, map[string]any{})
}

func (h *Handler) handleListApplicationAccessScopes(c *echo.Context, body []byte) error {
	var req struct {
		ApplicationArn string `json:"ApplicationArn"`
		NextToken      string `json:"NextToken"`
		MaxResults     int    `json:"MaxResults"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	scopes, err := h.Backend.ListApplicationAccessScopes(req.ApplicationArn)
	if err != nil {
		return handleBackendError(c, err, "application not found: "+req.ApplicationArn)
	}

	page, next := paginateBy(scopes, req.MaxResults, req.NextToken, func(s ScopeDetails) string {
		return s.Scope
	})

	return writeJSON(c, http.StatusOK, map[string]any{
		"Scopes":     page,
		keyNextToken: next,
	})
}

func (h *Handler) handlePutApplicationAccessScope(c *echo.Context, body []byte) error {
	var req struct {
		ApplicationArn    string   `json:"ApplicationArn"`
		Scope             string   `json:"Scope"`
		AuthorizedTargets []string `json:"AuthorizedTargets"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	if err := h.Backend.PutApplicationAccessScope(req.ApplicationArn, req.Scope, req.AuthorizedTargets); err != nil {
		return handleBackendError(c, err, "application not found: "+req.ApplicationArn)
	}

	return writeJSON(c, http.StatusOK, map[string]any{})
}

func (h *Handler) handleGetApplicationAccessScope(c *echo.Context, body []byte) error {
	var req struct {
		ApplicationArn string `json:"ApplicationArn"`
		Scope          string `json:"Scope"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	if req.ApplicationArn == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", "ApplicationArn is required")
	}
	if req.Scope == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", "Scope is required")
	}

	authorizedTargets, err := h.Backend.GetApplicationAccessScope(req.ApplicationArn, req.Scope)
	if err != nil {
		return handleBackendError(c, err, "scope not found: "+req.Scope)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"Scope":             req.Scope,
		"AuthorizedTargets": authorizedTargets,
	})
}
