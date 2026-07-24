package ssoadmin

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleDeleteApplicationAuthenticationMethod(c *echo.Context, body []byte) error {
	var req struct {
		ApplicationArn string `json:"ApplicationArn"`
		AuthMethodType string `json:"AuthenticationMethodType"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	if err := h.Backend.DeleteApplicationAuthenticationMethod(req.ApplicationArn, req.AuthMethodType); err != nil {
		return handleBackendError(c, err, "authentication method not found: "+req.AuthMethodType)
	}

	return writeJSON(c, http.StatusOK, map[string]any{})
}

func (h *Handler) handleListApplicationAuthenticationMethods(c *echo.Context, body []byte) error {
	var req struct {
		ApplicationArn string `json:"ApplicationArn"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	methods, err := h.Backend.ListApplicationAuthenticationMethods(req.ApplicationArn)
	if err != nil {
		return handleBackendError(c, err, "application not found: "+req.ApplicationArn)
	}

	out := make([]map[string]any, 0, len(methods))
	for _, method := range methods {
		out = append(out, map[string]any{
			"AuthenticationMethodType": method.AuthMethodType,
			keyAuthenticationMethod:    method.Body,
		})
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"AuthenticationMethods": out,
		keyNextToken:            nil,
	})
}

func (h *Handler) handlePutApplicationAuthenticationMethod(c *echo.Context, body []byte) error {
	var req struct {
		ApplicationArn       string          `json:"ApplicationArn"`
		AuthMethodType       string          `json:"AuthenticationMethodType"`
		AuthenticationType   string          `json:"AuthenticationType"`
		AuthenticationMethod json.RawMessage `json:"AuthenticationMethod"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	methodType := req.AuthMethodType
	if methodType == "" {
		methodType = req.AuthenticationType
	}
	if err := h.Backend.PutApplicationAuthenticationMethod(
		req.ApplicationArn, methodType, req.AuthenticationMethod,
	); err != nil {
		return handleBackendError(c, err, "application not found: "+req.ApplicationArn)
	}

	return writeJSON(c, http.StatusOK, map[string]any{})
}

func (h *Handler) handleGetApplicationAuthenticationMethod(c *echo.Context, body []byte) error {
	var req struct {
		ApplicationArn           string `json:"ApplicationArn"`
		AuthenticationMethodType string `json:"AuthenticationMethodType"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}
	if req.ApplicationArn == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", "ApplicationArn is required")
	}
	if req.AuthenticationMethodType == "" {
		return writeError(c, http.StatusBadRequest, "ValidationException", "AuthenticationMethodType is required")
	}

	authMethodBody, err := h.Backend.GetApplicationAuthenticationMethod(
		req.ApplicationArn,
		req.AuthenticationMethodType,
	)
	if err != nil {
		return handleBackendError(c, err, "authentication method not found: "+req.AuthenticationMethodType)
	}

	// Real GetApplicationAuthenticationMethodOutput is exactly
	// {AuthenticationMethod: <union>} -- unlike AuthenticationMethodItem (the
	// ListApplicationAuthenticationMethods item shape), it has NO sibling
	// AuthenticationMethodType member alongside the union value. gopherstack
	// previously double-wrapped the stored union body under an extra
	// "AuthenticationMethodType"/"AuthenticationMethod" pair one level too
	// deep, which would prevent a real client's union deserializer from ever
	// finding the "Iam" tag.
	return writeJSON(c, http.StatusOK, map[string]any{
		keyAuthenticationMethod: authMethodBody,
	})
}
