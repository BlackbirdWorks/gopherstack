package organizations

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

type enableAWSServiceAccessRequest struct {
	ServicePrincipal string `json:"ServicePrincipal"`
}

type disableAWSServiceAccessRequest struct {
	ServicePrincipal string `json:"ServicePrincipal"`
}

type listAWSServiceAccessRequest struct {
	NextToken  string `json:"NextToken,omitempty"`
	MaxResults int    `json:"MaxResults,omitempty"`
}

type enabledServicePrincipalObject struct {
	ServicePrincipal string  `json:"ServicePrincipal"`
	DateEnabled      float64 `json:"DateEnabled"`
}

type listAWSServiceAccessResponse struct {
	NextToken                string                          `json:"NextToken,omitempty"`
	EnabledServicePrincipals []enabledServicePrincipalObject `json:"EnabledServicePrincipals"`
}

// dispatchServiceAccess handles AWS service access operations.
func (h *Handler) dispatchServiceAccess(c *echo.Context, op string, body []byte) (bool, error) {
	switch op {
	case "EnableAWSServiceAccess":
		return true, h.handleEnableAWSServiceAccess(c, body)
	case "DisableAWSServiceAccess":
		return true, h.handleDisableAWSServiceAccess(c, body)
	case "ListAWSServiceAccessForOrganization":
		return true, h.handleListAWSServiceAccessForOrganization(c, body)
	}

	return false, nil
}

// ----------------------------------------
// Service access handlers
// ----------------------------------------

func (h *Handler) handleEnableAWSServiceAccess(c *echo.Context, body []byte) error {
	var req enableAWSServiceAccessRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if err := h.Backend.EnableAWSServiceAccess(req.ServicePrincipal); err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleDisableAWSServiceAccess(c *echo.Context, body []byte) error {
	var req disableAWSServiceAccessRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if err := h.Backend.DisableAWSServiceAccess(req.ServicePrincipal); err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleListAWSServiceAccessForOrganization(c *echo.Context, body []byte) error {
	var req listAWSServiceAccessRequest
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
		}
	}

	sps, err := h.Backend.ListAWSServiceAccessForOrganization()
	if err != nil {
		return h.handleBackendError(c, err)
	}

	objs := make([]enabledServicePrincipalObject, 0, len(sps))
	for _, sp := range sps {
		objs = append(objs, enabledServicePrincipalObject{
			DateEnabled:      epochSeconds(sp.DateEnabled),
			ServicePrincipal: sp.ServicePrincipal,
		})
	}

	p := page.New(objs, req.NextToken, req.MaxResults, defaultMaxResults)

	return c.JSON(http.StatusOK, listAWSServiceAccessResponse{EnabledServicePrincipals: p.Data, NextToken: p.Next})
}
