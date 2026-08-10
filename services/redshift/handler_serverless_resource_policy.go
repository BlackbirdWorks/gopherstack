package redshift

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *ServerlessHandler) handleGetResourcePolicySL(c *echo.Context, body []byte) error {
	var req struct {
		ResourceArn string `json:"resourceArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return slBadRequest(c, "invalid request body")
	}

	if req.ResourceArn == "" {
		return slBadRequest(c, "resourceArn is required")
	}

	rp, err := h.Backend.GetResourcePolicySL(req.ResourceArn)
	if err != nil {
		return slHandleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"resourcePolicy": rp})
}

func (h *ServerlessHandler) handlePutResourcePolicySL(c *echo.Context, body []byte) error {
	var req struct {
		ResourceArn string `json:"resourceArn"`
		Policy      string `json:"policy"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return slBadRequest(c, "invalid request body")
	}

	if req.ResourceArn == "" || req.Policy == "" {
		return slBadRequest(c, "resourceArn and policy are required")
	}

	rp, err := h.Backend.PutResourcePolicySL(req.ResourceArn, req.Policy)
	if err != nil {
		return slHandleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"resourcePolicy": rp})
}

func (h *ServerlessHandler) handleDeleteResourcePolicySL(c *echo.Context, body []byte) error {
	var req struct {
		ResourceArn string `json:"resourceArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return slBadRequest(c, "invalid request body")
	}

	if req.ResourceArn == "" {
		return slBadRequest(c, "resourceArn is required")
	}

	if _, err := h.Backend.DeleteResourcePolicySL(req.ResourceArn); err != nil {
		return slHandleErr(c, err)
	}

	// DeleteResourcePolicyResponse has zero members (confirmed against
	// service-2.json), same convention as DeleteCustomDomainAssociationResponse.
	return c.JSON(http.StatusOK, map[string]any{})
}
