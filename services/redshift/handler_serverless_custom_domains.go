package redshift

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

// Create/Get/UpdateCustomDomainAssociationResponse all serialize the
// association's fields directly at the top level -- NOT wrapped in an
// envelope key like Namespace/Workgroup/Snapshot's responses (confirmed
// against those three Response shapes in service-2.json: their "members" are
// customDomainCertificateArn/customDomainCertificateExpiryTime/
// customDomainName/workgroupName themselves, no wrapper). So
// *ServerlessCustomDomainAssociation is marshaled as the response body as-is.

func (h *ServerlessHandler) handleCreateCustomDomainAssociation(c *echo.Context, body []byte) error {
	var req struct {
		CustomDomainCertificateArn string `json:"customDomainCertificateArn"`
		CustomDomainName           string `json:"customDomainName"`
		WorkgroupName              string `json:"workgroupName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return slBadRequest(c, "invalid request body")
	}

	if req.CustomDomainName == "" || req.CustomDomainCertificateArn == "" || req.WorkgroupName == "" {
		return slBadRequest(c, "customDomainName, customDomainCertificateArn and workgroupName are required")
	}

	assoc, err := h.Backend.CreateCustomDomainAssociationSL(
		req.CustomDomainName, req.CustomDomainCertificateArn, req.WorkgroupName,
	)
	if err != nil {
		return slHandleErr(c, err)
	}

	return c.JSON(http.StatusOK, assoc)
}

func (h *ServerlessHandler) handleGetCustomDomainAssociation(c *echo.Context, body []byte) error {
	var req struct {
		CustomDomainName string `json:"customDomainName"`
		WorkgroupName    string `json:"workgroupName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return slBadRequest(c, "invalid request body")
	}

	assoc, err := h.Backend.GetCustomDomainAssociationSL(req.CustomDomainName, req.WorkgroupName)
	if err != nil {
		return slHandleErr(c, err)
	}

	return c.JSON(http.StatusOK, assoc)
}

func (h *ServerlessHandler) handleListCustomDomainAssociations(c *echo.Context, body []byte) error {
	var req struct {
		CustomDomainCertificateArn string `json:"customDomainCertificateArn"`
		CustomDomainName           string `json:"customDomainName"`
		NextToken                  string `json:"nextToken"`
		MaxResults                 int    `json:"maxResults"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return slBadRequest(c, "invalid request body")
		}
	}

	list, outToken := h.Backend.ListCustomDomainAssociationsSL(
		req.CustomDomainName, req.CustomDomainCertificateArn, req.MaxResults, req.NextToken,
	)
	resp := map[string]any{"associations": list}

	if outToken != "" {
		resp["nextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *ServerlessHandler) handleUpdateCustomDomainAssociation(c *echo.Context, body []byte) error {
	var req struct {
		CustomDomainCertificateArn string `json:"customDomainCertificateArn"`
		CustomDomainName           string `json:"customDomainName"`
		WorkgroupName              string `json:"workgroupName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return slBadRequest(c, "invalid request body")
	}

	if req.CustomDomainName == "" || req.WorkgroupName == "" || req.CustomDomainCertificateArn == "" {
		return slBadRequest(c, "customDomainName, workgroupName and customDomainCertificateArn are required")
	}

	assoc, err := h.Backend.UpdateCustomDomainAssociationSL(
		req.CustomDomainName, req.WorkgroupName, req.CustomDomainCertificateArn,
	)
	if err != nil {
		return slHandleErr(c, err)
	}

	return c.JSON(http.StatusOK, assoc)
}

func (h *ServerlessHandler) handleDeleteCustomDomainAssociation(c *echo.Context, body []byte) error {
	var req struct {
		CustomDomainName string `json:"customDomainName"`
		WorkgroupName    string `json:"workgroupName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return slBadRequest(c, "invalid request body")
	}

	if _, err := h.Backend.DeleteCustomDomainAssociationSL(req.CustomDomainName, req.WorkgroupName); err != nil {
		return slHandleErr(c, err)
	}

	// DeleteCustomDomainAssociationResponse has zero members (confirmed
	// against service-2.json), unlike the other serverless Delete* ops.
	return c.JSON(http.StatusOK, map[string]any{})
}
