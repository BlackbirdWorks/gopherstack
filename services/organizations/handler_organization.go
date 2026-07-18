package organizations

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

type createOrganizationRequest struct {
	FeatureSet string `json:"FeatureSet"`
}

type organizationObject struct {
	ID                   string             `json:"Id"`
	ARN                  string             `json:"Arn"`
	FeatureSet           string             `json:"FeatureSet"`
	MasterAccountID      string             `json:"MasterAccountId"`
	MasterAccountARN     string             `json:"MasterAccountArn"`
	MasterAccountEmail   string             `json:"MasterAccountEmail"`
	AvailablePolicyTypes []policyTypeObject `json:"AvailablePolicyTypes,omitempty"`
}

type createOrganizationResponse struct {
	Organization organizationObject `json:"Organization"`
}

type describeOrganizationResponse struct {
	Organization organizationObject `json:"Organization"`
}

// dispatchOrg handles organization-level operations.
func (h *Handler) dispatchOrg(c *echo.Context, op string, body []byte) (bool, error) {
	switch op {
	case "CreateOrganization":
		return true, h.handleCreateOrganization(c, body)
	case "DescribeOrganization":
		return true, h.handleDescribeOrganization(c, body)
	case "DeleteOrganization":
		return true, h.handleDeleteOrganization(c, body)
	}

	return false, nil
}

// ----------------------------------------
// Organization handlers
// ----------------------------------------

func (h *Handler) handleCreateOrganization(c *echo.Context, body []byte) error {
	var req createOrganizationRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	org, _, err := h.Backend.CreateOrganization(req.FeatureSet)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createOrganizationResponse{
		Organization: toOrganizationObject(org),
	})
}

func (h *Handler) handleDescribeOrganization(c *echo.Context, _ []byte) error {
	org, err := h.Backend.DescribeOrganization()
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, describeOrganizationResponse{
		Organization: toOrganizationObject(org),
	})
}

func (h *Handler) handleDeleteOrganization(c *echo.Context, _ []byte) error {
	if err := h.Backend.DeleteOrganization(); err != nil {
		return h.handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

// ----------------------------------------
// Conversion helpers
// ----------------------------------------

func toOrganizationObject(org *Organization) organizationObject {
	var pts []policyTypeObject
	if len(org.AvailablePolicyTypes) > 0 {
		pts = make([]policyTypeObject, 0, len(org.AvailablePolicyTypes))
		for _, pt := range org.AvailablePolicyTypes {
			pts = append(pts, policyTypeObject(pt))
		}
	}

	return organizationObject{
		AvailablePolicyTypes: pts,
		ID:                   org.ID,
		ARN:                  org.ARN,
		FeatureSet:           org.FeatureSet,
		MasterAccountID:      org.MasterAccountID,
		MasterAccountARN:     org.MasterAccountARN,
		MasterAccountEmail:   org.MasterAccountEmail,
	}
}
