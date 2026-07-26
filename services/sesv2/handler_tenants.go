package sesv2

import (
	"encoding/json"
	"fmt"

	"github.com/labstack/echo/v5"
)

// Tenant handlers.
//
// The real SES v2 tenant API is RPC-style: every operation is a POST to a
// fixed verb path (see parseTenantPath in handler_routes.go) with all
// identifying fields (TenantName, ResourceArn) carried in the JSON body, not
// as REST path parameters. Confirmed against
// aws-sdk-go-v2/service/sesv2 v1.60.1's serializers.go
// (awsRestjson1_serializeOpGetTenant etc. -- httpbinding.SplitURI paths have
// no {Placeholder} segments for this family at all).

func decodeSESv2Body(c *echo.Context, v any) error {
	if err := json.NewDecoder(c.Request().Body).Decode(v); err != nil {
		return fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	return nil
}

type createTenantInput struct {
	TenantName string     `json:"TenantName"`
	Tags       []tagEntry `json:"Tags"`
}

func (h *Handler) handleCreateTenant(c *echo.Context) (any, error) {
	var in createTenantInput
	if err := decodeSESv2Body(c, &in); err != nil {
		return nil, err
	}

	result, err := h.Backend.CreateTenant(in.TenantName, tagsFromEntries(in.Tags))
	if err != nil {
		return nil, err
	}

	return result, nil
}

type tenantNameInput struct {
	TenantName string `json:"TenantName"`
}

// handleGetTenant serves POST /v2/email/tenants/get.
func (h *Handler) handleGetTenant(c *echo.Context) (any, error) {
	var in tenantNameInput
	if err := decodeSESv2Body(c, &in); err != nil {
		return nil, err
	}

	result, err := h.Backend.GetTenant(in.TenantName)
	if err != nil {
		return nil, err
	}

	return map[string]any{"Tenant": result}, nil
}

// handleDeleteTenant serves POST /v2/email/tenants/delete.
func (h *Handler) handleDeleteTenant(c *echo.Context) (any, error) {
	var in tenantNameInput
	if err := decodeSESv2Body(c, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.DeleteTenant(in.TenantName); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

type listTenantsInput struct {
	NextToken string `json:"NextToken"`
	PageSize  int32  `json:"PageSize"`
}

// handleListTenants serves POST /v2/email/tenants/list.
func (h *Handler) handleListTenants(c *echo.Context) (any, error) {
	var in listTenantsInput
	if err := decodeSESv2Body(c, &in); err != nil {
		return nil, err
	}

	items, next, err := h.Backend.ListTenants(in.NextToken, int(in.PageSize))
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"Tenants":    items,
		keyNextToken: next,
	}, nil
}

type tenantResourceAssociationInput struct {
	TenantName  string `json:"TenantName"`
	ResourceArn string `json:"ResourceArn"`
}

// handleCreateTenantResourceAssociation serves POST /v2/email/tenants/resources.
func (h *Handler) handleCreateTenantResourceAssociation(c *echo.Context) (any, error) {
	var in tenantResourceAssociationInput
	if err := decodeSESv2Body(c, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.CreateTenantResourceAssociation(in.TenantName, in.ResourceArn); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

// handleDeleteTenantResourceAssociation serves POST /v2/email/tenants/resources/delete.
func (h *Handler) handleDeleteTenantResourceAssociation(c *echo.Context) (any, error) {
	var in tenantResourceAssociationInput
	if err := decodeSESv2Body(c, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.DeleteTenantResourceAssociation(in.TenantName, in.ResourceArn); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

type putTenantSuppressionAttributesInput struct {
	TenantName        string   `json:"TenantName"`
	SuppressionScope  string   `json:"SuppressionScope"`
	SuppressedReasons []string `json:"SuppressedReasons"`
}

// handlePutTenantSuppressionAttributes serves POST /v2/email/tenant/suppression
// -- note the singular "tenant" top-level segment, distinct from the rest of
// the tenant family's plural "tenants" (see parseTenantSuppressionPath).
func (h *Handler) handlePutTenantSuppressionAttributes(c *echo.Context) (any, error) {
	var in putTenantSuppressionAttributesInput
	if err := decodeSESv2Body(c, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.PutTenantSuppressionAttributes(
		in.TenantName, in.SuppressedReasons, in.SuppressionScope,
	); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

type listResourceTenantsInput struct {
	ResourceArn string `json:"ResourceArn"`
	NextToken   string `json:"NextToken"`
	PageSize    int32  `json:"PageSize"`
}

// handleListResourceTenants serves POST /v2/email/resources/tenants/list.
func (h *Handler) handleListResourceTenants(c *echo.Context) (any, error) {
	var in listResourceTenantsInput
	if err := decodeSESv2Body(c, &in); err != nil {
		return nil, err
	}

	items, next, err := h.Backend.ListResourceTenants(in.ResourceArn, in.NextToken, int(in.PageSize))
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"ResourceTenants": items,
		keyNextToken:      next,
	}, nil
}

type listTenantResourcesInput struct {
	TenantName string            `json:"TenantName"`
	Filter     map[string]string `json:"Filter"`
	NextToken  string            `json:"NextToken"`
	PageSize   int32             `json:"PageSize"`
}

// handleListTenantResources serves POST /v2/email/tenants/resources/list.
func (h *Handler) handleListTenantResources(c *echo.Context) (any, error) {
	var in listTenantResourcesInput
	if err := decodeSESv2Body(c, &in); err != nil {
		return nil, err
	}

	items, next, err := h.Backend.ListTenantResources(in.TenantName, in.Filter, in.NextToken, int(in.PageSize))
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"TenantResources": items,
		keyNextToken:      next,
	}, nil
}
