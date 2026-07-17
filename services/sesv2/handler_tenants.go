package sesv2

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"github.com/labstack/echo/v5"
)

// tenant handlers

type createTenantInput struct {
	TenantName string `json:"TenantName"`
}

func (h *Handler) handleCreateTenant(c *echo.Context) (any, error) {
	var in createTenantInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	result, err := h.Backend.CreateTenant(in.TenantName)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (h *Handler) handleGetTenant(tenantName string) (any, error) {
	result, err := h.Backend.GetTenant(tenantName)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (h *Handler) handleDeleteTenant(tenantName string) (any, error) {
	if err := h.Backend.DeleteTenant(tenantName); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

func (h *Handler) handleListTenants(c *echo.Context) (any, error) {
	nextToken := c.QueryParam("NextToken")

	items, next, err := h.Backend.ListTenants(nextToken, 0)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"Tenants":    items,
		keyNextToken: next,
	}, nil
}

type createTenantResourceAssociationInput struct {
	ResourceArn string `json:"ResourceArn"`
}

func (h *Handler) handleCreateTenantResourceAssociation(
	c *echo.Context,
	tenantName string,
) (any, error) {
	var in createTenantResourceAssociationInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	if err := h.Backend.CreateTenantResourceAssociation(tenantName, in.ResourceArn); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

func (h *Handler) handleDeleteTenantResourceAssociation(
	c *echo.Context,
	tenantName string,
) (any, error) {
	segments := strings.Split(strings.TrimPrefix(c.Request().URL.Path, sesv2PathPrefix), "/")
	resourceArn := ""

	if len(segments) >= 4 { //nolint:mnd // URL segment index is self-documenting in context
		resourceArn = segments[3]

		if decoded, err := url.PathUnescape(resourceArn); err == nil {
			resourceArn = decoded
		}
	}

	if err := h.Backend.DeleteTenantResourceAssociation(tenantName, resourceArn); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

func (h *Handler) handleListResourceTenants(c *echo.Context) (any, error) {
	nextToken := c.QueryParam("NextToken")

	items, next, err := h.Backend.ListResourceTenants(nextToken, 0)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"ResourceTenants": items,
		keyNextToken:      next,
	}, nil
}

// handleListTenantResources serves GET .../tenants/{tenantName}/resources.
//
// NOTE (not fixed here -- out of scope for this mechanical file split):
// NextToken is read from the query string but then discarded (`_ = nextToken`)
// and Backend.ListTenantResources is called without it, since the
// StorageBackend interface's ListTenantResources(tenantName string, pageSize int)
// signature has no token parameter at all. This is a real pagination gap.
func (h *Handler) handleListTenantResources(c *echo.Context, tenantName string) (any, error) {
	nextToken := c.QueryParam("NextToken")

	items, next, err := h.Backend.ListTenantResources(tenantName, 0)
	_ = nextToken

	if err != nil {
		return nil, err
	}

	return map[string]any{
		"TenantResources": items,
		keyNextToken:      next,
	}, nil
}
