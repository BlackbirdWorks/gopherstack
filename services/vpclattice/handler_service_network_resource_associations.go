package vpclattice

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// ------- ServiceNetworkResourceAssociation handlers -------

func (h *Handler) handleCreateSNRA(c *echo.Context, body map[string]any) error {
	snID, _ := body["serviceNetworkIdentifier"].(string)
	rcID, _ := body["resourceConfigurationIdentifier"].(string)

	if snID == "" || rcID == "" {
		return c.JSON(
			http.StatusBadRequest,
			map[string]any{
				keyMessage: "serviceNetworkIdentifier and resourceConfigurationIdentifier are required",
			},
		)
	}

	privateDNSEnabled, _ := body[keyPrivateDNSEnabled].(bool)

	ctx := c.Request().Context()
	tags := extractTags(body)

	assoc, err := h.Backend.CreateServiceNetworkResourceAssociation(ctx, snID, rcID, privateDNSEnabled, tags)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusCreated, snraToJSON(assoc))
}

func (h *Handler) handleGetSNRA(c *echo.Context, id string) error {
	assoc, err := h.Backend.GetServiceNetworkResourceAssociation(id)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, snraToJSON(assoc))
}

func (h *Handler) handleDeleteSNRA(c *echo.Context, id string) error {
	assoc, err := h.Backend.DeleteServiceNetworkResourceAssociation(id)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyARN:    assoc.ARN,
		"id":      assoc.ID,
		keyStatus: assoc.Status,
	})
}

func (h *Handler) handleListSNRAs(c *echo.Context) error {
	ctx := c.Request().Context()
	maxResults := queryInt32(c)
	nextToken := c.QueryParam("nextToken")
	snID := c.QueryParam("serviceNetworkIdentifier")
	rcID := c.QueryParam("resourceConfigurationIdentifier")

	items, next, err := h.Backend.ListServiceNetworkResourceAssociations(ctx, snID, rcID, maxResults, nextToken)
	if err != nil {
		return h.handleError(c, err)
	}

	summaries := make([]any, 0, len(items))
	for _, s := range items {
		summaries = append(summaries, map[string]any{
			keyARN:                      s.ARN,
			"id":                        s.ID,
			"resourceConfigurationArn":  s.ResourceConfigurationARN,
			"resourceConfigurationId":   s.ResourceConfigurationID,
			"resourceConfigurationName": s.ResourceConfigurationName,
			keyServiceNetworkARN:        s.ServiceNetworkARN,
			keyServiceNetworkID:         s.ServiceNetworkID,
			keyServiceNetworkName:       s.ServiceNetworkName,
			keyStatus:                   s.Status,
			keyCreatedAt:                s.CreatedAt.UTC().Format("2006-01-02T15:04:05.000Z"),
		})
	}

	resp := map[string]any{keyItems: summaries}
	if next != "" {
		resp["nextToken"] = next
	}

	return c.JSON(http.StatusOK, resp)
}

func snraToJSON(s *ServiceNetworkResourceAssociation) map[string]any {
	return map[string]any{
		keyARN:                      s.ARN,
		"id":                        s.ID,
		"resourceConfigurationArn":  s.ResourceConfigurationARN,
		"resourceConfigurationId":   s.ResourceConfigurationID,
		"resourceConfigurationName": s.ResourceConfigurationName,
		keyServiceNetworkARN:        s.ServiceNetworkARN,
		keyServiceNetworkID:         s.ServiceNetworkID,
		keyServiceNetworkName:       s.ServiceNetworkName,
		keyStatus:                   s.Status,
		keyCreatedBy:                s.CreatedBy,
		keyPrivateDNSEnabled:        s.PrivateDNSEnabled,
		keyCreatedAt:                s.CreatedAt.UTC().Format("2006-01-02T15:04:05.000Z"),
		keyLastUpdatedAt:            s.LastUpdatedAt.UTC().Format("2006-01-02T15:04:05.000Z"),
	}
}

// ------- ResourceEndpointAssociation / ServiceNetworkVpcEndpointAssociation handlers -------
//
// See service_network_resource_associations.go's family doc comment: this
// backend never creates either resource (no EC2 VPC-endpoint modeling), so
// List always returns an empty page and Delete always 404s.

func (h *Handler) handleListResourceEndpointAssociations(c *echo.Context) error {
	ctx := c.Request().Context()
	maxResults := queryInt32(c)
	nextToken := c.QueryParam("nextToken")

	items, next, err := h.Backend.ListResourceEndpointAssociations(ctx, maxResults, nextToken)
	if err != nil {
		return h.handleError(c, err)
	}

	resp := map[string]any{keyItems: make([]any, len(items))}
	if next != "" {
		resp["nextToken"] = next
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleDeleteResourceEndpointAssociation(c *echo.Context, id string) error {
	if err := h.Backend.DeleteResourceEndpointAssociation(id); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleListServiceNetworkVpcEndpointAssociations(c *echo.Context) error {
	ctx := c.Request().Context()
	maxResults := queryInt32(c)
	nextToken := c.QueryParam("nextToken")
	snID := c.QueryParam("serviceNetworkIdentifier")

	items, next, err := h.Backend.ListServiceNetworkVpcEndpointAssociations(ctx, snID, maxResults, nextToken)
	if err != nil {
		return h.handleError(c, err)
	}

	resp := map[string]any{keyItems: make([]any, len(items))}
	if next != "" {
		resp["nextToken"] = next
	}

	return c.JSON(http.StatusOK, resp)
}
