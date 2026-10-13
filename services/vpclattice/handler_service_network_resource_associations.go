package vpclattice

import (
	"net/http"
	"time"

	"github.com/labstack/echo/v5"
)

const (
	keyResourceConfigurationARN  = "resourceConfigurationArn"
	keyResourceConfigurationID   = "resourceConfigurationId"
	keyResourceConfigurationName = "resourceConfigurationName"
	keyVpcEndpointID             = "vpcEndpointId"
	keyState                     = "state"
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

	assoc, err := idemCreate(h, "CreateServiceNetworkResourceAssociation", "", body,
		func(a *ServiceNetworkResourceAssociation) string { return a.ID },
		h.Backend.GetServiceNetworkResourceAssociation,
		func() (*ServiceNetworkResourceAssociation, error) {
			return h.Backend.CreateServiceNetworkResourceAssociation(ctx, snID, rcID, privateDNSEnabled, tags)
		})
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
	includeChildren := c.QueryParam("includeChildren") == "true"

	items, next, err := h.Backend.ListServiceNetworkResourceAssociations(
		ctx, snID, rcID, includeChildren, maxResults, nextToken,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	summaries := make([]any, 0, len(items))
	for _, s := range items {
		m := map[string]any{
			keyARN:                       s.ARN,
			"id":                         s.ID,
			keyResourceConfigurationARN:  s.ResourceConfigurationARN,
			keyResourceConfigurationID:   s.ResourceConfigurationID,
			keyResourceConfigurationName: s.ResourceConfigurationName,
			keyServiceNetworkARN:         s.ServiceNetworkARN,
			keyServiceNetworkID:          s.ServiceNetworkID,
			keyServiceNetworkName:        s.ServiceNetworkName,
			keyStatus:                    s.Status,
			keyCreatedBy:                 s.CreatedBy,
			keyPrivateDNSEnabled:         s.PrivateDNSEnabled,
			keyIsManagedAssoc:            false,
			keyCreatedAt:                 s.CreatedAt.UTC().Format("2006-01-02T15:04:05.000Z"),
		}
		addPrivateDNSEntry(m, s.PrivateDNSDomain, s.PrivateDNSHostedZoneID)
		summaries = append(summaries, m)
	}

	resp := map[string]any{keyItems: summaries}
	if next != "" {
		resp["nextToken"] = next
	}

	return c.JSON(http.StatusOK, resp)
}

// keyIsManagedAssoc is always false: every association here is caller-created.
const keyIsManagedAssoc = "isManagedAssociation"

func addPrivateDNSEntry(m map[string]any, domain, hostedZoneID string) {
	if domain != "" {
		m["privateDnsEntry"] = dnsEntryToJSON(domain, hostedZoneID)
	}
}

func snraToJSON(s *ServiceNetworkResourceAssociation) map[string]any {
	m := map[string]any{
		keyARN:                       s.ARN,
		"id":                         s.ID,
		keyResourceConfigurationARN:  s.ResourceConfigurationARN,
		keyResourceConfigurationID:   s.ResourceConfigurationID,
		keyResourceConfigurationName: s.ResourceConfigurationName,
		keyServiceNetworkARN:         s.ServiceNetworkARN,
		keyServiceNetworkID:          s.ServiceNetworkID,
		keyServiceNetworkName:        s.ServiceNetworkName,
		keyStatus:                    s.Status,
		keyCreatedBy:                 s.CreatedBy,
		keyPrivateDNSEnabled:         s.PrivateDNSEnabled,
		keyIsManagedAssoc:            false,
		keyCreatedAt:                 s.CreatedAt.UTC().Format("2006-01-02T15:04:05.000Z"),
		keyLastUpdatedAt:             s.LastUpdatedAt.UTC().Format("2006-01-02T15:04:05.000Z"),
	}
	addPrivateDNSEntry(m, s.PrivateDNSDomain, s.PrivateDNSHostedZoneID)

	return m
}

// ------- ResourceEndpointAssociation / ServiceNetworkVpcEndpointAssociation handlers -------

func formatTime(t time.Time) string { return t.UTC().Format("2006-01-02T15:04:05.000Z") }

func (h *Handler) handleListResourceEndpointAssociations(c *echo.Context) error {
	ctx := c.Request().Context()
	filter := ResourceEndpointAssociationFilter{
		ResourceConfigurationIdentifier:       c.QueryParam("resourceConfigurationIdentifier"),
		ResourceEndpointAssociationIdentifier: c.QueryParam("resourceEndpointAssociationIdentifier"),
		VpcEndpointID:                         c.QueryParam(keyVpcEndpointID),
		VpcEndpointOwner:                      c.QueryParam("vpcEndpointOwner"),
	}

	items, next, err := h.Backend.ListResourceEndpointAssociations(
		ctx,
		filter,
		queryInt32(c),
		c.QueryParam("nextToken"),
	)
	if err != nil {
		return h.handleError(c, err)
	}

	out := make([]any, 0, len(items))
	for _, a := range items {
		out = append(out, map[string]any{
			keyARN:                       a.ARN,
			"id":                         a.ID,
			keyCreatedAt:                 formatTime(a.CreatedAt),
			keyResourceConfigurationARN:  a.ResourceConfigurationARN,
			keyResourceConfigurationID:   a.ResourceConfigurationID,
			keyResourceConfigurationName: a.ResourceConfigurationName,
			keyVpcEndpointID:             a.VpcEndpointID,
			"vpcEndpointOwner":           a.VpcEndpointOwner,
		})
	}

	resp := map[string]any{keyItems: out}
	if next != "" {
		resp["nextToken"] = next
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleDeleteResourceEndpointAssociation(c *echo.Context, id string) error {
	a, err := h.Backend.DeleteResourceEndpointAssociation(c.Request().Context(), id)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyARN: a.ARN, "id": a.ID, keyResourceConfigurationARN: a.ResourceConfigurationARN,
		keyResourceConfigurationID: a.ResourceConfigurationID, keyVpcEndpointID: a.VpcEndpointID,
	})
}

func (h *Handler) handleListServiceNetworkVpcEndpointAssociations(c *echo.Context) error {
	ctx := c.Request().Context()
	snID := c.QueryParam("serviceNetworkIdentifier")

	items, next, err := h.Backend.ListServiceNetworkVpcEndpointAssociations(
		ctx,
		snID,
		queryInt32(c),
		c.QueryParam("nextToken"),
	)
	if err != nil {
		return h.handleError(c, err)
	}

	out := make([]any, 0, len(items))
	for _, a := range items {
		out = append(out, map[string]any{
			"id": a.ID, keyCreatedAt: formatTime(a.CreatedAt), keyServiceNetworkARN: a.ServiceNetworkARN,
			keyState: a.State, keyVpcEndpointID: a.VpcEndpointID, "vpcId": a.VpcID,
			"vpcEndpointOwnerId": a.VpcEndpointOwner,
		})
	}

	resp := map[string]any{keyItems: out}
	if next != "" {
		resp["nextToken"] = next
	}

	return c.JSON(http.StatusOK, resp)
}
