package vpclattice

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// ------- ResourceGateway handlers -------

func (h *Handler) handleCreateResourceGateway(c *echo.Context, body map[string]any) error {
	name, _ := body[keyName].(string)
	if name == "" {
		return validationError(c, keyNameRequired)
	}

	vpcID, _ := body[keyVpcIdentifier].(string)
	ipAddressType, _ := body[keyIPAddressType].(string)
	dnsResolution, _ := body[keyResourceConfigDNSResolution].(string)
	ipv4PerENI := bodyInt32(body, keyIpv4AddressesPerENI)
	sgs := bodyStringSlice(body, keySecurityGroupIDs)
	subnets := bodyStringSlice(body, keySubnetIDs)

	ctx := c.Request().Context()
	tags := extractTags(body)

	gw, err := h.Backend.CreateResourceGateway(
		ctx, name, vpcID, ipAddressType, dnsResolution, ipv4PerENI, sgs, subnets, tags,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusCreated, map[string]any{
		keyARN:                         gw.ARN,
		"id":                           gw.ID,
		keyName:                        gw.Name,
		keyVpcIdentifier:               gw.VpcID,
		keyIPAddressType:               gw.IPAddressType,
		keyResourceConfigDNSResolution: gw.ResourceConfigDNSResolution,
		keyIpv4AddressesPerENI:         gw.Ipv4AddressesPerEni,
		keySecurityGroupIDs:            gw.SecurityGroupIDs,
		keySubnetIDs:                   gw.SubnetIDs,
		keyStatus:                      gw.Status,
	})
}

func (h *Handler) handleGetResourceGateway(c *echo.Context, id string) error {
	gw, err := h.Backend.GetResourceGateway(id)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyARN:                         gw.ARN,
		"id":                           gw.ID,
		keyName:                        gw.Name,
		keyVPCID:                       gw.VpcID,
		keyIPAddressType:               gw.IPAddressType,
		keyResourceConfigDNSResolution: gw.ResourceConfigDNSResolution,
		keyIpv4AddressesPerENI:         gw.Ipv4AddressesPerEni,
		keySecurityGroupIDs:            gw.SecurityGroupIDs,
		keySubnetIDs:                   gw.SubnetIDs,
		keyStatus:                      gw.Status,
		keyCreatedAt:                   gw.CreatedAt.UTC().Format("2006-01-02T15:04:05.000Z"),
		keyLastUpdatedAt:               gw.LastUpdatedAt.UTC().Format("2006-01-02T15:04:05.000Z"),
	})
}

func (h *Handler) handleUpdateResourceGateway(c *echo.Context, id string, body map[string]any) error {
	sgs := bodyStringSlice(body, keySecurityGroupIDs)

	gw, err := h.Backend.UpdateResourceGateway(id, sgs)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyARN:              gw.ARN,
		"id":                gw.ID,
		keyName:             gw.Name,
		keyVPCID:            gw.VpcID,
		keyIPAddressType:    gw.IPAddressType,
		keySecurityGroupIDs: gw.SecurityGroupIDs,
		keySubnetIDs:        gw.SubnetIDs,
		keyStatus:           gw.Status,
	})
}

func (h *Handler) handleDeleteResourceGateway(c *echo.Context, id string) error {
	gw, err := h.Backend.DeleteResourceGateway(id)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyARN:    gw.ARN,
		"id":      gw.ID,
		keyName:   gw.Name,
		keyStatus: gw.Status,
	})
}

func (h *Handler) handleListResourceGateways(c *echo.Context) error {
	ctx := c.Request().Context()
	maxResults := queryInt32(c)
	nextToken := c.QueryParam("nextToken")

	items, next, err := h.Backend.ListResourceGateways(ctx, maxResults, nextToken)
	if err != nil {
		return h.handleError(c, err)
	}

	summaries := make([]any, 0, len(items))
	for _, g := range items {
		summaries = append(summaries, map[string]any{
			keyARN:                         g.ARN,
			"id":                           g.ID,
			keyName:                        g.Name,
			keyVpcIdentifier:               g.VpcID,
			keyIPAddressType:               g.IPAddressType,
			keyResourceConfigDNSResolution: g.ResourceConfigDNSResolution,
			keyIpv4AddressesPerENI:         g.Ipv4AddressesPerEni,
			keySecurityGroupIDs:            g.SecurityGroupIDs,
			keySubnetIDs:                   g.SubnetIDs,
			keyStatus:                      g.Status,
			keyCreatedAt:                   g.CreatedAt.UTC().Format("2006-01-02T15:04:05.000Z"),
			keyLastUpdatedAt:               g.LastUpdatedAt.UTC().Format("2006-01-02T15:04:05.000Z"),
		})
	}

	resp := map[string]any{keyItems: summaries}
	if next != "" {
		resp["nextToken"] = next
	}

	return c.JSON(http.StatusOK, resp)
}

// bodyStringSlice reads key off body as a []string, ignoring non-string
// entries.
func bodyStringSlice(body map[string]any, key string) []string {
	raw, ok := body[key].([]any)
	if !ok {
		return nil
	}

	out := make([]string, 0, len(raw))

	for _, v := range raw {
		if s, ok2 := v.(string); ok2 {
			out = append(out, s)
		}
	}

	return out
}
