package vpclattice

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// ------- ResourceConfiguration handlers -------

func (h *Handler) handleCreateResourceConfiguration(c *echo.Context, body map[string]any) error {
	name, _ := body[keyName].(string)
	resourceType, _ := body[keyType].(string)

	if name == "" || resourceType == "" {
		return validationError(c, "name and type are required")
	}

	protocol, _ := body[keyProtocol].(string)
	rgID, _ := body["resourceGatewayIdentifier"].(string)
	groupID, _ := body["resourceConfigurationGroupIdentifier"].(string)
	allowShare, _ := body["allowAssociationToShareableServiceNetwork"].(bool)
	portRanges := bodyStringSlice(body, "portRanges")
	definition := extractResourceConfigurationDefinition(body)
	customDomainName, _ := body["customDomainName"].(string)
	domainVerificationID, _ := body["domainVerificationIdentifier"].(string)
	groupDomain, _ := body["groupDomain"].(string)

	ctx := c.Request().Context()
	tags := extractTags(body)

	rc, err := h.Backend.CreateResourceConfiguration(
		ctx, name, resourceType, protocol, rgID, groupID, allowShare, portRanges, definition,
		customDomainName, domainVerificationID, groupDomain, tags,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusCreated, resourceConfigurationToJSON(rc))
}

func (h *Handler) handleGetResourceConfiguration(c *echo.Context, id string) error {
	rc, err := h.Backend.GetResourceConfiguration(id)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, resourceConfigurationToJSON(rc))
}

func (h *Handler) handleUpdateResourceConfiguration(c *echo.Context, id string, body map[string]any) error {
	var allowShare *bool
	if v, ok := body["allowAssociationToShareableServiceNetwork"].(bool); ok {
		allowShare = &v
	}

	var portRanges []string
	if _, ok := body["portRanges"]; ok {
		portRanges = bodyStringSlice(body, "portRanges")
		if portRanges == nil {
			portRanges = []string{}
		}
	}

	definition := extractResourceConfigurationDefinition(body)

	rc, err := h.Backend.UpdateResourceConfiguration(id, allowShare, portRanges, definition)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, resourceConfigurationToJSON(rc))
}

func (h *Handler) handleDeleteResourceConfiguration(c *echo.Context, id string) error {
	if err := h.Backend.DeleteResourceConfiguration(id); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleListResourceConfigurations(c *echo.Context) error {
	ctx := c.Request().Context()
	maxResults := queryInt32(c)
	nextToken := c.QueryParam("nextToken")
	rgID := c.QueryParam("resourceGatewayIdentifier")
	groupID := c.QueryParam("resourceConfigurationGroupIdentifier")

	items, next, err := h.Backend.ListResourceConfigurations(ctx, rgID, groupID, maxResults, nextToken)
	if err != nil {
		return h.handleError(c, err)
	}

	summaries := make([]any, 0, len(items))
	for _, rc := range items {
		summaries = append(summaries, resourceConfigurationSummaryToJSON(rc))
	}

	resp := map[string]any{keyItems: summaries}
	if next != "" {
		resp["nextToken"] = next
	}

	return c.JSON(http.StatusOK, resp)
}

// resourceConfigurationSummaryToJSON builds a ListResourceConfigurations item.
func resourceConfigurationSummaryToJSON(rc *ResourceConfigurationSummary) map[string]any {
	m := map[string]any{
		keyARN:              rc.ARN,
		"id":                rc.ID,
		keyName:             rc.Name,
		keyType:             rc.Type,
		keyStatus:           rc.Status,
		"resourceGatewayId": rc.ResourceGatewayID,
		"amazonManaged":     rc.AmazonManaged,
		keyCreatedAt:        rc.CreatedAt.UTC().Format("2006-01-02T15:04:05.000Z"),
		keyLastUpdatedAt:    rc.LastUpdatedAt.UTC().Format("2006-01-02T15:04:05.000Z"),
	}

	if rc.ResourceConfigurationGroupID != "" {
		m["resourceConfigurationGroupId"] = rc.ResourceConfigurationGroupID
	}

	if rc.CustomDomainName != "" {
		m["customDomainName"] = rc.CustomDomainName
	}

	if rc.GroupDomain != "" {
		m["groupDomain"] = rc.GroupDomain
	}

	if rc.DomainVerificationID != "" {
		m["domainVerificationId"] = rc.DomainVerificationID
	}

	return m
}

// ------- ResourceConfiguration JSON serialization -------

func resourceConfigurationToJSON(rc *ResourceConfiguration) map[string]any {
	m := map[string]any{
		keyARN:       rc.ARN,
		"id":         rc.ID,
		keyName:      rc.Name,
		keyType:      rc.Type,
		keyStatus:    rc.Status,
		keyProtocol:  rc.Protocol,
		"portRanges": rc.PortRanges,
		"allowAssociationToShareableServiceNetwork": rc.AllowShareableAssoc,
		"amazonManaged":  rc.AmazonManaged,
		keyCreatedAt:     rc.CreatedAt.UTC().Format("2006-01-02T15:04:05.000Z"),
		keyLastUpdatedAt: rc.LastUpdatedAt.UTC().Format("2006-01-02T15:04:05.000Z"),
	}

	if rc.ResourceGatewayID != "" {
		m["resourceGatewayId"] = rc.ResourceGatewayID
	}

	if rc.ResourceConfigurationGroupID != "" {
		m["resourceConfigurationGroupId"] = rc.ResourceConfigurationGroupID
	}

	if rc.CustomDomainName != "" {
		m["customDomainName"] = rc.CustomDomainName
	}

	if rc.GroupDomain != "" {
		m["groupDomain"] = rc.GroupDomain
	}

	if rc.DomainVerificationID != "" {
		m["domainVerificationId"] = rc.DomainVerificationID
	}

	if rc.DomainVerificationARN != "" {
		m["domainVerificationArn"] = rc.DomainVerificationARN
	}

	if rc.DomainVerificationStatus != "" {
		m["domainVerificationStatus"] = rc.DomainVerificationStatus
	}

	if rc.FailureReason != "" {
		m["failureReason"] = rc.FailureReason
	}

	if def := resourceConfigurationDefinitionToJSON(rc.Definition); def != nil {
		m["resourceConfigurationDefinition"] = def
	}

	return m
}

// extractResourceConfigurationDefinition parses the
// resourceConfigurationDefinition wire union
// (types.ResourceConfigurationDefinition: arnResource{arn} |
// dnsResource{domainName,ipAddressType} | ipResource{ipAddress}).
func extractResourceConfigurationDefinition(body map[string]any) *ResourceConfigurationDefinition {
	raw, ok := body["resourceConfigurationDefinition"].(map[string]any)
	if !ok {
		return nil
	}

	if arnRes, isArn := raw[defKindArnResource].(map[string]any); isArn {
		v, _ := arnRes["arn"].(string)

		return &ResourceConfigurationDefinition{Kind: defKindArnResource, ArnValue: v}
	}

	if dnsRes, isDNS := raw[defKindDNSResource].(map[string]any); isDNS {
		domain, _ := dnsRes["domainName"].(string)
		ipType, _ := dnsRes[keyIPAddressType].(string)

		return &ResourceConfigurationDefinition{Kind: defKindDNSResource, DomainName: domain, IPAddressType: ipType}
	}

	if ipRes, isIP := raw[defKindIPResource].(map[string]any); isIP {
		v, _ := ipRes["ipAddress"].(string)

		return &ResourceConfigurationDefinition{Kind: defKindIPResource, IPAddress: v}
	}

	return nil
}

func resourceConfigurationDefinitionToJSON(d *ResourceConfigurationDefinition) map[string]any {
	if d == nil {
		return nil
	}

	switch d.Kind {
	case defKindArnResource:
		return map[string]any{defKindArnResource: map[string]any{"arn": d.ArnValue}}
	case defKindDNSResource:
		return map[string]any{
			defKindDNSResource: map[string]any{"domainName": d.DomainName, keyIPAddressType: d.IPAddressType},
		}
	case defKindIPResource:
		return map[string]any{defKindIPResource: map[string]any{"ipAddress": d.IPAddress}}
	default:
		return nil
	}
}
