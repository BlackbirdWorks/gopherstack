package vpclattice

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// ------- SNSA handlers -------

func (h *Handler) handleCreateSNSA(c *echo.Context, body map[string]any) error {
	snID, _ := body["serviceNetworkIdentifier"].(string)
	svcID, _ := body["serviceIdentifier"].(string)

	if snID == "" || svcID == "" {
		return c.JSON(
			http.StatusBadRequest,
			map[string]any{
				keyMessage: "serviceNetworkIdentifier and serviceIdentifier are required",
			},
		)
	}

	ctx := c.Request().Context()
	tags := extractTags(body)

	assoc, err := h.Backend.CreateServiceNetworkServiceAssociation(ctx, snID, svcID, tags)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusCreated, snsaToJSON(assoc))
}

func (h *Handler) handleGetSNSA(c *echo.Context, id string) error {
	assoc, err := h.Backend.GetServiceNetworkServiceAssociation(id)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, snsaToJSON(assoc))
}

func (h *Handler) handleDeleteSNSA(c *echo.Context, id string) error {
	if err := h.Backend.DeleteServiceNetworkServiceAssociation(id); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusAccepted, map[string]any{keyStatus: statusDeleteInProgress})
}

func (h *Handler) handleListSNSAs(c *echo.Context) error {
	ctx := c.Request().Context()
	maxResults := queryInt32(c)
	nextToken := c.QueryParam("nextToken")
	snID := c.QueryParam("serviceNetworkIdentifier")
	svcID := c.QueryParam("serviceIdentifier")

	items, next, err := h.Backend.ListServiceNetworkServiceAssociations(
		ctx,
		snID,
		svcID,
		maxResults,
		nextToken,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	summaries := make([]any, 0, len(items))
	for _, s := range items {
		summaries = append(summaries, snsaSummaryToJSON(s))
	}

	resp := map[string]any{keyItems: summaries}
	if next != "" {
		resp["nextToken"] = next
	}

	return c.JSON(http.StatusOK, resp)
}

// ------- SNVA handlers -------

func (h *Handler) handleCreateSNVA(c *echo.Context, body map[string]any) error {
	snID, _ := body["serviceNetworkIdentifier"].(string)
	vpcID, _ := body["vpcIdentifier"].(string)

	if snID == "" || vpcID == "" {
		return c.JSON(
			http.StatusBadRequest,
			map[string]any{keyMessage: "serviceNetworkIdentifier and vpcIdentifier are required"},
		)
	}

	var sgs []string
	if sgRaw, ok := body["securityGroupIds"].([]any); ok {
		for _, v := range sgRaw {
			if s, ok2 := v.(string); ok2 {
				sgs = append(sgs, s)
			}
		}
	}

	privateDNSEnabled, _ := body[keyPrivateDNSEnabled].(bool)

	ctx := c.Request().Context()
	tags := extractTags(body)

	assoc, err := h.Backend.CreateServiceNetworkVpcAssociation(ctx, snID, vpcID, sgs, privateDNSEnabled, tags)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusCreated, snvaToJSON(assoc))
}

func (h *Handler) handleGetSNVA(c *echo.Context, id string) error {
	assoc, err := h.Backend.GetServiceNetworkVpcAssociation(id)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, snvaToJSON(assoc))
}

func (h *Handler) handleUpdateSNVA(c *echo.Context, id string, body map[string]any) error {
	var sgs []string
	if sgRaw, ok := body["securityGroupIds"].([]any); ok {
		for _, v := range sgRaw {
			if s, ok2 := v.(string); ok2 {
				sgs = append(sgs, s)
			}
		}
	}

	assoc, err := h.Backend.UpdateServiceNetworkVpcAssociation(id, sgs)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, snvaToJSON(assoc))
}

func (h *Handler) handleDeleteSNVA(c *echo.Context, id string) error {
	if err := h.Backend.DeleteServiceNetworkVpcAssociation(id); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusAccepted, map[string]any{keyStatus: statusDeleteInProgress})
}

func (h *Handler) handleListSNVAs(c *echo.Context) error {
	ctx := c.Request().Context()
	maxResults := queryInt32(c)
	nextToken := c.QueryParam("nextToken")
	snID := c.QueryParam("serviceNetworkIdentifier")
	vpcID := c.QueryParam("vpcIdentifier")

	items, next, err := h.Backend.ListServiceNetworkVpcAssociations(
		ctx,
		snID,
		vpcID,
		maxResults,
		nextToken,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	summaries := make([]any, 0, len(items))
	for _, s := range items {
		summaries = append(summaries, snvaSummaryToJSON(s))
	}

	resp := map[string]any{keyItems: summaries}
	if next != "" {
		resp["nextToken"] = next
	}

	return c.JSON(http.StatusOK, resp)
}

// ------- SNSA/SNVA JSON serialization -------

func snsaToJSON(s *ServiceNetworkServiceAssociation) map[string]any {
	m := map[string]any{
		keyARN:                s.ARN,
		"id":                  s.ID,
		keyServiceARN:         s.ServiceARN,
		keyServiceID:          s.ServiceID,
		"serviceName":         s.ServiceName,
		keyServiceNetworkARN:  s.ServiceNetworkARN,
		keyServiceNetworkID:   s.ServiceNetworkID,
		keyServiceNetworkName: s.ServiceNetworkName,
		keyStatus:             s.Status,
		"createdBy":           s.CreatedBy,
		keyCreatedAt:          s.CreatedAt.Format("2006-01-02T15:04:05.000Z"),
	}

	if s.CustomDomainName != "" {
		m["customDomainName"] = s.CustomDomainName
	}

	if s.DNSName != "" {
		m["dnsEntry"] = dnsEntryToJSON(s.DNSName, s.HostedZoneID)
	}

	return m
}

func snsaSummaryToJSON(s *ServiceNetworkServiceAssociationSummary) map[string]any {
	m := map[string]any{
		keyARN:                s.ARN,
		"id":                  s.ID,
		keyServiceARN:         s.ServiceARN,
		keyServiceID:          s.ServiceID,
		"serviceName":         s.ServiceName,
		keyServiceNetworkARN:  s.ServiceNetworkARN,
		keyServiceNetworkID:   s.ServiceNetworkID,
		keyServiceNetworkName: s.ServiceNetworkName,
		keyStatus:             s.Status,
		keyCreatedAt:          s.CreatedAt.Format("2006-01-02T15:04:05.000Z"),
	}

	if s.CustomDomainName != "" {
		m["customDomainName"] = s.CustomDomainName
	}

	if s.DNSName != "" {
		m["dnsEntry"] = dnsEntryToJSON(s.DNSName, s.HostedZoneID)
	}

	return m
}

// dnsEntryToJSON builds the wire shape of a VPC Lattice DnsEntry
// (domainName + hostedZoneId), shared by Service and
// ServiceNetworkServiceAssociation responses.
func dnsEntryToJSON(domainName, hostedZoneID string) map[string]any {
	m := map[string]any{keyDomainName: domainName}
	if hostedZoneID != "" {
		m[keyHostedZoneID] = hostedZoneID
	}

	return m
}

func snvaToJSON(s *ServiceNetworkVpcAssociation) map[string]any {
	sgs := make([]string, len(s.SecurityGroupIDs))
	copy(sgs, s.SecurityGroupIDs)

	return map[string]any{
		keyARN:                s.ARN,
		"id":                  s.ID,
		keyVPCID:              s.VpcID,
		keyServiceNetworkARN:  s.ServiceNetworkARN,
		keyServiceNetworkID:   s.ServiceNetworkID,
		keyServiceNetworkName: s.ServiceNetworkName,
		"securityGroupIds":    sgs,
		keyStatus:             s.Status,
		"createdBy":           s.CreatedBy,
		keyPrivateDNSEnabled:  s.PrivateDNSEnabled,
		keyCreatedAt:          s.CreatedAt.Format("2006-01-02T15:04:05.000Z"),
		keyLastUpdatedAt:      s.LastUpdatedAt.Format("2006-01-02T15:04:05.000Z"),
	}
}

func snvaSummaryToJSON(s *ServiceNetworkVpcAssociationSummary) map[string]any {
	return map[string]any{
		keyARN:                s.ARN,
		"id":                  s.ID,
		keyVPCID:              s.VpcID,
		keyServiceNetworkARN:  s.ServiceNetworkARN,
		keyServiceNetworkID:   s.ServiceNetworkID,
		keyServiceNetworkName: s.ServiceNetworkName,
		keyStatus:             s.Status,
		keyPrivateDNSEnabled:  s.PrivateDNSEnabled,
		keyCreatedAt:          s.CreatedAt.Format("2006-01-02T15:04:05.000Z"),
	}
}
