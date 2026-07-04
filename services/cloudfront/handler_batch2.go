package cloudfront

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"
)

// ---------------------------------------------------------------------------
// DistributionTenant XML helpers
// ---------------------------------------------------------------------------

func (h *Handler) distributionTenantXML(t *DistributionTenant) string {
	var domainsXML strings.Builder
	for _, d := range t.Domains {
		fmt.Fprintf(&domainsXML, `<Item><Domain>%s</Domain><Status>Active</Status></Item>`, d)
	}

	webACLArn := h.Backend.TenantWebACLArn(t.ID)

	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<DistributionTenant xmlns="%s">`+
		`<Id>%s</Id>`+
		`<Arn>%s</Arn>`+
		`<DistributionId>%s</DistributionId>`+
		`<Name>%s</Name>`+
		`<Domain>%s</Domain>`+
		`<Domains>%s</Domains>`+
		`<ConnectionGroupId>%s</ConnectionGroupId>`+
		`<Enabled>%v</Enabled>`+
		`<WebACLArn>%s</WebACLArn>`+
		`<Status>%s</Status>`+
		`</DistributionTenant>`,
		cfNS, t.ID, t.ARN, t.DistributionID, t.Name, t.Domain, domainsXML.String(),
		t.ConnectionGroupID, t.Enabled, webACLArn, t.Status)
}

// ---------------------------------------------------------------------------
// DistributionTenant request XML types
// ---------------------------------------------------------------------------

type createDistributionTenantXML struct {
	XMLName        xml.Name `xml:"CreateDistributionTenantRequest"`
	DistributionID string   `xml:"DistributionId"`
	Name           string   `xml:"Name"`
	Domain         string   `xml:"Domain"`
	Domains        []string `xml:"Domains>Item>Domain"`
	Tags           []tagXML `xml:"Tags>Tag"`
}

type updateDistributionTenantXML struct {
	Enabled           *bool    `xml:"Enabled"`
	XMLName           xml.Name `xml:"UpdateDistributionTenantRequest"`
	Domain            string   `xml:"Domain"`
	ConnectionGroupID string   `xml:"ConnectionGroupId"`
	Domains           []string `xml:"Domains>Item>Domain"`
}

// ---------------------------------------------------------------------------
// DistributionTenant handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateDistributionTenant(c *echo.Context) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	var req createDistributionTenantXML
	if len(body) > 0 {
		_ = xml.Unmarshal(body, &req)
	}

	tags := make(map[string]string, len(req.Tags))
	for _, tag := range req.Tags {
		tags[tag.Key] = tag.Value
	}

	domains := req.Domains
	if req.Domain != "" {
		domains = append([]string{req.Domain}, domains...)
	}

	t, createErr := h.Backend.CreateDistributionTenant(req.DistributionID, req.Name, domains, tags)
	if createErr != nil {
		return h.handleError(c, createErr)
	}

	c.Response().Header().Set("ETag", t.ETag)
	c.Response().Header().Set("Location", cfPathPrefix+"distribution-tenant/"+t.ID)

	return xmlResp(c, http.StatusCreated, h.distributionTenantXML(t))
}

func (h *Handler) handleGetDistributionTenant(c *echo.Context, id string) error {
	t, err := h.Backend.GetDistributionTenant(id)
	if err != nil {
		return h.handleError(c, err)
	}

	c.Response().Header().Set("ETag", t.ETag)

	return xmlResp(c, http.StatusOK, h.distributionTenantXML(t))
}

func (h *Handler) handleGetDistributionTenantByDomain(c *echo.Context) error {
	domain := c.Request().URL.Query().Get("domain")
	if domain == "" {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("InvalidArgument", "domain query parameter required"))
	}

	t, err := h.Backend.GetDistributionTenantByDomain(domain)
	if err != nil {
		return h.handleError(c, err)
	}

	c.Response().Header().Set("ETag", t.ETag)

	return xmlResp(c, http.StatusOK, h.distributionTenantXML(t))
}

func (h *Handler) handleUpdateDistributionTenant(c *echo.Context, id string) error {
	current, getErr := h.Backend.GetDistributionTenant(id)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	ifMatch := c.Request().Header.Get("If-Match")
	if ifMatch == "" || ifMatch != current.ETag {
		return xmlResp(c, http.StatusPreconditionFailed,
			cfErrorXML("PreconditionFailed", "If-Match ETag did not match the current distribution tenant ETag"))
	}

	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	var req updateDistributionTenantXML
	if len(body) > 0 {
		_ = xml.Unmarshal(body, &req)
	}

	domains := req.Domains
	if req.Domain != "" {
		domains = append([]string{req.Domain}, domains...)
	}

	t, updateErr := h.Backend.UpdateDistributionTenant(id, DistributionTenantUpdate{
		Domains:           domains,
		ConnectionGroupID: req.ConnectionGroupID,
		Enabled:           req.Enabled,
	})
	if updateErr != nil {
		return h.handleError(c, updateErr)
	}

	c.Response().Header().Set("ETag", t.ETag)

	return xmlResp(c, http.StatusOK, h.distributionTenantXML(t))
}

func (h *Handler) handleDeleteDistributionTenant(c *echo.Context, id string) error {
	current, getErr := h.Backend.GetDistributionTenant(id)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	ifMatch := c.Request().Header.Get("If-Match")
	if ifMatch == "" || ifMatch != current.ETag {
		return xmlResp(c, http.StatusPreconditionFailed,
			cfErrorXML("PreconditionFailed", "If-Match ETag did not match the current distribution tenant ETag"))
	}

	if err := h.Backend.DeleteDistributionTenant(id); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// tenantSummaryXML is the list-view representation of a DistributionTenant.
type tenantSummaryXML struct {
	XMLName           xml.Name `xml:"DistributionTenant"`
	ID                string   `xml:"Id"`
	ARN               string   `xml:"Arn"`
	DistributionID    string   `xml:"DistributionId"`
	Name              string   `xml:"Name,omitempty"`
	Domain            string   `xml:"Domain"`
	ConnectionGroupID string   `xml:"ConnectionGroupId,omitempty"`
	Status            string   `xml:"Status"`
	Enabled           bool     `xml:"Enabled"`
}

type tenantListXML struct {
	XMLName  xml.Name           `xml:"DistributionTenantList"`
	XMLNS    string             `xml:"xmlns,attr"`
	Items    []tenantSummaryXML `xml:"Items>DistributionTenant"`
	MaxItems int                `xml:"MaxItems"`
	Quantity int                `xml:"Quantity"`
}

func tenantsToSummaryList(tenants []*DistributionTenant) tenantListXML {
	summaries := make([]tenantSummaryXML, 0, len(tenants))
	for _, t := range tenants {
		summaries = append(summaries, tenantSummaryXML{
			ID:                t.ID,
			ARN:               t.ARN,
			DistributionID:    t.DistributionID,
			Name:              t.Name,
			Domain:            t.Domain,
			ConnectionGroupID: t.ConnectionGroupID,
			Enabled:           t.Enabled,
			Status:            t.Status,
		})
	}

	return tenantListXML{XMLNS: cfNS, MaxItems: maxItems, Quantity: len(summaries), Items: summaries}
}

func (h *Handler) handleListDistributionTenants(c *echo.Context) error {
	tenants := h.Backend.ListDistributionTenants()

	out, xmlErr := xml.Marshal(tenantsToSummaryList(tenants))
	if xmlErr != nil {
		return h.handleError(c, xmlErr)
	}

	return xmlResp(c, http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>`+string(out))
}

// handleListDistributionTenantsByCustomization returns distribution tenants filtered by the
// WebACLArn query parameter, i.e. tenants that have that WAF web ACL associated.
func (h *Handler) handleListDistributionTenantsByCustomization(c *echo.Context) error {
	webACLArn := c.Request().URL.Query().Get("WebACLArn")
	tenants := h.Backend.ListDistributionTenantsByCustomization(webACLArn)

	out, xmlErr := xml.Marshal(tenantsToSummaryList(tenants))
	if xmlErr != nil {
		return h.handleError(c, xmlErr)
	}

	return xmlResp(c, http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>`+string(out))
}

// ---------------------------------------------------------------------------
// DisassociateWebACL handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleDisassociateDistributionWebACL(c *echo.Context, distID string) error {
	d, err := h.Backend.GetDistribution(distID)
	if err != nil {
		return h.handleError(c, err)
	}

	if disErr := h.Backend.DisassociateDistributionWebACL(distID); disErr != nil {
		return h.handleError(c, disErr)
	}

	return xmlResp(c, http.StatusOK, distributionResponseXML(d))
}

func (h *Handler) handleDisassociateDistributionTenantWebACL(c *echo.Context, tenantID string) error {
	t, err := h.Backend.GetDistributionTenant(tenantID)
	if err != nil {
		return h.handleError(c, err)
	}

	if disErr := h.Backend.DisassociateDistributionTenantWebACL(tenantID); disErr != nil {
		return h.handleError(c, disErr)
	}

	return xmlResp(c, http.StatusOK, h.distributionTenantXML(t))
}

// ---------------------------------------------------------------------------
// CreateDistributionWithTags handler
// ---------------------------------------------------------------------------

type distributionConfigWithTagsXML struct {
	XMLName            xml.Name                  `xml:"DistributionConfigWithTags"`
	DistributionConfig distributionConfigMinimal `xml:"DistributionConfig"`
	Tags               []tagXML                  `xml:"Tags>Tag"`
}

func (h *Handler) handleCreateDistributionWithTags(c *echo.Context) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	var req distributionConfigWithTagsXML
	if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "invalid DistributionConfigWithTags XML"))
	}

	// Marshal the inner DistributionConfig to pass as raw config.
	rawConfig, marshalErr := xml.Marshal(req.DistributionConfig)
	if marshalErr != nil {
		rawConfig = body
	}

	d, createErr := h.Backend.CreateDistribution(
		req.DistributionConfig.CallerReference,
		req.DistributionConfig.Comment,
		req.DistributionConfig.Enabled,
		rawConfig,
	)
	if createErr != nil {
		return h.handleError(c, createErr)
	}

	// Apply tags if provided.
	if len(req.Tags) > 0 {
		tags := make(map[string]string, len(req.Tags))
		for _, tag := range req.Tags {
			tags[tag.Key] = tag.Value
		}
		if tagErr := h.Backend.TagResource(d.ARN, tags); tagErr != nil {
			return h.handleError(c, tagErr)
		}
	}

	c.Response().Header().Set("Location", cfPathPrefix+"distribution/"+d.ID)
	c.Response().Header().Set("ETag", d.ETag)

	return xmlResp(c, http.StatusCreated, distributionResponseXML(d))
}

// ---------------------------------------------------------------------------
// UpdateDistributionWithStagingConfig handler
// ---------------------------------------------------------------------------

type updateWithStagingConfigXML struct {
	XMLName   xml.Name `xml:"UpdateDistributionWithStagingConfigRequest"`
	StagingID string   `xml:"StagingDistributionId"`
}

func (h *Handler) handleUpdateDistributionWithStagingConfig(c *echo.Context, primaryID string) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	var req updateWithStagingConfigXML
	if len(body) > 0 {
		_ = xml.Unmarshal(body, &req)
	}

	// If staging ID not in body, try query param.
	if req.StagingID == "" {
		req.StagingID = c.Request().URL.Query().Get("StagingDistributionId")
	}

	// If still no staging ID, use same distribution (no-op copy).
	if req.StagingID == "" {
		req.StagingID = primaryID
	}

	d, updateErr := h.Backend.UpdateDistributionWithStagingConfig(primaryID, req.StagingID)
	if updateErr != nil {
		return h.handleError(c, updateErr)
	}

	c.Response().Header().Set("ETag", d.ETag)

	return xmlResp(c, http.StatusOK, distributionResponseXML(d))
}

// ---------------------------------------------------------------------------
// UpdateDomainAssociation handler
// ---------------------------------------------------------------------------

// updateDomainAssociationTargetXML identifies the resource a domain should be (re-)associated
// with: exactly one of DistributionID / DistributionTenantID is expected to be set.
type updateDomainAssociationTargetXML struct {
	DistributionID       string `xml:"DistributionId"`
	DistributionTenantID string `xml:"DistributionTenantId"`
}

type updateDomainAssociationXML struct {
	XMLName        xml.Name                         `xml:"UpdateDomainAssociationRequest"`
	Domain         string                           `xml:"Domain"`
	TargetResource updateDomainAssociationTargetXML `xml:"TargetResource"`
}

// handleUpdateDomainAssociation moves a Domain's association to the distribution or distribution
// tenant named in TargetResource, persisting the change in the backend. The Identifier is carried
// entirely in the request body (real AWS routes this operation without a path parameter).
func (h *Handler) handleUpdateDomainAssociation(c *echo.Context) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	var req updateDomainAssociationXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			return xmlResp(
				c,
				http.StatusBadRequest,
				cfErrorXML("MalformedXML", "invalid UpdateDomainAssociationRequest XML"),
			)
		}
	}

	result, updateErr := h.Backend.UpdateDomainAssociation(
		req.Domain, req.TargetResource.DistributionTenantID, req.TargetResource.DistributionID,
	)
	if updateErr != nil {
		return h.handleError(c, updateErr)
	}

	resp := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<DomainAssociation xmlns="%s">`+
		`<Domain>%s</Domain>`+
		`<DistributionId>%s</DistributionId>`+
		`<DistributionTenantId>%s</DistributionTenantId>`+
		`</DomainAssociation>`,
		cfNS, result.Domain, result.DistributionID, result.DistributionTenantID)

	return xmlResp(c, http.StatusOK, resp)
}

// ---------------------------------------------------------------------------
// VerifyDNSConfiguration handler
// ---------------------------------------------------------------------------

type verifyDNSConfigurationXML struct {
	XMLName    xml.Name `xml:"VerifyDnsConfigurationRequest"`
	Identifier string   `xml:"Identifier"`
}

// handleVerifyDNSConfiguration verifies the DNS configuration for the distribution tenant (or
// distribution) named by Identifier, returning a real per-domain status list. When Identifier is
// omitted, a single generic PASSED entry is returned for backward compatibility.
func (h *Handler) handleVerifyDNSConfiguration(c *echo.Context) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	var req verifyDNSConfigurationXML
	if len(body) > 0 {
		_ = xml.Unmarshal(body, &req)
	}

	configs, verifyErr := h.Backend.VerifyDNSConfiguration(req.Identifier)
	if verifyErr != nil {
		return h.handleError(c, verifyErr)
	}

	var items strings.Builder
	for _, dc := range configs {
		fmt.Fprintf(&items, `<Item><Domain>%s</Domain><Status>%s</Status></Item>`, dc.Domain, dc.Status)
	}

	resp := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<VerifyDnsConfigurationResponse xmlns="%s">`+
		`<DnsConfigurationList>%s</DnsConfigurationList>`+
		`</VerifyDnsConfigurationResponse>`,
		cfNS, items.String())

	return xmlResp(c, http.StatusOK, resp)
}

// ---------------------------------------------------------------------------
// GetManagedCertificateDetails handler
// ---------------------------------------------------------------------------

func (h *Handler) handleGetManagedCertificateDetails(c *echo.Context, _ string) error {
	resp := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<ManagedCertificateDetails xmlns="%s">`+
		`<ValidationTokens/>`+
		`<Status>SUCCESS</Status>`+
		`</ManagedCertificateDetails>`,
		cfNS)

	return xmlResp(c, http.StatusOK, resp)
}

// ---------------------------------------------------------------------------
// Tenant invalidation handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateInvalidationForTenant(c *echo.Context, tenantID string) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusInternalServerError, cfErrorXML("InternalFailure", err.Error()))
	}

	var batch invalidationBatchXML
	if len(body) > 0 {
		_ = xml.Unmarshal(body, &batch)
	}

	inv, backendErr := h.Backend.CreateInvalidationForTenant(tenantID, batch.Paths.Items)
	if backendErr != nil {
		return h.handleError(c, backendErr)
	}

	resp := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<Invalidation xmlns="%s">`+
		`<Id>%s</Id>`+
		`<Status>%s</Status>`+
		`<CreateTime>%s</CreateTime>`+
		`</Invalidation>`,
		cfNS, inv.ID, inv.Status, inv.CreateTime.Format(time.RFC3339))

	c.Response().Header().Set(
		"Location",
		cfPathPrefix+"distribution-tenant/"+tenantID+"/invalidation/"+inv.ID,
	)

	return xmlResp(c, http.StatusCreated, resp)
}

func (h *Handler) handleGetInvalidationForTenant(c *echo.Context, tenantID string) error {
	// Extract invalidation ID from the URL path after /invalidation/.
	path := c.Request().URL.Path
	invID := ""
	if _, after, ok := strings.Cut(path, "/invalidation/"); ok {
		invID = after
		if slash := strings.Index(invID, "/"); slash >= 0 {
			invID = invID[:slash]
		}
	}
	if invID == "" {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("InvalidArgument", "invalidation ID is required"))
	}

	inv, err := h.Backend.GetInvalidationForTenant(tenantID, invID)
	if err != nil {
		return h.handleError(c, err)
	}

	resp := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<Invalidation xmlns="%s">`+
		`<Id>%s</Id>`+
		`<Status>%s</Status>`+
		`<CreateTime>%s</CreateTime>`+
		`</Invalidation>`,
		cfNS, inv.ID, inv.Status, inv.CreateTime.Format(time.RFC3339))

	return xmlResp(c, http.StatusOK, resp)
}

func (h *Handler) handleListInvalidationsForTenant(c *echo.Context, tenantID string) error {
	invs := h.Backend.ListInvalidationsForTenant(tenantID)

	type invSummary struct {
		XMLName    xml.Name `xml:"InvalidationSummary"`
		ID         string   `xml:"Id"`
		Status     string   `xml:"Status"`
		CreateTime string   `xml:"CreateTime"`
	}
	type invList struct {
		XMLName     xml.Name     `xml:"InvalidationList"`
		XMLNS       string       `xml:"xmlns,attr"`
		Items       []invSummary `xml:"Items>InvalidationSummary"`
		MaxItems    int          `xml:"MaxItems"`
		Quantity    int          `xml:"Quantity"`
		IsTruncated bool         `xml:"IsTruncated"`
	}

	summaries := make([]invSummary, 0, len(invs))
	for _, inv := range invs {
		summaries = append(summaries, invSummary{
			ID:         inv.ID,
			Status:     inv.Status,
			CreateTime: inv.CreateTime.Format(time.RFC3339),
		})
	}
	list := invList{XMLNS: cfNS, MaxItems: maxItems, Quantity: len(summaries), Items: summaries}
	out, xmlErr := xml.Marshal(list)
	if xmlErr != nil {
		return h.handleError(c, xmlErr)
	}

	return xmlResp(c, http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>`+string(out))
}

// ---------------------------------------------------------------------------
// ListDistributionsBy* handlers (config-search based)
// ---------------------------------------------------------------------------

func (h *Handler) handleListDistributionsByKeyGroup(c *echo.Context, keyGroupID string) error {
	dists := h.Backend.ListDistributionsByKeyGroup(keyGroupID)

	return h.marshalDistributionList(c, dists)
}

func (h *Handler) handleListDistributionsByVpcOriginID(c *echo.Context, vpcOriginID string) error {
	dists := h.Backend.ListDistributionsByVpcOriginID(vpcOriginID)

	return h.marshalDistributionList(c, dists)
}

func (h *Handler) handleListDistributionsByAnycastIPListID(c *echo.Context, anycastID string) error {
	dists := h.Backend.ListDistributionsByAnycastIPListID(anycastID)

	return h.marshalDistributionList(c, dists)
}

func (h *Handler) handleListDistributionsByConnectionFunction(c *echo.Context, funcID string) error {
	dists := h.Backend.ListDistributionsByConnectionFunction(funcID)

	return h.marshalDistributionList(c, dists)
}

func (h *Handler) handleListDistributionsByConnectionMode(c *echo.Context, mode string) error {
	dists := h.Backend.ListDistributionsByConnectionMode(mode)

	return h.marshalDistributionList(c, dists)
}

func (h *Handler) handleListDistributionsByTrustStore(c *echo.Context, trustStoreID string) error {
	dists := h.Backend.ListDistributionsByTrustStore(trustStoreID)

	return h.marshalDistributionList(c, dists)
}

func (h *Handler) handleListDistributionsByOwnedResource(c *echo.Context, resourceARN string) error {
	dists := h.Backend.ListDistributionsByOwnedResource(resourceARN)

	return h.marshalDistributionList(c, dists)
}

// ---------------------------------------------------------------------------
// ListConflictingAliases handler
// ---------------------------------------------------------------------------

func (h *Handler) handleListConflictingAliases(c *echo.Context) error {
	alias := c.Request().URL.Query().Get("Alias")
	dists := h.Backend.ListConflictingAliasesByDomain(alias)

	type conflictingSummary struct {
		XMLName   xml.Name `xml:"ConflictingAlias"`
		Alias     string   `xml:"Alias"`
		DistID    string   `xml:"DistributionId"`
		AccountID string   `xml:"AccountId"`
	}
	type conflictList struct {
		XMLName     xml.Name             `xml:"ConflictingAliasesList"`
		XMLNS       string               `xml:"xmlns,attr"`
		Items       []conflictingSummary `xml:"Items>ConflictingAlias"`
		MaxItems    int                  `xml:"MaxItems"`
		Quantity    int                  `xml:"Quantity"`
		IsTruncated bool                 `xml:"IsTruncated"`
	}

	summaries := make([]conflictingSummary, 0, len(dists))
	for _, d := range dists {
		summaries = append(summaries, conflictingSummary{
			Alias:     alias,
			DistID:    d.ID,
			AccountID: "",
		})
	}
	list := conflictList{XMLNS: cfNS, MaxItems: maxItems, Quantity: len(summaries), Items: summaries}
	out, xmlErr := xml.Marshal(list)
	if xmlErr != nil {
		return h.handleError(c, xmlErr)
	}

	return xmlResp(c, http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>`+string(out))
}

// ---------------------------------------------------------------------------
// ListDomainConflicts handler
// ---------------------------------------------------------------------------

type listDomainConflictsXML struct {
	XMLName xml.Name `xml:"ListDomainConflictsRequest"`
	Domain  string   `xml:"Domain"`
}

// handleListDomainConflicts reports every existing distribution or distribution tenant that
// already claims the requested Domain.
func (h *Handler) handleListDomainConflicts(c *echo.Context) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	var req listDomainConflictsXML
	if len(body) > 0 {
		_ = xml.Unmarshal(body, &req)
	}

	if req.Domain == "" {
		req.Domain = c.Request().URL.Query().Get("Domain")
	}

	conflicts := h.Backend.ListDomainConflicts(req.Domain)

	var items strings.Builder
	for _, dc := range conflicts {
		fmt.Fprintf(
			&items,
			`<DomainConflict><Domain>%s</Domain><ResourceType>%s</ResourceType>`+
				`<ResourceId>%s</ResourceId><AccountId>%s</AccountId></DomainConflict>`,
			dc.Domain, dc.ResourceType, dc.ResourceID, dc.AccountID,
		)
	}

	resp := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<DomainConflictList xmlns="%s">`+
		`<Items>%s</Items>`+
		`<Quantity>%d</Quantity>`+
		`</DomainConflictList>`,
		cfNS, items.String(), len(conflicts))

	return xmlResp(c, http.StatusOK, resp)
}

// ---------------------------------------------------------------------------
// UpdateKeyValueStore handler
// ---------------------------------------------------------------------------

func (h *Handler) handleUpdateKeyValueStore(c *echo.Context, id string) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	var req keyValueStoreRequestXML
	if len(body) > 0 {
		_ = xml.Unmarshal(body, &req)
	}

	kvs, updateErr := h.Backend.UpdateKeyValueStore(id, req.Comment)
	if updateErr != nil {
		return h.handleError(c, updateErr)
	}

	c.Response().Header().Set("ETag", kvs.ETag)

	return xmlResp(c, http.StatusOK, kvsResponseXML(kvs))
}
