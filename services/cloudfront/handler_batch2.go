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

func distributionTenantXML(t *DistributionTenant) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<DistributionTenant xmlns="%s">`+
		`<Id>%s</Id>`+
		`<DistributionId>%s</DistributionId>`+
		`<Domain>%s</Domain>`+
		`<Status>%s</Status>`+
		`</DistributionTenant>`,
		cfNS, t.ID, t.DistributionID, t.Domain, t.Status)
}

// ---------------------------------------------------------------------------
// DistributionTenant request XML types
// ---------------------------------------------------------------------------

type createDistributionTenantXML struct {
	XMLName        xml.Name `xml:"CreateDistributionTenantRequest"`
	DistributionID string   `xml:"DistributionId"`
	Domain         string   `xml:"Domain"`
	Tags           []tagXML `xml:"Tags>Tag"`
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

	t, createErr := h.Backend.CreateDistributionTenant(req.DistributionID, req.Domain, tags)
	if createErr != nil {
		return h.handleError(c, createErr)
	}

	c.Response().Header().Set("ETag", t.ETag)
	c.Response().Header().Set("Location", cfPathPrefix+"distribution-tenant/"+t.ID)

	return xmlResp(c, http.StatusCreated, distributionTenantXML(t))
}

func (h *Handler) handleGetDistributionTenant(c *echo.Context, id string) error {
	t, err := h.Backend.GetDistributionTenant(id)
	if err != nil {
		return h.handleError(c, err)
	}

	c.Response().Header().Set("ETag", t.ETag)

	return xmlResp(c, http.StatusOK, distributionTenantXML(t))
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

	return xmlResp(c, http.StatusOK, distributionTenantXML(t))
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

	t, updateErr := h.Backend.UpdateDistributionTenant(id, nil)
	if updateErr != nil {
		return h.handleError(c, updateErr)
	}

	c.Response().Header().Set("ETag", t.ETag)

	return xmlResp(c, http.StatusOK, distributionTenantXML(t))
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

func (h *Handler) handleListDistributionTenants(c *echo.Context) error {
	tenants := h.Backend.ListDistributionTenants()

	type tenantSummary struct {
		XMLName        xml.Name `xml:"DistributionTenant"`
		ID             string   `xml:"Id"`
		DistributionID string   `xml:"DistributionId"`
		Domain         string   `xml:"Domain"`
		Status         string   `xml:"Status"`
	}
	type tenantList struct {
		XMLName  xml.Name        `xml:"DistributionTenantList"`
		XMLNS    string          `xml:"xmlns,attr"`
		Items    []tenantSummary `xml:"Items>DistributionTenant"`
		Quantity int             `xml:"Quantity"`
	}

	summaries := make([]tenantSummary, 0, len(tenants))
	for _, t := range tenants {
		summaries = append(summaries, tenantSummary{
			ID:             t.ID,
			DistributionID: t.DistributionID,
			Domain:         t.Domain,
			Status:         t.Status,
		})
	}
	list := tenantList{XMLNS: cfNS, Quantity: len(summaries), Items: summaries}
	out, xmlErr := xml.Marshal(list)
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

	return xmlResp(c, http.StatusOK, distributionTenantXML(t))
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

type updateDomainAssociationXML struct {
	XMLName xml.Name `xml:"UpdateDomainAssociationRequest"`
	Domains []string `xml:"Domains>Domain"`
}

func (h *Handler) handleUpdateDomainAssociation(c *echo.Context, distID string) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	var req updateDomainAssociationXML
	if len(body) > 0 {
		_ = xml.Unmarshal(body, &req)
	}

	// Build domain list XML.
	var sb strings.Builder
	for _, d := range req.Domains {
		fmt.Fprintf(&sb, `<Domain>%s</Domain>`, d)
	}
	domainsXML := sb.String()

	resp := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<DomainAssociation xmlns="%s">`+
		`<DistributionId>%s</DistributionId>`+
		`<Domains>%s</Domains>`+
		`</DomainAssociation>`,
		cfNS, distID, domainsXML)

	return xmlResp(c, http.StatusOK, resp)
}

// ---------------------------------------------------------------------------
// VerifyDNSConfiguration handler
// ---------------------------------------------------------------------------

func (h *Handler) handleVerifyDNSConfiguration(c *echo.Context) error {
	resp := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<VerifyDnsConfigurationResponse xmlns="%s">`+
		`<VerifyDNS>PASSED</VerifyDNS>`+
		`</VerifyDnsConfigurationResponse>`,
		cfNS)

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

func (h *Handler) handleListDomainConflicts(c *echo.Context) error {
	resp := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<DomainConflictList xmlns="%s">`+
		`<Items/>`+
		`<Quantity>0</Quantity>`+
		`</DomainConflictList>`,
		cfNS)

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
