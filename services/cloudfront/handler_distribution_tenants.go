package cloudfront

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleAssociateDistributionTenantWebACL(c *echo.Context, tenantID string) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	if qErr := validateQuantities(body); qErr != nil {
		return h.handleError(c, qErr)
	}

	var req webACLAssociationXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			return xmlResp(
				c,
				http.StatusBadRequest,
				cfErrorXML("MalformedXML", "invalid WebACLAssociation XML"),
			)
		}
	}

	if assocErr := h.Backend.AssociateDistributionTenantWebACL(tenantID, req.WebACLID); assocErr != nil {
		return h.handleError(c, assocErr)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) distributionTenantXML(t *DistributionTenant) string {
	// The real SDK deserializer (awsRestxml_deserializeDocumentDomainResultList) expects each
	// Domains entry wrapped in <member>, not <Item>.
	var domainsXML strings.Builder
	for _, d := range t.Domains {
		fmt.Fprintf(&domainsXML, `<member><Domain>%s</Domain><Status>Active</Status></member>`, d)
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
	Domains        []string `xml:"Domains>member>Domain"`
	// Tags is *types.Tags on the wire: Items wraps the Tag list, not a bare
	// Tags>Tag path (cloudfront@v1.67.4 serializers.go
	// awsRestxml_serializeDocumentTags).
	Tags []tagXML `xml:"Tags>Items>Tag"`
}

type updateDistributionTenantXML struct {
	Enabled           *bool    `xml:"Enabled"`
	XMLName           xml.Name `xml:"UpdateDistributionTenantRequest"`
	Domain            string   `xml:"Domain"`
	ConnectionGroupID string   `xml:"ConnectionGroupId"`
	Domains           []string `xml:"Domains>member>Domain"`
}

// ---------------------------------------------------------------------------
// DistributionTenant handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateDistributionTenant(c *echo.Context) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	if qErr := validateQuantities(body); qErr != nil {
		return h.handleError(c, qErr)
	}

	var req createDistributionTenantXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			return xmlResp(
				c,
				http.StatusBadRequest,
				cfErrorXML("MalformedXML", "invalid CreateDistributionTenantRequest XML"),
			)
		}
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

	if qErr := validateQuantities(body); qErr != nil {
		return h.handleError(c, qErr)
	}

	var req updateDistributionTenantXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			return xmlResp(
				c,
				http.StatusBadRequest,
				cfErrorXML("MalformedXML", "invalid UpdateDistributionTenantRequest XML"),
			)
		}
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
	XMLName           xml.Name `xml:"DistributionTenantSummary"`
	ID                string   `xml:"Id"`
	ARN               string   `xml:"Arn"`
	DistributionID    string   `xml:"DistributionId"`
	Name              string   `xml:"Name,omitempty"`
	Domain            string   `xml:"Domain"`
	ConnectionGroupID string   `xml:"ConnectionGroupId,omitempty"`
	Status            string   `xml:"Status"`
	Enabled           bool     `xml:"Enabled"`
}

// tenantListXML models the real ListDistributionTenants response shape (see
// awsRestxml_deserializeDocumentDistributionTenantList): each DistributionTenantSummary is a
// direct child of DistributionTenantList, with no extra <Items> wrapper.
type tenantListXML struct {
	XMLName  xml.Name           `xml:"DistributionTenantList"`
	Items    []tenantSummaryXML `xml:"DistributionTenantSummary"`
	MaxItems int                `xml:"MaxItems"`
	Quantity int                `xml:"Quantity"`
}

// tenantListResultXML wraps tenantListXML in a response root. Neither ListDistributionTenants
// nor ListDistributionTenantsByCustomization has an httpPayload member (both also carry
// NextMarker), so the real deserializers
// (awsRestxml_deserializeOpDocumentListDistributionTenants{,ByCustomization}Output) read
// DistributionTenantList as a CHILD of the response root, not as the root itself.
type tenantListResultXML struct {
	XMLName                xml.Name      `xml:"ListDistributionTenantsResult"`
	XMLNS                  string        `xml:"xmlns,attr"`
	NextMarker             string        `xml:"NextMarker,omitempty"`
	DistributionTenantList tenantListXML `xml:"DistributionTenantList"`
}

func tenantsToSummaryList(tenants []*DistributionTenant) tenantListResultXML {
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

	return tenantListResultXML{
		XMLNS: cfNS,
		DistributionTenantList: tenantListXML{
			MaxItems: maxItems, Quantity: len(summaries), Items: summaries,
		},
	}
}

func (h *Handler) handleListDistributionTenants(c *echo.Context) error {
	tenants := h.Backend.ListDistributionTenants()

	out, xmlErr := xml.Marshal(tenantsToSummaryList(tenants))
	if xmlErr != nil {
		return h.handleError(c, xmlErr)
	}

	return xmlResp(c, http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>`+string(out))
}

// listDistributionTenantsByCustomizationXML is the XML body of a
// ListDistributionTenantsByCustomization request. cloudfront@v1.67.4 serializers.go
// awsRestxml_serializeOpHttpBindingsListDistributionTenantsByCustomizationInput returns nil (no
// HTTP-bound fields), so CertificateArn, Marker, MaxItems, and WebACLArn all serialize into the
// XML body, not the query string.
type listDistributionTenantsByCustomizationXML struct {
	XMLName        xml.Name `xml:"ListDistributionTenantsByCustomizationRequest"`
	CertificateArn string   `xml:"CertificateArn"`
	Marker         string   `xml:"Marker"`
	WebACLArn      string   `xml:"WebACLArn"`
	MaxItems       int      `xml:"MaxItems"`
}

// filterTenantsByCertificateArn narrows tenants to those whose certificate ARN matches certArn.
// A blank certArn is a no-op (no filter requested).
func (h *Handler) filterTenantsByCertificateArn(tenants []*DistributionTenant, certArn string) []*DistributionTenant {
	if certArn == "" {
		return tenants
	}

	filtered := make([]*DistributionTenant, 0, len(tenants))
	for _, t := range tenants {
		if h.Backend.TenantCertificateArn(t.ID) == certArn {
			filtered = append(filtered, t)
		}
	}

	return filtered
}

// paginateTenants applies the Marker/MaxItems page window to an already-sorted tenant list,
// returning the page, the effective page size, and whether more results follow.
func paginateTenants(tenants []*DistributionTenant, marker string, maxItemsReq int) ([]*DistributionTenant, int, bool) {
	pageSize := maxItems
	if maxItemsReq > 0 && maxItemsReq < maxItems {
		pageSize = maxItemsReq
	}

	// Tenants are already sorted by ID (see ListDistributionTenantsByCustomization); the marker
	// is the ID of the last item returned on the previous page.
	if marker != "" {
		cut := 0
		for cut < len(tenants) && tenants[cut].ID <= marker {
			cut++
		}
		tenants = tenants[cut:]
	}

	isTruncated := len(tenants) > pageSize
	if isTruncated {
		tenants = tenants[:pageSize]
	}

	return tenants, pageSize, isTruncated
}

// handleListDistributionTenantsByCustomization returns distribution tenants filtered by
// WebACLArn and/or CertificateArn, paginated by Marker/MaxItems.
func (h *Handler) handleListDistributionTenantsByCustomization(c *echo.Context) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	var req listDistributionTenantsByCustomizationXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			return xmlResp(
				c,
				http.StatusBadRequest,
				cfErrorXML("MalformedXML", "invalid ListDistributionTenantsByCustomizationRequest XML"),
			)
		}
	}

	tenants := h.Backend.ListDistributionTenantsByCustomization(req.WebACLArn)
	tenants = h.filterTenantsByCertificateArn(tenants, req.CertificateArn)

	page, pageSize, isTruncated := paginateTenants(tenants, req.Marker, req.MaxItems)

	result := tenantsToSummaryList(page)
	result.DistributionTenantList.MaxItems = pageSize
	if isTruncated && len(page) > 0 {
		result.NextMarker = page[len(page)-1].ID
	}

	out, xmlErr := xml.Marshal(result)
	if xmlErr != nil {
		return h.handleError(c, xmlErr)
	}

	return xmlResp(c, http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>`+string(out))
}

// ---------------------------------------------------------------------------
// DisassociateWebACL handlers
// ---------------------------------------------------------------------------

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

	if qErr := validateQuantities(body); qErr != nil {
		return h.handleError(c, qErr)
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

	if qErr := validateQuantities(body); qErr != nil {
		return h.handleError(c, qErr)
	}

	var req verifyDNSConfigurationXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			return xmlResp(
				c,
				http.StatusBadRequest,
				cfErrorXML("MalformedXML", "invalid VerifyDnsConfigurationRequest XML"),
			)
		}
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

func (h *Handler) handleGetManagedCertificateDetails(c *echo.Context, tenantID string) error {
	details, err := h.Backend.GetManagedCertificateDetails(tenantID)
	if err != nil {
		return h.handleError(c, err)
	}

	var tokens strings.Builder
	for _, tok := range details.ValidationTokenDetails {
		fmt.Fprintf(
			&tokens,
			`<ValidationTokenDetail><Domain>%s</Domain><RedirectFrom>%s</RedirectFrom>`+
				`<RedirectTo>%s</RedirectTo></ValidationTokenDetail>`,
			tok.Domain, tok.RedirectFrom, tok.RedirectTo,
		)
	}

	resp := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<ManagedCertificateDetails xmlns="%s">`+
		`<CertificateArn>%s</CertificateArn>`+
		`<CertificateStatus>%s</CertificateStatus>`+
		`<ValidationTokenHost>%s</ValidationTokenHost>`+
		`<ValidationTokenDetails>%s</ValidationTokenDetails>`+
		`</ManagedCertificateDetails>`,
		cfNS, details.CertificateARN, details.CertificateStatus, details.ValidationTokenHost, tokens.String())

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

	if qErr := validateQuantities(body); qErr != nil {
		return h.handleError(c, qErr)
	}

	var batch invalidationBatchXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &batch); xmlErr != nil {
			return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "invalid InvalidationBatch XML"))
		}
	}

	inv, backendErr := h.Backend.CreateInvalidationForTenant(tenantID, batch.Paths.Items)
	if backendErr != nil {
		return h.handleError(c, backendErr)
	}

	var pathsSB strings.Builder
	for _, p := range inv.Paths {
		fmt.Fprintf(&pathsSB, "<Path>%s</Path>", p)
	}

	resp := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<Invalidation xmlns="%s">`+
		`<Id>%s</Id>`+
		`<Status>%s</Status>`+
		`<CreateTime>%s</CreateTime>`+
		`<InvalidationBatch>`+
		`<CallerReference>%s</CallerReference>`+
		`<Paths><Quantity>%d</Quantity><Items>%s</Items></Paths>`+
		`</InvalidationBatch>`+
		`</Invalidation>`,
		cfNS, inv.ID, inv.Status, inv.CreateTime.Format(time.RFC3339),
		batch.CallerReference, len(inv.Paths), pathsSB.String())

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

	if qErr := validateQuantities(body); qErr != nil {
		return h.handleError(c, qErr)
	}

	var req listDomainConflictsXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			return xmlResp(
				c,
				http.StatusBadRequest,
				cfErrorXML("MalformedXML", "invalid ListDomainConflictsRequest XML"),
			)
		}
	}

	if req.Domain == "" {
		req.Domain = c.Request().URL.Query().Get("Domain")
	}

	conflicts := h.Backend.ListDomainConflicts(req.Domain)

	// The real deserializer (awsRestxml_deserializeDocumentDomainConflictsList,
	// cloudfront@v1.67.4) wraps the list in <DomainConflicts>, and each entry
	// is ALSO named <DomainConflicts> (not <Items>/<DomainConflict>).
	var items strings.Builder
	for _, dc := range conflicts {
		fmt.Fprintf(
			&items,
			`<DomainConflicts><Domain>%s</Domain><ResourceType>%s</ResourceType>`+
				`<ResourceId>%s</ResourceId><AccountId>%s</AccountId></DomainConflicts>`,
			dc.Domain, dc.ResourceType, dc.ResourceID, dc.AccountID,
		)
	}

	resp := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<DomainConflictList xmlns="%s">`+
		`<DomainConflicts>%s</DomainConflicts>`+
		`</DomainConflictList>`,
		cfNS, items.String())

	return xmlResp(c, http.StatusOK, resp)
}

// ---------------------------------------------------------------------------
// UpdateKeyValueStore handler
// ---------------------------------------------------------------------------
