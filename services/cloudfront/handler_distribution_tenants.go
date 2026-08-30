package cloudfront

import (
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/labstack/echo/v5"
)

// handleDomainAssociationError maps UpdateDomainAssociation errors. Its own
// deserializer (cloudfront@v1.67.4 deserializers.go) models EntityNotFound
// for an unknown target distribution, not NoSuchDistribution -- unlike most
// other distribution ops that reuse ErrNotFound.
func (h *Handler) handleDomainAssociationError(c *echo.Context, err error) error {
	if errors.Is(err, ErrNotFound) {
		return xmlResp(c, http.StatusNotFound, cfErrorXML(codeEntityNotFound, err.Error()))
	}

	return h.handleError(c, err)
}

// associateDistributionTenantWebACLRequestXML models a real
// AssociateDistributionTenantWebACLRequest body: root
// AssociateDistributionTenantWebACLRequest with a single WebACLArn child
// element (cloudfront@v1.67.4 serializers.go:
// awsRestxml_serializeOpDocumentAssociateDistributionTenantWebACLInput). The
// previous shared webACLAssociationXML{root: WebACLAssociation, field:
// WebACLId} matched neither the real root nor the real field name (an ARN,
// not an ID) -- since encoding/xml's Unmarshal errors when the root doesn't
// match a tagged XMLName, every real client's request 400'd MalformedXML
// (see PARITY.md gaps).
type associateDistributionTenantWebACLRequestXML struct {
	XMLName   xml.Name `xml:"AssociateDistributionTenantWebACLRequest"`
	WebACLArn string   `xml:"WebACLArn"`
}

func (h *Handler) handleAssociateDistributionTenantWebACL(c *echo.Context, tenantID string) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	if qErr := validateQuantities(body); qErr != nil {
		return h.handleError(c, qErr)
	}

	var req associateDistributionTenantWebACLRequestXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			return xmlResp(
				c,
				http.StatusBadRequest,
				cfErrorXML("MalformedXML", "invalid AssociateDistributionTenantWebACLRequest XML"),
			)
		}
	}

	if assocErr := h.Backend.AssociateDistributionTenantWebACL(tenantID, req.WebACLArn); assocErr != nil {
		return h.handleError(c, assocErr)
	}

	t, getErr := h.Backend.GetDistributionTenant(tenantID)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	c.Response().Header().Set("ETag", t.ETag)

	return xmlResp(c, http.StatusOK, fmt.Sprintf(
		`<?xml version="1.0" encoding="UTF-8"?>`+
			`<AssociateDistributionTenantWebACLResult xmlns="%s"><Id>%s</Id><WebACLArn>%s</WebACLArn>`+
			`</AssociateDistributionTenantWebACLResult>`,
		cfNS, tenantID, req.WebACLArn))
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
		return xmlResp(
			c,
			http.StatusBadRequest,
			cfErrorXML("InvalidArgument", "domain query parameter required"),
		)
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
		return xmlResp(
			c,
			http.StatusPreconditionFailed,
			cfErrorXML(
				"PreconditionFailed",
				"If-Match ETag did not match the current distribution tenant ETag",
			),
		)
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
		return xmlResp(
			c,
			http.StatusPreconditionFailed,
			cfErrorXML(
				"PreconditionFailed",
				"If-Match ETag did not match the current distribution tenant ETag",
			),
		)
	}

	if err := h.Backend.DeleteDistributionTenant(id); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// domainResultXML is a single entry of a DistributionTenantSummary's Domains list. The real
// deserializer (awsRestxml_deserializeDocumentDomainResultList) wraps each entry in <member>,
// not <Item>, matching distributionTenantXML's hand-built Domains XML below.
type domainResultXML struct {
	Domain string `xml:"Domain"`
	Status string `xml:"Status"`
}

// tenantSummaryXML is the list-view representation of a DistributionTenant. Domains must be a
// <Domains><member>...</member></Domains> list (types.DistributionTenantSummary.Domains,
// awsRestxml_deserializeDocumentDistributionTenantSummary): a flat <Domain> field here decodes
// to an always-empty Domains slice on a real client, even though the singular
// distributionTenantXML above already emits the list correctly.
type tenantSummaryXML struct {
	XMLName           xml.Name          `xml:"DistributionTenantSummary"`
	ID                string            `xml:"Id"`
	ARN               string            `xml:"Arn"`
	DistributionID    string            `xml:"DistributionId"`
	Name              string            `xml:"Name,omitempty"`
	ConnectionGroupID string            `xml:"ConnectionGroupId,omitempty"`
	Status            string            `xml:"Status"`
	Domains           []domainResultXML `xml:"Domains>member"`
	Enabled           bool              `xml:"Enabled"`
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
		domains := make([]domainResultXML, 0, len(t.Domains))
		for _, d := range t.Domains {
			domains = append(domains, domainResultXML{Domain: d, Status: "Active"})
		}

		summaries = append(summaries, tenantSummaryXML{
			ID:                t.ID,
			ARN:               t.ARN,
			DistributionID:    t.DistributionID,
			Name:              t.Name,
			Domains:           domains,
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
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	var req listDistributionTenantsRequestXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			return xmlResp(
				c,
				http.StatusBadRequest,
				cfErrorXML("MalformedXML", "invalid ListDistributionTenantsRequest XML"),
			)
		}
	}

	tenants := h.Backend.ListDistributionTenants()
	tenants = filterTenantsByAssociation(
		tenants,
		req.AssociationFilter.ConnectionGroupID,
		req.AssociationFilter.DistributionID,
	)

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
func (h *Handler) filterTenantsByCertificateArn(
	tenants []*DistributionTenant,
	certArn string,
) []*DistributionTenant {
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
// returning the page, the effective page size, and whether more results follow. Tenants are
// already sorted by ID (see ListDistributionTenants/ByCustomization backend methods); the
// marker is the ID of the last item returned on the previous page.
func paginateTenants(
	tenants []*DistributionTenant,
	marker string,
	maxItemsReq int,
) ([]*DistributionTenant, int, bool) {
	return paginateByMarkerValue(tenants, func(t *DistributionTenant) string { return t.ID }, marker, maxItemsReq)
}

// distributionTenantAssociationFilterXML models the nested AssociationFilter element of a
// ListDistributionTenantsRequest body (cloudfront@v1.67.4 types.DistributionTenantAssociationFilter:
// ConnectionGroupId, DistributionId).
type distributionTenantAssociationFilterXML struct {
	ConnectionGroupID string `xml:"ConnectionGroupId"`
	DistributionID    string `xml:"DistributionId"`
}

// listDistributionTenantsRequestXML models a ListDistributionTenants request body.
// cloudfront@v1.67.4 serializers.go awsRestxml_serializeOpHttpBindingsListDistributionTenantsInput
// returns nil (no HTTP-bound fields), so AssociationFilter, Marker, and MaxItems all serialize
// into the XML body, not the query string.
type listDistributionTenantsRequestXML struct {
	XMLName           xml.Name                               `xml:"ListDistributionTenantsRequest"`
	AssociationFilter distributionTenantAssociationFilterXML `xml:"AssociationFilter"`
	Marker            string                                 `xml:"Marker"`
	MaxItems          int                                    `xml:"MaxItems"`
}

// filterTenantsByAssociation narrows tenants to those matching the given connection group
// and/or distribution ID. Blank filters are a no-op.
func filterTenantsByAssociation(
	tenants []*DistributionTenant,
	connectionGroupID, distributionID string,
) []*DistributionTenant {
	if connectionGroupID == "" && distributionID == "" {
		return tenants
	}

	filtered := make([]*DistributionTenant, 0, len(tenants))
	for _, t := range tenants {
		if connectionGroupID != "" && t.ConnectionGroupID != connectionGroupID {
			continue
		}
		if distributionID != "" && t.DistributionID != distributionID {
			continue
		}
		filtered = append(filtered, t)
	}

	return filtered
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
				cfErrorXML(
					"MalformedXML",
					"invalid ListDistributionTenantsByCustomizationRequest XML",
				),
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

func (h *Handler) handleDisassociateDistributionTenantWebACL(
	c *echo.Context,
	tenantID string,
) error {
	t, err := h.Backend.GetDistributionTenant(tenantID)
	if err != nil {
		return h.handleError(c, err)
	}

	if disErr := h.Backend.DisassociateDistributionTenantWebACL(tenantID); disErr != nil {
		return h.handleError(c, disErr)
	}

	c.Response().Header().Set("ETag", t.ETag)

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
		return h.handleDomainAssociationError(c, updateErr)
	}

	// Real UpdateDomainAssociationOutput carries a single ResourceId (not a
	// DistributionId/DistributionTenantId split) plus ETag as a response header
	// (cloudfront@v1.67.4 api_op_UpdateDomainAssociation.go:60-68,
	// deserializers.go:23927-23938,23974). The previous DistributionId/
	// DistributionTenantId elements and missing ETag header meant a real client's
	// ResourceId/ETag were always empty.
	c.Response().Header().Set("ETag", result.ETag)

	resp := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<UpdateDomainAssociationResult xmlns="%s">`+
		`<Domain>%s</Domain>`+
		`<ResourceId>%s</ResourceId>`+
		`</UpdateDomainAssociationResult>`,
		cfNS, result.Domain, result.ResourceID())

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
		fmt.Fprintf(
			&items,
			`<Item><Domain>%s</Domain><Status>%s</Status></Item>`,
			dc.Domain,
			dc.Status,
		)
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

	resp := fmt.Sprintf(
		`<?xml version="1.0" encoding="UTF-8"?>`+
			`<ManagedCertificateDetails xmlns="%s">`+
			`<CertificateArn>%s</CertificateArn>`+
			`<CertificateStatus>%s</CertificateStatus>`+
			`<ValidationTokenHost>%s</ValidationTokenHost>`+
			`<ValidationTokenDetails>%s</ValidationTokenDetails>`+
			`</ManagedCertificateDetails>`,
		cfNS,
		details.CertificateARN,
		details.CertificateStatus,
		details.ValidationTokenHost,
		tokens.String(),
	)

	return xmlResp(c, http.StatusOK, resp)
}

// ---------------------------------------------------------------------------
// Tenant invalidation handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateInvalidationForTenant(c *echo.Context, tenantID string) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(
			c,
			http.StatusInternalServerError,
			cfErrorXML("InternalFailure", err.Error()),
		)
	}

	if qErr := validateQuantities(body); qErr != nil {
		return h.handleError(c, qErr)
	}

	var batch invalidationBatchXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &batch); xmlErr != nil {
			return xmlResp(
				c,
				http.StatusBadRequest,
				cfErrorXML("MalformedXML", "invalid InvalidationBatch XML"),
			)
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
		return xmlResp(
			c,
			http.StatusBadRequest,
			cfErrorXML("InvalidArgument", "invalidation ID is required"),
		)
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

	sort.Slice(invs, func(i, j int) bool { return invs[i].ID < invs[j].ID })

	page, pageSize, isTruncated, nextMarker := paginateByMarkerID(
		c,
		invs,
		func(inv *Invalidation) string { return inv.ID },
	)

	type invSummary struct {
		XMLName    xml.Name `xml:"InvalidationSummary"`
		ID         string   `xml:"Id"`
		Status     string   `xml:"Status"`
		CreateTime string   `xml:"CreateTime"`
	}
	type invList struct {
		XMLName     xml.Name     `xml:"InvalidationList"`
		XMLNS       string       `xml:"xmlns,attr"`
		NextMarker  string       `xml:"NextMarker,omitempty"`
		Items       []invSummary `xml:"Items>InvalidationSummary"`
		MaxItems    int          `xml:"MaxItems"`
		Quantity    int          `xml:"Quantity"`
		IsTruncated bool         `xml:"IsTruncated"`
	}

	summaries := make([]invSummary, 0, len(page))
	for _, inv := range page {
		summaries = append(summaries, invSummary{
			ID:         inv.ID,
			Status:     inv.Status,
			CreateTime: inv.CreateTime.Format(time.RFC3339),
		})
	}
	list := invList{
		XMLNS: cfNS, MaxItems: pageSize, Quantity: len(summaries), Items: summaries,
		IsTruncated: isTruncated, NextMarker: nextMarker,
	}
	out, xmlErr := xml.Marshal(list)
	if xmlErr != nil {
		return h.handleError(c, xmlErr)
	}

	return xmlResp(c, http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>`+string(out))
}

// ---------------------------------------------------------------------------
// ListDistributionsBy* handlers (config-search based)
// ---------------------------------------------------------------------------

// distributionResourceIDXML models the real DistributionResourceId shape (cloudfront@v1.67.4
// types.go): exactly one of DistributionId/DistributionTenantId identifies the resource whose
// certificate is being used to validate control of the domain.
type distributionResourceIDXML struct {
	DistributionID       string `xml:"DistributionId"`
	DistributionTenantID string `xml:"DistributionTenantId"`
}

// listDomainConflictsXML models the real ListDomainConflictsRequest body (cloudfront@v1.67.4
// serializers.go:10053, awsRestxml_serializeOpDocumentListDomainConflictsInput).
// DomainControlValidationResource is a pointer so a present-but-empty element still nil-checks
// false and an absent element nil-checks true, matching the SDK's own required-field check
// (validators.go: validateOpListDomainConflictsInput requires the member itself, not any
// particular child of it, to be non-nil).
type listDomainConflictsXML struct {
	DomainControlValidationResource *distributionResourceIDXML `xml:"DomainControlValidationResource"`
	XMLName                         xml.Name                   `xml:"ListDomainConflictsRequest"`
	Domain                          string                     `xml:"Domain"`
	Marker                          string                     `xml:"Marker"`
	MaxItems                        int                        `xml:"MaxItems"`
}

// handleListDomainConflicts reports every existing distribution or distribution tenant that
// already claims the requested Domain, other than the resource identified by
// DomainControlValidationResource -- the resource whose certificate is being used to validate
// control of the domain. Real AWS excludes that resource from its own conflict list
// (api_op_ListDomainConflicts.go:73-77); gopherstack used to drop the field entirely and return
// conflicts for the domain globally.
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
		return xmlResp(
			c,
			http.StatusBadRequest,
			cfErrorXML("InvalidArgument", "Domain is required"),
		)
	}

	if req.DomainControlValidationResource == nil {
		return xmlResp(
			c,
			http.StatusBadRequest,
			cfErrorXML("InvalidArgument", "DomainControlValidationResource is required"),
		)
	}

	distID := req.DomainControlValidationResource.DistributionID
	tenantID := req.DomainControlValidationResource.DistributionTenantID
	if (distID == "") == (tenantID == "") {
		return xmlResp(
			c,
			http.StatusBadRequest,
			cfErrorXML(
				"InvalidArgument",
				"exactly one of DistributionId or DistributionTenantId must be set "+
					"in DomainControlValidationResource",
			),
		)
	}

	conflicts, err := h.Backend.ListDomainConflicts(req.Domain, distID, tenantID)
	if err != nil {
		return h.handleError(c, err)
	}

	// Marker/MaxItems travel in the request body alongside Domain (cloudfront@v1.67.4
	// serializers.go: awsRestxml_serializeOpDocumentListDomainConflictsInput), so pagination
	// uses paginateByMarkerValue, not the query-bound paginateByMarkerID. ResourceID is the
	// cursor key -- findDomainConflicts sorts by it.
	page, _, isTruncated := paginateByMarkerValue(
		conflicts, func(dc DomainConflict) string { return dc.ResourceID }, req.Marker, req.MaxItems,
	)

	nextMarker := ""
	if isTruncated && len(page) > 0 {
		nextMarker = page[len(page)-1].ResourceID
	}

	// The real deserializer (awsRestxml_deserializeDocumentDomainConflictsList,
	// cloudfront@v1.67.4) wraps the list in <DomainConflicts>, and each entry
	// is ALSO named <DomainConflicts> (not <Items>/<DomainConflict>). NextMarker is a
	// sibling of the DomainConflicts entries, not nested inside them.
	var items strings.Builder
	for _, dc := range page {
		fmt.Fprintf(
			&items,
			`<DomainConflicts><Domain>%s</Domain><ResourceType>%s</ResourceType>`+
				`<ResourceId>%s</ResourceId><AccountId>%s</AccountId></DomainConflicts>`,
			dc.Domain, dc.ResourceType, dc.ResourceID, dc.AccountID,
		)
	}

	nextMarkerXML := ""
	if isTruncated {
		nextMarkerXML = fmt.Sprintf(`<NextMarker>%s</NextMarker>`, nextMarker)
	}

	resp := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<DomainConflictList xmlns="%s">`+
		`<DomainConflicts>%s</DomainConflicts>%s`+
		`</DomainConflictList>`,
		cfNS, items.String(), nextMarkerXML)

	return xmlResp(c, http.StatusOK, resp)
}

// ---------------------------------------------------------------------------
// UpdateKeyValueStore handler
// ---------------------------------------------------------------------------
