package cloudfront

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v5"
)

// handleWebACLAssociationError maps AssociateDistributionWebACL/
// DisassociateDistributionWebACL errors. Both ops' own deserializers
// (cloudfront@v1.67.4 deserializers.go) model EntityNotFound for a missing
// distribution, not NoSuchDistribution -- unlike most other distribution
// ops that reuse ErrNotFound.
func (h *Handler) handleWebACLAssociationError(c *echo.Context, err error) error {
	if errors.Is(err, ErrNotFound) {
		return xmlResp(c, http.StatusNotFound, cfErrorXML(codeEntityNotFound, err.Error()))
	}

	return h.handleError(c, err)
}

// backfillOriginGroups inserts an empty <OriginGroups> container into a
// <DistributionConfig> document if the client's request omitted it. Real AWS
// always populates OriginGroups (Quantity 0, empty Items) in every
// GetDistribution/CreateDistribution response even though the SDK models it
// as optional (types.DistributionConfig.OriginGroups, no "required" doc
// comment) -- unlike this repo, the request client (terraform-provider-aws
// v5.100.0 distribution.go resourceDistributionRead) relies on that real-AWS
// guarantee and nil-derefs distributionConfig.OriginGroups.Quantity with no
// nil check, unlike every other optional DistributionConfig member it reads.
// Storing the client's request bytes verbatim as RawConfig (matching real
// AWS's request/response type reuse) otherwise omits OriginGroups whenever
// the caller didn't configure one, crashing that client.
func backfillOriginGroups(rawConfig []byte) []byte {
	if bytes.Contains(rawConfig, []byte("<OriginGroups>")) || bytes.Contains(rawConfig, []byte("<OriginGroups/>")) {
		return rawConfig
	}

	return bytes.Replace(
		rawConfig,
		[]byte("</DistributionConfig>"),
		[]byte("<OriginGroups><Quantity>0</Quantity><Items></Items></OriginGroups></DistributionConfig>"),
		1,
	)
}

type distributionConfigMinimal struct {
	CallerReference string `xml:"CallerReference"`
	Comment         string `xml:"Comment"`
	PriceClass      string `xml:"PriceClass"`
	HTTPVersion     string `xml:"HttpVersion"`
	IsIPV6Enabled   bool   `xml:"IsIPV6Enabled"`
	Enabled         bool   `xml:"Enabled"`
}

// Fallback blobs used when RawConfig has no such child element at all (e.g.
// the minimal test-only DistributionConfig bodies that omit Origins/
// DefaultCacheBehavior/CacheBehaviors/CustomErrorResponses entirely) -- these
// keep the wire element present with an honest zero-quantity/empty shape
// rather than dropping it, since all four are required on DistributionSummary
// (cloudfront@v1.67.4 types.go).
const (
	emptyOriginsXML              = "<Origins><Quantity>0</Quantity><Items></Items></Origins>"
	emptyDefaultCacheBehaviorXML = "<DefaultCacheBehavior></DefaultCacheBehavior>"
	emptyCacheBehaviorsXML       = "<CacheBehaviors><Quantity>0</Quantity></CacheBehaviors>"
	emptyCustomErrorResponsesXML = "<CustomErrorResponses><Quantity>0</Quantity></CustomErrorResponses>"
)

type distributionSummaryXML struct {
	XMLName                 xml.Name `xml:"DistributionSummary"`
	Status                  string   `xml:"Status"`
	LastModifiedTime        string   `xml:"LastModifiedTime"`
	DomainName              string   `xml:"DomainName"`
	Comment                 string   `xml:"Comment"`
	ARN                     string   `xml:"ARN"`
	ID                      string   `xml:"Id"`
	PriceClass              string   `xml:"PriceClass"`
	HTTPVersion             string   `xml:"HttpVersion"`
	ETag                    string   `xml:"ETag,omitempty"`
	WebACLID                string   `xml:"WebACLId"`
	OriginsXML              string   `xml:",innerxml"`
	DefaultCacheBehaviorXML string   `xml:",innerxml"`
	CacheBehaviorsXML       string   `xml:",innerxml"`
	CustomErrorResponsesXML string   `xml:",innerxml"`
	Restrictions            struct {
		GeoRestriction struct {
			RestrictionType string `xml:"RestrictionType"`
			Quantity        int    `xml:"Quantity"`
		} `xml:"GeoRestriction"`
	} `xml:"Restrictions"`
	Aliases struct {
		Items    []string `xml:"Items>CNAME"`
		Quantity int      `xml:"Quantity"`
	} `xml:"Aliases"`
	Enabled           bool `xml:"Enabled"`
	ViewerCertificate struct {
		CloudFrontDefaultCertificate bool `xml:"CloudFrontDefaultCertificate"`
	} `xml:"ViewerCertificate"`
	IsIPV6Enabled bool `xml:"IsIPV6Enabled"`
	Staging       bool `xml:"Staging"`
}

// toDistributionSummaryXML builds the DistributionSummary item shape shared by
// ListDistributions and every ListDistributionsBy* op that returns full
// DistributionList (cloudfront@v1.67.4 deserializers.go,
// awsRestxml_deserializeDocumentDistributionSummary) rather than a bare
// DistributionIdList. ETag and Aliases were previously dropped by the By*
// variants' own minimal item shape even though both are backed by real state
// (d.ETag; h.Backend.ListAliases) -- the ByX list ops disagreed with this
// service's own ListDistributions about the same DistributionSummary shape.
//
// Origins/DefaultCacheBehavior/CacheBehaviors/CustomErrorResponses/WebACLId/
// Staging were FIXED (gopherstack over-wide-response census, 2026-09-18):
// Origins and DefaultCacheBehavior were declared on this struct but never
// populated (always emitted empty regardless of the distribution's real
// config), and CacheBehaviors/CustomErrorResponses/WebACLId/Staging -- all
// required except WebACLId -- were missing as struct members entirely. Now
// projected verbatim from RawConfig (the same byte-passthrough convention
// GetDistributionConfig already uses for the whole document) via
// rawConfigChildElementXML, and from the backend's own WebACL association
// index / Staging flag for the two fields RawConfig doesn't carry.
func (h *Handler) toDistributionSummaryXML(d *Distribution) distributionSummaryXML {
	aliases := h.Backend.ListAliases(d.ID)
	defaultCacheBehaviorXML := rawConfigChildOrDefault(
		d.RawConfig, "DefaultCacheBehavior", emptyDefaultCacheBehaviorXML,
	)
	customErrorResponsesXML := rawConfigChildOrDefault(
		d.RawConfig, "CustomErrorResponses", emptyCustomErrorResponsesXML,
	)
	s := distributionSummaryXML{
		ID:                      d.ID,
		ARN:                     d.ARN,
		Status:                  d.Status,
		DomainName:              d.DomainName,
		Comment:                 d.Comment,
		ETag:                    d.ETag,
		Enabled:                 d.Enabled,
		IsIPV6Enabled:           distributionSummaryIsIPV6(d),
		LastModifiedTime:        d.LastModifiedTime,
		WebACLID:                h.Backend.DistributionWebACLID(d.ID),
		Staging:                 d.Staging,
		OriginsXML:              rawConfigChildOrDefault(d.RawConfig, "Origins", emptyOriginsXML),
		DefaultCacheBehaviorXML: defaultCacheBehaviorXML,
		CacheBehaviorsXML:       rawConfigChildOrDefault(d.RawConfig, "CacheBehaviors", emptyCacheBehaviorsXML),
		CustomErrorResponsesXML: customErrorResponsesXML,
	}
	s.Aliases.Items = aliases
	s.Aliases.Quantity = len(aliases)
	s.ViewerCertificate.CloudFrontDefaultCertificate = true
	s.Restrictions.GeoRestriction.RestrictionType = "none"
	s.PriceClass = distributionSummaryPriceClass(d)
	s.HTTPVersion = distributionSummaryHTTPVersion(d)

	return s
}

// rawConfigChildOrDefault extracts a named child element from RawConfig,
// falling back to def when RawConfig has no such element (see
// rawConfigChildElementXML).
func rawConfigChildOrDefault(raw []byte, name, def string) string {
	if v := rawConfigChildElementXML(raw, name); v != "" {
		return v
	}

	return def
}

// distributionResponseXML builds the full Distribution XML response.
func distributionResponseXML(d *Distribution, inProgressCount int) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<Distribution xmlns="%s">`+
		`<Id>%s</Id>`+
		`<ARN>%s</ARN>`+
		`<Status>%s</Status>`+
		`<LastModifiedTime>%s</LastModifiedTime>`+
		`<DomainName>%s</DomainName>`+
		`<InProgressInvalidationBatches>%d</InProgressInvalidationBatches>`+
		`%s`+
		`</Distribution>`,
		cfNS, d.ID, d.ARN, d.Status, d.LastModifiedTime, d.DomainName, inProgressCount, string(d.RawConfig))
}

func (h *Handler) handleCreateDistribution(c *echo.Context) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	if qErr := validateQuantities(body); qErr != nil {
		return h.handleError(c, qErr)
	}

	var cfg distributionConfigMinimal
	if xmlErr := xml.Unmarshal(body, &cfg); xmlErr != nil {
		return xmlResp(
			c,
			http.StatusBadRequest,
			cfErrorXML("MalformedXML", "invalid DistributionConfig XML"),
		)
	}

	d, createErr := h.Backend.CreateDistribution(
		cfg.CallerReference,
		cfg.Comment,
		cfg.Enabled,
		backfillOriginGroups(body),
	)
	if createErr != nil {
		return h.handleError(c, createErr)
	}

	c.Response().Header().Set("Location", cfPathPrefix+"distribution/"+d.ID)
	c.Response().Header().Set("ETag", d.ETag)

	return xmlResp(c, http.StatusCreated, distributionResponseXML(d, h.Backend.CountInProgressInvalidations(d.ID)))
}

func (h *Handler) handleGetDistribution(c *echo.Context, id string) error {
	d, err := h.Backend.GetDistribution(id)
	if err != nil {
		return h.handleError(c, err)
	}

	c.Response().Header().Set("ETag", d.ETag)

	return xmlResp(c, http.StatusOK, distributionResponseXML(d, h.Backend.CountInProgressInvalidations(d.ID)))
}

func (h *Handler) handleGetDistributionConfig(c *echo.Context, id string) error {
	d, err := h.Backend.GetDistribution(id)
	if err != nil {
		return h.handleError(c, err)
	}

	c.Response().Header().Set("ETag", d.ETag)

	return xmlResp(c, http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>`+string(d.RawConfig))
}

func (h *Handler) handleUpdateDistribution(c *echo.Context, id string) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	if qErr := validateQuantities(body); qErr != nil {
		return h.handleError(c, qErr)
	}

	var cfg distributionConfigMinimal
	if xmlErr := xml.Unmarshal(body, &cfg); xmlErr != nil {
		return xmlResp(
			c,
			http.StatusBadRequest,
			cfErrorXML("MalformedXML", "invalid DistributionConfig XML"),
		)
	}

	current, getErr := h.Backend.GetDistribution(id)
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
				"If-Match ETag did not match the current distribution config ETag",
			),
		)
	}

	d, updateErr := h.Backend.UpdateDistribution(id, cfg.Comment, cfg.Enabled, backfillOriginGroups(body))
	if updateErr != nil {
		return h.handleError(c, updateErr)
	}

	c.Response().Header().Set("ETag", d.ETag)

	return xmlResp(c, http.StatusOK, distributionResponseXML(d, h.Backend.CountInProgressInvalidations(d.ID)))
}

func (h *Handler) handleDeleteDistribution(c *echo.Context, id string) error {
	current, getErr := h.Backend.GetDistribution(id)
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
				"If-Match ETag did not match the current distribution ETag",
			),
		)
	}

	if err := h.Backend.DeleteDistribution(id); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// distributionSummaryPriceClass returns the PriceClass for a distribution, falling back to
// "PriceClass_All" when none was stored (legacy or minimal config).
func distributionSummaryPriceClass(d *Distribution) string {
	if d.PriceClass != "" {
		return d.PriceClass
	}

	if len(d.RawConfig) > 0 {
		var cfg distributionConfigMinimal
		if err := xml.Unmarshal(d.RawConfig, &cfg); err == nil && cfg.PriceClass != "" {
			return cfg.PriceClass
		}
	}

	return "PriceClass_All"
}

// distributionSummaryHTTPVersion returns the HttpVersion for a distribution.
func distributionSummaryHTTPVersion(d *Distribution) string {
	if d.HTTPVersion != "" {
		return d.HTTPVersion
	}

	if len(d.RawConfig) > 0 {
		var cfg distributionConfigMinimal
		if err := xml.Unmarshal(d.RawConfig, &cfg); err == nil && cfg.HTTPVersion != "" {
			return cfg.HTTPVersion
		}
	}

	return "http2"
}

// distributionSummaryIsIPV6(d) reads IsIPV6Enabled from the stored raw config when the
// Distribution field is false (zero value is ambiguous, so raw config is authoritative).
func distributionSummaryIsIPV6(d *Distribution) bool {
	if d.IsIPV6Enabled {
		return true
	}

	if len(d.RawConfig) > 0 {
		var cfg distributionConfigMinimal
		if err := xml.Unmarshal(d.RawConfig, &cfg); err == nil {
			return cfg.IsIPV6Enabled
		}
	}

	return false
}

func (h *Handler) handleListDistributions(c *echo.Context) error {
	dists := h.Backend.ListDistributions()

	// Parse pagination query params.
	marker := c.QueryParam("Marker")
	pageSize := maxItems
	if s := c.QueryParam("MaxItems"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 && n < maxItems {
			pageSize = n
		}
	}

	// Advance past the marker (marker == ID of first item on next page).
	if marker != "" {
		cut := 0
		for cut < len(dists) && dists[cut].ID <= marker {
			cut++
		}
		dists = dists[cut:]
	}

	isTruncated := len(dists) > pageSize
	if isTruncated {
		dists = dists[:pageSize]
	}

	nextMarker := ""
	if isTruncated && len(dists) > 0 {
		nextMarker = dists[len(dists)-1].ID
	}

	summaries := make([]distributionSummaryXML, 0, len(dists))
	for _, d := range dists {
		summaries = append(summaries, h.toDistributionSummaryXML(d))
	}

	// Marker is required on DistributionList (cloudfront@v1.67.4 types/types.go:2528-2538): the
	// echo of the request's Marker, present even when empty/not truncated.
	type distListXML struct {
		XMLName     xml.Name                 `xml:"DistributionList"`
		XMLNS       string                   `xml:"xmlns,attr"`
		Marker      string                   `xml:"Marker"`
		NextMarker  string                   `xml:"NextMarker,omitempty"`
		Items       []distributionSummaryXML `xml:"Items>DistributionSummary"`
		MaxItems    int                      `xml:"MaxItems"`
		Quantity    int                      `xml:"Quantity"`
		IsTruncated bool                     `xml:"IsTruncated"`
	}

	list := distListXML{
		XMLNS:       cfNS,
		Marker:      marker,
		MaxItems:    pageSize,
		Quantity:    len(summaries),
		Items:       summaries,
		IsTruncated: isTruncated,
		NextMarker:  nextMarker,
	}

	out, xmlErr := xml.Marshal(list)
	if xmlErr != nil {
		return xmlResp(
			c,
			http.StatusInternalServerError,
			cfErrorXML("InternalFailure", xmlErr.Error()),
		)
	}

	return xmlResp(c, http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>`+string(out))
}

// --- New operation handlers ---

// associateDistributionWebACLRequestXML models the real
// AssociateDistributionWebACLRequest body: root AssociateDistributionWebACLRequest
// with a single WebACLArn child element (cloudfront@v1.67.4 serializers.go:255,
// awsRestxml_serializeOpDocumentAssociateDistributionWebACLInput). This is a
// different real root from its tenant sibling
// (AssociateDistributionTenantWebACLRequest, handler_distribution_tenants.go) --
// two ops that look identical can have different real root names. The
// previously shared webACLAssociationXML{root: WebACLAssociation, field:
// WebACLId} matched neither this op's real root nor its real field name (an
// ARN, not an ID).
type associateDistributionWebACLRequestXML struct {
	XMLName   xml.Name `xml:"AssociateDistributionWebACLRequest"`
	WebACLArn string   `xml:"WebACLArn"`
}

type copyDistributionRequestXML struct {
	Enabled         *bool    `xml:"Enabled"`
	XMLName         xml.Name `xml:"CopyDistributionRequest"`
	CallerReference string   `xml:"CallerReference"`
}

func (h *Handler) handleAssociateAlias(c *echo.Context, distributionID string) error {
	alias := c.Request().URL.Query().Get("Alias")

	if err := h.Backend.AssociateAlias(distributionID, alias); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleAssociateDistributionWebACL(c *echo.Context, distributionID string) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	if qErr := validateQuantities(body); qErr != nil {
		return h.handleError(c, qErr)
	}

	var req associateDistributionWebACLRequestXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			return xmlResp(
				c,
				http.StatusBadRequest,
				cfErrorXML("MalformedXML", "invalid AssociateDistributionWebACLRequest XML"),
			)
		}
	}

	d, getErr := h.Backend.GetDistribution(distributionID)
	if getErr != nil {
		return h.handleWebACLAssociationError(c, getErr)
	}

	if assocErr := h.Backend.AssociateDistributionWebACL(distributionID, req.WebACLArn); assocErr != nil {
		return h.handleWebACLAssociationError(c, assocErr)
	}

	c.Response().Header().Set("ETag", d.ETag)

	return xmlResp(c, http.StatusOK, fmt.Sprintf(
		`<?xml version="1.0" encoding="UTF-8"?>`+
			`<AssociateDistributionWebACLResult xmlns="%s"><Id>%s</Id><WebACLArn>%s</WebACLArn>`+
			`</AssociateDistributionWebACLResult>`,
		cfNS, d.ID, req.WebACLArn))
}

func (h *Handler) handleCopyDistribution(c *echo.Context, primaryDistID string) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	if qErr := validateQuantities(body); qErr != nil {
		return h.handleError(c, qErr)
	}

	var req copyDistributionRequestXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			return xmlResp(
				c,
				http.StatusBadRequest,
				cfErrorXML("MalformedXML", "invalid CopyDistributionRequest XML"),
			)
		}
	}

	d, copyErr := h.Backend.CopyDistribution(primaryDistID, req.CallerReference, req.Enabled)
	if copyErr != nil {
		return h.handleError(c, copyErr)
	}

	c.Response().Header().Set("Location", cfPathPrefix+"distribution/"+d.ID)
	c.Response().Header().Set("ETag", d.ETag)

	return xmlResp(c, http.StatusCreated, distributionResponseXML(d, h.Backend.CountInProgressInvalidations(d.ID)))
}

type functionAssociationXML struct {
	FunctionARN string `xml:"FunctionARN"`
	EventType   string `xml:"EventType"`
}

type functionAssociationsXML struct {
	XMLName  xml.Name                 `xml:"FunctionAssociations"`
	Items    []functionAssociationXML `xml:"Items>FunctionAssociation"`
	Quantity int                      `xml:"Quantity"`
}

func (h *Handler) handleGetFunctionAssociations(c *echo.Context, distributionID string) error {
	assocs, err := h.Backend.GetDistributionFunctionAssociations(distributionID)
	if err != nil {
		return h.handleError(c, err)
	}

	items := make([]functionAssociationXML, 0, len(assocs))
	for _, a := range assocs {
		items = append(items, functionAssociationXML(a))
	}

	resp := functionAssociationsXML{
		Quantity: len(items),
		Items:    items,
	}

	body, err := xml.MarshalIndent(resp, "", "  ")
	if err != nil {
		return xmlResp(c, http.StatusInternalServerError, cfErrorXML("InternalError", err.Error()))
	}

	return xmlResp(c, http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>`+string(body))
}

func (h *Handler) handleSetFunctionAssociations(c *echo.Context, distributionID string) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	if qErr := validateQuantities(body); qErr != nil {
		return h.handleError(c, qErr)
	}

	var req functionAssociationsXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			return xmlResp(c, http.StatusBadRequest,
				cfErrorXML("MalformedXML", "invalid FunctionAssociations XML"))
		}
	}

	associations := make([]FunctionAssociation, 0, len(req.Items))
	for _, item := range req.Items {
		associations = append(associations, FunctionAssociation(item))
	}

	if setErr := h.Backend.SetDistributionFunctionAssociations(distributionID, associations); setErr != nil {
		return h.handleError(c, setErr)
	}

	return c.NoContent(http.StatusOK)
}

// --- OAI handlers ---

func (h *Handler) handleDisassociateDistributionWebACL(c *echo.Context, distID string) error {
	d, err := h.Backend.GetDistribution(distID)
	if err != nil {
		return h.handleWebACLAssociationError(c, err)
	}

	if disErr := h.Backend.DisassociateDistributionWebACL(distID); disErr != nil {
		return h.handleWebACLAssociationError(c, disErr)
	}

	c.Response().Header().Set("ETag", d.ETag)

	return xmlResp(c, http.StatusOK, distributionResponseXML(d, h.Backend.CountInProgressInvalidations(d.ID)))
}

type distributionConfigWithTagsXML struct {
	XMLName xml.Name `xml:"DistributionConfigWithTags"`
	// DistributionConfig captures only the inner XML verbatim (encoding/xml
	// rejects two fields bound to the same "DistributionConfig" path), which is
	// then parsed a second time into distributionConfigMinimal below and
	// reassembled as the real RawConfig -- NOT re-marshaled from
	// distributionConfigMinimal, which drops nearly every field
	// (Origins/DefaultCacheBehavior/CacheBehaviors/Restrictions/ViewerCertificate/
	// Staging/etc.) and emits the wrong root element name (Go's zero-value
	// XMLName default, "distributionConfigMinimal", not "DistributionConfig"),
	// breaking every client that parses GetDistribution's DistributionConfig,
	// including the real aws-sdk-go-v2 CloudFront client's own deserializer.
	DistributionConfig struct {
		Inner string `xml:",innerxml"`
	} `xml:"DistributionConfig"`
	// Tags is *types.Tags on the wire: Items wraps the Tag list (cloudfront@v1.67.4
	// serializers.go awsRestxml_serializeDocumentTags), not a bare Tags>Tag path.
	Tags []tagXML `xml:"Tags>Items>Tag"`
}

func (h *Handler) handleCreateDistributionWithTags(c *echo.Context) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	if qErr := validateQuantities(body); qErr != nil {
		return h.handleError(c, qErr)
	}

	var req distributionConfigWithTagsXML
	if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "invalid DistributionConfigWithTags XML"))
	}

	// Reconstruct the original <DistributionConfig> element verbatim from its
	// captured inner XML, then parse the shallow fields CreateDistribution needs
	// out of that same reconstruction -- see the struct's doc comment above.
	rawConfig := backfillOriginGroups(
		[]byte("<DistributionConfig>" + req.DistributionConfig.Inner + "</DistributionConfig>"),
	)

	var cfg distributionConfigMinimal
	if xmlErr := xml.Unmarshal(rawConfig, &cfg); xmlErr != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "invalid DistributionConfig XML"))
	}

	d, createErr := h.Backend.CreateDistribution(
		cfg.CallerReference,
		cfg.Comment,
		cfg.Enabled,
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

	return xmlResp(c, http.StatusCreated, distributionResponseXML(d, h.Backend.CountInProgressInvalidations(d.ID)))
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

	if qErr := validateQuantities(body); qErr != nil {
		return h.handleError(c, qErr)
	}

	var req updateWithStagingConfigXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			return xmlResp(
				c, http.StatusBadRequest,
				cfErrorXML("MalformedXML", "invalid UpdateDistributionWithStagingConfigRequest XML"),
			)
		}
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

	return xmlResp(c, http.StatusOK, distributionResponseXML(d, h.Backend.CountInProgressInvalidations(d.ID)))
}

// ---------------------------------------------------------------------------
// UpdateDomainAssociation handler
// ---------------------------------------------------------------------------

func (h *Handler) handleListDistributionsByKeyGroup(c *echo.Context, keyGroupID string) error {
	dists := h.Backend.ListDistributionsByKeyGroup(keyGroupID)

	return h.marshalDistributionIDList(c, dists)
}

func (h *Handler) handleListDistributionsByVpcOriginID(c *echo.Context, vpcOriginID string) error {
	dists := h.Backend.ListDistributionsByVpcOriginID(vpcOriginID)

	return h.marshalDistributionIDList(c, dists)
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

// handleListDistributionsByOwnedResource returns a DistributionIdOwnerList, not the
// DistributionList/DistributionIdList shapes the other ListDistributionsBy* operations use --
// it's the only one in the family (cloudfront@v1.67.4 api_op_ListDistributionsByOwnedResource.go:
// Output.DistributionList is *types.DistributionIdOwnerList).
func (h *Handler) handleListDistributionsByOwnedResource(c *echo.Context, resourceARN string) error {
	dists := h.Backend.ListDistributionsByOwnedResource(resourceARN)

	return h.marshalDistributionIDOwnerList(c, dists)
}

// ---------------------------------------------------------------------------
// ListConflictingAliases handler
// ---------------------------------------------------------------------------

// handleListConflictingAliases paginates via Marker/MaxItems (both query-bound,
// cloudfront@v1.67.4 serializers.go: awsRestxml_serializeOpHttpBindingsListConflictingAliasesInput).
// Real ConflictingAliasesList has no IsTruncated field -- NextMarker's presence alone signals
// truncation (types/types.go:1129-1146).
func (h *Handler) handleListConflictingAliases(c *echo.Context) error {
	alias := c.Request().URL.Query().Get("Alias")
	dists := h.Backend.ListConflictingAliasesByDomain(alias)

	page, pageSize, _, nextMarker := paginateByMarkerID(c, dists, func(d *Distribution) string { return d.ID })

	type conflictingSummary struct {
		XMLName   xml.Name `xml:"ConflictingAlias"`
		Alias     string   `xml:"Alias"`
		DistID    string   `xml:"DistributionId"`
		AccountID string   `xml:"AccountId"`
	}
	type conflictList struct {
		XMLName    xml.Name             `xml:"ConflictingAliasesList"`
		XMLNS      string               `xml:"xmlns,attr"`
		NextMarker string               `xml:"NextMarker,omitempty"`
		Items      []conflictingSummary `xml:"Items>ConflictingAlias"`
		MaxItems   int                  `xml:"MaxItems"`
		Quantity   int                  `xml:"Quantity"`
	}

	summaries := make([]conflictingSummary, 0, len(page))
	for _, d := range page {
		summaries = append(summaries, conflictingSummary{
			Alias:     alias,
			DistID:    d.ID,
			AccountID: h.Backend.AccountID(),
		})
	}
	list := conflictList{
		XMLNS:      cfNS,
		NextMarker: nextMarker,
		MaxItems:   pageSize,
		Quantity:   len(summaries),
		Items:      summaries,
	}
	out, xmlErr := xml.Marshal(list)
	if xmlErr != nil {
		return h.handleError(c, xmlErr)
	}

	return xmlResp(c, http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>`+string(out))
}

// ---------------------------------------------------------------------------
// ListDomainConflicts handler
// ---------------------------------------------------------------------------

func (h *Handler) handleListDistributionsByWebACLID(c *echo.Context, webACLID string) error {
	dists := h.Backend.ListDistributionsByWebACLID(webACLID)

	return h.marshalDistributionList(c, dists)
}

func (h *Handler) handleListDistributionsByCachePolicyID(c *echo.Context, policyID string) error {
	dists := h.Backend.ListDistributionsByCachePolicyID(policyID)

	return h.marshalDistributionIDList(c, dists)
}

func (h *Handler) handleListDistributionsByOriginRequestPolicyID(c *echo.Context, policyID string) error {
	dists := h.Backend.ListDistributionsByOriginRequestPolicyID(policyID)

	return h.marshalDistributionIDList(c, dists)
}

func (h *Handler) handleListDistributionsByResponseHeadersPolicyID(c *echo.Context, policyID string) error {
	dists := h.Backend.ListDistributionsByResponseHeadersPolicyID(policyID)

	return h.marshalDistributionIDList(c, dists)
}

// listDistributionsByRealtimeLogConfigBody decodes ListDistributionsByRealtimeLogConfigInput.
// Real ListDistributionsByRealtimeLogConfig is POST with no URI label or query binding at all --
// RealtimeLogConfigArn, Marker and MaxItems all travel as XML elements under the root
// ListDistributionsByRealtimeLogConfigRequest (cloudfront@v1.67.4 serializers.go:
// awsRestxml_serializeOpDocumentListDistributionsByRealtimeLogConfigInput), unlike every other
// operation in the ListDistributionsBy* family, which binds Marker/MaxItems to the query string.
type listDistributionsByRealtimeLogConfigBody struct {
	RealtimeLogConfigArn string `xml:"RealtimeLogConfigArn"`
	Marker               string `xml:"Marker"`
	MaxItems             int    `xml:"MaxItems"`
}

func decodeListDistributionsByRealtimeLogConfigBody(c *echo.Context) listDistributionsByRealtimeLogConfigBody {
	body, err := readBody(c)
	if err != nil {
		return listDistributionsByRealtimeLogConfigBody{}
	}

	var req listDistributionsByRealtimeLogConfigBody
	_ = xml.Unmarshal(body, &req)

	return req
}

func (h *Handler) handleListDistributionsByRealtimeLogConfig(
	c *echo.Context, req listDistributionsByRealtimeLogConfigBody,
) error {
	dists := h.Backend.ListDistributionsByRealtimeLogConfigARN(req.RealtimeLogConfigArn)

	page, pageSize, isTruncated := paginateByMarkerValue(
		dists,
		func(d *Distribution) string { return d.ID },
		req.Marker,
		req.MaxItems,
	)

	return h.writeDistributionList(c, req.Marker, page, pageSize, isTruncated)
}

// marshalDistributionList paginates via Marker/MaxItems (both query-bound for every caller
// except ListDistributionsByRealtimeLogConfig, which calls writeDistributionList directly with
// its own body-bound pagination) and writes the DistributionList shape (cloudfront@v1.67.4
// types/types.go:2522-2554): ListDistributionsByAnycastIpListId, ByConnectionFunction,
// ByConnectionMode, ByTrustStore, ByWebACLId, and ByRealtimeLogConfig all return this shape.
func (h *Handler) marshalDistributionList(c *echo.Context, dists []*Distribution) error {
	page, pageSize, isTruncated, _ := paginateByMarkerID(c, dists, func(d *Distribution) string { return d.ID })

	return h.writeDistributionList(c, c.QueryParam("Marker"), page, pageSize, isTruncated)
}

// Marker is required on DistributionList (cloudfront@v1.67.4 types/types.go:2528-2538): the
// echo of the request's Marker, present even when empty/not truncated. marker is passed in
// rather than read from c since ByRealtimeLogConfig's Marker travels in the XML body, not the
// query string.
func (h *Handler) writeDistributionList(
	c *echo.Context, marker string, page []*Distribution, pageSize int, isTruncated bool,
) error {
	type distList struct {
		XMLName     xml.Name                 `xml:"DistributionList"`
		XMLNS       string                   `xml:"xmlns,attr"`
		Marker      string                   `xml:"Marker"`
		NextMarker  string                   `xml:"NextMarker,omitempty"`
		Items       []distributionSummaryXML `xml:"Items>DistributionSummary"`
		MaxItems    int                      `xml:"MaxItems"`
		Quantity    int                      `xml:"Quantity"`
		IsTruncated bool                     `xml:"IsTruncated"`
	}
	summaries := make([]distributionSummaryXML, 0, len(page))
	for _, d := range page {
		summaries = append(summaries, h.toDistributionSummaryXML(d))
	}
	nextMarker := ""
	if isTruncated && len(page) > 0 {
		nextMarker = page[len(page)-1].ID
	}
	list := distList{
		XMLNS: cfNS, Marker: marker, NextMarker: nextMarker, MaxItems: pageSize, Quantity: len(summaries),
		Items: summaries, IsTruncated: isTruncated,
	}
	out, xmlErr := xml.Marshal(list)
	if xmlErr != nil {
		return h.handleError(c, xmlErr)
	}

	return xmlResp(c, http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>`+string(out))
}

// marshalDistributionIDList paginates via Marker/MaxItems (query-bound) and writes the
// DistributionIdList shape (cloudfront@v1.67.4 types/types.go:2429-2459): used by
// ListDistributionsByCachePolicyId, ByKeyGroup, ByOriginRequestPolicyId,
// ByResponseHeadersPolicyId, and ByVpcOriginId -- these return only distribution IDs, not full
// DistributionSummary objects.
func (h *Handler) marshalDistributionIDList(c *echo.Context, dists []*Distribution) error {
	page, pageSize, isTruncated, nextMarker := paginateByMarkerID(
		c,
		dists,
		func(d *Distribution) string { return d.ID },
	)

	// Marker is required on DistributionIdList (cloudfront@v1.67.4 types/types.go:2435-2445): the
	// echo of the request's Marker, present even when empty/not truncated.
	type distIDList struct {
		XMLName     xml.Name `xml:"DistributionIdList"`
		XMLNS       string   `xml:"xmlns,attr"`
		Marker      string   `xml:"Marker"`
		NextMarker  string   `xml:"NextMarker,omitempty"`
		Items       []string `xml:"Items>DistributionId"`
		MaxItems    int      `xml:"MaxItems"`
		Quantity    int      `xml:"Quantity"`
		IsTruncated bool     `xml:"IsTruncated"`
	}
	ids := make([]string, 0, len(page))
	for _, d := range page {
		ids = append(ids, d.ID)
	}
	list := distIDList{
		XMLNS: cfNS, Marker: c.QueryParam("Marker"), NextMarker: nextMarker, MaxItems: pageSize,
		Quantity: len(ids), Items: ids, IsTruncated: isTruncated,
	}
	out, xmlErr := xml.Marshal(list)
	if xmlErr != nil {
		return h.handleError(c, xmlErr)
	}

	return xmlResp(c, http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>`+string(out))
}

// marshalDistributionIDOwnerList paginates via Marker/MaxItems (query-bound) and writes the
// DistributionIdOwnerList shape (cloudfront@v1.67.4 types/types.go:2482-2520), used only by
// ListDistributionsByOwnedResource. This emulator is single-account, so OwnerAccountId is
// always the backend's own account.
func (h *Handler) marshalDistributionIDOwnerList(c *echo.Context, dists []*Distribution) error {
	page, pageSize, isTruncated, nextMarker := paginateByMarkerID(
		c,
		dists,
		func(d *Distribution) string { return d.ID },
	)

	type distIDOwner struct {
		XMLName        xml.Name `xml:"DistributionIdOwner"`
		DistributionID string   `xml:"DistributionId"`
		OwnerAccountID string   `xml:"OwnerAccountId"`
	}
	// Marker is required on DistributionIdOwnerList (cloudfront@v1.67.4
	// types/types.go:2488-2496): the echo of the request's Marker, present even when
	// empty/not truncated.
	type distIDOwnerList struct {
		XMLName     xml.Name      `xml:"DistributionIdOwnerList"`
		XMLNS       string        `xml:"xmlns,attr"`
		Marker      string        `xml:"Marker"`
		NextMarker  string        `xml:"NextMarker,omitempty"`
		Items       []distIDOwner `xml:"Items>DistributionIdOwner"`
		MaxItems    int           `xml:"MaxItems"`
		Quantity    int           `xml:"Quantity"`
		IsTruncated bool          `xml:"IsTruncated"`
	}
	items := make([]distIDOwner, 0, len(page))
	for _, d := range page {
		items = append(items, distIDOwner{DistributionID: d.ID, OwnerAccountID: h.Backend.AccountID()})
	}
	list := distIDOwnerList{
		XMLNS: cfNS, Marker: c.QueryParam("Marker"), NextMarker: nextMarker, MaxItems: pageSize,
		Quantity: len(items), Items: items, IsTruncated: isTruncated,
	}
	out, xmlErr := xml.Marshal(list)
	if xmlErr != nil {
		return h.handleError(c, xmlErr)
	}

	return xmlResp(c, http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>`+string(out))
}
