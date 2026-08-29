package cloudfront

import (
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

type distributionConfigMinimal struct {
	CallerReference string `xml:"CallerReference"`
	Comment         string `xml:"Comment"`
	PriceClass      string `xml:"PriceClass"`
	HTTPVersion     string `xml:"HttpVersion"`
	IsIPV6Enabled   bool   `xml:"IsIPV6Enabled"`
	Enabled         bool   `xml:"Enabled"`
}

type distributionSummaryXML struct {
	XMLName xml.Name `xml:"DistributionSummary"`
	Origins struct {
		Inner    string `xml:",innerxml"`
		Quantity int    `xml:"Quantity"`
	} `xml:"Origins"`
	DefaultCacheBehavior struct {
		Inner string `xml:",innerxml"`
	} `xml:"DefaultCacheBehavior"`
	Status           string `xml:"Status"`
	LastModifiedTime string `xml:"LastModifiedTime"`
	DomainName       string `xml:"DomainName"`
	Comment          string `xml:"Comment"`
	ARN              string `xml:"ARN"`
	ID               string `xml:"Id"`
	PriceClass       string `xml:"PriceClass"`
	HTTPVersion      string `xml:"HttpVersion"`
	Restrictions     struct {
		GeoRestriction struct {
			RestrictionType string `xml:"RestrictionType"`
			Quantity        int    `xml:"Quantity"`
		} `xml:"GeoRestriction"`
	} `xml:"Restrictions"`
	Aliases struct {
		Quantity int `xml:"Quantity"`
	} `xml:"Aliases"`
	Enabled           bool `xml:"Enabled"`
	ViewerCertificate struct {
		CloudFrontDefaultCertificate bool `xml:"CloudFrontDefaultCertificate"`
	} `xml:"ViewerCertificate"`
	IsIPV6Enabled bool `xml:"IsIPV6Enabled"`
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
		body,
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

	return xmlResp(c, http.StatusOK, string(d.RawConfig))
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

	d, updateErr := h.Backend.UpdateDistribution(id, cfg.Comment, cfg.Enabled, body)
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
		aliases := h.Backend.ListAliases(d.ID)
		s := distributionSummaryXML{
			ID:               d.ID,
			ARN:              d.ARN,
			Status:           d.Status,
			DomainName:       d.DomainName,
			Comment:          d.Comment,
			Enabled:          d.Enabled,
			IsIPV6Enabled:    distributionSummaryIsIPV6(d),
			LastModifiedTime: d.LastModifiedTime,
		}
		s.Aliases.Quantity = len(aliases)
		s.ViewerCertificate.CloudFrontDefaultCertificate = true
		s.Restrictions.GeoRestriction.RestrictionType = "none"
		s.PriceClass = distributionSummaryPriceClass(d)
		s.HTTPVersion = distributionSummaryHTTPVersion(d)
		summaries = append(summaries, s)
	}

	type distListXML struct {
		XMLName     xml.Name                 `xml:"DistributionList"`
		XMLNS       string                   `xml:"xmlns,attr"`
		NextMarker  string                   `xml:"NextMarker,omitempty"`
		Items       []distributionSummaryXML `xml:"Items>DistributionSummary"`
		MaxItems    int                      `xml:"MaxItems"`
		Quantity    int                      `xml:"Quantity"`
		IsTruncated bool                     `xml:"IsTruncated"`
	}

	list := distListXML{
		XMLNS:       cfNS,
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

	d, copyErr := h.Backend.CopyDistribution(primaryDistID, req.CallerReference)
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
	XMLName            xml.Name                  `xml:"DistributionConfigWithTags"`
	DistributionConfig distributionConfigMinimal `xml:"DistributionConfig"`
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

func (h *Handler) handleListDistributionsByWebACLID(c *echo.Context, webACLID string) error {
	dists := h.Backend.ListDistributionsByWebACLID(webACLID)

	return h.marshalDistributionList(c, dists)
}

func (h *Handler) handleListDistributionsByCachePolicyID(c *echo.Context, policyID string) error {
	dists := h.Backend.ListDistributionsByCachePolicyID(policyID)

	return h.marshalDistributionList(c, dists)
}

func (h *Handler) handleListDistributionsByOriginRequestPolicyID(c *echo.Context, policyID string) error {
	dists := h.Backend.ListDistributionsByOriginRequestPolicyID(policyID)

	return h.marshalDistributionList(c, dists)
}

func (h *Handler) handleListDistributionsByResponseHeadersPolicyID(c *echo.Context, policyID string) error {
	dists := h.Backend.ListDistributionsByResponseHeadersPolicyID(policyID)

	return h.marshalDistributionList(c, dists)
}

// listDistributionsByRealtimeLogConfigBody decodes the ARN out of the request
// body. Real ListDistributionsByRealtimeLogConfig is POST with no URI label
// or query binding at all -- RealtimeLogConfigArn travels as an XML element
// under the root ListDistributionsByRealtimeLogConfigRequest (cloudfront@v1.67.4
// serializers.go: awsRestxml_serializeOpDocumentListDistributionsByRealtimeLogConfigInput).
type listDistributionsByRealtimeLogConfigBody struct {
	RealtimeLogConfigArn string `xml:"RealtimeLogConfigArn"`
}

func extractRealtimeLogConfigArn(c *echo.Context) string {
	body, err := readBody(c)
	if err != nil {
		return ""
	}

	var req listDistributionsByRealtimeLogConfigBody
	if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
		return ""
	}

	return req.RealtimeLogConfigArn
}

func (h *Handler) handleListDistributionsByRealtimeLogConfig(c *echo.Context, arn string) error {
	dists := h.Backend.ListDistributionsByRealtimeLogConfigARN(arn)

	return h.marshalDistributionList(c, dists)
}

func (h *Handler) marshalDistributionList(c *echo.Context, dists []*Distribution) error {
	type distSummary struct {
		XMLName    xml.Name `xml:"DistributionSummary"`
		ID         string   `xml:"Id"`
		ARN        string   `xml:"ARN"`
		Status     string   `xml:"Status"`
		DomainName string   `xml:"DomainName"`
	}
	type distList struct {
		XMLName     xml.Name      `xml:"DistributionList"`
		XMLNS       string        `xml:"xmlns,attr"`
		Items       []distSummary `xml:"Items>DistributionSummary"`
		MaxItems    int           `xml:"MaxItems"`
		Quantity    int           `xml:"Quantity"`
		IsTruncated bool          `xml:"IsTruncated"`
	}
	summaries := make([]distSummary, 0, len(dists))
	for _, d := range dists {
		summaries = append(summaries, distSummary{
			ID: d.ID, ARN: d.ARN, Status: d.Status, DomainName: d.DomainName,
		})
	}
	list := distList{XMLNS: cfNS, MaxItems: maxItems, Quantity: len(summaries), Items: summaries}
	out, xmlErr := xml.Marshal(list)
	if xmlErr != nil {
		return h.handleError(c, xmlErr)
	}

	return xmlResp(c, http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>`+string(out))
}
