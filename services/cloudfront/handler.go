package cloudfront

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	cfNS         = "http://cloudfront.amazonaws.com/doc/2020-05-31/"
	cfPathPrefix = "/2020-05-31/"
	maxItems     = 100
)

// Handler is the Echo HTTP handler for AWS CloudFront operations (REST-XML protocol).
type Handler struct {
	Backend *InMemoryBackend
}

// NewHandler creates a new CloudFront handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "CloudFront" }

// GetSupportedOperations returns the list of supported CloudFront operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateDistribution",
		"GetDistribution",
		"GetDistributionConfig",
		"UpdateDistribution",
		"DeleteDistribution",
		"ListDistributions",
		"CreateOriginAccessIdentity",
		"GetOriginAccessIdentity",
		"ListOriginAccessIdentities",
		"DeleteOriginAccessIdentity",
		"TagResource",
		"UntagResource",
		"ListTagsForResource",
		"CreateInvalidation",
		"ListInvalidations",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "cloudfront" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this CloudFront instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a function that matches AWS CloudFront REST requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().URL.Path, cfPathPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityPathVersioned }

// ExtractOperation extracts the CloudFront operation name from the request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	op, _ := parseCFPath(c.Request().Method, c.Request().URL.Path, c.Request().URL.Query().Get("Resource"))

	return op
}

// ExtractResource extracts the primary resource identifier from the URL path.
func (h *Handler) ExtractResource(c *echo.Context) string {
	_, res := parseCFPath(c.Request().Method, c.Request().URL.Path, c.Request().URL.Query().Get("Resource"))

	return res
}

// cfErrorXML returns an XML error response string.
func cfErrorXML(code, message string) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<ErrorResponse xmlns="%s"><Error><Type>Sender</Type><Code>%s</Code><Message>%s</Message></Error></ErrorResponse>`,
		cfNS, code, message)
}

// xmlResp writes an XML response with the given status code.
func xmlResp(c *echo.Context, status int, body string) error {
	c.Response().Header().Set("Content-Type", "text/xml")

	return c.XMLBlob(status, []byte(body))
}

// parseCFPath maps HTTP method + path to (operationName, resourceID).
//
//nolint:cyclop // dispatch table for 15 REST operations is inherently wide
func parseCFPath(method, path, resourceParam string) (string, string) {
	suffix := strings.TrimPrefix(path, cfPathPrefix)

	switch {
	case suffix == "distribution" && method == http.MethodPost:
		return "CreateDistribution", ""
	case suffix == "distribution" && method == http.MethodGet:
		return "ListDistributions", ""
	case strings.HasPrefix(suffix, "distribution/") && strings.HasSuffix(suffix, "/config"):
		id := strings.TrimPrefix(suffix, "distribution/")
		id = strings.TrimSuffix(id, "/config")
		switch method {
		case http.MethodGet:
			return "GetDistributionConfig", id
		case http.MethodPut:
			return "UpdateDistribution", id
		}
	case strings.HasPrefix(suffix, "distribution/") && strings.HasSuffix(suffix, "/invalidation"):
		id := strings.TrimPrefix(suffix, "distribution/")
		id = strings.TrimSuffix(id, "/invalidation")
		switch method {
		case http.MethodPost:
			return "CreateInvalidation", id
		case http.MethodGet:
			return "ListInvalidations", id
		}
	case strings.HasPrefix(suffix, "distribution/") && !strings.Contains(strings.TrimPrefix(suffix, "distribution/"), "/"):
		id := strings.TrimPrefix(suffix, "distribution/")
		switch method {
		case http.MethodGet:
			return "GetDistribution", id
		case http.MethodDelete:
			return "DeleteDistribution", id
		}
	case suffix == "origin-access-identity/cloudfront" && method == http.MethodPost:
		return "CreateOriginAccessIdentity", ""
	case suffix == "origin-access-identity/cloudfront" && method == http.MethodGet:
		return "ListOriginAccessIdentities", ""
	case strings.HasPrefix(suffix, "origin-access-identity/cloudfront/"):
		id := strings.TrimPrefix(suffix, "origin-access-identity/cloudfront/")
		switch method {
		case http.MethodGet:
			return "GetOriginAccessIdentity", id
		case http.MethodDelete:
			return "DeleteOriginAccessIdentity", id
		}
	case suffix == "tagging" || strings.HasPrefix(suffix, "tagging?"):
		switch method {
		case http.MethodGet:
			return "ListTagsForResource", resourceParam
		case http.MethodPost:
			return "TagResource", resourceParam
		case http.MethodDelete:
			return "UntagResource", resourceParam
		}
	}

	return "Unknown", ""
}

// --- Incoming XML structs ---

type distributionConfigMinimal struct {
	CallerReference string `xml:"CallerReference"`
	Comment         string `xml:"Comment"`
	Enabled         bool   `xml:"Enabled"`
}

type oaiConfigXML struct {
	XMLName         xml.Name `xml:"CloudFrontOriginAccessIdentityConfig"`
	CallerReference string   `xml:"CallerReference"`
	Comment         string   `xml:"Comment"`
}

type oaiResponseXML struct {
	XMLName           xml.Name     `xml:"CloudFrontOriginAccessIdentity"`
	XMLNS             string       `xml:"xmlns,attr"`
	ID                string       `xml:"Id"`
	S3CanonicalUserID string       `xml:"S3CanonicalUserId"`
	Config            oaiConfigXML `xml:"CloudFrontOriginAccessIdentityConfig"`
}

type oaiSummary struct {
	XMLName           xml.Name `xml:"CloudFrontOriginAccessIdentitySummary"`
	ID                string   `xml:"Id"`
	S3CanonicalUserID string   `xml:"S3CanonicalUserId"`
	Comment           string   `xml:"Comment"`
}

type oaiList struct {
	XMLName     xml.Name     `xml:"CloudFrontOriginAccessIdentityList"`
	XMLNS       string       `xml:"xmlns,attr"`
	Items       []oaiSummary `xml:"Items>CloudFrontOriginAccessIdentitySummary"`
	MaxItems    int          `xml:"MaxItems"`
	Quantity    int          `xml:"Quantity"`
	IsTruncated bool         `xml:"IsTruncated"`
}

type tagXML struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

type tagsXML struct {
	XMLName xml.Name `xml:"Tags"`
	XMLNS   string   `xml:"xmlns,attr,omitempty"`
	Items   []tagXML `xml:"Items>Tag"`
}

type untagBody struct {
	Keys []string `xml:"TagKeys>Key"`
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
	Status       string `xml:"Status"`
	DomainName   string `xml:"DomainName"`
	Comment      string `xml:"Comment"`
	ARN          string `xml:"ARN"`
	ID           string `xml:"Id"`
	PriceClass   string `xml:"PriceClass"`
	HTTPVersion  string `xml:"HttpVersion"`
	Restrictions struct {
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
func distributionResponseXML(d *Distribution) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<Distribution xmlns="%s">`+
		`<Id>%s</Id>`+
		`<ARN>%s</ARN>`+
		`<Status>%s</Status>`+
		`<DomainName>%s</DomainName>`+
		`<InProgressInvalidationBatches>0</InProgressInvalidationBatches>`+
		`%s`+
		`</Distribution>`,
		cfNS, d.ID, d.ARN, d.Status, d.DomainName, string(d.RawConfig))
}

// Handler returns the Echo handler function for CloudFront requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		log := logger.Load(c.Request().Context())
		operation, resource := parseCFPath(
			c.Request().Method,
			c.Request().URL.Path,
			c.Request().URL.Query().Get("Resource"),
		)

		log.Debug("cloudfront request", "operation", operation, "resource", resource)

		return h.dispatch(c, operation, resource)
	}
}

//nolint:cyclop // dispatch table for 15 REST operations is inherently wide
func (h *Handler) dispatch(c *echo.Context, operation, resource string) error {
	switch operation {
	case "CreateDistribution":
		return h.handleCreateDistribution(c)
	case "GetDistribution":
		return h.handleGetDistribution(c, resource)
	case "GetDistributionConfig":
		return h.handleGetDistributionConfig(c, resource)
	case "UpdateDistribution":
		return h.handleUpdateDistribution(c, resource)
	case "DeleteDistribution":
		return h.handleDeleteDistribution(c, resource)
	case "ListDistributions":
		return h.handleListDistributions(c)
	case "CreateOriginAccessIdentity":
		return h.handleCreateOAI(c)
	case "GetOriginAccessIdentity":
		return h.handleGetOAI(c, resource)
	case "ListOriginAccessIdentities":
		return h.handleListOAIs(c)
	case "DeleteOriginAccessIdentity":
		return h.handleDeleteOAI(c, resource)
	case "TagResource":
		return h.handleTagResource(c)
	case "UntagResource":
		return h.handleUntagResource(c)
	case "ListTagsForResource":
		return h.handleListTagsForResource(c)
	case "CreateInvalidation":
		return h.handleCreateInvalidation(c, resource)
	case "ListInvalidations":
		return h.handleListInvalidations(c, resource)
	default:
		return xmlResp(c, http.StatusNotFound, cfErrorXML("NoSuchOperation", "unknown operation: "+operation))
	}
}

func (h *Handler) handleError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, ErrNotFound):
		return xmlResp(c, http.StatusNotFound, cfErrorXML("NoSuchDistribution", err.Error()))
	case errors.Is(err, ErrOAINotFound):
		return xmlResp(c, http.StatusNotFound, cfErrorXML("NoSuchCloudFrontOriginAccessIdentity", err.Error()))
	default:
		return xmlResp(c, http.StatusInternalServerError, cfErrorXML("InternalFailure", err.Error()))
	}
}

// --- Distribution handlers ---

func (h *Handler) handleCreateDistribution(c *echo.Context) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	var cfg distributionConfigMinimal
	if xmlErr := xml.Unmarshal(body, &cfg); xmlErr != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "invalid DistributionConfig XML"))
	}

	d, createErr := h.Backend.CreateDistribution(cfg.CallerReference, cfg.Comment, cfg.Enabled, body)
	if createErr != nil {
		return h.handleError(c, createErr)
	}

	c.Response().Header().Set("Location", cfPathPrefix+"distribution/"+d.ID)
	c.Response().Header().Set("ETag", d.ETag)

	return xmlResp(c, http.StatusCreated, distributionResponseXML(d))
}

func (h *Handler) handleGetDistribution(c *echo.Context, id string) error {
	d, err := h.Backend.GetDistribution(id)
	if err != nil {
		return h.handleError(c, err)
	}

	c.Response().Header().Set("ETag", d.ETag)

	return xmlResp(c, http.StatusOK, distributionResponseXML(d))
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

	var cfg distributionConfigMinimal
	if xmlErr := xml.Unmarshal(body, &cfg); xmlErr != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "invalid DistributionConfig XML"))
	}

	d, updateErr := h.Backend.UpdateDistribution(id, cfg.Comment, cfg.Enabled, body)
	if updateErr != nil {
		return h.handleError(c, updateErr)
	}

	c.Response().Header().Set("ETag", d.ETag)

	return xmlResp(c, http.StatusOK, distributionResponseXML(d))
}

func (h *Handler) handleDeleteDistribution(c *echo.Context, id string) error {
	if err := h.Backend.DeleteDistribution(id); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleListDistributions(c *echo.Context) error {
	dists := h.Backend.ListDistributions()

	summaries := make([]distributionSummaryXML, 0, len(dists))
	for _, d := range dists {
		s := distributionSummaryXML{
			ID:         d.ID,
			ARN:        d.ARN,
			Status:     d.Status,
			DomainName: d.DomainName,
			Comment:    d.Comment,
			Enabled:    d.Enabled,
		}
		s.ViewerCertificate.CloudFrontDefaultCertificate = true
		s.Restrictions.GeoRestriction.RestrictionType = "none"
		s.PriceClass = "PriceClass_All"
		s.HTTPVersion = "http2"
		summaries = append(summaries, s)
	}

	type distListXML struct {
		XMLName     xml.Name                 `xml:"DistributionList"`
		XMLNS       string                   `xml:"xmlns,attr"`
		Items       []distributionSummaryXML `xml:"Items>DistributionSummary"`
		MaxItems    int                      `xml:"MaxItems"`
		Quantity    int                      `xml:"Quantity"`
		IsTruncated bool                     `xml:"IsTruncated"`
	}

	list := distListXML{
		XMLNS:    cfNS,
		MaxItems: maxItems,
		Quantity: len(summaries),
		Items:    summaries,
	}

	out, xmlErr := xml.Marshal(list)
	if xmlErr != nil {
		return xmlResp(c, http.StatusInternalServerError, cfErrorXML("InternalFailure", xmlErr.Error()))
	}

	return xmlResp(c, http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>`+string(out))
}

// --- OAI handlers ---

func (h *Handler) handleCreateOAI(c *echo.Context) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	var cfg oaiConfigXML
	if xmlErr := xml.Unmarshal(body, &cfg); xmlErr != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "invalid OAI config XML"))
	}

	oai, createErr := h.Backend.CreateOAI(cfg.CallerReference, cfg.Comment)
	if createErr != nil {
		return h.handleError(c, createErr)
	}

	c.Response().Header().Set("Location", cfPathPrefix+"origin-access-identity/cloudfront/"+oai.ID)
	c.Response().Header().Set("ETag", oai.ETag)

	resp := oaiResponseXML{
		XMLNS:             cfNS,
		ID:                oai.ID,
		S3CanonicalUserID: oai.S3CanonicalUserID,
		Config: oaiConfigXML{
			CallerReference: oai.CallerReference,
			Comment:         oai.Comment,
		},
	}

	out, xmlErr := xml.Marshal(resp)
	if xmlErr != nil {
		return h.handleError(c, xmlErr)
	}

	return xmlResp(c, http.StatusCreated, `<?xml version="1.0" encoding="UTF-8"?>`+string(out))
}

func (h *Handler) handleGetOAI(c *echo.Context, id string) error {
	oai, err := h.Backend.GetOAI(id)
	if err != nil {
		return h.handleError(c, err)
	}

	c.Response().Header().Set("ETag", oai.ETag)

	resp := oaiResponseXML{
		XMLNS:             cfNS,
		ID:                oai.ID,
		S3CanonicalUserID: oai.S3CanonicalUserID,
		Config: oaiConfigXML{
			CallerReference: oai.CallerReference,
			Comment:         oai.Comment,
		},
	}

	out, xmlErr := xml.Marshal(resp)
	if xmlErr != nil {
		return h.handleError(c, xmlErr)
	}

	return xmlResp(c, http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>`+string(out))
}

func (h *Handler) handleListOAIs(c *echo.Context) error {
	oais := h.Backend.ListOAIs()

	summaries := make([]oaiSummary, 0, len(oais))
	for _, oai := range oais {
		summaries = append(summaries, oaiSummary{
			ID:                oai.ID,
			S3CanonicalUserID: oai.S3CanonicalUserID,
			Comment:           oai.Comment,
		})
	}

	list := oaiList{
		XMLNS:    cfNS,
		MaxItems: maxItems,
		Quantity: len(summaries),
		Items:    summaries,
	}

	out, xmlErr := xml.Marshal(list)
	if xmlErr != nil {
		return h.handleError(c, xmlErr)
	}

	return xmlResp(c, http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>`+string(out))
}

func (h *Handler) handleDeleteOAI(c *echo.Context, id string) error {
	if err := h.Backend.DeleteOAI(id); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// --- Tagging handlers ---

func (h *Handler) handleTagResource(c *echo.Context) error {
	resourceARN := c.Request().URL.Query().Get("Resource")

	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	var tags tagsXML
	if xmlErr := xml.Unmarshal(body, &tags); xmlErr != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "invalid Tags XML"))
	}

	kv := make(map[string]string, len(tags.Items))
	for _, t := range tags.Items {
		kv[t.Key] = t.Value
	}

	if tagErr := h.Backend.TagResource(resourceARN, kv); tagErr != nil {
		return h.handleError(c, tagErr)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleUntagResource(c *echo.Context) error {
	resourceARN := c.Request().URL.Query().Get("Resource")

	var keys []string

	// Keys may be in query params or body.
	keys = append(keys, c.Request().URL.Query()["TagKeys.Key"]...)

	if len(keys) == 0 {
		body, err := readBody(c)
		if err == nil && len(body) > 0 {
			var ub untagBody
			if xmlErr := xml.Unmarshal(body, &ub); xmlErr == nil {
				keys = ub.Keys
			}
		}
	}

	if untagErr := h.Backend.UntagResource(resourceARN, keys); untagErr != nil {
		return h.handleError(c, untagErr)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleListTagsForResource(c *echo.Context) error {
	resourceARN := c.Request().URL.Query().Get("Resource")

	kv, err := h.Backend.ListTags(resourceARN)
	if err != nil {
		return h.handleError(c, err)
	}

	items := make([]tagXML, 0, len(kv))
	for k, v := range kv {
		items = append(items, tagXML{Key: k, Value: v})
	}

	tags := tagsXML{XMLNS: cfNS, Items: items}

	type listTagsResp struct {
		XMLName xml.Name `xml:"ListTagsForResourceResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Tags    tagsXML  `xml:"Tags"`
	}

	resp := listTagsResp{XMLNS: cfNS, Tags: tags}

	out, xmlErr := xml.Marshal(resp)
	if xmlErr != nil {
		return h.handleError(c, xmlErr)
	}

	return xmlResp(c, http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>`+string(out))
}

// --- Invalidation stubs ---

func (h *Handler) handleCreateInvalidation(c *echo.Context, _ string) error {
	id := generateID()
	resp := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<Invalidation xmlns="%s">`+
		`<Id>%s</Id>`+
		`<Status>InProgress</Status>`+
		`<CreateTime>2024-01-01T00:00:00Z</CreateTime>`+
		`</Invalidation>`,
		cfNS, id)

	c.Response().Header().Set("Location", cfPathPrefix+"distribution/*/invalidation/"+id)

	return xmlResp(c, http.StatusCreated, resp)
}

func (h *Handler) handleListInvalidations(c *echo.Context, _ string) error {
	resp := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<InvalidationList xmlns="%s">`+
		`<IsTruncated>false</IsTruncated>`+
		`<MaxItems>100</MaxItems>`+
		`<Quantity>0</Quantity>`+
		`</InvalidationList>`,
		cfNS)

	return xmlResp(c, http.StatusOK, resp)
}

// readBody reads the entire request body.
func readBody(c *echo.Context) ([]byte, error) {
	if c.Request().Body == nil {
		return []byte{}, nil
	}

	return io.ReadAll(c.Request().Body)
}
