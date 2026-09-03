package cloudfront

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

type orpHeadersConfigXML struct {
	HeaderBehavior string   `xml:"HeaderBehavior"`
	Headers        []string `xml:"Headers>Items>Name"`
}

type orpCookiesConfigXML struct {
	CookieBehavior string   `xml:"CookieBehavior"`
	Cookies        []string `xml:"Cookies>Items>Name"`
}

type orpQueryStringsConfigXML struct {
	QueryStringBehavior string   `xml:"QueryStringBehavior"`
	QueryStrings        []string `xml:"QueryStrings>Items>Name"`
}

type orpConfigXML struct {
	HeadersConfig      *orpHeadersConfigXML      `xml:"HeadersConfig"`
	CookiesConfig      *orpCookiesConfigXML      `xml:"CookiesConfig"`
	QueryStringsConfig *orpQueryStringsConfigXML `xml:"QueryStringsConfig"`
	XMLName            xml.Name                  `xml:"OriginRequestPolicyConfig"`
	Name               string                    `xml:"Name"`
	Comment            string                    `xml:"Comment"`
}

func orpConfigFromXML(x orpConfigXML) *OriginRequestPolicyConfig {
	cfg := &OriginRequestPolicyConfig{}
	if x.HeadersConfig != nil {
		cfg.HeadersConfig = &ORPHeadersConfig{
			HeaderBehavior: x.HeadersConfig.HeaderBehavior,
			Headers:        x.HeadersConfig.Headers,
		}
	}
	if x.CookiesConfig != nil {
		cfg.CookiesConfig = &ORPCookiesConfig{
			CookieBehavior: x.CookiesConfig.CookieBehavior,
			Cookies:        x.CookiesConfig.Cookies,
		}
	}
	if x.QueryStringsConfig != nil {
		cfg.QueryStringsConfig = &ORPQueryStringsConfig{
			QueryStringBehavior: x.QueryStringsConfig.QueryStringBehavior,
			QueryStrings:        x.QueryStringsConfig.QueryStrings,
		}
	}
	if cfg.HeadersConfig == nil && cfg.CookiesConfig == nil && cfg.QueryStringsConfig == nil {
		return nil
	}

	return cfg
}

func (h *Handler) handleCreateOriginRequestPolicy(c *echo.Context) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	if qErr := validateQuantities(body); qErr != nil {
		return h.handleError(c, qErr)
	}

	if len(body) == 0 {
		return xmlResp(c, http.StatusBadRequest,
			cfErrorXML("MalformedXML", "OriginRequestPolicyConfig body is required"))
	}

	var req orpConfigXML
	if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
		const orpMsg = "invalid OriginRequestPolicyConfig XML"

		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", orpMsg))
	}

	if req.Name == "" {
		return xmlResp(c, http.StatusBadRequest,
			cfErrorXML("InvalidArgument", "OriginRequestPolicyConfig Name is required"))
	}

	p, createErr := h.Backend.CreateOriginRequestPolicy(req.Name, req.Comment, orpConfigFromXML(req))
	if createErr != nil {
		return h.handleError(c, createErr)
	}

	c.Response().Header().Set("ETag", p.ETag)
	c.Response().Header().Set("Location", cfPathPrefix+"origin-request-policy/"+p.ID)

	return xmlResp(c, http.StatusCreated, orpResponseXML(p))
}

func (h *Handler) handleGetOriginRequestPolicy(c *echo.Context, id string) error {
	p, err := h.Backend.GetOriginRequestPolicy(id)
	if err != nil {
		return h.handleError(c, err)
	}

	c.Response().Header().Set("ETag", p.ETag)

	return xmlResp(c, http.StatusOK, orpResponseXML(p))
}

func (h *Handler) handleGetOriginRequestPolicyConfig(c *echo.Context, id string) error {
	p, err := h.Backend.GetOriginRequestPolicy(id)
	if err != nil {
		return h.handleError(c, err)
	}

	c.Response().Header().Set("ETag", p.ETag)

	resp := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<OriginRequestPolicyConfig xmlns="%s">%s</OriginRequestPolicyConfig>`,
		cfNS, orpConfigXMLBlock(p))

	return xmlResp(c, http.StatusOK, resp)
}

// handleListOriginRequestPolicies paginates via Marker/MaxItems (both query-bound,
// cloudfront@v1.67.4 serializers.go). Real OriginRequestPolicyList has no IsTruncated
// field -- NextMarker's presence alone signals truncation (types/types.go:4746-4766).
//
//nolint:dupl // list handlers for different CloudFront resource types share XML list structure
func (h *Handler) handleListOriginRequestPolicies(c *echo.Context) error {
	policies := h.Backend.ListOriginRequestPolicies()
	policies = filterByManagedType(
		c.QueryParam("Type"), func(p *OriginRequestPolicy) bool { return p.Managed }, policies,
	)

	page, pageSize, isTruncated, nextMarker := paginateByMarkerID(
		c, policies, func(p *OriginRequestPolicy) string { return p.ID },
	)

	var sb strings.Builder

	for _, p := range page {
		fmt.Fprintf(&sb,
			`<OriginRequestPolicySummary><Type>%s</Type><OriginRequestPolicy><Id>%s</Id>`+
				`<OriginRequestPolicyConfig>%s</OriginRequestPolicyConfig>`+
				`</OriginRequestPolicy></OriginRequestPolicySummary>`,
			policyTypeString(p.Managed), p.ID, orpConfigXMLBlock(p))
	}

	nextMarkerXML := ""
	if isTruncated {
		nextMarkerXML = fmt.Sprintf(`<NextMarker>%s</NextMarker>`, nextMarker)
	}

	resp := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<OriginRequestPolicyList xmlns="%s">`+
		`<MaxItems>%d</MaxItems>`+
		`<Quantity>%d</Quantity>`+
		`<Items>%s</Items>%s`+
		`</OriginRequestPolicyList>`,
		cfNS, pageSize, len(page), sb.String(), nextMarkerXML)

	return xmlResp(c, http.StatusOK, resp)
}

func (h *Handler) handleUpdateOriginRequestPolicy(c *echo.Context, id string) error {
	current, getErr := h.Backend.GetOriginRequestPolicy(id)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	if qErr := validateQuantities(body); qErr != nil {
		return h.handleError(c, qErr)
	}

	ifMatch := c.Request().Header.Get("If-Match")
	if ifMatch == "" || ifMatch != current.ETag {
		return xmlResp(
			c,
			http.StatusPreconditionFailed,
			cfErrorXML(
				"PreconditionFailed",
				"If-Match ETag did not match the current origin request policy ETag",
			),
		)
	}

	var req orpConfigXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			const orpMsg = "invalid OriginRequestPolicyConfig XML"

			return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", orpMsg))
		}
	}

	if req.Name == "" {
		req.Name = current.Name
	}

	p, updateErr := h.Backend.UpdateOriginRequestPolicy(id, req.Name, req.Comment, orpConfigFromXML(req))
	if updateErr != nil {
		return h.handleError(c, updateErr)
	}

	c.Response().Header().Set("ETag", p.ETag)

	return xmlResp(c, http.StatusOK, orpResponseXML(p))
}

func (h *Handler) handleDeleteOriginRequestPolicy(c *echo.Context, id string) error {
	current, getErr := h.Backend.GetOriginRequestPolicy(id)
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
				"If-Match ETag did not match the current origin request policy ETag",
			),
		)
	}

	if err := h.Backend.DeleteOriginRequestPolicy(id); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// orpConfigXMLBlock builds the <OriginRequestPolicyConfig>...</...> body shared
// by the full OriginRequestPolicy response, the config-only response, and each
// OriginRequestPolicySummary in a ListOriginRequestPolicies response.
//
// Each Config's Items list (Headers>Items>Name, Cookies>Items>Name,
// QueryStrings>Items>Name) matches the real wire shape exactly (verified against
// the CreateOriginRequestPolicy API reference request syntax): previously this
// response only emitted a bare <Quantity>, silently dropping every whitelisted/
// excepted header, cookie, and query-string name in every read response.
func orpConfigXMLBlock(p *OriginRequestPolicy) string {
	var sb strings.Builder

	fmt.Fprintf(&sb, `<Name>%s</Name><Comment>%s</Comment>`, p.Name, p.Comment)

	if h := p.HeadersConfig; h != nil {
		fmt.Fprintf(&sb,
			`<HeadersConfig><HeaderBehavior>%s</HeaderBehavior><Headers>%s</Headers></HeadersConfig>`,
			h.HeaderBehavior, xmlNameItems(h.Headers))
	}

	if c := p.CookiesConfig; c != nil {
		fmt.Fprintf(&sb,
			`<CookiesConfig><CookieBehavior>%s</CookieBehavior><Cookies>%s</Cookies></CookiesConfig>`,
			c.CookieBehavior, xmlNameItems(c.Cookies))
	}

	if q := p.QueryStringsConfig; q != nil {
		fmt.Fprintf(&sb,
			`<QueryStringsConfig><QueryStringBehavior>%s</QueryStringBehavior>`+
				`<QueryStrings>%s</QueryStrings></QueryStringsConfig>`,
			q.QueryStringBehavior, xmlNameItems(q.QueryStrings))
	}

	return sb.String()
}

func orpResponseXML(p *OriginRequestPolicy) string {
	return fmt.Sprintf(
		`<?xml version="1.0" encoding="UTF-8"?>`+
			`<OriginRequestPolicy xmlns="%s"><Id>%s</Id>`+
			`<OriginRequestPolicyConfig>%s</OriginRequestPolicyConfig></OriginRequestPolicy>`,
		cfNS, p.ID, orpConfigXMLBlock(p),
	)
}

// --- Field Level Encryption handlers ---
