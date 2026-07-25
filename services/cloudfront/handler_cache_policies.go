package cloudfront

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

// Item-list element names below (Headers>Items>Name, Cookies>Items>Name,
// QueryStrings>Items>Name) match the real CachePolicyConfig wire shape exactly
// (verified against the CreateCachePolicy/UpdateCachePolicy API reference request
// syntax): each wraps a []string in an <Items> list of <Name> elements alongside
// a sibling <Quantity>. The previous tags here (Headers>Header, Cookies>Cookie,
// QueryStrings>QueryString) matched no real CloudFront wire format at all, so any
// real SDK-generated whitelist/allExcept request silently lost every listed
// header/cookie/query-string name on unmarshal.
type cachePolicyHeadersConfigXML struct {
	HeaderBehavior string   `xml:"HeaderBehavior"`
	Headers        []string `xml:"Headers>Items>Name"`
}

type cachePolicyCookiesConfigXML struct {
	CookieBehavior string   `xml:"CookieBehavior"`
	Cookies        []string `xml:"Cookies>Items>Name"`
}

type cachePolicyQueryStringsConfigXML struct {
	QueryStringBehavior string   `xml:"QueryStringBehavior"`
	QueryStrings        []string `xml:"QueryStrings>Items>Name"`
}

type cachePolicyParamsXML struct {
	HeadersConfig      cachePolicyHeadersConfigXML      `xml:"HeadersConfig"`
	CookiesConfig      cachePolicyCookiesConfigXML      `xml:"CookiesConfig"`
	QueryStringsConfig cachePolicyQueryStringsConfigXML `xml:"QueryStringsConfig"`
	EnableGzip         bool                             `xml:"EnableAcceptEncodingGzip"`
	EnableBrotli       bool                             `xml:"EnableAcceptEncodingBrotli"`
}

type cachePolicyConfigXML struct {
	XMLName    xml.Name             `xml:"CachePolicyConfig"`
	Name       string               `xml:"Name"`
	Comment    string               `xml:"Comment"`
	Params     cachePolicyParamsXML `xml:"ParametersInCacheKeyAndForwardedToOrigin"`
	DefaultTTL int64                `xml:"DefaultTTL"`
	MaxTTL     int64                `xml:"MaxTTL"`
	MinTTL     int64                `xml:"MinTTL"`
}

// cachePolicyParamsFromXML converts the XML params struct to the backend model.
// Returns nil when no meaningful params were provided.
func cachePolicyParamsFromXML(x cachePolicyParamsXML) *CachePolicyParams {
	p := &CachePolicyParams{
		EnableAcceptEncodingGzip:   x.EnableGzip,
		EnableAcceptEncodingBrotli: x.EnableBrotli,
		HeadersConfig: CachePolicyHeadersConfig{
			HeaderBehavior: x.HeadersConfig.HeaderBehavior,
			Headers:        x.HeadersConfig.Headers,
		},
		CookiesConfig: CachePolicyCookiesConfig{
			CookieBehavior: x.CookiesConfig.CookieBehavior,
			Cookies:        x.CookiesConfig.Cookies,
		},
		QueryStringsConfig: CachePolicyQueryStringsConfig{
			QueryStringBehavior: x.QueryStringsConfig.QueryStringBehavior,
			QueryStrings:        x.QueryStringsConfig.QueryStrings,
		},
	}
	// Return nil when the params are entirely default (no config provided).
	if p.HeadersConfig.HeaderBehavior == "" && p.CookiesConfig.CookieBehavior == "" &&
		p.QueryStringsConfig.QueryStringBehavior == "" &&
		!p.EnableAcceptEncodingGzip && !p.EnableAcceptEncodingBrotli {
		return nil
	}

	return p
}

// xmlNameItems renders the <Items><Name>x</Name>...</Items><Quantity>n</Quantity>
// pair CloudFront uses for every whitelist/allExcept-style string list (Headers,
// CookieNames, QueryStringNames, and their OriginRequestPolicy equivalents).
func xmlNameItems(items []string) string {
	var sb strings.Builder

	for _, it := range items {
		fmt.Fprintf(&sb, "<Name>%s</Name>", it)
	}

	return fmt.Sprintf("<Items>%s</Items><Quantity>%d</Quantity>", sb.String(), len(items))
}

// cachePolicyParamsXMLBlock builds the ParametersInCacheKeyAndForwardedToOrigin
// element body, including the Headers/Cookies/QueryStrings Items lists that were
// previously dropped entirely (see cachePolicyHeadersConfigXML doc comment).
func cachePolicyParamsXMLBlock(p *CachePolicyParams) string {
	if p == nil {
		return ""
	}

	return fmt.Sprintf(
		`<ParametersInCacheKeyAndForwardedToOrigin>`+
			`<EnableAcceptEncodingGzip>%v</EnableAcceptEncodingGzip>`+
			`<EnableAcceptEncodingBrotli>%v</EnableAcceptEncodingBrotli>`+
			`<HeadersConfig><HeaderBehavior>%s</HeaderBehavior><Headers>%s</Headers></HeadersConfig>`+
			`<CookiesConfig><CookieBehavior>%s</CookieBehavior><Cookies>%s</Cookies></CookiesConfig>`+
			`<QueryStringsConfig><QueryStringBehavior>%s</QueryStringBehavior>`+
			`<QueryStrings>%s</QueryStrings></QueryStringsConfig>`+
			`</ParametersInCacheKeyAndForwardedToOrigin>`,
		p.EnableAcceptEncodingGzip,
		p.EnableAcceptEncodingBrotli,
		p.HeadersConfig.HeaderBehavior, xmlNameItems(p.HeadersConfig.Headers),
		p.CookiesConfig.CookieBehavior, xmlNameItems(p.CookiesConfig.Cookies),
		p.QueryStringsConfig.QueryStringBehavior, xmlNameItems(p.QueryStringsConfig.QueryStrings),
	)
}

// cachePolicyConfigXMLBlock builds the <CachePolicyConfig>...</CachePolicyConfig>
// body shared by the full CachePolicy response, the config-only response, and each
// CachePolicySummary in a ListCachePolicies response.
func cachePolicyConfigXMLBlock(p *CachePolicy) string {
	return fmt.Sprintf(
		`<Name>%s</Name><Comment>%s</Comment><DefaultTTL>%d</DefaultTTL>`+
			`<MaxTTL>%d</MaxTTL><MinTTL>%d</MinTTL>%s`,
		p.Name, p.Comment, p.DefaultTTL, p.MaxTTL, p.MinTTL, cachePolicyParamsXMLBlock(p.Params),
	)
}

// cachePolicyResponseXML builds the full CachePolicy XML response.
func cachePolicyResponseXML(p *CachePolicy) string {
	return fmt.Sprintf(
		`<?xml version="1.0" encoding="UTF-8"?>`+
			`<CachePolicy xmlns="%s"><Id>%s</Id><CachePolicyConfig>%s</CachePolicyConfig></CachePolicy>`,
		cfNS, p.ID, cachePolicyConfigXMLBlock(p),
	)
}

func (h *Handler) handleCreateCachePolicy(c *echo.Context) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	if qErr := validateQuantities(body); qErr != nil {
		return h.handleError(c, qErr)
	}

	var req cachePolicyConfigXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			return xmlResp(
				c,
				http.StatusBadRequest,
				cfErrorXML("MalformedXML", "invalid CachePolicyConfig XML"),
			)
		}
	}

	params := cachePolicyParamsFromXML(req.Params)
	policy, createErr := h.Backend.CreateCachePolicy(
		req.Name,
		req.Comment,
		req.DefaultTTL,
		req.MaxTTL,
		req.MinTTL,
		params,
	)
	if createErr != nil {
		return h.handleError(c, createErr)
	}

	c.Response().Header().Set("ETag", policy.ETag)
	c.Response().Header().Set("Location", cfPathPrefix+"cache-policy/"+policy.ID)

	return xmlResp(c, http.StatusCreated, cachePolicyResponseXML(policy))
}

func (h *Handler) handleGetCachePolicy(c *echo.Context, id string) error {
	p, err := h.Backend.GetCachePolicy(id)
	if err != nil {
		return h.handleError(c, err)
	}

	c.Response().Header().Set("ETag", p.ETag)

	return xmlResp(c, http.StatusOK, cachePolicyResponseXML(p))
}

func (h *Handler) handleGetCachePolicyConfig(c *echo.Context, id string) error {
	p, err := h.Backend.GetCachePolicy(id)
	if err != nil {
		return h.handleError(c, err)
	}

	c.Response().Header().Set("ETag", p.ETag)

	resp := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<CachePolicyConfig xmlns="%s">%s</CachePolicyConfig>`,
		cfNS, cachePolicyConfigXMLBlock(p))

	return xmlResp(c, http.StatusOK, resp)
}

// filterByManagedType applies the ListCachePolicies/ListOriginRequestPolicies/
// ListResponseHeadersPolicies "Type" query filter (managed|custom|"" for all),
// matching the real ListXPoliciesInput.Type field.
func filterByManagedType[T any](typeParam string, managed func(T) bool, items []T) []T {
	switch typeParam {
	case "managed":
		return filterSlice(items, managed)
	case "custom":
		return filterSlice(items, func(v T) bool { return !managed(v) })
	default:
		return items
	}
}

func filterSlice[T any](items []T, keep func(T) bool) []T {
	out := make([]T, 0, len(items))

	for _, v := range items {
		if keep(v) {
			out = append(out, v)
		}
	}

	return out
}

func policyTypeString(managed bool) string {
	if managed {
		return "managed"
	}

	return "custom"
}

func (h *Handler) handleListCachePolicies(c *echo.Context) error {
	policies := h.Backend.ListCachePolicies()
	policies = filterByManagedType(c.QueryParam("Type"), func(p *CachePolicy) bool { return p.Managed }, policies)

	var sb strings.Builder

	for _, p := range policies {
		fmt.Fprintf(&sb,
			`<CachePolicySummary><Type>%s</Type><CachePolicy><Id>%s</Id>`+
				`<CachePolicyConfig>%s</CachePolicyConfig></CachePolicy></CachePolicySummary>`,
			policyTypeString(p.Managed), p.ID, cachePolicyConfigXMLBlock(p))
	}

	resp := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<CachePolicyList xmlns="%s">`+
		`<MaxItems>%d</MaxItems>`+
		`<Quantity>%d</Quantity>`+
		`<Items>%s</Items>`+
		`</CachePolicyList>`,
		cfNS, maxItems, len(policies), sb.String())

	return xmlResp(c, http.StatusOK, resp)
}

func (h *Handler) handleUpdateCachePolicy(c *echo.Context, id string) error {
	current, getErr := h.Backend.GetCachePolicy(id)
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
				"If-Match ETag did not match the current cache policy ETag",
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

	var req cachePolicyConfigXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			return xmlResp(
				c,
				http.StatusBadRequest,
				cfErrorXML("MalformedXML", "invalid CachePolicyConfig XML"),
			)
		}
	}

	params := cachePolicyParamsFromXML(req.Params)
	p, updateErr := h.Backend.UpdateCachePolicy(
		id,
		req.Name,
		req.Comment,
		req.DefaultTTL,
		req.MaxTTL,
		req.MinTTL,
		params,
	)
	if updateErr != nil {
		return h.handleError(c, updateErr)
	}

	c.Response().Header().Set("ETag", p.ETag)

	return xmlResp(c, http.StatusOK, cachePolicyResponseXML(p))
}

func (h *Handler) handleDeleteCachePolicy(c *echo.Context, id string) error {
	current, getErr := h.Backend.GetCachePolicy(id)
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
				"If-Match ETag did not match the current cache policy ETag",
			),
		)
	}

	if err := h.Backend.DeleteCachePolicy(id); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// --- Origin Access Control handlers ---
