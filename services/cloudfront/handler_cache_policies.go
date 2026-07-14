package cloudfront

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

type cachePolicyHeadersConfigXML struct {
	HeaderBehavior string   `xml:"HeaderBehavior"`
	Headers        []string `xml:"Headers>Header"`
}

type cachePolicyCookiesConfigXML struct {
	CookieBehavior string   `xml:"CookieBehavior"`
	Cookies        []string `xml:"Cookies>Cookie"`
}

type cachePolicyQueryStringsConfigXML struct {
	QueryStringBehavior string   `xml:"QueryStringBehavior"`
	QueryStrings        []string `xml:"QueryStrings>QueryString"`
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

// cachePolicyResponseXML builds the full CachePolicy XML response.
func cachePolicyResponseXML(p *CachePolicy) string {
	var paramsXML string
	if p.Params != nil {
		paramsXML = fmt.Sprintf(
			`<ParametersInCacheKeyAndForwardedToOrigin>`+
				`<EnableAcceptEncodingGzip>%v</EnableAcceptEncodingGzip>`+
				`<EnableAcceptEncodingBrotli>%v</EnableAcceptEncodingBrotli>`+
				`<HeadersConfig><HeaderBehavior>%s</HeaderBehavior></HeadersConfig>`+
				`<CookiesConfig><CookieBehavior>%s</CookieBehavior></CookiesConfig>`+
				`<QueryStringsConfig><QueryStringBehavior>%s</QueryStringBehavior></QueryStringsConfig>`+
				`</ParametersInCacheKeyAndForwardedToOrigin>`,
			p.Params.EnableAcceptEncodingGzip,
			p.Params.EnableAcceptEncodingBrotli,
			p.Params.HeadersConfig.HeaderBehavior,
			p.Params.CookiesConfig.CookieBehavior,
			p.Params.QueryStringsConfig.QueryStringBehavior,
		)
	}

	return fmt.Sprintf(
		`<?xml version="1.0" encoding="UTF-8"?>`+
			`<CachePolicy xmlns="%s">`+
			`<Id>%s</Id>`+
			`<CachePolicyConfig>`+
			`<Name>%s</Name>`+
			`<Comment>%s</Comment>`+
			`<DefaultTTL>%d</DefaultTTL>`+
			`<MaxTTL>%d</MaxTTL>`+
			`<MinTTL>%d</MinTTL>`+
			`%s`+
			`</CachePolicyConfig>`+
			`</CachePolicy>`,
		cfNS,
		p.ID,
		p.Name,
		p.Comment,
		p.DefaultTTL,
		p.MaxTTL,
		p.MinTTL,
		paramsXML,
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
		`<CachePolicyConfig xmlns="%s">`+
		`<Name>%s</Name>`+
		`<Comment>%s</Comment>`+
		`<DefaultTTL>%d</DefaultTTL>`+
		`<MaxTTL>%d</MaxTTL>`+
		`<MinTTL>%d</MinTTL>`+
		`</CachePolicyConfig>`,
		cfNS, p.Name, p.Comment, p.DefaultTTL, p.MaxTTL, p.MinTTL)

	return xmlResp(c, http.StatusOK, resp)
}

func (h *Handler) handleListCachePolicies(c *echo.Context) error {
	policies := h.Backend.ListCachePolicies()

	var sb strings.Builder

	for _, p := range policies {
		fmt.Fprintf(&sb,
			`<CachePolicySummary>`+
				`<CachePolicy>`+
				`<Id>%s</Id>`+
				`<CachePolicyConfig>`+
				`<Name>%s</Name>`+
				`<Comment>%s</Comment>`+
				`<DefaultTTL>%d</DefaultTTL>`+
				`<MaxTTL>%d</MaxTTL>`+
				`<MinTTL>%d</MinTTL>`+
				`</CachePolicyConfig>`+
				`</CachePolicy>`+
				`</CachePolicySummary>`,
			p.ID, p.Name, p.Comment, p.DefaultTTL, p.MaxTTL, p.MinTTL)
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
