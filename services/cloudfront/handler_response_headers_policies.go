package cloudfront

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

type rhpCorsConfigXML struct {
	AccessControlAllowOrigins     []string `xml:"AccessControlAllowOrigins>Items>Origin"`
	AccessControlAllowHeaders     []string `xml:"AccessControlAllowHeaders>Items>Header"`
	AccessControlAllowMethods     []string `xml:"AccessControlAllowMethods>Items>Method"`
	AccessControlExposeHeaders    []string `xml:"AccessControlExposeHeaders>Items>Header"`
	AccessControlMaxAgeSec        int64    `xml:"AccessControlMaxAgeSec"`
	AccessControlAllowCredentials bool     `xml:"AccessControlAllowCredentials"`
	OriginOverride                bool     `xml:"OriginOverride"`
}

type rhpSecurityHeadersXML struct {
	FrameOptionsValue              string `xml:"FrameOptions>FrameOption"`
	ReferrerPolicy                 string `xml:"ReferrerPolicy>ReferrerPolicy"`
	ContentSecurityPolicy          string `xml:"ContentSecurityPolicy>ContentSecurityPolicy"`
	XSSProtection                  string `xml:"XSSProtection>ReportUri"`
	StrictTransportSecuritySeconds int64  `xml:"StrictTransportSecurity>AccessControlMaxAgeSec"`
	IncludeSubdomains              bool   `xml:"StrictTransportSecurity>IncludeSubdomains"`
	Preload                        bool   `xml:"StrictTransportSecurity>Preload"`
	ContentTypeOptionsOverride     bool   `xml:"ContentTypeOptions>Override"`
}

type rhpCustomHeaderXML struct {
	Header   string `xml:"Header"`
	Value    string `xml:"Value"`
	Override bool   `xml:"Override"`
}

type rhpConfigXML struct {
	XMLName         xml.Name               `xml:"ResponseHeadersPolicyConfig"`
	Name            string                 `xml:"Name"`
	Comment         string                 `xml:"Comment"`
	CorsConfig      *rhpCorsConfigXML      `xml:"CorsConfig"`
	SecurityHeaders *rhpSecurityHeadersXML `xml:"SecurityHeadersConfig"`
	CustomHeaders   []rhpCustomHeaderXML   `xml:"CustomHeadersConfig>Items>ResponseHeadersPolicyCustomHeader"`
	RemoveHeaders   []string               `xml:"RemoveHeadersConfig>Items>ResponseHeadersPolicyRemoveHeader>Header"`
}

func rhpConfigFromXML(x rhpConfigXML) *ResponseHeadersPolicyConfig {
	cfg := &ResponseHeadersPolicyConfig{}
	if x.CorsConfig != nil {
		cfg.CorsConfig = &RHPCorsConfig{
			AccessControlAllowOrigins:     x.CorsConfig.AccessControlAllowOrigins,
			AccessControlAllowHeaders:     x.CorsConfig.AccessControlAllowHeaders,
			AccessControlAllowMethods:     x.CorsConfig.AccessControlAllowMethods,
			AccessControlExposeHeaders:    x.CorsConfig.AccessControlExposeHeaders,
			AccessControlMaxAgeSec:        x.CorsConfig.AccessControlMaxAgeSec,
			AccessControlAllowCredentials: x.CorsConfig.AccessControlAllowCredentials,
			OriginOverride:                x.CorsConfig.OriginOverride,
		}
	}
	if x.SecurityHeaders != nil {
		cfg.SecurityHeaders = &RHPSecurityHeaders{
			StrictTransportSecuritySeconds: x.SecurityHeaders.StrictTransportSecuritySeconds,
			ContentTypeOptionsOverride:     x.SecurityHeaders.ContentTypeOptionsOverride,
			FrameOptionsValue:              x.SecurityHeaders.FrameOptionsValue,
			ReferrerPolicy:                 x.SecurityHeaders.ReferrerPolicy,
			ContentSecurityPolicy:          x.SecurityHeaders.ContentSecurityPolicy,
			XSSProtection:                  x.SecurityHeaders.XSSProtection,
			IncludeSubdomains:              x.SecurityHeaders.IncludeSubdomains,
			Preload:                        x.SecurityHeaders.Preload,
		}
	}
	for _, h := range x.CustomHeaders {
		cfg.CustomHeaders = append(cfg.CustomHeaders, RHPCustomHeader(h))
	}
	cfg.RemoveHeaders = x.RemoveHeaders
	if cfg.CorsConfig == nil && cfg.SecurityHeaders == nil && len(cfg.CustomHeaders) == 0 &&
		len(cfg.RemoveHeaders) == 0 {
		return nil
	}

	return cfg
}

func (h *Handler) handleCreateResponseHeadersPolicy(c *echo.Context) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	if qErr := validateQuantities(body); qErr != nil {
		return h.handleError(c, qErr)
	}

	var req rhpConfigXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			const rhpMsg = "invalid ResponseHeadersPolicyConfig XML"

			return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", rhpMsg))
		}
	}

	if req.Name == "" {
		return xmlResp(c, http.StatusBadRequest,
			cfErrorXML("InvalidArgument", "ResponseHeadersPolicyConfig Name is required"))
	}

	p, createErr := h.Backend.CreateResponseHeadersPolicy(req.Name, req.Comment, rhpConfigFromXML(req))
	if createErr != nil {
		return h.handleError(c, createErr)
	}

	c.Response().Header().Set("ETag", p.ETag)
	c.Response().Header().Set("Location", cfPathPrefix+"response-headers-policy/"+p.ID)

	return xmlResp(c, http.StatusCreated, rhpResponseXML(p))
}

func (h *Handler) handleGetResponseHeadersPolicy(c *echo.Context, id string) error {
	p, err := h.Backend.GetResponseHeadersPolicy(id)
	if err != nil {
		return h.handleError(c, err)
	}

	c.Response().Header().Set("ETag", p.ETag)

	return xmlResp(c, http.StatusOK, rhpResponseXML(p))
}

func (h *Handler) handleGetResponseHeadersPolicyConfig(c *echo.Context, id string) error {
	p, err := h.Backend.GetResponseHeadersPolicy(id)
	if err != nil {
		return h.handleError(c, err)
	}

	c.Response().Header().Set("ETag", p.ETag)

	resp := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<ResponseHeadersPolicyConfig xmlns="%s">%s</ResponseHeadersPolicyConfig>`,
		cfNS, rhpConfigXMLBlock(p))

	return xmlResp(c, http.StatusOK, resp)
}

// handleListResponseHeadersPolicies paginates via Marker/MaxItems (both query-bound,
// cloudfront@v1.67.4 serializers.go). Real ResponseHeadersPolicyList has no IsTruncated
// field -- NextMarker's presence alone signals truncation (types/types.go:5729-5749).
//
//nolint:dupl // list handlers for different CloudFront resource types share XML list structure
func (h *Handler) handleListResponseHeadersPolicies(c *echo.Context) error {
	policies := h.Backend.ListResponseHeadersPolicies()
	policies = filterByManagedType(
		c.QueryParam("Type"), func(p *ResponseHeadersPolicy) bool { return p.Managed }, policies,
	)

	page, pageSize, isTruncated, nextMarker := paginateByMarkerID(
		c, policies, func(p *ResponseHeadersPolicy) string { return p.ID },
	)

	var sb strings.Builder

	for _, p := range page {
		fmt.Fprintf(&sb,
			`<ResponseHeadersPolicySummary><Type>%s</Type><ResponseHeadersPolicy><Id>%s</Id>`+
				`<ResponseHeadersPolicyConfig>%s</ResponseHeadersPolicyConfig>`+
				`</ResponseHeadersPolicy></ResponseHeadersPolicySummary>`,
			policyTypeString(p.Managed), p.ID, rhpConfigXMLBlock(p))
	}

	nextMarkerXML := ""
	if isTruncated {
		nextMarkerXML = fmt.Sprintf(`<NextMarker>%s</NextMarker>`, nextMarker)
	}

	resp := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<ResponseHeadersPolicyList xmlns="%s">`+
		`<MaxItems>%d</MaxItems>`+
		`<Quantity>%d</Quantity>`+
		`<Items>%s</Items>%s`+
		`</ResponseHeadersPolicyList>`,
		cfNS, pageSize, len(page), sb.String(), nextMarkerXML)

	return xmlResp(c, http.StatusOK, resp)
}

func (h *Handler) handleUpdateResponseHeadersPolicy(c *echo.Context, id string) error {
	current, getErr := h.Backend.GetResponseHeadersPolicy(id)
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
				"If-Match ETag did not match the current response headers policy ETag",
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

	var req rhpConfigXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			const rhpMsg = "invalid ResponseHeadersPolicyConfig XML"

			return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", rhpMsg))
		}
	}

	if req.Name == "" {
		req.Name = current.Name
	}

	p, updateErr := h.Backend.UpdateResponseHeadersPolicy(id, req.Name, req.Comment, rhpConfigFromXML(req))
	if updateErr != nil {
		return h.handleError(c, updateErr)
	}

	c.Response().Header().Set("ETag", p.ETag)

	return xmlResp(c, http.StatusOK, rhpResponseXML(p))
}

func (h *Handler) handleDeleteResponseHeadersPolicy(c *echo.Context, id string) error {
	current, getErr := h.Backend.GetResponseHeadersPolicy(id)
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
				"If-Match ETag did not match the current response headers policy ETag",
			),
		)
	}

	if err := h.Backend.DeleteResponseHeadersPolicy(id); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// xmlPluralItems renders <Items><leaf>x</leaf>...</Items><Quantity>n</Quantity>
// for the CORS list shapes, each of which uses its own leaf element name
// (Origin/Header/Method) rather than the policy-config Name convention.
func xmlPluralItems(leaf string, items []string) string {
	var sb strings.Builder

	for _, it := range items {
		fmt.Fprintf(&sb, "<%s>%s</%s>", leaf, it, leaf)
	}

	return fmt.Sprintf("<Items>%s</Items><Quantity>%d</Quantity>", sb.String(), len(items))
}

// rhpConfigXMLBlock builds the <ResponseHeadersPolicyConfig>...</...> body shared
// by the full ResponseHeadersPolicy response, the config-only response, and each
// ResponseHeadersPolicySummary in a ListResponseHeadersPolicies response.
//
// The CorsConfig list fields (AccessControlAllowOrigins/Headers/Methods,
// AccessControlExposeHeaders) and SecurityHeadersConfig's ContentSecurityPolicy /
// ContentTypeOptions were previously omitted from every read response even though
// rhpConfigXML parses all of them from requests -- see rhpConfigXML's field tags,
// which already used the correct wire paths on the request side.
func rhpConfigXMLBlock(p *ResponseHeadersPolicy) string {
	var sb strings.Builder

	fmt.Fprintf(&sb, `<Name>%s</Name><Comment>%s</Comment>`, p.Name, p.Comment)

	if c := p.CorsConfig; c != nil {
		fmt.Fprintf(&sb,
			`<CorsConfig>`+
				`<AccessControlAllowCredentials>%v</AccessControlAllowCredentials>`+
				`<AccessControlAllowHeaders>%s</AccessControlAllowHeaders>`+
				`<AccessControlAllowMethods>%s</AccessControlAllowMethods>`+
				`<AccessControlAllowOrigins>%s</AccessControlAllowOrigins>`+
				`<AccessControlExposeHeaders>%s</AccessControlExposeHeaders>`+
				`<AccessControlMaxAgeSec>%d</AccessControlMaxAgeSec>`+
				`<OriginOverride>%v</OriginOverride>`+
				`</CorsConfig>`,
			c.AccessControlAllowCredentials,
			xmlPluralItems("Header", c.AccessControlAllowHeaders),
			xmlPluralItems("Method", c.AccessControlAllowMethods),
			xmlPluralItems("Origin", c.AccessControlAllowOrigins),
			xmlPluralItems("Header", c.AccessControlExposeHeaders),
			c.AccessControlMaxAgeSec, c.OriginOverride)
	}

	if s := p.SecurityHeaders; s != nil {
		// Only ContentTypeOptions.Override is tracked on RHPSecurityHeaders (matching
		// what rhpConfigXML parses from requests -- see its doc comment). The other
		// four sub-elements' Override flags aren't modeled; emitting false for them
		// is not a placeholder guess, it matches the real "Override: No" default that
		// every AWS-managed security-headers policy uses for these same headers (see
		// managedSecurityHeaders in managed_policies.go).
		var cspXML string
		if s.ContentSecurityPolicy != "" {
			cspXML = fmt.Sprintf(
				`<ContentSecurityPolicy><ContentSecurityPolicy>%s</ContentSecurityPolicy>`+
					`<Override>false</Override></ContentSecurityPolicy>`,
				s.ContentSecurityPolicy)
		}

		fmt.Fprintf(&sb,
			`<SecurityHeadersConfig>`+
				`<ContentTypeOptions><Override>%v</Override></ContentTypeOptions>`+
				`%s`+
				`<StrictTransportSecurity>`+
				`<AccessControlMaxAgeSec>%d</AccessControlMaxAgeSec>`+
				`<IncludeSubdomains>%v</IncludeSubdomains>`+
				`<Preload>%v</Preload>`+
				`<Override>false</Override>`+
				`</StrictTransportSecurity>`+
				`<FrameOptions><FrameOption>%s</FrameOption><Override>false</Override></FrameOptions>`+
				`<ReferrerPolicy><ReferrerPolicy>%s</ReferrerPolicy><Override>false</Override></ReferrerPolicy>`+
				`</SecurityHeadersConfig>`,
			s.ContentTypeOptionsOverride, cspXML,
			s.StrictTransportSecuritySeconds, s.IncludeSubdomains, s.Preload,
			s.FrameOptionsValue, s.ReferrerPolicy)
	}

	if len(p.CustomHeaders) > 0 {
		sb.WriteString(`<CustomHeadersConfig><Items>`)

		for _, h := range p.CustomHeaders {
			fmt.Fprintf(&sb,
				`<ResponseHeadersPolicyCustomHeader>`+
					`<Header>%s</Header>`+
					`<Value>%s</Value>`+
					`<Override>%v</Override>`+
					`</ResponseHeadersPolicyCustomHeader>`,
				h.Header, h.Value, h.Override)
		}

		fmt.Fprintf(&sb, `</Items><Quantity>%d</Quantity></CustomHeadersConfig>`, len(p.CustomHeaders))
	}

	if len(p.RemoveHeaders) > 0 {
		sb.WriteString(`<RemoveHeadersConfig><Items>`)

		for _, h := range p.RemoveHeaders {
			fmt.Fprintf(
				&sb,
				`<ResponseHeadersPolicyRemoveHeader><Header>%s</Header></ResponseHeadersPolicyRemoveHeader>`,
				h,
			)
		}

		fmt.Fprintf(&sb, `</Items><Quantity>%d</Quantity></RemoveHeadersConfig>`, len(p.RemoveHeaders))
	}

	return sb.String()
}

func rhpResponseXML(p *ResponseHeadersPolicy) string {
	return fmt.Sprintf(
		`<?xml version="1.0" encoding="UTF-8"?>`+
			`<ResponseHeadersPolicy xmlns="%s"><Id>%s</Id>`+
			`<ResponseHeadersPolicyConfig>%s</ResponseHeadersPolicyConfig></ResponseHeadersPolicy>`,
		cfNS, p.ID, rhpConfigXMLBlock(p),
	)
}

// --- CloudFront Function handlers ---
