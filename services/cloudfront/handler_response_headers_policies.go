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
		`<ResponseHeadersPolicyConfig xmlns="%s">`+
		`<Name>%s</Name>`+
		`<Comment>%s</Comment>`+
		`</ResponseHeadersPolicyConfig>`,
		cfNS, p.Name, p.Comment)

	return xmlResp(c, http.StatusOK, resp)
}

func (h *Handler) handleListResponseHeadersPolicies(c *echo.Context) error {
	policies := h.Backend.ListResponseHeadersPolicies()

	var sb strings.Builder

	for _, p := range policies {
		fmt.Fprintf(&sb,
			`<ResponseHeadersPolicySummary>`+
				`<ResponseHeadersPolicy>`+
				`<Id>%s</Id>`+
				`<ResponseHeadersPolicyConfig>`+
				`<Name>%s</Name>`+
				`<Comment>%s</Comment>`+
				`</ResponseHeadersPolicyConfig>`+
				`</ResponseHeadersPolicy>`+
				`</ResponseHeadersPolicySummary>`,
			p.ID, p.Name, p.Comment)
	}

	resp := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<ResponseHeadersPolicyList xmlns="%s">`+
		`<MaxItems>%d</MaxItems>`+
		`<Quantity>%d</Quantity>`+
		`<Items>%s</Items>`+
		`</ResponseHeadersPolicyList>`,
		cfNS, maxItems, len(policies), sb.String())

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

func rhpResponseXML(p *ResponseHeadersPolicy) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, `<?xml version="1.0" encoding="UTF-8"?>`+
		`<ResponseHeadersPolicy xmlns="%s">`+
		`<Id>%s</Id>`+
		`<ResponseHeadersPolicyConfig>`+
		`<Name>%s</Name>`+
		`<Comment>%s</Comment>`,
		cfNS, p.ID, p.Name, p.Comment)
	if c := p.CorsConfig; c != nil {
		fmt.Fprintf(&sb,
			`<CorsConfig>`+
				`<AccessControlAllowCredentials>%v</AccessControlAllowCredentials>`+
				`<AccessControlMaxAgeSec>%d</AccessControlMaxAgeSec>`+
				`<OriginOverride>%v</OriginOverride>`+
				`</CorsConfig>`,
			c.AccessControlAllowCredentials, c.AccessControlMaxAgeSec, c.OriginOverride)
	}
	if s := p.SecurityHeaders; s != nil {
		fmt.Fprintf(&sb,
			`<SecurityHeadersConfig>`+
				`<StrictTransportSecurity>`+
				`<AccessControlMaxAgeSec>%d</AccessControlMaxAgeSec>`+
				`<IncludeSubdomains>%v</IncludeSubdomains>`+
				`<Preload>%v</Preload>`+
				`</StrictTransportSecurity>`+
				`<FrameOptions><FrameOption>%s</FrameOption></FrameOptions>`+
				`<ReferrerPolicy><ReferrerPolicy>%s</ReferrerPolicy></ReferrerPolicy>`+
				`</SecurityHeadersConfig>`,
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
	sb.WriteString(`</ResponseHeadersPolicyConfig></ResponseHeadersPolicy>`)

	return sb.String()
}

// --- CloudFront Function handlers ---
