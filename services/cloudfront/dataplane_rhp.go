package cloudfront

import (
	"net/http"
	"strconv"
	"strings"
)

func (h *Handler) responseHeadersPolicy(beh *dpBehavior) *ResponseHeadersPolicy {
	if beh.ResponseHeadersID == "" {
		return nil
	}

	p, err := h.Backend.GetResponseHeadersPolicy(beh.ResponseHeadersID)
	if err != nil {
		return nil
	}

	return p
}

func setPolicyHeader(hd http.Header, name, value string, override bool) {
	if value == "" {
		return
	}

	if override || hd.Get(name) == "" {
		hd.Set(name, value)
	}
}

func applyResponseHeadersPolicy(hd http.Header, p *ResponseHeadersPolicy, r *http.Request) {
	if c := p.CorsConfig; c != nil {
		applyCORS(hd, c, r)
	}

	if s := p.SecurityHeaders; s != nil {
		applySecurityHeaders(hd, s)
	}

	for _, ch := range p.CustomHeaders {
		setPolicyHeader(hd, ch.Header, ch.Value, ch.Override)
	}

	for _, name := range p.RemoveHeaders {
		hd.Del(name)
	}
}

func applyCORS(hd http.Header, c *RHPCorsConfig, r *http.Request) {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return
	}

	allowed := ""

	for _, o := range c.AccessControlAllowOrigins {
		if o == "*" {
			allowed = "*"

			break
		}

		if globMatchCS(o, origin) {
			allowed = origin
		}
	}

	if allowed == "" {
		return
	}

	setPolicyHeader(hd, "Access-Control-Allow-Origin", allowed, c.OriginOverride)

	if allowed != "*" {
		hd.Add("Vary", "Origin")
	}

	if c.AccessControlAllowCredentials {
		setPolicyHeader(hd, "Access-Control-Allow-Credentials", "true", c.OriginOverride)
	}

	setPolicyHeader(
		hd,
		"Access-Control-Expose-Headers",
		strings.Join(c.AccessControlExposeHeaders, ", "),
		c.OriginOverride,
	)

	if r.Method == http.MethodOptions && r.Header.Get("Access-Control-Request-Method") != "" {
		setPolicyHeader(
			hd,
			"Access-Control-Allow-Methods",
			strings.Join(c.AccessControlAllowMethods, ", "),
			c.OriginOverride,
		)
		setPolicyHeader(
			hd,
			"Access-Control-Allow-Headers",
			strings.Join(c.AccessControlAllowHeaders, ", "),
			c.OriginOverride,
		)

		if c.AccessControlMaxAgeSec > 0 {
			setPolicyHeader(
				hd,
				"Access-Control-Max-Age",
				strconv.FormatInt(c.AccessControlMaxAgeSec, 10),
				c.OriginOverride,
			)
		}
	}
}

func applySecurityHeaders(hd http.Header, s *RHPSecurityHeaders) {
	if s.StrictTransportSecuritySeconds > 0 {
		v := "max-age=" + strconv.FormatInt(s.StrictTransportSecuritySeconds, 10)
		if s.IncludeSubdomains {
			v += "; includeSubDomains"
		}

		if s.Preload {
			v += "; preload"
		}

		hd.Set("Strict-Transport-Security", v)
	}

	if s.ContentTypeOptionsOverride {
		hd.Set("X-Content-Type-Options", "nosniff")
	}

	setPolicyHeader(hd, "X-Frame-Options", s.FrameOptionsValue, true)
	setPolicyHeader(hd, "Referrer-Policy", s.ReferrerPolicy, true)
	setPolicyHeader(hd, "Content-Security-Policy", s.ContentSecurityPolicy, true)
	setPolicyHeader(hd, "X-XSS-Protection", s.XSSProtection, true)
}
