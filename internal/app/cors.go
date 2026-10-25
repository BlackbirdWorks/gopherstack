package app

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	strfold "github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	headerOrigin        = "Origin"
	headerACRequestMeth = "Access-Control-Request-Method"
	headerACRequestHdrs = "Access-Control-Request-Headers"
	corsMaxAgeSeconds   = "86400"
	corsAllMethods      = "GET, PUT, POST, DELETE, HEAD, PATCH, OPTIONS"
)

// CORSSettings configures the optional global CORS middleware; unset keeps today's behaviour.
type CORSSettings struct {
	AllowOrigins            []string `name:"allow-origins"     env:"GOPHERSTACK_CORS_ALLOW_ORIGINS,EXTRA_CORS_ALLOWED_ORIGINS" help:"Origins allowed by CORS (comma-separated, or *). Empty = no global CORS handling."`                                                                 //nolint:lll // config struct tags are intentionally verbose
	AllowHeaders            []string `name:"allow-headers"     env:"GOPHERSTACK_CORS_ALLOW_HEADERS,EXTRA_CORS_ALLOWED_HEADERS" help:"Extra headers allowed in CORS requests (comma-separated)."`                                                                                         //nolint:lll // config struct tags are intentionally verbose
	ExposeHeaders           []string `name:"expose-headers"    env:"GOPHERSTACK_CORS_EXPOSE_HEADERS,EXTRA_CORS_EXPOSE_HEADERS" help:"Response headers exposed to browsers (comma-separated)."`                                                                                           //nolint:lll // config struct tags are intentionally verbose
	AllowCredentials        bool     `name:"allow-credentials" env:"GOPHERSTACK_CORS_ALLOW_CREDENTIALS"                        help:"Send Access-Control-Allow-Credentials: true."`                                                                                                      //nolint:lll // config struct tags are intentionally verbose
	DisableCustomAPIGateway bool     `name:"disable-custom-apigateway" env:"GOPHERSTACK_CORS_DISABLE_CUSTOM_APIGATEWAY,DISABLE_CUSTOM_CORS_APIGATEWAY" help:"Ignore API Gateway CORS configuration so only the global CORS settings apply (LocalStack DISABLE_CUSTOM_CORS_APIGATEWAY)."` //nolint:lll // config struct tags are intentionally verbose
	DisableChecks           bool     `name:"disable-checks"    env:"GOPHERSTACK_DISABLE_CORS_CHECKS,DISABLE_CORS_CHECKS"       help:"Allow any origin (LocalStack DISABLE_CORS_CHECKS)."`                                                                                                //nolint:lll // config struct tags are intentionally verbose
}

func (s CORSSettings) enabled() bool { return len(s.AllowOrigins) > 0 || s.DisableChecks }

func (s CORSSettings) originAllowed(origin string) bool {
	if s.DisableChecks {
		return true
	}

	for _, o := range s.AllowOrigins {
		o = strings.TrimSpace(o)
		if o == "*" || strfold.Equal(o, origin) {
			return true
		}
	}

	return false
}

func (s CORSSettings) wildcard() bool {
	if s.AllowCredentials {
		return false
	}

	return s.DisableChecks || strfold.ContainsFold(trimAll(s.AllowOrigins), "*")
}

func trimAll(in []string) []string {
	out := make([]string, 0, len(in))
	for _, v := range in {
		out = append(out, strings.TrimSpace(v))
	}

	return out
}

// corsMiddleware answers preflights and decorates responses for allowed origins; nil when unconfigured.
// Disallowed origins fall through so per-bucket S3 CORS still applies.
func corsMiddleware(s CORSSettings) echo.MiddlewareFunc {
	if !s.enabled() {
		return nil
	}

	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			r := c.Request()
			origin := r.Header.Get(headerOrigin)

			if origin == "" || !s.originAllowed(origin) {
				return next(c)
			}

			s.setResponseHeaders(c.Response().Header(), origin)

			if r.Method == http.MethodOptions && r.Header.Get(headerACRequestMeth) != "" {
				s.setPreflightHeaders(c.Response().Header(), r)

				return c.NoContent(http.StatusNoContent)
			}

			return next(c)
		}
	}
}

func (s CORSSettings) setResponseHeaders(h http.Header, origin string) {
	if s.wildcard() {
		h.Set("Access-Control-Allow-Origin", "*")
	} else {
		h.Set("Access-Control-Allow-Origin", origin)
		h.Add("Vary", headerOrigin)
	}

	if s.AllowCredentials {
		h.Set("Access-Control-Allow-Credentials", "true")
	}

	if len(s.ExposeHeaders) > 0 {
		h.Set("Access-Control-Expose-Headers", strings.Join(trimAll(s.ExposeHeaders), ", "))
	}
}

func (s CORSSettings) setPreflightHeaders(h http.Header, r *http.Request) {
	h.Set("Access-Control-Allow-Methods", corsAllMethods)
	h.Set("Access-Control-Max-Age", corsMaxAgeSeconds)

	allow := trimAll(s.AllowHeaders)
	if req := r.Header.Get(headerACRequestHdrs); req != "" {
		allow = append(allow, req)
	}

	if len(allow) > 0 {
		h.Set("Access-Control-Allow-Headers", strings.Join(allow, ", "))
	}
}
