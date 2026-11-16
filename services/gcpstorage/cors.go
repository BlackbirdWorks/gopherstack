package gcpstorage

import (
	"encoding/json/v2"
	"net/http"
	"slices"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

// CORS semantics: docs.cloud.google.com/storage/docs/cross-origin. JSON API endpoints always allow CORS with
// fixed preflight values; XML API endpoints follow the bucket's cors configuration.
const (
	defaultCORSMaxAge  = 3600
	headerOrigin       = "Origin"
	headerACAO         = "Access-Control-Allow-Origin"
	headerACAM         = "Access-Control-Allow-Methods"
	headerACAH         = "Access-Control-Allow-Headers"
	headerACAC         = "Access-Control-Allow-Credentials"
	headerACMA         = "Access-Control-Max-Age"
	headerACEH         = "Access-Control-Expose-Headers"
	headerACRM         = "Access-Control-Request-Method"
	headerACRH         = "Access-Control-Request-Headers"
	jsonAPICORSMethods = "DELETE,GET,HEAD,PATCH,POST,PUT"
)

type corsRule struct {
	MaxAge         *int64   `json:"maxAgeSeconds"`
	Origin         []string `json:"origin"`
	Method         []string `json:"method"`
	ResponseHeader []string `json:"responseHeader"`
}

func corsRules(bk *Bucket) []corsRule {
	raw, ok := bk.Config["cors"]
	if !ok {
		return nil
	}

	var rules []corsRule
	if err := json.Unmarshal(raw, &rules, jsonx.V1()); err != nil {
		return nil
	}

	return rules
}

func (r corsRule) allowsOrigin(origin string) bool {
	return slices.Contains(r.Origin, "*") || slices.Contains(r.Origin, origin)
}

func (r corsRule) allowsMethod(method string) bool {
	return slices.ContainsFunc(r.Method, func(m string) bool { return m == "*" || strings.EqualFold(m, method) })
}

func (r corsRule) allowsHeaders(requested string) bool {
	for h := range strings.SplitSeq(requested, ",") {
		h = strings.TrimSpace(h)
		if h == "" {
			continue
		}

		if !slices.ContainsFunc(r.ResponseHeader, func(a string) bool { return a == "*" || strings.EqualFold(a, h) }) {
			return false
		}
	}

	return true
}

func (r corsRule) maxAge() int64 {
	if r.MaxAge != nil {
		return *r.MaxAge
	}

	return defaultCORSMaxAge
}

func isJSONAPIPath(path string) bool {
	segs := splitSegments(path)

	return hasPrefix(segs, "storage", "v1") || hasPrefix(segs, "upload", "storage", "v1") ||
		hasPrefix(segs, "download", "storage", "v1")
}

// handleCORS answers a preflight request, or tags the response of a simple request; it reports whether the
// request was fully answered.
func (h *Handler) handleCORS(c *echo.Context) bool {
	r := c.Request()

	origin := r.Header.Get(headerOrigin)
	if origin == "" {
		return false
	}

	preflight := r.Method == http.MethodOptions

	if isJSONAPIPath(r.URL.EscapedPath()) {
		return jsonAPICORS(c, origin, preflight)
	}

	return h.xmlCORS(c, origin, preflight)
}

func jsonAPICORS(c *echo.Context, origin string, preflight bool) bool {
	r, w := c.Request(), c.Response().Header()
	w.Set(headerACAO, origin)

	if !preflight {
		return false
	}

	if r.Header.Get(headerACRM) == "" {
		w.Set(headerACAC, "true")
		_ = c.NoContent(http.StatusNotFound)

		return true
	}

	w.Set(headerACAM, jsonAPICORSMethods)

	if hs := r.Header.Get(headerACRH); hs != "" {
		w.Set(headerACAH, hs)
	}

	w.Set(headerACMA, strconv.Itoa(defaultCORSMaxAge))
	_ = c.NoContent(http.StatusOK)

	return true
}

func (h *Handler) xmlCORS(c *echo.Context, origin string, preflight bool) bool {
	r, w := c.Request(), c.Response().Header()

	segs := virtualHostSegments(r.Host, splitSegments(r.URL.EscapedPath()))
	if len(segs) == 0 {
		return false
	}

	rules := h.Backend.bucketCORS(segs[0])
	method := r.Method

	if preflight {
		method = r.Header.Get(headerACRM)
	}

	for _, rule := range rules {
		if !rule.allowsOrigin(origin) || !rule.allowsMethod(method) {
			continue
		}

		if preflight {
			hs := r.Header.Get(headerACRH)
			if !rule.allowsHeaders(hs) {
				continue
			}

			w.Set(headerACAO, origin)
			w.Set(headerACAM, method)

			if hs != "" {
				w.Set(headerACAH, hs)
			}

			w.Set(headerACMA, strconv.FormatInt(rule.maxAge(), 10))

			break
		}

		w.Set(headerACAO, origin)

		if len(rule.ResponseHeader) > 0 {
			w.Set(headerACEH, strings.Join(rule.ResponseHeader, ","))
		}

		break
	}

	if preflight {
		_ = c.NoContent(http.StatusOK)
	}

	return preflight
}

func (b *InMemoryBackend) bucketCORS(name string) []corsRule {
	b.mu.RLock("CORS")
	defer b.mu.RUnlock()

	bk, ok := b.buckets[name]
	if !ok {
		return nil
	}

	return corsRules(bk)
}
