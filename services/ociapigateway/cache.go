package ociapigateway

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	cacheTypeExternal = "EXTERNAL_RESP_CACHE"
	cacheTypeNone     = "NONE"
	cacheMaxEntries   = 10000
	maxTCPPort        = 65535
	cacheMaxBody      = 1 << 20
	cacheStatusHeader = "X-Cache-Status"
	cacheHit          = "HIT"
	cacheMiss         = "MISS"
	cacheBypass       = "BYPASS"
)

var cacheAdditionRe = regexp.MustCompile(`^request\.(query|headers|auth)\[[^\]]+\]$`)

type cacheEntry struct {
	expires time.Time
	header  http.Header
	dep     string
	body    []byte
	status  int
}

// respCache is the in-process stand-in for the external response cache server (the server is never contacted).
type respCache struct {
	mu      *lockmetrics.RWMutex
	entries map[string]*cacheEntry
}

func newRespCache() *respCache {
	return &respCache{mu: lockmetrics.New("ociapigateway.cache"), entries: map[string]*cacheEntry{}}
}

func (c *respCache) reset() {
	c.mu.Lock("reset")
	c.entries = map[string]*cacheEntry{}
	c.mu.Unlock()
}

func (c *respCache) forgetDeployment(depID string) {
	c.mu.Lock("forgetDeployment")
	defer c.mu.Unlock()

	for k, e := range c.entries {
		if e.dep == depID {
			delete(c.entries, k)
		}
	}
}

func (c *respCache) get(key string, now time.Time) (*cacheEntry, bool) {
	c.mu.RLock("get")
	defer c.mu.RUnlock()

	e, ok := c.entries[key]
	if !ok || !e.expires.After(now) {
		return nil, false
	}

	return e, true
}

func (c *respCache) put(key string, e *cacheEntry, now time.Time) {
	c.mu.Lock("put")
	defer c.mu.Unlock()

	if len(c.entries) >= cacheMaxEntries {
		for k, old := range c.entries {
			if !old.expires.After(now) {
				delete(c.entries, k)
			}
		}

		if len(c.entries) >= cacheMaxEntries {
			return
		}
	}

	c.entries[key] = e
}

// cachedStatus lists the response codes Oracle stores (docs: apigatewayresponsecaching.htm).
func cachedStatus(code int) bool {
	return code == http.StatusOK || code == http.StatusNoContent || code == http.StatusMovedPermanently ||
		code == http.StatusGone
}

func cachedMethod(m string) bool {
	return m == http.MethodGet || m == http.MethodHead || m == http.MethodOptions
}

// routeCache is the caching decision for one request.
type routeCache struct {
	key    string
	ttl    time.Duration
	active bool
	bypass bool
}

// cachePlan decides whether and how the request uses the response cache.
func cachePlan(tgt target, route *compiledRoute, r *http.Request, claims map[string]any) routeCache {
	var lookup *cacheLookupDoc

	if p := route.doc.RequestPolicies; p != nil {
		lookup = p.ResponseCacheLookup
	}

	var store *cacheStoreDoc
	if p := route.doc.ResponsePolicies; p != nil {
		store = p.ResponseCacheStore
	}

	if lookup == nil || store == nil || (lookup.IsEnabled != nil && !*lookup.IsEnabled) || !cachedMethod(r.Method) {
		return routeCache{}
	}

	rc := routeCache{active: true, ttl: time.Duration(*store.TTL) * time.Second}
	if !tgt.cacheOn || (!isTrue(lookup.IsPrivate) && privateRequest(tgt.spec.doc.RequestPolicies, r)) {
		rc.bypass = true

		return rc
	}

	parts := []string{tgt.depID, r.Method, r.URL.Path}
	for _, add := range lookup.Additions {
		parts = append(parts, add+"="+additionValue(add, r, claims))
	}

	rc.key = strings.Join(parts, "\x00")

	return rc
}

func isTrue(b *bool) bool { return b != nil && *b }

// privateRequest reports requests carrying credentials: an Authorization header or the authentication policy's token.
func privateRequest(rp *requestPolicies, r *http.Request) bool {
	if r.Header.Get("Authorization") != "" {
		return true
	}

	if rp == nil || rp.Authentication == nil {
		return false
	}

	a := rp.Authentication

	if a.TokenHeader != nil && *a.TokenHeader != "" && r.Header.Get(*a.TokenHeader) != "" {
		return true
	}

	return a.TokenQueryParam != nil && *a.TokenQueryParam != "" && r.URL.Query().Has(*a.TokenQueryParam)
}

func additionValue(add string, r *http.Request, claims map[string]any) string {
	if add == "request.host" {
		return r.Host
	}

	m := cacheAdditionRe.FindStringSubmatch(add)
	if m == nil {
		return ""
	}

	name := add[strings.IndexByte(add, '[')+1 : len(add)-1]

	switch m[1] {
	case "query":
		return r.URL.Query().Get(name)
	case "headers":
		return r.Header.Get(name)
	default:
		if v, ok := claims[name]; ok {
			out, _ := json.Marshal(v, jsonx.V1()) //nolint:errchkjson // claim values are JSON

			return string(out)
		}

		return ""
	}
}

func (b *InMemoryBackend) cacheServe(w http.ResponseWriter, r *http.Request, rc routeCache, cors *corsDoc) bool {
	e, ok := b.cache.get(rc.key, b.clock())
	if !ok {
		return false
	}

	h := w.Header()
	for k, vs := range e.header {
		h[k] = append([]string(nil), vs...)
	}

	corsHeaders(h, cors, r.Header.Get("Origin"))
	h.Set(cacheStatusHeader, cacheHit)
	w.WriteHeader(e.status)

	if r.Method != http.MethodHead {
		_, _ = w.Write(e.body)
	}

	return true
}

// cacheOnFor reports whether the gateway has an external response cache configured.
func cacheOnFor(g *Gateway) bool {
	var d struct {
		Type string `json:"type"`
	}

	return json.Unmarshal(g.ResponseCache, &d, jsonx.V1()) == nil && d.Type == cacheTypeExternal
}

type respCacheServer struct {
	Host *string `json:"host"`
	Port *int    `json:"port"`
}

type respCacheDetails struct {
	AuthSecretVersion *int64            `json:"authenticationSecretVersionNumber"`
	AuthSecretID      string            `json:"authenticationSecretId"`
	Type              string            `json:"type"`
	Servers           []respCacheServer `json:"servers"`
}

// validateResponseCache checks a gateway's responseCacheDetails (NONE or EXTERNAL_RESP_CACHE).
func validateResponseCache(raw jsontext.Value) *APIError {
	if len(raw) == 0 || string(raw) == jsonNull {
		return nil
	}

	var d respCacheDetails
	if err := json.Unmarshal(raw, &d, jsonx.V1()); err != nil {
		return errParam("responseCacheDetails is not valid: " + err.Error())
	}

	switch d.Type {
	case cacheTypeNone:
		return nil
	case cacheTypeExternal:
		return validateExternalCache(&d)
	default:
		return errParam("responseCacheDetails.type must be NONE or EXTERNAL_RESP_CACHE")
	}
}

func validateExternalCache(d *respCacheDetails) *APIError {
	if len(d.Servers) == 0 {
		return errParam("responseCacheDetails.servers must not be empty")
	}

	for _, s := range d.Servers {
		if s.Host == nil || *s.Host == "" || s.Port == nil || *s.Port < 1 || *s.Port > maxTCPPort {
			return errParam("responseCacheDetails.servers need a host and a port between 1 and 65535")
		}
	}

	if d.AuthSecretID == "" || d.AuthSecretVersion == nil || *d.AuthSecretVersion < 1 {
		return errParam("responseCacheDetails needs authenticationSecretId and authenticationSecretVersionNumber")
	}

	return nil
}

// forgetGatewayCache drops the cached responses of every deployment on the gateway.
func (b *InMemoryBackend) forgetGatewayCache(gwID string) {
	for id, d := range b.deployments {
		if d.GatewayID == gwID {
			b.cache.forgetDeployment(id)
		}
	}
}
