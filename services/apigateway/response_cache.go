package apigateway

import (
	"bytes"
	"net/http"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	defaultCacheTTL     = 300 * time.Second
	maxCachedResponses  = 1024
	cacheKeyQueryPrefix = "method.request.querystring."
	cacheKeyHeaderPref  = "method.request.header."
	cacheKeyPathPrefix  = "method.request.path."
)

type cachedResponse struct {
	expires time.Time
	header  http.Header
	body    []byte
	status  int
}

// responseCache is a bounded, TTL-expiring store of stage method responses.
type responseCache struct {
	now     func() time.Time
	entries map[string]*cachedResponse
	mu      sync.Mutex
}

func newResponseCache() *responseCache {
	return &responseCache{now: time.Now, entries: make(map[string]*cachedResponse)}
}

func cacheEntryPrefix(apiID, stageName string) string {
	return apiID + "\x00" + stageName + "\x00"
}

func (c *responseCache) get(key string) (*cachedResponse, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	e, ok := c.entries[key]
	if !ok {
		return nil, false
	}

	if !c.now().Before(e.expires) {
		delete(c.entries, key)

		return nil, false
	}

	return e, true
}

func (c *responseCache) put(key string, e *cachedResponse) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.entries) >= maxCachedResponses {
		c.evictLocked()
	}

	c.entries[key] = e
}

// evictLocked drops expired entries, and the soonest-to-expire one if none were expired.
func (c *responseCache) evictLocked() {
	now := c.now()
	oldestKey := ""

	for k, e := range c.entries {
		if !now.Before(e.expires) {
			delete(c.entries, k)

			continue
		}

		if oldestKey == "" || e.expires.Before(c.entries[oldestKey].expires) {
			oldestKey = k
		}
	}

	if len(c.entries) >= maxCachedResponses && oldestKey != "" {
		delete(c.entries, oldestKey)
	}
}

// flush drops every entry cached for the stage.
func (c *responseCache) flush(apiID, stageName string) {
	prefix := cacheEntryPrefix(apiID, stageName)

	c.mu.Lock()
	defer c.mu.Unlock()

	for k := range c.entries {
		if strings.HasPrefix(k, prefix) {
			delete(c.entries, k)
		}
	}
}

// flushAPI drops every entry cached for the REST API.
func (c *responseCache) flushAPI(apiID string) {
	prefix := apiID + "\x00"

	c.mu.Lock()
	defer c.mu.Unlock()

	for k := range c.entries {
		if strings.HasPrefix(k, prefix) {
			delete(c.entries, k)
		}
	}
}

// cacheTTL reports the TTL for a method's responses, or ok=false when caching is off for it.
func cacheTTL(stage *Stage, resourcePath, httpMethod string) (time.Duration, bool) {
	if !stage.CacheClusterEnabled || httpMethod != http.MethodGet {
		return 0, false
	}

	ms, _ := stageMethodSettingFor(stage, resourcePath, httpMethod)
	if ms == nil || !ms.CachingEnabled {
		return 0, false
	}

	if ms.CacheTTLInSeconds > 0 {
		return time.Duration(ms.CacheTTLInSeconds) * time.Second, true
	}

	return defaultCacheTTL, true
}

// responseCacheKey is the stage-scoped key: request path plus the integration's cache key parameters.
func responseCacheKey(
	apiID, stageName, deploymentID string, r *http.Request, integration *Integration, pathParams map[string]string,
) string {
	var sb strings.Builder

	sb.WriteString(cacheEntryPrefix(apiID, stageName))
	sb.WriteString(deploymentID)
	sb.WriteByte('\x00')
	sb.WriteString(r.Method)
	sb.WriteByte(' ')
	sb.WriteString(r.URL.Path)

	params := append([]string(nil), integration.CacheKeyParameters...)
	sort.Strings(params)

	for _, p := range params {
		sb.WriteByte('\x00')
		sb.WriteString(p)
		sb.WriteByte('=')

		switch {
		case strings.HasPrefix(p, cacheKeyQueryPrefix):
			sb.WriteString(r.URL.Query().Get(strings.TrimPrefix(p, cacheKeyQueryPrefix)))
		case strings.HasPrefix(p, cacheKeyHeaderPref):
			sb.WriteString(r.Header.Get(strings.TrimPrefix(p, cacheKeyHeaderPref)))
		case strings.HasPrefix(p, cacheKeyPathPrefix):
			sb.WriteString(pathParams[strings.TrimPrefix(p, cacheKeyPathPrefix)])
		}
	}

	return sb.String()
}

// bufferedResponse holds a dispatched response so it can be replayed and cached.
type bufferedResponse struct {
	header http.Header
	body   bytes.Buffer
	status int
}

func (b *bufferedResponse) Header() http.Header { return b.header }

func (b *bufferedResponse) WriteHeader(code int) {
	if b.status == 0 {
		b.status = code
	}
}

func (b *bufferedResponse) Write(p []byte) (int, error) {
	if b.status == 0 {
		b.status = http.StatusOK
	}

	return b.body.Write(p)
}

func replayCached(w http.ResponseWriter, resp *cachedResponse) {
	for k, v := range resp.header {
		w.Header()[k] = append([]string(nil), v...)
	}

	w.WriteHeader(resp.status)
	_, _ = w.Write(resp.body)
}

const (
	cacheStrategyFail    = "FAIL_WITH_403"
	cacheStrategyWithout = "SUCCEED_WITHOUT_RESPONSE_HEADER"
	sigV4Prefix          = "AWS4-HMAC-SHA256"
)

type cacheControl struct {
	bypass bool
	denied bool
	warn   bool
}

// cacheInvalidation interprets a Cache-Control: max-age=0 request: bypass refetches and replaces
// the entry, denied rejects with 403, warn serves normally with a Warning header. Without IAM
// policy evaluation, a request is authorized when it is SigV4-signed.
func cacheInvalidation(r *http.Request, ms *MethodSetting) cacheControl {
	if !slices.ContainsFunc(r.Header.Values("Cache-Control"), func(v string) bool {
		return slices.ContainsFunc(strings.Split(v, ","), func(d string) bool {
			return strings.EqualFold(strings.TrimSpace(d), "max-age=0")
		})
	}) {
		return cacheControl{}
	}

	if ms == nil || !ms.RequireAuthorizationForCacheControl ||
		strings.HasPrefix(r.Header.Get("Authorization"), sigV4Prefix) {
		return cacheControl{bypass: true}
	}

	switch ms.UnauthorizedCacheControlHeaderStrategy {
	case cacheStrategyFail:
		return cacheControl{denied: true}
	case cacheStrategyWithout:
		return cacheControl{}
	default:
		return cacheControl{warn: true}
	}
}

// serveWithCache serves GET responses from the stage cache when the method has caching
// enabled, recording hits and misses on obs; otherwise it dispatches unchanged.
func (h *Handler) serveWithCache(
	w http.ResponseWriter, r *http.Request, obs *proxyObs,
	stage *Stage, apiID, resourcePath string, integration *Integration, pathParams map[string]string,
	dispatch func(http.ResponseWriter),
) {
	ttl, ok := cacheTTL(stage, resourcePath, r.Method)
	if !ok {
		dispatch(w)

		return
	}

	key := responseCacheKey(apiID, stage.StageName, stage.DeploymentID, r, integration, pathParams)

	ms, _ := stageMethodSettingFor(stage, resourcePath, r.Method)
	cc := cacheInvalidation(r, ms)

	if cc.denied {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"message":"Unauthorized"}`))

		return
	}

	if cc.warn {
		w.Header().Set("Warning", `199 - "Cache-Control header ignored: not authorized to invalidate the cache"`)
	}

	if hit, found := h.respCache.get(key); found && !cc.bypass {
		obs.setCache(true)
		replayCached(w, hit)

		return
	}

	obs.setCache(false)

	buf := &bufferedResponse{header: w.Header().Clone()}
	dispatch(buf)

	if buf.status == 0 {
		buf.status = http.StatusOK
	}

	resp := &cachedResponse{
		expires: h.respCache.now().Add(ttl),
		header:  buf.header,
		body:    buf.body.Bytes(),
		status:  buf.status,
	}

	if resp.status >= http.StatusOK && resp.status < http.StatusMultipleChoices {
		h.respCache.put(key, resp)
	}

	replayCached(w, resp)
}
