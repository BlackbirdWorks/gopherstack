package apigateway

import (
	"bytes"
	"net/http"
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

// capturingWriter records a response while passing it through.
type capturingWriter struct {
	http.ResponseWriter
	body   bytes.Buffer
	status int
}

func (c *capturingWriter) WriteHeader(code int) {
	if c.status == 0 {
		c.status = code
	}

	c.ResponseWriter.WriteHeader(code)
}

func (c *capturingWriter) Write(p []byte) (int, error) {
	if c.status == 0 {
		c.status = http.StatusOK
	}

	c.body.Write(p)

	return c.ResponseWriter.Write(p)
}

func (c *capturingWriter) Unwrap() http.ResponseWriter { return c.ResponseWriter }

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

	if hit, found := h.respCache.get(key); found {
		obs.setCache(true)

		for k, v := range hit.header {
			w.Header()[k] = append([]string(nil), v...)
		}

		w.WriteHeader(hit.status)
		_, _ = w.Write(hit.body)

		return
	}

	obs.setCache(false)

	cw := &capturingWriter{ResponseWriter: w}
	dispatch(cw)

	if cw.status >= http.StatusOK && cw.status < http.StatusMultipleChoices {
		h.respCache.put(key, &cachedResponse{
			expires: h.respCache.now().Add(ttl),
			header:  w.Header().Clone(),
			body:    cw.body.Bytes(),
			status:  cw.status,
		})
	}
}
