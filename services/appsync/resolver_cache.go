package appsync

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/vektah/gqlparser/v2/ast"
)

const (
	cachingFullRequest  = "FULL_REQUEST_CACHING"
	cachingPerResolver  = "PER_RESOLVER_CACHING"
	cachingOperation    = "OPERATION_LEVEL_CACHING"
	maxCacheEntriesPerA = 10000
)

type cacheEntry struct {
	expires time.Time
	value   any
}

// cachePolicy is how one resolver call is cached: its TTL and the ctx paths
// that make up the key (empty keys: arguments, source and identity).
type cachePolicy struct {
	keys []string
	ttl  time.Duration
}

// apiCacheOf returns a copy of the API cache config, or nil when none.
func (b *InMemoryBackend) apiCacheOf(apiID string) *APICache {
	b.mu.RLock("ApiCacheOf")
	defer b.mu.RUnlock()

	c, ok := b.apiCaches.Get(apiID)
	if !ok {
		return nil
	}

	cp := *c

	return &cp
}

// policyFor applies the AppSync server-side caching rules: FULL_REQUEST_CACHING
// and OPERATION_LEVEL_CACHING cache query root fields for the cache TTL;
// PER_RESOLVER_CACHING caches resolvers with a cachingConfig for its ttl.
func policyFor(cache *APICache, parentType string, r *Resolver) (cachePolicy, bool) {
	if cache == nil || cache.Status != "AVAILABLE" {
		return cachePolicy{}, false
	}

	switch cache.APICachingBehavior {
	case cachingFullRequest, cachingOperation:
		if parentType != "Query" || cache.TTL <= 0 {
			return cachePolicy{}, false
		}

		return cachePolicy{ttl: time.Duration(cache.TTL) * time.Second}, true
	case cachingPerResolver:
		if r.CachingConfig == nil || r.CachingConfig.TTL <= 0 {
			return cachePolicy{}, false
		}

		return cachePolicy{
			ttl: time.Duration(r.CachingConfig.TTL) * time.Second, keys: r.CachingConfig.CachingKeys,
		}, true
	}

	return cachePolicy{}, false
}

// normalizeCacheKey strips the "$context." / "context." / "$ctx." prefix so a
// cachingKeys entry and an evictFromApiCache key compare equal.
func normalizeCacheKey(k string) string {
	k = strings.TrimPrefix(k, "$")

	for _, p := range []string{"context.", "ctx."} {
		if rest, ok := strings.CutPrefix(k, p); ok {
			return rest
		}
	}

	return k
}

func ctxPath(ctxMap map[string]any, path string) any {
	var cur any = ctxMap

	for _, part := range strings.Split(path, ".") {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil
		}

		cur = m[part]
	}

	return cur
}

// cacheKeyParts is the key material: cachingKeys values, or the whole
// arguments/source/identity when the resolver lists none.
func (p cachePolicy) keyParts(ctxMap map[string]any) map[string]any {
	parts := map[string]any{}

	if len(p.keys) == 0 {
		for _, k := range []string{"arguments", "source", "identity"} {
			parts[k] = ctxMap[k]
		}

		return parts
	}

	for _, k := range p.keys {
		n := normalizeCacheKey(k)
		parts[n] = ctxPath(ctxMap, n)
	}

	return parts
}

func encodeKey(typeName, fieldName string, parts map[string]any) string {
	raw, _ := json.Marshal(parts)

	return typeName + "." + fieldName + "|" + string(raw)
}

// cachedResolve serves a resolver call from the API cache, running run on a miss.
func (e *execEnv) cachedResolve(
	parentType string, field *ast.Field, resolver *Resolver, cc *callCtx, args map[string]any, source map[string]any,
	run func() (any, error),
) (any, error) {
	pol, ok := policyFor(e.cache, parentType, resolver)
	if !ok {
		return run()
	}

	key := encodeKey(parentType, field.Name, pol.keyParts(cc.ctxMap(args, nil, nil, sourceValue(source), false)))

	if v, hit := e.backend.cacheGet(e.apiID, key); hit {
		return v, nil
	}

	v, err := run()
	if err != nil {
		return nil, err
	}

	e.backend.cachePut(e.apiID, key, v, pol.ttl)

	return v, nil
}

func (b *InMemoryBackend) cacheGet(apiID, key string) (any, bool) {
	b.mu.Lock("CacheGet")
	defer b.mu.Unlock()

	ent, ok := b.respCache[apiID][key]
	if !ok {
		return nil, false
	}

	if !time.Now().Before(ent.expires) {
		delete(b.respCache[apiID], key)

		return nil, false
	}

	return ent.value, true
}

func (b *InMemoryBackend) cachePut(apiID, key string, v any, ttl time.Duration) {
	b.mu.Lock("CachePut")
	defer b.mu.Unlock()

	if b.respCache == nil {
		b.respCache = map[string]map[string]cacheEntry{}
	}

	m := b.respCache[apiID]
	if m == nil {
		m = map[string]cacheEntry{}
		b.respCache[apiID] = m
	}

	now := time.Now()

	if len(m) >= maxCacheEntriesPerA {
		for k, ent := range m {
			if !now.Before(ent.expires) {
				delete(m, k)
			}
		}

		if len(m) >= maxCacheEntriesPerA {
			clear(m)
		}
	}

	m[key] = cacheEntry{value: v, expires: now.Add(ttl)}
}

// flushCacheLocked drops every cached response of the API.
func (b *InMemoryBackend) flushCacheLocked(apiID string) {
	delete(b.respCache, apiID)
}

// applyCacheEvictions performs $extensions.evictFromApiCache(typeName,
// fieldName, keyValuePairs): entries of that resolver whose key material
// matches the given pairs (all entries when the pairs are empty).
func (b *InMemoryBackend) applyCacheEvictions(apiID string, calls []map[string]any) {
	for _, rec := range calls {
		if rec["kind"] != "evictFromApiCache" {
			continue
		}

		args, _ := rec["args"].([]any)
		typeName, field := vrender(argAt(args, 0)), vrender(argAt(args, 1))

		plain, err := plainJSON(argAt(args, 2))
		if err != nil {
			continue
		}

		pairs, _ := plain.(map[string]any)
		want := map[string]any{}

		for k, v := range pairs {
			want[normalizeCacheKey(k)] = v
		}

		b.evictMatching(apiID, typeName+"."+field+"|", want)
	}
}

func (b *InMemoryBackend) evictMatching(apiID, prefix string, want map[string]any) {
	b.mu.Lock("EvictFromApiCache")
	defer b.mu.Unlock()

	for k := range b.respCache[apiID] {
		rest, ok := strings.CutPrefix(k, prefix)
		if !ok {
			continue
		}

		var have map[string]any
		if json.Unmarshal([]byte(rest), &have) != nil {
			continue
		}

		if cacheKeyCovers(have, want) {
			delete(b.respCache[apiID], k)
		}
	}
}

func cacheKeyCovers(have, want map[string]any) bool {
	for k, v := range want {
		hv, ok := have[k]
		if !ok || !looseEqual(hv, v) {
			return false
		}
	}

	return true
}
