package azurearm

import (
	"bytes"
	"io"
	"net/http"
	"net/textproto"
	"slices"
	"strconv"
	"strings"
	"time"
)

const (
	apimCCacheLookup      = "cache-lookup"
	apimCCacheStore       = "cache-store"
	apimCCacheLookupValue = "cache-lookup-value"
	apimCCacheStoreValue  = "cache-store-value"
	apimCCacheRemoveValue = "cache-remove-value"
	apimCVaryHeader       = "vary-by-header"
	apimCVaryQuery        = "vary-by-query-parameter"
	apimCCachingType      = "caching-type"
	apimCDownstream       = "downstream-caching-type"
	apimCKey              = "key"
	apimCVariableName     = "variable-name"
	apimCDefaultValue     = "default-value"
	apimCDuration         = "duration"
)

type gwCacheKey struct {
	lookup *polNode
	key    string
}

func (g *gwState) cacheLookup(n *polNode) {
	if g.r.Method != http.MethodGet {
		return
	}

	if g.header.Get(apimCAuthorizationX) != "" && !g.boolAttr(n, "allow-private-response-caching", false) {
		return
	}

	parts := []string{g.plan.svcKey, "resp", strings.ToLower(hostFromHostHeader(g.r.Host)), g.plan.path}

	if g.boolAttr(n, "vary-by-developer", false) {
		parts = append(parts, "dev", g.plan.ownerID)
	}

	if g.boolAttr(n, "vary-by-developer-groups", false) {
		parts = append(parts, "grp", strings.Join(g.plan.groups, ","))
	}

	for _, h := range n.values(apimCVaryHeader) {
		parts = append(
			parts,
			"h:"+strings.ToLower(h),
			strings.Join(g.header.Values(textproto.CanonicalMIMEHeaderKey(h)), ","),
		)
	}

	for _, q := range n.values(apimCVaryQuery) {
		parts = append(parts, "q:"+q, strings.Join(g.query[q], ","))
	}

	if g.evalErr != nil {
		return
	}

	key := apimKeyOf(parts...)
	g.cacheKey = &gwCacheKey{key: key, lookup: n}

	v, expires, ok := g.p.cache.get(key, g.now())
	if !ok {
		return
	}

	cr, isResp := v.(*gwCachedResponse)
	if !isResp {
		return
	}

	h := http.Header{}
	for k, vals := range cr.header {
		h[k] = slices.Clone(vals)
	}

	g.setResponse(cr.status, h, cr.body)
	g.cacheHit = true
	g.downstream(n, expires.Sub(g.now()))
}

func (g *gwState) downstream(lookup *polNode, ttl time.Duration) {
	kind := g.sub(lookup.Attrs[apimCDownstream])

	if kind != "private" && kind != "public" {
		return
	}

	cc := kind + ", max-age=" + strconv.Itoa(max(0, int(ttl.Seconds())))
	if g.boolAttr(lookup, "must-revalidate", true) {
		cc += ", must-revalidate"
	}

	g.respHeader.Set("Cache-Control", cc)
}

func (g *gwState) cacheStore(n *polNode) {
	if g.cacheKey == nil || g.cacheHit {
		return
	}

	secs := g.intAttr(n, apimCDuration, 0)
	any200 := g.boolAttr(n, "cache-response", false)

	if g.evalErr != nil || secs <= 0 || (g.status != http.StatusOK && !any200) || g.respBody == nil {
		return
	}

	body, err := io.ReadAll(io.LimitReader(g.respBody, apimMaxCacheEntry+1))
	if err != nil {
		return
	}

	if len(body) > apimMaxCacheEntry {
		g.respBody = &gwJoinedBody{Reader: io.MultiReader(bytes.NewReader(body), g.respBody), c: g.respBody}

		return
	}

	_ = g.respBody.Close()
	g.respBody = io.NopCloser(bytes.NewReader(body))

	h := http.Header{}
	for k, v := range g.respHeader {
		h[k] = slices.Clone(v)
	}

	ttl := time.Duration(secs) * time.Second
	g.p.cache.put(g.cacheKey.key, &gwCachedResponse{status: g.status, header: h, body: body}, len(body), ttl, g.now())
	g.downstream(g.cacheKey.lookup, ttl)
}

type gwJoinedBody struct {
	io.Reader

	c io.Closer
}

func (b *gwJoinedBody) Close() error { return b.c.Close() }

func (g *gwState) cacheValue(n *polNode) {
	key := g.sub(n.Attrs[apimCKey])
	if g.evalErr != nil {
		return
	}

	if len(key) > apimMaxCounterKey {
		g.evalErr = apimExprFail("the key is longer than %d characters", apimMaxCounterKey)

		return
	}

	full := apimKeyOf(g.plan.svcKey, "val", key)

	switch n.Name {
	case apimCCacheLookupValue:
		name := n.Attrs[apimCVariableName]
		if v, _, ok := g.p.cache.get(full, g.now()); ok {
			g.vars[name] = v

			return
		}

		if d, has := n.Attrs[apimCDefaultValue]; has {
			g.vars[name] = g.subVal(d)
		}
	case apimCCacheStoreValue:
		v := g.subVal(n.Attrs[fieldValue])
		secs := g.intAttr(n, apimCDuration, 0)

		if g.evalErr == nil && secs > 0 {
			g.p.cache.put(full, v, len(apimExprString(v)), time.Duration(secs)*time.Second, g.now())
		}
	case apimCCacheRemoveValue:
		g.p.cache.remove(full)
	}
}
