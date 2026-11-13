package ociapigateway

import (
	"bytes"
	"encoding/json"
	"io"
	"maps"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const maxDataBody = 10 << 20

// target is the deployment a data-plane request resolved to.
type target struct {
	spec    *compiledSpec
	depID   string
	rel     string
	gateway string
	cacheOn bool
}

func writeGWError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	out, _ := json.Marshal(map[string]any{"code": status, "message": msg}) //nolint:errchkjson // static shape
	_, _ = w.Write(out)
}

// resolveTarget maps a request to a deployment by gateway hostname (Host header) or by /_apigw/<gatewayId> path.
func (b *InMemoryBackend) resolveTarget(r *http.Request) (target, bool) {
	path := r.URL.Path
	gwRef, byID := "", false

	if rest, ok := strings.CutPrefix(path, dataPathPrefix); ok {
		var tail string

		gwRef, tail, _ = strings.Cut(rest, "/")
		path, byID = "/"+tail, true
	}

	b.mu.RLock("resolveTarget")
	defer b.mu.RUnlock()

	gwID := b.findGateway(gwRef, stripPort(r.Host), byID)
	if gwID == "" {
		return target{}, false
	}

	best := b.findDeployment(gwID, path)
	if best == nil || b.specs[best.ID] == nil {
		return target{}, false
	}

	rel := strings.TrimPrefix(path, strings.TrimSuffix(best.PathPrefix, "/"))
	if rel == "" {
		rel = "/"
	}

	return target{
		spec: b.specs[best.ID], depID: best.ID, rel: rel, gateway: gwID, cacheOn: cacheOnFor(b.gateways[gwID]),
	}, true
}

func servable(state string) bool { return state == stateActive || state == stateUpdating }

func (b *InMemoryBackend) findGateway(ref, host string, byID bool) string {
	for _, g := range b.gateways {
		if !servable(g.State) {
			continue
		}

		if (byID && g.ID == ref) || (!byID && strs.Equal(g.Hostname, host)) {
			return g.ID
		}
	}

	return ""
}

// findDeployment returns the servable deployment with the longest pathPrefix matching the path.
func (b *InMemoryBackend) findDeployment(gwID, path string) *Deployment {
	var best *Deployment

	for _, d := range b.deployments {
		if d.GatewayID != gwID || !servable(d.State) || !prefixMatches(d.PathPrefix, path) {
			continue
		}

		if best == nil || len(d.PathPrefix) > len(best.PathPrefix) {
			best = d
		}
	}

	return best
}

func prefixMatches(prefix, path string) bool {
	return prefix == "/" || path == prefix || strings.HasPrefix(path, prefix+"/")
}

// ServeData serves a request addressed to a deployed API.
func (b *InMemoryBackend) ServeData(w http.ResponseWriter, r *http.Request) {
	tgt, ok := b.resolveTarget(r)
	if !ok {
		writeGWError(w, http.StatusNotFound, "Not Found")

		return
	}

	rp := tgt.spec.doc.RequestPolicies

	if d := b.checkMTLS(r, tgt); d.status != 0 {
		writeGWError(w, d.status, d.msg)

		return
	}

	if r.Method == http.MethodOptions && r.Header.Get("Origin") != "" &&
		r.Header.Get("Access-Control-Request-Method") != "" {
		if c := effectiveCors(rp, tgt.spec.anyRouteFor(tgt.rel)); c != nil {
			preflight(w, r, c)

			return
		}
	}

	m := tgt.spec.match(tgt.rel, r.Method)
	if m.route == nil {
		if len(m.allow) > 0 {
			w.Header().Set("Allow", strings.Join(m.allow, ", "))
			writeGWError(w, http.StatusMethodNotAllowed, "Method Not Allowed")

			return
		}

		writeGWError(w, http.StatusNotFound, "Not Found")

		return
	}

	b.serveRoute(w, r, tgt, m)
}

func effectiveCors(rp *requestPolicies, route *compiledRoute) *corsDoc {
	if route != nil && route.doc.RequestPolicies != nil && route.doc.RequestPolicies.Cors != nil {
		return route.doc.RequestPolicies.Cors
	}

	if rp != nil {
		return rp.Cors
	}

	return nil
}

// routeCtx is one data-plane request that resolved to a route.
type routeCtx struct {
	w    http.ResponseWriter
	r    *http.Request
	cors *corsDoc
	tgt  target
	m    matchResult
}

// deny answers with a gateway error, decorated with the CORS headers of the route.
func (c *routeCtx) deny(status int, msg string, h ...string) {
	for i := 0; i+1 < len(h); i += 2 {
		c.w.Header().Set(h[i], h[i+1])
	}

	corsHeaders(c.w.Header(), c.cors, c.r.Header.Get("Origin"))
	writeGWError(c.w, status, msg)
}

func (b *InMemoryBackend) rateLimited(c *routeCtx) bool {
	rp := c.tgt.spec.doc.RequestPolicies
	if rp == nil || rp.RateLimiting == nil {
		return false
	}

	key := c.tgt.depID + "|total"
	if rp.RateLimiting.Key == rateKeyClient {
		key = c.tgt.depID + "|" + clientKey(c.r)
	}

	ok, retry := b.limiter.allow(key, *rp.RateLimiting.Rate, b.clock())
	if !ok {
		c.deny(http.StatusTooManyRequests, "Too Many Requests", "Retry-After", strconv.Itoa(retry))
	}

	return !ok
}

// admitted is what the request policies learned about an accepted request.
type admitted struct {
	claims map[string]any
	refund []string
}

// gate runs authentication, usage plans and request validations; ok is false once the request was answered.
func (b *InMemoryBackend) gate(c *routeCtx, rv *reqView, body []byte) (admitted, bool) {
	rp := c.tgt.spec.doc.RequestPolicies

	d := b.authorize(c.r, rp, c.m.route, rv)
	if d.status != 0 {
		h := d.hdr
		if d.www != "" {
			h = []string{"WWW-Authenticate", d.www}
		}

		c.deny(d.status, d.msg, h...)

		return admitted{}, false
	}

	uv := b.enforceUsage(c.r, usagePolicy(rp), c.tgt.depID)
	if uv.status != 0 {
		var h []string
		if uv.retry > 0 {
			h = []string{"Retry-After", strconv.Itoa(uv.retry)}
		}

		c.deny(uv.status, uv.msg, h...)

		return admitted{}, false
	}

	fail, soft := validateRequest(c.r, c.m.route.doc.RequestPolicies, body)
	if fail != "" {
		b.refundQuota(uv.refund)
		c.deny(http.StatusBadRequest, fail)

		return admitted{}, false
	}

	if soft != "" {
		logger.Load(c.r.Context()).
			InfoContext(c.r.Context(), "ociapigateway: request failed PERMISSIVE validation", "reason", soft)
	}

	return admitted{claims: d.claims, refund: uv.refund}, true
}

func (b *InMemoryBackend) serveRoute(w http.ResponseWriter, r *http.Request, tgt target, m matchResult) {
	c := &routeCtx{w: w, r: r, tgt: tgt, m: m, cors: effectiveCors(tgt.spec.doc.RequestPolicies, m.route)}

	if b.rateLimited(c) {
		return
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxDataBody))
	if err != nil {
		writeGWError(w, http.StatusRequestEntityTooLarge, "Request Entity Too Large")

		return
	}

	rv := &reqView{params: m.params, query: cloneValues(r.URL.Query()), header: r.Header.Clone()}

	adm, ok := b.gate(c, rv, body)
	if !ok {
		return
	}

	if pol := m.route.doc.RequestPolicies; pol != nil {
		transformHeaders(rv.header, pol.HeaderTransform, rv)
		transformQuery(rv.query, pol.QueryTransform, rv)
	}

	rc := cachePlan(tgt, m.route, r, adm.claims)
	if rc.active && !rc.bypass && b.cacheServe(w, r, rc, c.cors) {
		return
	}

	res := b.runBackend(r.Context(), m.route.doc.Backend, r, rv, body)
	if res.err != nil || res.status >= http.StatusInternalServerError {
		b.refundQuota(adm.refund)
	}

	if res.err != nil {
		c.deny(http.StatusBadGateway, "Bad Gateway")

		return
	}

	defer res.body.Close()

	if pol := m.route.doc.ResponsePolicies; pol != nil {
		transformHeaders(res.header, pol.HeaderTransform, rv)
	}

	b.respond(w, r, res, respondOpts{cors: c.cors, cache: rc, tgt: tgt})
}

type respondOpts struct {
	cors  *corsDoc
	tgt   target
	cache routeCache
}

// respond writes the backend response, storing it in the response cache when the route's policies call for it.
func (b *InMemoryBackend) respond(w http.ResponseWriter, r *http.Request, res backendResponse, o respondOpts) {
	var captured []byte

	storable := o.cache.active && !o.cache.bypass && cachedStatus(res.status)
	if storable {
		captured, _ = io.ReadAll(io.LimitReader(res.body, cacheMaxBody+1))
		storable = len(captured) <= cacheMaxBody

		res.body = io.NopCloser(io.MultiReader(bytes.NewReader(captured), res.body))
	}

	if storable {
		stored := res.header.Clone()
		now := b.clock()
		b.cache.put(o.cache.key, &cacheEntry{
			status: res.status, header: stored, body: captured, dep: o.tgt.depID, expires: now.Add(o.cache.ttl),
		}, now)
	}

	switch {
	case o.cache.bypass:
		res.header.Set(cacheStatusHeader, cacheBypass)
	case o.cache.active:
		res.header.Set(cacheStatusHeader, cacheMiss)
	}

	corsHeaders(res.header, o.cors, r.Header.Get("Origin"))

	maps.Copy(w.Header(), res.header)

	w.WriteHeader(res.status)

	if r.Method != http.MethodHead {
		_, _ = io.Copy(w, res.body)
	}
}

func cloneValues(v url.Values) url.Values {
	out := make(url.Values, len(v))
	for k, vs := range v {
		out[k] = append([]string(nil), vs...)
	}

	return out
}

// denial is an authentication or authorization refusal; a zero status means the request may proceed.
type denial struct {
	claims map[string]any
	www    string
	msg    string
	hdr    []string
	status int
}

func usagePolicy(rp *requestPolicies) *usagePlansDoc {
	if rp == nil {
		return nil
	}

	return rp.UsagePlans
}

func (b *InMemoryBackend) authorize(r *http.Request, rp *requestPolicies, route *compiledRoute, rv *reqView) denial {
	a, ok := authPolicy(rp, rv)
	if !ok {
		return denial{status: http.StatusUnauthorized, msg: msgUnauthorized}
	}

	if a == nil {
		return denial{}
	}

	var authz *authzDoc

	if route.doc.RequestPolicies != nil {
		authz = route.doc.RequestPolicies.Authorization
	}

	if authz != nil && authz.Type == authzAnonymous {
		return denial{}
	}

	res, anonymous := b.authenticate(r.Context(), a, r, rv)
	if res.upstream {
		return denial{status: http.StatusBadGateway, msg: "Bad Gateway"}
	}

	if !res.ok && a.ValidationFailure != nil {
		return failureDenial(a.ValidationFailure, rv)
	}

	if !res.ok {
		return denial{status: http.StatusUnauthorized, www: res.wwwAuth, msg: msgUnauthorized}
	}

	if authz != nil && authz.Type == anyOf && (anonymous || !hasAnyScope(res.scopes, authz.AllowedScope)) {
		return denial{status: http.StatusForbidden, msg: "Forbidden"}
	}

	return denial{claims: res.claims}
}

func hasAnyScope(have, allowed []string) bool {
	for _, s := range have {
		if containsStr(allowed, s) {
			return true
		}
	}

	return false
}

// failureDenial builds the MODIFY_RESPONSE answer for a missing or invalid token.
func failureDenial(f *failureDoc, rv *reqView) denial {
	d := denial{status: http.StatusUnauthorized, msg: msgUnauthorized}

	if f.ResponseCode != "" {
		if n, ok := failureStatus(rv.interpolate(f.ResponseCode, nil)); ok {
			d.status = n
		}
	}

	if f.ResponseMessage != "" {
		d.msg = rv.interpolate(f.ResponseMessage, nil)
	}

	if t := f.ResponseHeaders; t != nil && t.SetHeaders != nil {
		h := http.Header{}
		setValues(h, t.SetHeaders.Items, http.CanonicalHeaderKey, rv, h.Add)

		for k, vs := range h {
			for _, v := range vs {
				d.hdr = append(d.hdr, k, v)
			}
		}
	}

	return d
}
