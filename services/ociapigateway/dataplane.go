package ociapigateway

import (
	"bytes"
	"encoding/json/v2"
	"errors"
	"io"
	"maps"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	maxDataBody = 10 << 20
	// maxBackendResponse bounds a proxied response body; a longer one aborts the client connection.
	maxBackendResponse = 128 << 20
	copyChunk          = 32 << 10
)

// target is the deployment a data-plane request resolved to.
type target struct {
	spec     *compiledSpec
	cacheCfg *respCacheDetails
	reach    func(srcIP, vnicID string, port int) Reachability
	logSink  LogSink
	depID    string
	rel      string
	gateway  string
	vnic     string
	gwName   string
	private  bool
}

func writeGWError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	out, _ := json.Marshal(
		map[string]any{"code": status, keyMessage: msg},
		jsonx.V1(),
	) //nolint:errchkjson // static shape
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
		spec: b.specs[best.ID], depID: best.ID, rel: rel, gateway: gwID, cacheCfg: externalCacheOf(b.gateways[gwID]),
		vnic: b.gateways[gwID].VnicID, private: b.gateways[gwID].EndpointType == endpointPrivate, reach: b.reachFn(),
		logSink: b.logSink, gwName: b.gateways[gwID].DisplayName,
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

	if !peerAllowed(r, tgt) {
		dropConnection(w)

		return
	}

	started := b.clock()
	r, lg := b.startLog(r, tgt)
	rec := &statusRecorder{ResponseWriter: w}
	w = rec

	defer lg.access(r, rec, started)

	rp := tgt.spec.doc.RequestPolicies

	if d := b.checkMTLS(r, tgt); d.status != 0 {
		code := d.code
		if code == "" {
			code = "mutualTls.clientCertificateInvalid"
		}

		lg.exec(levelWarn, code, "The client certificate was refused.")
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

	lg.routeLevel(m.route.doc.LoggingPolicies)
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
	lg := logFrom(c.r.Context())

	if ok {
		lg.exec(levelInfo, "rateLimiting.requestPermitted", "The request was permitted by the rate limiting policy.",
			"configuredLimit", *rp.RateLimiting.Rate, "configuredUnit", "SECOND")
	} else {
		lg.exec(levelWarn, "rateLimiting.requestDenied", "The request was denied by the rate limiting policy.",
			"configuredLimit", *rp.RateLimiting.Rate, "configuredUnit", "SECOND")
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

	lg := logFrom(c.r.Context())
	d := b.authorize(c.r, rp, c.m.route, rv)
	logAuth(lg, rp, rv, d)

	if d.status != 0 {
		h := d.hdr
		if d.www != "" {
			h = []string{"WWW-Authenticate", d.www}
		}

		c.deny(d.status, d.msg, h...)

		return admitted{}, false
	}

	uv := b.enforceUsage(c.r, usagePolicy(rp), c.tgt.depID)
	logUsage(lg, uv, usagePolicy(rp) != nil)

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
		lg.exec(levelWarn, "requestValidation.validationError", fail)
		b.refundQuota(uv.refund)
		c.deny(http.StatusBadRequest, fail)

		return admitted{}, false
	}

	if soft != "" {
		lg.exec(levelWarn, "requestValidation.validationError", soft)
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
		logFrom(r.Context()).exec(levelWarn, "request.bodyTooLarge", "The request body was too large.")
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
	rc, served := b.serveCached(w, r, tgt, rc, c.cors)
	if served {
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
		b.cacheStore(r.Context(), o.tgt, o.cache.key, &storedResponse{
			Status: res.status, Header: stored, Body: captured, Expires: now.Add(o.cache.ttl).UnixNano(),
		}, o.cache.ttl)
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
		copyBody(w, res.body, b.responseCap())
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
	code   string
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

func logAuth(lg *reqLog, rp *requestPolicies, rv *reqView, d denial) {
	if lg == nil {
		return
	}

	a, _ := authPolicy(rp, rv)
	if a == nil {
		return
	}

	prefix := map[string]string{
		authJWT: "jwtAuthentication", authToken: "tokenAuthentication", authCustom: "customAuthentication",
	}[a.Type]

	switch {
	case d.status == http.StatusForbidden:
		lg.exec(levelWarn, "authorization.unauthorizedRequest", "Authorization failed for the request.")
	case d.status != 0:
		lg.exec(levelWarn, prefix+".authenticationFailed", "Authentication failed.")
	default:
		lg.exec(levelInfo, prefix+".successfulAuthentication", "Authentication successful.")
	}
}

func logUsage(lg *reqLog, v usageVerdict, enabled bool) {
	switch {
	case !enabled:
	case v.status == http.StatusForbidden:
		lg.exec(levelWarn, "usagePlans.eligibleNotEntitled", "The deployment is not the target of an entitlement.")
	case v.status != 0:
		lg.exec(levelWarn, "usagePlans.requestRejected", "Request from a usage plan subscriber was rejected.")
	case v.breached:
		lg.exec(levelWarn, "usagePlans.requestBreachedButAllowed",
			"The request was allowed although the usage plan quota was exceeded.")
	default:
		lg.exec(levelInfo, "usagePlans.requestPermitted", "Request from a usage plan subscriber was allowed.")
	}
}

// serveCached answers from the response cache; a cache server problem turns the request into a BYPASS.
func (b *InMemoryBackend) serveCached(
	w http.ResponseWriter, r *http.Request, tgt target, rc routeCache, cors *corsDoc,
) (routeCache, bool) {
	lg := logFrom(r.Context())

	if rc.active && !rc.bypass {
		hit, err := b.cacheLookup(r.Context(), tgt, rc.key)

		switch {
		case err != nil:
			rc.bypass = true
		case hit != nil:
			lg.exec(levelInfo, "responseCache.lookupResultSuccess", "A response was read from the cache.")
			b.writeCached(w, r, hit, cors)

			return rc, true
		default:
			lg.exec(levelInfo, "responseCache.lookupResultNotFound", "A response was not found in the cache.")
		}
	}

	if rc.active && rc.bypass {
		lg.exec(levelInfo, "responseCache.lookupAborted", "The response cache was not used.")
	}

	return rc, false
}

func (b *InMemoryBackend) responseCap() int64 {
	b.mu.RLock("responseCap")
	defer b.mu.RUnlock()

	if b.respCap > 0 {
		return b.respCap
	}

	return maxBackendResponse
}

// copyBody streams src to w, flushing each chunk so a streaming back end reaches the client as it is produced.
func copyBody(w http.ResponseWriter, src io.Reader, limit int64) {
	rc := http.NewResponseController(w)
	buf := make([]byte, copyChunk)

	var sent int64

	for {
		n, err := src.Read(buf)
		if n > 0 {
			sent += int64(n)
			if sent > limit {
				panic(http.ErrAbortHandler)
			}

			if _, werr := w.Write(buf[:n]); werr != nil {
				return
			}

			_ = rc.Flush()
		}

		if err != nil {
			if !errors.Is(err, io.EOF) {
				panic(http.ErrAbortHandler)
			}

			return
		}
	}
}
