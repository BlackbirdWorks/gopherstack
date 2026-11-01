package ociapigateway

import (
	"encoding/json"
	"io"
	"maps"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const maxDataBody = 10 << 20

// target is the deployment a data-plane request resolved to.
type target struct {
	spec    *compiledSpec
	depID   string
	rel     string
	gateway string
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

	return target{spec: b.specs[best.ID], depID: best.ID, rel: rel, gateway: gwID}, true
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

func (b *InMemoryBackend) serveRoute(w http.ResponseWriter, r *http.Request, tgt target, m matchResult) {
	rp := tgt.spec.doc.RequestPolicies
	cors := effectiveCors(rp, m.route)
	origin := r.Header.Get("Origin")

	if rp != nil && rp.RateLimiting != nil {
		key := tgt.depID + "|total"
		if rp.RateLimiting.Key == rateKeyClient {
			key = tgt.depID + "|" + clientKey(r)
		}

		if ok, retry := b.limiter.allow(key, *rp.RateLimiting.Rate, b.clock()); !ok {
			w.Header().Set("Retry-After", strconv.Itoa(retry))
			corsHeaders(w.Header(), cors, origin)
			writeGWError(w, http.StatusTooManyRequests, "Too Many Requests")

			return
		}
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxDataBody))
	if err != nil {
		writeGWError(w, http.StatusRequestEntityTooLarge, "Request Entity Too Large")

		return
	}

	rv := &reqView{params: m.params, query: cloneValues(r.URL.Query()), header: r.Header.Clone()}

	if d := b.authorize(r, rp, m.route, rv); d.status != 0 {
		if d.www != "" {
			w.Header().Set("WWW-Authenticate", d.www)
		}

		corsHeaders(w.Header(), cors, origin)
		writeGWError(w, d.status, d.msg)

		return
	}

	if pol := m.route.doc.RequestPolicies; pol != nil {
		transformHeaders(rv.header, pol.HeaderTransform, rv)
		transformQuery(rv.query, pol.QueryTransform, rv)
	}

	res := b.runBackend(r.Context(), m.route.doc.Backend, r, rv, body)
	if res.err != nil {
		corsHeaders(w.Header(), cors, origin)
		writeGWError(w, http.StatusBadGateway, "Bad Gateway")

		return
	}

	defer res.body.Close()

	if pol := m.route.doc.ResponsePolicies; pol != nil {
		transformHeaders(res.header, pol.HeaderTransform, rv)
	}

	corsHeaders(res.header, cors, origin)

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
	www    string
	msg    string
	status int
}

func (b *InMemoryBackend) authorize(r *http.Request, rp *requestPolicies, route *compiledRoute, rv *reqView) denial {
	if rp == nil || rp.Authentication == nil {
		return denial{}
	}

	var authz *authzDoc

	if route.doc.RequestPolicies != nil {
		authz = route.doc.RequestPolicies.Authorization
	}

	if authz != nil && authz.Type == authzAnonymous {
		return denial{}
	}

	res, anonymous := b.authenticate(r.Context(), rp.Authentication, r, rv)
	if !res.ok {
		return denial{status: http.StatusUnauthorized, www: res.wwwAuth, msg: "Unauthorized"}
	}

	if authz != nil && authz.Type == anyOf && (anonymous || !hasAnyScope(res.scopes, authz.AllowedScope)) {
		return denial{status: http.StatusForbidden, msg: "Forbidden"}
	}

	return denial{}
}

func hasAnyScope(have, allowed []string) bool {
	for _, s := range have {
		if containsStr(allowed, s) {
			return true
		}
	}

	return false
}
