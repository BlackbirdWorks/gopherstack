package ociapigateway

import (
	"encoding/json"
	"io"
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

// resolveTarget maps a request to a deployment by gateway hostname (Host header) or by /_apigw/<gatewayId>/ path.
func (b *InMemoryBackend) resolveTarget(r *http.Request) (target, bool) {
	path := r.URL.Path
	gwRef, byID := "", false

	if strings.HasPrefix(path, dataPathPrefix) {
		rest := strings.TrimPrefix(path, dataPathPrefix)
		gwRef, path, _ = strings.Cut(rest, "/")
		path, byID = "/"+path, true
	}

	b.mu.RLock("resolveTarget")
	defer b.mu.RUnlock()

	var gwID string

	for _, g := range b.gateways {
		if g.State != stateActive && g.State != stateUpdating {
			continue
		}

		if (byID && g.ID == gwRef) || (!byID && strs.Equal(g.Hostname, stripPort(r.Host))) {
			gwID = g.ID

			break
		}
	}

	if gwID == "" {
		return target{}, false
	}

	var best *Deployment

	for _, d := range b.deployments {
		if d.GatewayID != gwID || (d.State != stateActive && d.State != stateUpdating) {
			continue
		}

		if prefixMatches(d.PathPrefix, path) && (best == nil || len(d.PathPrefix) > len(best.PathPrefix)) {
			best = d
		}
	}

	if best == nil || b.specs[best.ID] == nil {
		return target{}, false
	}

	rel := strings.TrimPrefix(path, strings.TrimSuffix(best.PathPrefix, "/"))
	if rel == "" {
		rel = "/"
	}

	return target{spec: b.specs[best.ID], depID: best.ID, rel: rel, gateway: gwID}, true
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

	if status, www, msg := b.authorize(r, rp, m.route, rv); status != 0 {
		if www != "" {
			w.Header().Set("WWW-Authenticate", www)
		}

		corsHeaders(w.Header(), cors, origin)
		writeGWError(w, status, msg)

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

	for k, vs := range res.header {
		w.Header()[k] = vs
	}

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

// authorize applies deployment authentication and route authorization; a zero status means the request may proceed.
func (b *InMemoryBackend) authorize(
	r *http.Request, rp *requestPolicies, route *compiledRoute, rv *reqView,
) (status int, www, msg string) {
	if rp == nil || rp.Authentication == nil {
		return 0, "", ""
	}

	var authz *struct {
		Type         string   `json:"type"`
		AllowedScope []string `json:"allowedScope"`
	}

	if route.doc.RequestPolicies != nil {
		authz = route.doc.RequestPolicies.Authorization
	}

	if authz != nil && authz.Type == "ANONYMOUS" {
		return 0, "", ""
	}

	res, anonymous := b.authenticate(r.Context(), rp.Authentication, r, rv)
	if !res.ok {
		return http.StatusUnauthorized, res.wwwAuth, "Unauthorized"
	}

	if authz != nil && authz.Type == "ANY_OF" {
		if anonymous || !hasAnyScope(res.scopes, authz.AllowedScope) {
			return http.StatusForbidden, "", "Forbidden"
		}
	}

	return 0, "", ""
}

func hasAnyScope(have, allowed []string) bool {
	for _, s := range have {
		if containsStr(allowed, s) {
			return true
		}
	}

	return false
}
