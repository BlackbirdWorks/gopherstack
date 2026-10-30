package gcpapigateway

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const (
	// PathPrefix fronts the path-style data-plane URL: /_apigateway/{project}/{location}/{gateway}/{path}.
	PathPrefix       = "/_apigateway/"
	userinfoHeader   = "X-Apigateway-Api-Userinfo"
	forwardedAuth    = "X-Forwarded-Authorization"
	pathStyleSegment = 4
)

//nolint:gochecknoglobals // immutable lookup table
var hopHeaders = map[string]bool{
	"Connection": true, "Keep-Alive": true, "Proxy-Authenticate": true, "Proxy-Authorization": true,
	"Te": true, "Trailer": true, "Transfer-Encoding": true, "Upgrade": true, "Proxy-Connection": true,
}

// served is a gateway resolved for one data-plane request.
type served struct {
	spec    *compiledSpec
	metrics map[string]bool
	limits  map[string]quotaLimit
	project string
	managed string
	path    string
}

// MatchesDataPlane reports whether a request addresses a gateway, by default hostname or by path prefix.
func (b *InMemoryBackend) MatchesDataPlane(r *http.Request) bool {
	if strings.HasPrefix(r.URL.Path, PathPrefix) {
		return true
	}

	host := hostOnly(r.Host)
	if !strings.HasSuffix(host, hostSuffix) {
		return false
	}

	b.mu.RLock("MatchesDataPlane")
	defer b.mu.RUnlock()

	for _, g := range b.gateways {
		if g.pb.GetDefaultHostname() == host {
			return true
		}
	}

	return false
}

func (b *InMemoryBackend) gatewayForLocked(r *http.Request) (*gatewayRec, string) {
	if rest, ok := strings.CutPrefix(r.URL.Path, PathPrefix); ok {
		parts := strings.SplitN(rest, "/", pathStyleSegment)
		if len(parts) < pathStyleSegment-1 {
			return nil, ""
		}

		p := ""
		if len(parts) == pathStyleSegment {
			p = parts[3]
		}

		return b.gateways[childName(parts[0], parts[1], kindGateways, parts[2])], "/" + p
	}

	host := hostOnly(r.Host)

	for _, g := range b.gateways {
		if g.pb.GetDefaultHostname() == host {
			return g, r.URL.Path
		}
	}

	return nil, ""
}

func (b *InMemoryBackend) resolve(r *http.Request) (*served, *authError) {
	b.mu.RLock("resolve")
	defer b.mu.RUnlock()

	g, p := b.gatewayForLocked(r)
	if g == nil || g.life.gone(b.now()) || g.life.deleting() {
		return nil, &authError{status: http.StatusNotFound, msg: "Gateway not found."}
	}

	if g.life.state(b.now(), stateActive) == stateCreating {
		return nil, &authError{status: http.StatusServiceUnavailable, msg: "Gateway is not ready."}
	}

	c, ok := b.liveConfigLocked(g.pb.GetApiConfig())
	if !ok || c.spec == nil {
		return nil, &authError{status: http.StatusServiceUnavailable, msg: "Gateway API config is not available."}
	}

	project, _, _ := parseGatewayName(g.pb.GetName())
	managed := ""

	if api, found := b.apis[apiOfConfig(c.pb.GetName())]; found {
		managed = api.pb.GetManagedService()
	}

	sv := &served{
		spec: c.spec, project: project, managed: managed, path: p, metrics: c.spec.Metrics, limits: c.spec.Limits,
	}
	if latest := b.latestSpecLocked(apiOfConfig(c.pb.GetName())); latest != nil {
		sv.metrics, sv.limits = latest.Metrics, latest.Limits
	}

	return sv, nil
}

// latestSpecLocked returns the spec of the API's newest usable config: metrics and quota apply to the whole API,
// and the most recently declared definition governs.
func (b *InMemoryBackend) latestSpecLocked(api string) *compiledSpec {
	var (
		best *compiledSpec
		at   time.Time
	)

	for name, c := range b.configs {
		if c.spec == nil || apiOfConfig(name) != api || c.life.gone(b.now()) || c.life.deleting() {
			continue
		}

		if t := c.pb.GetCreateTime().AsTime(); best == nil || t.After(at) {
			best, at = c.spec, t
		}
	}

	return best
}

func apiOfConfig(config string) string {
	api, _, _ := strings.Cut(config, "/"+kindConfigs+"/")

	return api
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"code": status, "message": msg})
}

// DataPlane serves requests to gateways.
func (b *InMemoryBackend) DataPlane() http.Handler { return http.HandlerFunc(b.serveGateway) }

func (b *InMemoryBackend) serveGateway(w http.ResponseWriter, r *http.Request) {
	sv, aerr := b.resolve(r)
	if aerr != nil {
		writeErr(w, aerr.status, aerr.msg)

		return
	}

	rt, vars, other := sv.spec.match(r.Method, sv.path)
	if rt == nil {
		if other != "" {
			writeErr(w, http.StatusMethodNotAllowed, fmt.Sprintf(
				"The current request is matched to the defined url template \"%s\" but its http method is not allowed",
				other))

			return
		}

		writeErr(w, http.StatusNotFound, "The current request is not defined by this API.")

		return
	}

	claims, aerr := b.authorize(r, sv, rt)
	if aerr != nil {
		writeErr(w, aerr.status, aerr.msg)

		return
	}

	d, missing := b.consume(sv, rt)
	if missing {
		writeErr(w, http.StatusInternalServerError, "Failed to call Service Control Quota.")

		return
	}

	if d != nil {
		writeErr(w, http.StatusTooManyRequests, fmt.Sprintf(
			"RESOURCE_EXHAUSTED: Quota exceeded for quota metric '%s' and limit '%s' of service '%s' for consumer "+
				"'project:%s'.", d.metric, d.limit, sv.managed, sv.project))

		return
	}

	b.forward(w, r, sv, rt, vars, claims)
}

func backendURL(be *backendCfg, reqPath, rawQuery string, vars map[string]string) (*url.URL, error) {
	u, err := url.Parse(be.Address)
	if err != nil {
		return nil, err
	}

	q := url.Values{}
	maps.Copy(q, u.Query())

	if be.Translation == translateAppend {
		u.Path = strings.TrimRight(u.Path, "/") + reqPath
	} else {
		for _, k := range sortedVars(vars) {
			q.Add(k, vars[k])
		}
	}

	for k, vs := range mustQuery(rawQuery) {
		q[k] = append(q[k], vs...)
	}

	u.RawPath, u.RawQuery = "", q.Encode()

	return u, nil
}

func sortedVars(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	slices.Sort(keys)

	return keys
}

func mustQuery(raw string) url.Values {
	v, _ := url.ParseQuery(raw)

	return v
}

func (b *InMemoryBackend) forward(
	w http.ResponseWriter, r *http.Request, sv *served, rt *route, vars map[string]string, claims jwt.MapClaims,
) {
	be := rt.Backend

	u, err := backendURL(be, sv.path, r.URL.RawQuery, vars)
	if err != nil {
		writeErr(w, http.StatusBadGateway, "Invalid backend address.")

		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), be.Deadline)
	defer cancel()

	out, err := http.NewRequestWithContext(ctx, r.Method, u.String(), r.Body)
	if err != nil {
		writeErr(w, http.StatusBadGateway, "Invalid backend request.")

		return
	}

	out.ContentLength = r.ContentLength
	copyHeaders(out.Header, r.Header)
	out.Header.Del(userinfoHeader)

	if !be.DisableAuth {
		if a := r.Header.Get("Authorization"); a != "" {
			out.Header.Set(forwardedAuth, a)
		}

		out.Header.Del("Authorization")
	}

	if claims != nil {
		if raw, mErr := json.Marshal(claims); mErr == nil {
			out.Header.Set(userinfoHeader, base64.RawURLEncoding.EncodeToString(raw))
		}
	}

	if host, _, sErr := net.SplitHostPort(r.RemoteAddr); sErr == nil {
		out.Header.Add("X-Forwarded-For", host)
	}

	resp, err := b.client.Do(out) //nolint:gosec // G704 loopback-only by default; guarded dialer
	if err != nil {
		writeBackendErr(ctx, w, err)

		return
	}

	defer resp.Body.Close()

	for k, vs := range resp.Header {
		if !hopHeaders[http.CanonicalHeaderKey(k)] {
			w.Header()[k] = vs
		}
	}

	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func copyHeaders(dst, src http.Header) {
	for k, vs := range src {
		if hopHeaders[k] || k == "Host" || k == "Content-Length" {
			continue
		}

		dst[k] = slices.Clone(vs)
	}
}

func writeBackendErr(ctx context.Context, w http.ResponseWriter, err error) {
	switch {
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		writeErr(w, http.StatusGatewayTimeout, "upstream request timeout")
	case errors.Is(err, ErrDestinationNotPermitted):
		writeErr(w, http.StatusBadGateway, "backend destination is not permitted")
	default:
		writeErr(w, http.StatusServiceUnavailable, "upstream connect error or disconnect/reset before headers")
	}
}
