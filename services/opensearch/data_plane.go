package opensearch

import (
	"context"
	"crypto/pbkdf2"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"net/http"
	"net/http/httputil"
	"net/url"
	"path"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
)

const pbkdf2Iterations = 100_000

type dataPlaneCtxKey struct{}

// dataPlaneTarget is a request addressed to a domain's OpenSearch endpoint.
type dataPlaneTarget struct {
	domain string
	// path is the request path on the domain endpoint itself (e.g. /idx/_search).
	path string
}

// legacyPasswordMatches verifies the "<salt>$<pbkdf2>" hash format written by earlier snapshots.
func legacyPasswordMatches(stored, password string) bool {
	saltHex, keyHex, ok := strings.Cut(stored, "$")
	if !ok {
		return false
	}

	salt, saltErr := hex.DecodeString(saltHex)
	want, keyErr := hex.DecodeString(keyHex)

	if saltErr != nil || keyErr != nil {
		return false
	}

	got, err := pbkdf2.Key(sha256.New, password, salt, pbkdf2Iterations, len(want))
	if err != nil {
		return false
	}

	return subtle.ConstantTimeCompare(got, want) == 1
}

// withDataPlaneTarget marks a strategy-addressed request as data plane, remembering its endpoint path.
func withDataPlaneTarget(r *http.Request, t dataPlaneTarget) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), dataPlaneCtxKey{}, t))
}

// dataPlaneTargetOf recognises data-plane requests: those addressed to a strategy endpoint, and the
// {domain}/index/{index}/_doc|_search|_count and {domain}/_bulk document routes.
func dataPlaneTargetOf(r *http.Request) (dataPlaneTarget, bool) {
	if t, ok := r.Context().Value(dataPlaneCtxKey{}).(dataPlaneTarget); ok {
		return t, true
	}

	rest, ok := strings.CutPrefix(r.URL.Path, openSearchPathPrefix+"/")
	if !ok {
		return dataPlaneTarget{}, false
	}

	domain, sub, _ := strings.Cut(rest, "/")
	if _, isSecurity := securityAPISubpath(sub); isSecurity {
		return dataPlaneTarget{domain: domain, path: "/" + sub}, true
	}

	if sub == segBulk {
		return dataPlaneTarget{domain: domain, path: "/_bulk"}, true
	}

	indexed, ok := strings.CutPrefix(sub, "index/")
	if !ok {
		return dataPlaneTarget{}, false
	}

	if segs := strings.Split(indexed, "/"); len(segs) >= 2 && isDocumentOp(segs[1]) {
		return dataPlaneTarget{domain: domain, path: "/" + indexed}, true
	}

	return dataPlaneTarget{}, false
}

func isDocumentOp(seg string) bool {
	switch seg {
	case indexOpDoc, indexOpSearch, indexOpCount, segBulk, "_create", "_update":
		return true
	default:
		return false
	}
}

// serveDataPlane forwards data-plane requests to a configured custom backend (whose own security plugin judges
// credentials) or enforces fine-grained access control on the built-in engine.
// It reports whether the request was fully handled.
func (h *Handler) serveDataPlane(w http.ResponseWriter, r *http.Request) bool {
	target, ok := dataPlaneTargetOf(r)
	if !ok {
		return false
	}

	dom, err := h.Backend.DescribeDomain(target.domain)
	if err != nil {
		return false
	}

	if h.customBackend != nil {
		h.proxyToCustomBackend(w, r, target.path)

		return true
	}

	if aso := dom.AdvancedSecurityOptions; aso != nil && aso.Enabled {
		h.enforceFGAC(w, r, target, dom)

		return true
	}

	return false
}

// proxyToCustomBackend forwards to the configured URL only: scheme and host never come from the request.
func (h *Handler) proxyToCustomBackend(w http.ResponseWriter, r *http.Request, endpointPath string) {
	target := h.customBackend
	clean := path.Clean("/" + endpointPath)

	proxy := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.Out.URL.Scheme = target.Scheme
			pr.Out.URL.Host = target.Host
			pr.Out.URL.Path = path.Join("/", target.Path, clean)
			pr.Out.URL.RawPath = ""
			pr.Out.Host = target.Host
		},
		Transport: h.proxyTransport,
		ErrorHandler: func(w http.ResponseWriter, req *http.Request, err error) {
			msg := "custom OpenSearch backend unreachable: " + err.Error()
			h.writeError(req, w, http.StatusBadGateway, "BadGateway", msg)
		},
	}

	proxy.ServeHTTP(w, r)
}

// setCustomBackend configures the proxy target and transport.
func (h *Handler) setCustomBackend(u *url.URL) {
	h.customBackend = u
	if u != nil {
		h.proxyTransport = httpclient.NewTransport()
	}
}
