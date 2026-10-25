package opensearch

import (
	"context"
	"crypto/pbkdf2"
	"crypto/rand"
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

const (
	pbkdf2Iterations = 100_000
	pbkdf2KeyLen     = 32
	pbkdf2SaltLen    = 16
)

type dataPlaneCtxKey struct{}

// dataPlaneTarget is a request addressed to a domain's OpenSearch endpoint.
type dataPlaneTarget struct {
	domain string
	// path is the request path on the domain endpoint itself (e.g. /idx/_search).
	path string
}

// hashMasterPassword returns "<salt>$<pbkdf2>" in hex; the password itself is never stored.
func hashMasterPassword(password string) string {
	salt := make([]byte, pbkdf2SaltLen)
	_, _ = rand.Read(salt)

	key, err := pbkdf2.Key(sha256.New, password, salt, pbkdf2Iterations, pbkdf2KeyLen)
	if err != nil {
		return ""
	}

	return hex.EncodeToString(salt) + "$" + hex.EncodeToString(key)
}

func masterPasswordMatches(stored, password string) bool {
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
	segs := strings.Split(sub, "/")

	switch {
	case sub == "_bulk":
		return dataPlaneTarget{domain: domain, path: "/_bulk"}, true
	case len(segs) >= 3 && segs[0] == "index" && isDocumentOp(segs[2]):
		return dataPlaneTarget{domain: domain, path: "/" + strings.Join(segs[1:], "/")}, true
	default:
		return dataPlaneTarget{}, false
	}
}

func isDocumentOp(seg string) bool {
	switch seg {
	case indexOpDoc, indexOpSearch, indexOpCount, "_bulk", "_create", "_update":
		return true
	default:
		return false
	}
}

// serveDataPlane enforces fine-grained access control and the custom backend for data-plane requests.
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

	aso := dom.AdvancedSecurityOptions
	if aso == nil || !aso.Enabled || !aso.InternalUserDatabaseEnabled || aso.MasterUserName == "" {
		return false
	}

	user, pass, hasBasic := r.BasicAuth()
	if hasBasic && subtle.ConstantTimeCompare([]byte(user), []byte(aso.MasterUserName)) == 1 &&
		masterPasswordMatches(aso.MasterUserPasswordHash, pass) {
		return false
	}

	w.Header().Set("WWW-Authenticate", `Basic realm="OpenSearch Security"`)
	http.Error(w, "Unauthorized", http.StatusUnauthorized)

	return true
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
			h.writeError(req, w, http.StatusBadGateway, "BadGateway", "custom OpenSearch backend unreachable: "+err.Error())
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
