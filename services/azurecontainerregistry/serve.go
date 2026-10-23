package azurecontainerregistry

import (
	"encoding/base64"
	"encoding/json"
	"net"
	"net/http"
	"net/url"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/ociregistry"
)

const (
	prefixV2     = "/v2"
	prefixOAuth  = "/oauth2/"
	prefixACR    = "/acr/v1/"
	maxFormBytes = 1 << 20
)

// ServeHTTP routes a request to the registry, token or metadata API.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Server", "AzureContainerRegistry")

	path := r.URL.Path

	switch {
	case path == prefixV2 || strings.HasPrefix(path, prefixV2+"/"):
		h.serveRegistry(w, r)
	case strings.HasPrefix(path, prefixOAuth):
		h.serveOAuth(w, r)
	case strings.HasPrefix(path, prefixACR):
		h.serveACR(w, r)
	default:
		writeRegistryError(w, http.StatusNotFound, "NOT_FOUND", "The requested resource was not found.")
	}
}

type errorBody struct {
	Errors []errorEntry `json:"errors"`
}

type errorEntry struct {
	Detail  any    `json:"detail,omitempty"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	raw, err := json.Marshal(v)
	if err != nil {
		status, raw = http.StatusInternalServerError, []byte(`{"errors":[{"code":"UNKNOWN","message":"encode"}]}`)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(raw)
}

func writeRegistryError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, errorBody{Errors: []errorEntry{{Code: code, Message: msg}}})
}

func hostOnly(host string) string {
	if h, _, err := net.SplitHostPort(host); err == nil {
		return h
	}

	return host
}

// resolve picks the registry a request addresses: the host's first label, else the only registry.
func (h *Handler) resolve(r *http.Request) (string, bool) {
	host := hostOnly(r.Host)

	if net.ParseIP(host) == nil {
		if label, _, ok := strings.Cut(host, "."); ok {
			if _, found := h.Backend.Registry(label); found {
				return label, true
			}
		}
	}

	if names := h.Backend.Names(); len(names) == 1 {
		return names[0], true
	}

	return "", false
}

func (h *Handler) unknownRegistry(w http.ResponseWriter) {
	writeRegistryError(w, http.StatusNotFound, "NAME_UNKNOWN", "The registry for this host does not exist.")
}

func scheme(r *http.Request) string {
	if r.TLS != nil {
		return "https"
	}

	if p := r.Header.Get("X-Forwarded-Proto"); p == "https" {
		return p
	}

	return "http"
}

// authenticate accepts the registry's admin credentials, a refresh token as the Basic password, or a
// Bearer access token.
func (h *Handler) authenticate(r *http.Request, reg string) bool {
	auth := r.Header.Get("Authorization")
	b := h.Backend

	switch {
	case hasFold(auth, "Basic "):
		raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(auth[len("Basic "):]))
		if err != nil {
			return false
		}

		user, pass, ok := strings.Cut(string(raw), ":")

		return ok && (b.VerifyAdmin(reg, user, pass) || b.VerifyRefresh(pass, reg) || b.VerifyAccess(pass, reg))
	case hasFold(auth, "Bearer "):
		return b.VerifyAccess(strings.TrimSpace(auth[len("Bearer "):]), reg)
	}

	return false
}

func hasFold(s, prefix string) bool {
	return len(s) >= len(prefix) && strings.EqualFold(s[:len(prefix)], prefix)
}

func challenge(w http.ResponseWriter, r *http.Request, scope string) {
	realm := scheme(r) + "://" + r.Host + "/oauth2/token"
	value := `Bearer realm="` + realm + `",service="` + r.Host + `"`

	if scope != "" {
		value += `,scope="` + scope + `"`
	}

	w.Header().Set("WWW-Authenticate", value)
	w.Header().Set("Docker-Distribution-Api-Version", "registry/2.0")
	writeJSON(w, http.StatusUnauthorized, errorBody{Errors: []errorEntry{{
		Code:    "UNAUTHORIZED",
		Message: "authentication required, visit https://aka.ms/acr/authorization for more information.",
	}}})
}

func v2Scope(r *http.Request) string {
	rest := strings.TrimPrefix(r.URL.Path, prefixV2+"/")
	if rest == "_catalog" {
		return "registry:catalog:*"
	}

	for _, kind := range []string{"/manifests/", "/blobs/", "/tags/"} {
		if i := strings.Index(rest, kind); i > 0 {
			return "repository:" + rest[:i] + ":" + actionsFor(r.Method)
		}
	}

	return ""
}

func actionsFor(method string) string {
	switch method {
	case http.MethodGet, http.MethodHead:
		return "pull"
	case http.MethodDelete:
		return "delete"
	}

	return "pull,push"
}

func (h *Handler) serveRegistry(w http.ResponseWriter, r *http.Request) {
	reg, ok := h.resolve(r)
	if !ok {
		h.unknownRegistry(w)

		return
	}

	if !h.authenticate(r, reg) {
		challenge(w, r, v2Scope(r))

		return
	}

	app, ok := h.Backend.app(reg)
	if !ok {
		h.unknownRegistry(w)

		return
	}

	ociregistry.ServeObserved(app, w, r, ociregistry.Observer{
		Permit: func(w http.ResponseWriter, repo, ref, op string) bool {
			if h.Backend.Allowed(reg, repo, ref, op) {
				return true
			}

			writeRegistryError(w, http.StatusForbidden, "DENIED", "The operation is disabled.")

			return false
		},
		Put:    func(repo, ref, digest string) { h.Backend.RecordManifestPut(reg, repo, ref, digest) },
		Delete: func(repo, ref string) { h.Backend.RecordManifestDelete(reg, repo, ref) },
	})
}

func parseForm(w http.ResponseWriter, r *http.Request) (url.Values, bool) {
	r.Body = http.MaxBytesReader(w, r.Body, maxFormBytes)

	if err := r.ParseForm(); err != nil {
		writeOAuthError(w, http.StatusBadRequest, "invalid_request", "The request body is not a valid form.")

		return nil, false
	}

	return r.Form, true
}

func decodeBody(w http.ResponseWriter, r *http.Request, out any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxFormBytes)

	if err := json.NewDecoder(r.Body).Decode(out); err != nil {
		writeRegistryError(w, http.StatusBadRequest, "INVALID_REQUEST", "The request body is not valid JSON.")

		return false
	}

	return true
}
