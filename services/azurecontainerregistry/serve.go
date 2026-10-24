package azurecontainerregistry

import (
	"encoding/base64"
	"encoding/json"
	"net"
	"net/http"
	"net/url"
	"slices"
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

// principal is an authenticated caller; scoped callers are limited to the access their token records.
type principal struct {
	access []tokenAccess
	scoped bool
}

// authenticate accepts the registry's admin credentials, a refresh token as the Basic password, or an access
// token as the Bearer token (or Basic password).
func (h *Handler) authenticate(r *http.Request, reg string) (principal, bool) {
	auth := r.Header.Get("Authorization")
	b := h.Backend

	switch {
	case hasFold(auth, "Basic "):
		raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(auth[len("Basic "):]))
		if err != nil {
			return principal{}, false
		}

		user, pass, ok := strings.Cut(string(raw), ":")
		if !ok {
			return principal{}, false
		}

		if b.VerifyAdmin(reg, user, pass) || b.VerifyRefresh(pass, reg) {
			return principal{}, true
		}

		access, valid := b.accessScopes(pass, reg)

		return principal{access: access, scoped: valid}, valid
	case hasFold(auth, "Bearer "):
		access, valid := b.accessScopes(strings.TrimSpace(auth[len("Bearer "):]), reg)

		return principal{access: access, scoped: valid}, valid
	}

	return principal{}, false
}

// need is one access a request requires: action on the named resource.
type need struct {
	typ    string
	name   string
	action string
	hint   string
}

func (n need) scope() string { return n.typ + ":" + n.name + ":" + n.hint }

func (p principal) allows(n need) bool {
	if !p.scoped {
		return true
	}

	return slices.ContainsFunc(p.access, func(a tokenAccess) bool {
		return a.Type == n.typ && a.Name == n.name &&
			(slices.Contains(a.Actions, n.action) || slices.Contains(a.Actions, "*"))
	})
}

func hasFold(s, prefix string) bool {
	return len(s) >= len(prefix) && strings.EqualFold(s[:len(prefix)], prefix)
}

const unauthorizedMessage = "authentication required, visit https://aka.ms/acr/authorization for more information."

func setChallengeHeader(w http.ResponseWriter, r *http.Request, scope, errCode string) {
	realm := scheme(r) + "://" + r.Host + "/oauth2/token"
	value := `Bearer realm="` + realm + `",service="` + r.Host + `"`

	if scope != "" {
		value += `,scope="` + scope + `"`
	}

	if errCode != "" {
		value += `,error="` + errCode + `"`
	}

	w.Header().Set("WWW-Authenticate", value)
	w.Header().Set("Docker-Distribution-Api-Version", "registry/2.0")
}

func challenge(w http.ResponseWriter, r *http.Request, scope string) {
	setChallengeHeader(w, r, scope, "")
	writeRegistryError(w, http.StatusUnauthorized, "UNAUTHORIZED", unauthorizedMessage)
}

func v2Scope(r *http.Request) string {
	needs := v2Needs(r)
	scopes := make([]string, 0, len(needs))

	for _, n := range needs {
		scopes = append(scopes, n.scope())
	}

	return strings.Join(scopes, " ")
}

// v2Needs lists the accesses a registry request requires, per the distribution token scope rules.
func v2Needs(r *http.Request) []need {
	rest := strings.TrimPrefix(r.URL.Path, prefixV2+"/")
	if rest == "_catalog" {
		return []need{{typ: scopeCatalog, name: "catalog", action: "*", hint: "*"}}
	}

	for _, kind := range []string{"/manifests/", "/blobs/", "/tags/"} {
		i := strings.Index(rest, kind)
		if i <= 0 {
			continue
		}

		action, hint := actionsFor(r.Method)
		out := []need{{typ: "repository", name: rest[:i], action: action, hint: hint}}

		if from := r.URL.Query().Get("from"); r.Method == http.MethodPost && from != "" {
			out = append(out, need{typ: "repository", name: from, action: "pull", hint: "pull"})
		}

		return out
	}

	return nil
}

func actionsFor(method string) (string, string) {
	switch method {
	case http.MethodGet, http.MethodHead:
		return "pull", "pull"
	case http.MethodDelete:
		return "delete", "delete"
	}

	return "push", "pull,push"
}

func denyScope(w http.ResponseWriter, r *http.Request, needs []need) {
	detail := make([]tokenAccess, 0, len(needs))
	for _, n := range needs {
		detail = append(detail, tokenAccess{Type: n.typ, Name: n.name, Actions: strings.Split(n.hint, ",")})
	}

	setChallengeHeader(w, r, v2Scope(r), "insufficient_scope")
	writeJSON(w, http.StatusUnauthorized, errorBody{Errors: []errorEntry{{
		Code: "UNAUTHORIZED", Message: unauthorizedMessage, Detail: detail,
	}}})
}

func (h *Handler) serveRegistry(w http.ResponseWriter, r *http.Request) {
	reg, ok := h.resolve(r)
	if !ok {
		h.unknownRegistry(w)

		return
	}

	who, ok := h.authenticate(r, reg)
	if !ok {
		challenge(w, r, v2Scope(r))

		return
	}

	if needs := v2Needs(r); !who.allowsAll(needs) {
		denyScope(w, r, needs)

		return
	}

	app, ok := h.Backend.app(reg)
	if !ok {
		h.unknownRegistry(w)

		return
	}

	ociregistry.ServeObserved(app, w, r, h.observer(reg, app, r))
}

func (p principal) allowsAll(needs []need) bool {
	return slices.IndexFunc(needs, func(n need) bool { return !p.allows(n) }) < 0
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
