package ociartifacts

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/ociregistry"
)

const (
	apiVersionHeader = "Docker-Distribution-Api-Version"
	apiVersion       = "registry/2.0"
)

type regErrorBody struct {
	Errors []regErrorEntry `json:"errors"`
}

type regErrorEntry struct {
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

func writeRegError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, regErrorBody{Errors: []regErrorEntry{{Code: code, Message: msg}}})
}

// principal is who a registry request is made by.
type principal struct {
	user   string
	claims tokenClaims
	scoped bool
	anon   bool
}

// authenticate accepts a Bearer token from the token endpoint or Basic credentials (<namespace>/<user>:<token>).
func (h *Handler) authenticate(r *http.Request) (principal, bool) {
	auth := r.Header.Get("Authorization")

	switch {
	case hasFold(auth, "Bearer "):
		c, ok := h.Backend.ParseToken(strings.TrimSpace(auth[len("Bearer "):]))
		if !ok {
			return principal{}, false
		}

		return principal{claims: c, user: c.Sub, scoped: true, anon: c.Anon}, true
	case hasFold(auth, "Basic "):
		raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(auth[len("Basic "):]))
		if err != nil {
			return principal{}, false
		}

		name, pass, ok := strings.Cut(string(raw), ":")
		if !ok {
			return principal{}, false
		}

		user, valid := h.Backend.ValidLogin(name, pass)

		return principal{user: user}, valid
	}

	return principal{}, false
}

func hasFold(s, prefix string) bool {
	return len(s) >= len(prefix) && strings.EqualFold(s[:len(prefix)], prefix)
}

func scheme(r *http.Request) string {
	if r.TLS != nil || r.Header.Get("X-Forwarded-Proto") == "https" {
		return "https"
	}

	return "http"
}

func challenge(w http.ResponseWriter, r *http.Request, scope, errCode string) {
	value := `Bearer realm="` + scheme(r) + `://` + r.Host + tokenPath + `",service="` + r.Host + `"`

	if scope != "" {
		value += `,scope="` + scope + `"`
	}

	if errCode != "" {
		value += `,error="` + errCode + `"`
	}

	w.Header().Set("WWW-Authenticate", value)
	w.Header().Set(apiVersionHeader, apiVersion)
	writeRegError(w, http.StatusUnauthorized, "UNAUTHORIZED", "authentication required")
}

// need is one access a registry request requires.
type need struct {
	name   string
	action string
}

func (n need) scope() string {
	hint := n.action
	if n.action == actionPush {
		hint = "pull,push"
	}

	return Scope{Name: n.name, Actions: strings.Split(hint, ",")}.String()
}

func actionFor(method string) string {
	switch method {
	case http.MethodGet, http.MethodHead:
		return actionPull
	case http.MethodDelete:
		return actionDel
	}

	return actionPush
}

// splitRegistryPath returns the repository name of /v2/<name>/{manifests,blobs,tags}/...
func splitRegistryPath(path string) (string, bool) {
	rest := strings.TrimPrefix(path, registryPrefix+"/")

	for _, kind := range []string{"manifests", "blobs", "tags"} {
		if i := strings.Index(rest, "/"+kind+"/"); i > 0 {
			return rest[:i], true
		}
	}

	return "", false
}

func (h *Handler) serveRegistry(w http.ResponseWriter, r *http.Request) {
	w.Header().Set(apiVersionHeader, apiVersion)

	name, isRepo := splitRegistryPath(r.URL.Path)
	needs := []need(nil)

	if isRepo {
		needs = append(needs, need{name: name, action: actionFor(r.Method)})

		if from := r.URL.Query().Get("from"); r.Method == http.MethodPost && from != "" {
			needs = append(needs, need{name: from, action: actionPull})
		}
	}

	who, ok := h.authenticate(r)
	if !ok {
		challenge(w, r, joinScopes(needs), "")

		return
	}

	if !isRepo {
		h.servePing(w, r)

		return
	}

	h.serveRepoRequest(w, r, who, needs)
}

func joinScopes(needs []need) string {
	parts := make([]string, 0, len(needs))
	for _, n := range needs {
		parts = append(parts, n.scope())
	}

	return strings.Join(parts, " ")
}

func (h *Handler) servePing(w http.ResponseWriter, r *http.Request) {
	if p := strings.TrimSuffix(r.URL.Path, "/"); p == registryPrefix {
		writeJSON(w, http.StatusOK, struct{}{})

		return
	}

	writeRegError(w, http.StatusNotFound, "UNSUPPORTED", "The requested registry API is not supported")
}

func (h *Handler) serveRepoRequest(
	w http.ResponseWriter, r *http.Request, who principal, needs []need,
) {
	display, inNS := h.Backend.SplitRegistryName(needs[0].name)
	if !inNS {
		writeRegError(w, http.StatusNotFound, "NAME_UNKNOWN", "repository name must start with the tenancy namespace")

		return
	}

	acc, exists := h.Backend.RepoAccess(display)
	action := needs[0].action

	if who.anon && (action != actionPull || !exists || !acc.Public) {
		challenge(w, r, joinScopes(needs), "")

		return
	}

	for _, n := range needs {
		if who.scoped && !who.claims.allows(n.name, n.action) {
			challenge(w, r, joinScopes(needs), "insufficient_scope")

			return
		}
	}

	if !exists && !h.admit(w, display, action, who) {
		return
	}

	app := h.Backend.registry()
	ociregistry.ServeObserved(app, w, r, h.observer(app, r, display, who.user))
}

// admit handles a request for a repository that does not exist yet: a push creates it when the container
// configuration allows, anything else is unknown.
func (h *Handler) admit(w http.ResponseWriter, display, action string, who principal) bool {
	if action != actionPush {
		writeRegError(w, http.StatusNotFound, "NAME_UNKNOWN", "repository name not known to registry")

		return false
	}

	if ae := h.Backend.EnsureRepo(display, who.user); ae != nil {
		if ae.Code == "InvalidParameter" {
			writeRegError(w, http.StatusBadRequest, "NAME_INVALID", ae.Message)
		} else {
			writeRegError(w, ae.Status, ae.Code, ae.Message)
		}

		return false
	}

	return true
}

func manifestDigest(r *http.Request) string {
	if r.Method != http.MethodPut || r.Body == nil {
		return ""
	}

	raw, err := io.ReadAll(io.LimitReader(r.Body, maxManifest+1))
	if err != nil || len(raw) > maxManifest {
		r.Body = io.NopCloser(bytes.NewReader(raw))

		return ""
	}

	r.Body = io.NopCloser(bytes.NewReader(raw))
	sum := sha256.Sum256(raw)

	return "sha256:" + hex.EncodeToString(sum[:])
}

func (h *Handler) observer(app http.Handler, r *http.Request, display, user string) ociregistry.Observer {
	regName := func(repo string) string {
		d, _ := h.Backend.SplitRegistryName(repo)

		return d
	}

	return ociregistry.Observer{
		Permit: func(w http.ResponseWriter, _, ref, op string) bool {
			digest := ""
			if op == ociregistry.OpWrite && !ociregistry.IsDigest(ref) {
				digest = manifestDigest(r)
			}

			switch h.Backend.PermitManifest(display, ref, op, digest, r.Method == http.MethodGet) {
			case permitUnknown:
				writeRegError(w, http.StatusNotFound, "MANIFEST_UNKNOWN", "manifest unknown")

				return false
			case permitTagExists:
				writeRegError(w, http.StatusConflict, "TAG_INVALID",
					"The tag already exists in an immutable repository and cannot be overwritten")

				return false
			}

			return true
		},
		Put: func(repo, ref, digest string) {
			var body []byte

			if m, err := ociregistry.NewClient(app).GetManifest(r.Context(), repo, digest); err == nil {
				body = m.Body
			}

			h.Backend.RecordPush(regName(repo), ref, digest, user, body)
		},
		Delete: func(repo, ref string) { h.Backend.RecordDelete(regName(repo), ref) },
	}
}

type tokenResponse struct {
	Token       string `json:"token"`
	AccessToken string `json:"access_token"`
	Scope       string `json:"scope"`
	ExpiresIn   int    `json:"expires_in"`
}

// serveToken is the Container Registry token endpoint (GetAccessToken, /20180419/docker/token): Basic
// credentials get a token for the scopes asked for, a request without credentials gets an anonymous pull token.
func (h *Handler) serveToken(w http.ResponseWriter, r *http.Request) {
	user, anon := "", true

	if auth := r.Header.Get("Authorization"); auth != "" {
		u, ok := h.basicUser(auth)
		if !ok {
			w.Header().Set("WWW-Authenticate", `Basic realm="`+r.Host+`"`)
			writeRegError(w, http.StatusUnauthorized, "UNAUTHORIZED", "authentication required")

			return
		}

		user, anon = u, false
	}

	var scopes []Scope

	for _, raw := range r.URL.Query()["scope"] {
		for field := range strings.FieldsSeq(raw) {
			if s, ok := ParseScope(field); ok {
				scopes = append(scopes, s)
			}
		}
	}

	token, granted, ttl := h.Backend.IssueToken(user, anon, scopes)
	parts := make([]string, 0, len(granted))

	for _, s := range granted {
		parts = append(parts, s.String())
	}

	writeJSON(w, http.StatusOK, tokenResponse{
		Token: token, AccessToken: token, Scope: strings.Join(parts, " "), ExpiresIn: ttl,
	})
}

func (h *Handler) basicUser(auth string) (string, bool) {
	if !hasFold(auth, "Basic ") {
		return "", false
	}

	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(auth[len("Basic "):]))
	if err != nil {
		return "", false
	}

	name, pass, ok := strings.Cut(string(raw), ":")
	if !ok {
		return "", false
	}

	return h.Backend.ValidLogin(name, pass)
}
