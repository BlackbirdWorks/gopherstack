package azurecontainerregistry

import (
	"encoding/base64"
	"net/http"
	"strings"
)

const grantRefresh = "refresh_token"

type oauthError struct {
	Error       string `json:"error"`
	Description string `json:"error_description"`
}

func writeOAuthError(w http.ResponseWriter, status int, code, desc string) {
	writeJSON(w, status, oauthError{Error: code, Description: desc})
}

func (h *Handler) serveOAuth(w http.ResponseWriter, r *http.Request) {
	reg, ok := h.resolve(r)
	if !ok {
		h.unknownRegistry(w)

		return
	}

	switch strings.TrimPrefix(r.URL.Path, prefixOAuth) {
	case "exchange":
		if r.Method == http.MethodPost {
			h.exchange(w, r, reg)

			return
		}
	case "token":
		switch r.Method {
		case http.MethodGet:
			h.tokenGet(w, r, reg)

			return
		case http.MethodPost:
			h.tokenPost(w, r, reg)

			return
		}
	}

	writeOAuthError(w, http.StatusNotFound, "invalid_request", "Unknown OAuth2 endpoint or method.")
}

// exchange trades a Microsoft Entra access token (any non-empty one, in emulator mode) or a refresh token for
// a registry refresh token, as POST /oauth2/exchange does for az acr login.
func (h *Handler) exchange(w http.ResponseWriter, r *http.Request, reg string) {
	form, ok := parseForm(w, r)
	if !ok {
		return
	}

	switch form.Get("grant_type") {
	case "access_token", "access_token_refresh_token":
		if form.Get("access_token") == "" {
			writeOAuthError(w, http.StatusBadRequest, "invalid_request", "access_token is required.")

			return
		}
	case grantRefresh:
		if !h.Backend.VerifyRefresh(form.Get("refresh_token"), reg) {
			writeOAuthError(w, http.StatusUnauthorized, "invalid_grant", "The refresh token is invalid or expired.")

			return
		}
	default:
		writeOAuthError(w, http.StatusBadRequest, "unsupported_grant_type", "Unsupported grant_type.")

		return
	}

	audience := form.Get("service")
	if audience == "" {
		audience = r.Host
	}

	tok, err := h.Backend.IssueRefreshToken(reg, audience)
	if err != nil {
		writeOAuthError(w, http.StatusInternalServerError, "server_error", "Could not sign the token.")

		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"refresh_token": tok})
}

// tokenPost trades a refresh token for an access token scoped to the requested repositories.
func (h *Handler) tokenPost(w http.ResponseWriter, r *http.Request, reg string) {
	form, ok := parseForm(w, r)
	if !ok {
		return
	}

	if form.Get("grant_type") != grantRefresh {
		writeOAuthError(w, http.StatusBadRequest, "unsupported_grant_type", "grant_type must be refresh_token.")

		return
	}

	if !h.Backend.VerifyRefresh(form.Get("refresh_token"), reg) {
		writeOAuthError(w, http.StatusUnauthorized, "invalid_grant", "The refresh token is invalid or expired.")

		return
	}

	h.issueAccess(w, r, reg, form.Get("service"), form["scope"])
}

// tokenGet is the Docker token endpoint: Basic credentials in, access token out.
func (h *Handler) tokenGet(w http.ResponseWriter, r *http.Request, reg string) {
	if !h.basicValid(r, reg) {
		challengeToken(w)

		return
	}

	h.issueAccess(w, r, reg, r.URL.Query().Get("service"), r.URL.Query()["scope"])
}

func (h *Handler) basicValid(r *http.Request, reg string) bool {
	auth := r.Header.Get("Authorization")
	if !hasFold(auth, "Basic ") {
		return false
	}

	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(auth[len("Basic "):]))
	if err != nil {
		return false
	}

	user, pass, ok := strings.Cut(string(raw), ":")

	return ok && (h.Backend.VerifyAdmin(reg, user, pass) || h.Backend.VerifyRefresh(pass, reg))
}

func challengeToken(w http.ResponseWriter) {
	w.Header().Set("WWW-Authenticate", `Basic realm="Azure Container Registry"`)
	writeRegistryError(w, http.StatusUnauthorized, "UNAUTHORIZED", "Invalid credentials.")
}

func (h *Handler) issueAccess(w http.ResponseWriter, r *http.Request, reg, service string, scopes []string) {
	if service == "" {
		service = r.Host
	}

	tok, err := h.Backend.IssueAccessToken(reg, service, scopes)
	if err != nil {
		writeOAuthError(w, http.StatusInternalServerError, "server_error", "Could not sign the token.")

		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"access_token": tok, "token": tok})
}
