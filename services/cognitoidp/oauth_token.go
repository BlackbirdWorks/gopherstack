package cognitoidp

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"net/http"
	"net/url"
	"slices"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	maxOAuthBodyBytes = 1 << 20
	flowCode          = "code"
	flowImplicit      = "implicit"
	flowClientCreds   = "client_credentials"
	explicitClientTok = "ALLOW_CLIENT_TOKEN_AUTH"
	pkceVerifierMin   = 43
	pkceVerifierMax   = 128

	errInvalidRequest       = "invalid_request"
	errInvalidClient        = "invalid_client"
	errInvalidGrant         = "invalid_grant"
	errUnauthorizedClient   = "unauthorized_client"
	errUnsupportedGrantType = "unsupported_grant_type"
	errInvalidScope         = "invalid_scope"
	errServerError          = "server_error"
)

// oauthError is the documented token-endpoint error body: {"error": "...", "error_description": "..."}.
type oauthError struct {
	code        string
	description string
	status      int
}

func newOAuthError(code, description string) *oauthError {
	return &oauthError{code: code, description: description, status: http.StatusBadRequest}
}

func (e *oauthError) write(c *echo.Context) error {
	body := map[string]string{"error": e.code}
	if e.description != "" {
		body["error_description"] = e.description
	}

	return writeOAuthJSON(c, e.status, body)
}

func writeOAuthJSON(c *echo.Context, status int, body any) error {
	hdr := c.Response().Header()
	hdr.Set("Cache-Control", "no-store")
	hdr.Set("Pragma", "no-cache")
	hdr.Set("Access-Control-Allow-Origin", "*")

	return c.JSON(status, body)
}

func handleOAuthPreflight(c *echo.Context) error {
	hdr := c.Response().Header()
	hdr.Set("Access-Control-Allow-Origin", "*")
	hdr.Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	hdr.Set("Access-Control-Allow-Headers", "Authorization, Content-Type")

	return c.NoContent(http.StatusNoContent)
}

func clientSecrets(client *UserPoolClient) []string {
	var out []string
	if client.ClientSecret != "" {
		out = append(out, client.ClientSecret)
	}

	for _, s := range client.ExtraClientSecrets {
		out = append(out, s.ClientSecretValue)
	}

	return out
}

// secretMatches compares digests in constant time and never short-circuits across candidates.
func secretMatches(candidates []string, provided string) bool {
	want := sha256.Sum256([]byte(provided))
	matched := 0

	for _, s := range candidates {
		have := sha256.Sum256([]byte(s))
		matched |= subtle.ConstantTimeCompare(have[:], want[:])
	}

	return matched == 1
}

func readOAuthForm(c *echo.Context) (url.Values, *oauthError) {
	r := c.Request()
	r.Body = http.MaxBytesReader(c.Response(), r.Body, maxOAuthBodyBytes)

	if err := r.ParseForm(); err != nil {
		return nil, newOAuthError(errInvalidRequest, "malformed form body")
	}

	return r.PostForm, nil
}

// authenticateOAuthClient resolves the app client from client_secret_basic or client_secret_post credentials.
func (h *Handler) authenticateOAuthClient(r *http.Request, form url.Values) (*UserPoolClient, *oauthError) {
	id, secret := form.Get("client_id"), form.Get("client_secret")

	if user, pass, ok := r.BasicAuth(); ok {
		if id != "" && id != user {
			return nil, &oauthError{code: errInvalidClient, status: http.StatusBadRequest}
		}

		id, secret = user, pass
	}

	if id == "" {
		return nil, newOAuthError(errInvalidRequest, "client_id is required")
	}

	client, ok := h.Backend.oauthClient(id)
	if !ok {
		return nil, newOAuthError(errInvalidClient, "")
	}

	if secrets := clientSecrets(client); len(secrets) > 0 && !secretMatches(secrets, secret) {
		return nil, newOAuthError(errInvalidClient, "")
	}

	return client, nil
}

func clientAllowsFlow(client *UserPoolClient, flow string) bool {
	return client.AllowedOAuthFlowsUserPoolClient && slices.Contains(client.AllowedOAuthFlows, flow)
}

func (h *Handler) handleOAuthToken(c *echo.Context) error {
	form, oerr := readOAuthForm(c)
	if oerr != nil {
		return oerr.write(c)
	}

	client, oerr := h.authenticateOAuthClient(c.Request(), form)
	if oerr != nil {
		return oerr.write(c)
	}

	var resp map[string]any

	switch form.Get("grant_type") {
	case "authorization_code":
		resp, oerr = h.grantAuthorizationCode(client, form)
	case "refresh_token":
		resp, oerr = h.grantRefreshToken(client, form)
	case flowClientCreds:
		resp, oerr = h.grantClientCredentials(client, form)
	case "":
		oerr = newOAuthError(errInvalidRequest, "grant_type is required")
	default:
		oerr = newOAuthError(errUnsupportedGrantType, "")
	}

	if oerr != nil {
		return oerr.write(c)
	}

	return writeOAuthJSON(c, http.StatusOK, resp)
}

func (h *Handler) grantAuthorizationCode(client *UserPoolClient, form url.Values) (map[string]any, *oauthError) {
	if !clientAllowsFlow(client, flowCode) {
		return nil, newOAuthError(errUnauthorizedClient, "")
	}

	code, redirect := form.Get("code"), form.Get("redirect_uri")
	if code == "" || redirect == "" {
		return nil, newOAuthError(errInvalidRequest, "code and redirect_uri are required")
	}

	entry, err := h.Backend.consumeAuthCode(code)
	if err != nil || entry.ClientID != client.ClientID {
		return nil, newOAuthError(errInvalidGrant, "")
	}

	if entry.RedirectURI != redirect {
		return nil, newOAuthError(errUnauthorizedClient, "invalid_redirect")
	}

	if oerr := verifyPKCE(entry.CodeChallenge, form.Get("code_verifier")); oerr != nil {
		return nil, oerr
	}

	tokens, err := h.Backend.issueOAuthTokens(entry.PoolID, entry.Username, client.ClientID, entry.Scopes, true)
	if err != nil {
		return nil, tokenIssueError(err)
	}

	return tokenResponse(tokens, entry.Scopes, true), nil
}

func tokenIssueError(err error) *oauthError {
	if errors.Is(err, ErrNotAuthorized) || errors.Is(err, ErrUserNotFound) || errors.Is(err, ErrUserNotConfirmed) {
		return newOAuthError(errInvalidGrant, "")
	}

	return &oauthError{code: errServerError, status: http.StatusInternalServerError}
}

// verifyPKCE enforces the S256 challenge stored with the code; no challenge means no PKCE was requested.
func verifyPKCE(challenge, verifier string) *oauthError {
	if challenge == "" {
		return nil
	}

	if verifier == "" {
		return newOAuthError(errInvalidRequest, "code_verifier is required")
	}

	if len(verifier) < pkceVerifierMin || len(verifier) > pkceVerifierMax || !isUnreserved(verifier) {
		return newOAuthError(errInvalidGrant, "")
	}

	sum := sha256.Sum256([]byte(verifier))
	got := base64.RawURLEncoding.EncodeToString(sum[:])

	if subtle.ConstantTimeCompare([]byte(got), []byte(challenge)) != 1 {
		return newOAuthError(errInvalidGrant, "")
	}

	return nil
}

func isUnreserved(s string) bool {
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '.', r == '_', r == '~':
		default:
			return false
		}
	}

	return true
}

func tokenResponse(tokens *TokenResult, scopes []string, withRefresh bool) map[string]any {
	resp := map[string]any{
		"access_token": tokens.AccessToken,
		"token_type":   authTypeBearer,
		"expires_in":   tokens.ExpiresIn,
	}

	if slices.Contains(scopes, scopeOpenID) {
		resp["id_token"] = tokens.IDToken
	}

	if withRefresh && tokens.RefreshToken != "" {
		resp["refresh_token"] = tokens.RefreshToken
	}

	return resp
}

func (h *Handler) grantRefreshToken(client *UserPoolClient, form url.Values) (map[string]any, *oauthError) {
	token := form.Get("refresh_token")
	if token == "" {
		return nil, newOAuthError(errInvalidRequest, "refresh_token is required")
	}

	tokens, scopes, err := h.Backend.oauthRefresh(client.ClientID, token)
	if err != nil {
		return nil, newOAuthError(errInvalidGrant, "")
	}

	return tokenResponse(tokens, scopes, true), nil
}

func (h *Handler) grantClientCredentials(client *UserPoolClient, form url.Values) (map[string]any, *oauthError) {
	allowed := clientAllowsFlow(client, flowClientCreds) || slices.Contains(client.ExplicitAuthFlows, explicitClientTok)
	if !allowed || len(clientSecrets(client)) == 0 {
		return nil, newOAuthError(errUnauthorizedClient, "")
	}

	scopes, oerr := h.clientCredentialScopes(client, strings.Fields(form.Get("scope")))
	if oerr != nil {
		return nil, oerr
	}

	tok, expires, err := h.Backend.issueClientCredentialsToken(client.ClientID, scopes)
	if err != nil {
		return nil, &oauthError{code: errServerError, status: http.StatusInternalServerError}
	}

	return map[string]any{"access_token": tok, "expires_in": expires, "token_type": authTypeBearer}, nil
}

// clientCredentialScopes applies the documented rules: unknown scope is invalid_scope, a known but
// client-disallowed scope is invalid_grant, and no scope means every custom scope the client allows.
func (h *Handler) clientCredentialScopes(client *UserPoolClient, requested []string) ([]string, *oauthError) {
	if len(requested) == 0 {
		requested = h.Backend.customScopesFor(client)
		if len(requested) == 0 {
			return nil, newOAuthError(errInvalidScope, "")
		}

		return requested, nil
	}

	for _, s := range requested {
		if !h.Backend.scopeKnown(client.UserPoolID, s) {
			return nil, newOAuthError(errInvalidScope, "")
		}

		if !slices.Contains(client.AllowedOAuthScopes, s) {
			return nil, newOAuthError(errInvalidGrant, "")
		}
	}

	return requested, nil
}
