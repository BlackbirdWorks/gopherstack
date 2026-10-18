package cognitoidp

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	pathOAuthIDPResponse = "/oauth2/idpresponse"
	opOAuthIDPResponse   = "OAuth2IDPResponse"
	idpNonceBytes        = 18
	maxIDPErrorDesc      = 256
)

// federationTarget returns the requested provider name or identifier from the authorize query.
func federationTarget(q url.Values) (string, string, bool) {
	name, ident := q.Get("identity_provider"), q.Get("idp_identifier")

	return name, ident, name != "" || ident != ""
}

func idpCallbackURL(r *http.Request) string {
	scheme := "http"
	if r.TLS != nil || r.Header.Get("X-Forwarded-Proto") == schemeHTTPS {
		scheme = schemeHTTPS
	}

	return scheme + "://" + r.Host + pathOAuthIDPResponse
}

func (h *Handler) startFederation(c *echo.Context, req *authorizeRequest, name, ident string) error {
	fail := func(code, desc string) error {
		return (&authorizeFailure{redirect: req.redirectURI, code: code, desc: desc, state: req.state}).respond(c)
	}

	idp, ok := h.Backend.federationProvider(req.client.UserPoolID, name, ident)
	if !ok {
		return fail(errInvalidRequest, "identity provider not found")
	}

	if !slices.Contains(req.client.SupportedIdentityProviders, idp.ProviderName) {
		return fail(errInvalidRequest, "identity provider is not supported by this client")
	}

	if idp.ProviderType == providerTypeSAML {
		return h.startSAML(c, req, idp)
	}

	if !oidcStyleProvider(idp.ProviderType) {
		return fail(errInvalidRequest, "identity provider type "+idp.ProviderType+" is not supported")
	}

	cfg, err := loadOIDCConfig(c.Request().Context(), idp.ProviderType, idp.ProviderDetails)
	if err != nil {
		return fail(errServerError, "identity provider is misconfigured")
	}

	nonce, err := randomToken(idpNonceBytes)
	if err != nil {
		return fail(errServerError, "")
	}

	callback := idpCallbackURL(c.Request())

	key, err := h.Backend.storeIDPState(&idpState{
		PoolID: req.client.UserPoolID, ClientID: req.client.ClientID, ProviderName: idp.ProviderName,
		RedirectURI: req.redirectURI, AppState: req.state, CodeChallenge: req.codeChallenge,
		AppNonce: req.nonce, ResponseType: req.responseType, Scopes: req.scopes,
		IDPNonce: nonce, IDPRedirect: callback,
	})
	if err != nil {
		return fail(errServerError, "")
	}

	target, err := url.Parse(cfg.authorizeURL)
	if err != nil {
		return fail(errServerError, "identity provider is misconfigured")
	}

	q := target.Query()
	q.Set("response_type", "code")
	q.Set("client_id", cfg.clientID)
	q.Set("redirect_uri", callback)
	q.Set("scope", cfg.scopes)
	q.Set("state", key)
	q.Set("nonce", nonce)

	if idp.ProviderType == providerTypeApple {
		q.Set("response_mode", "form_post")
	}

	target.RawQuery = q.Encode()

	c.Response().Header().Set("Cache-Control", "no-store")

	return c.Redirect(http.StatusFound, target.String())
}

func randomToken(n int) (string, error) {
	raw := make([]byte, n)
	if _, err := rand.Read(raw); err != nil {
		return "", err //nolint:wrapcheck // caller only tests for failure
	}

	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func truncateDesc(s string) string {
	if len(s) > maxIDPErrorDesc {
		return s[:maxIDPErrorDesc]
	}

	return s
}

func oidcStyleProvider(t string) bool {
	switch t {
	case providerTypeOID, providerTypeGoogle, providerTypeFacebook, providerTypeAmazon, providerTypeApple:
		return true
	default:
		return false
	}
}

// idpResponseParams returns the callback parameters: the query, plus the body for form_post responses.
func idpResponseParams(c *echo.Context) (url.Values, error) {
	r := c.Request()
	if r.Method != http.MethodPost {
		return r.URL.Query(), nil
	}

	r.Body = http.MaxBytesReader(c.Response(), r.Body, maxOAuthBodyBytes)
	if err := r.ParseForm(); err != nil {
		return nil, fmt.Errorf("parsing form: %w", err)
	}

	return r.PostForm, nil
}

func (h *Handler) handleIDPResponse(c *echo.Context) error {
	q, perr := idpResponseParams(c)
	if perr != nil {
		return renderErrorPage(c, http.StatusBadRequest, errInvalidRequest, "malformed form body")
	}

	st, ok := h.Backend.consumeIDPState(q.Get("state"))
	if !ok {
		return renderErrorPage(c, http.StatusBadRequest, errInvalidRequest, "state is invalid or has expired")
	}

	fail := func(code, desc string) error {
		return (&authorizeFailure{redirect: st.RedirectURI, code: code, desc: desc, state: st.AppState}).respond(c)
	}

	if idpErr := q.Get("error"); idpErr != "" {
		return fail(truncateDesc(idpErr), truncateDesc(q.Get("error_description")))
	}

	code := q.Get("code")
	if code == "" {
		return fail(errInvalidRequest, "authorization response has no code")
	}

	username, err := h.completeFederation(c, st, code)
	if err != nil {
		return fail(errInvalidRequest, truncateDesc(err.Error()))
	}

	return h.completeFederatedLogin(c, st, username)
}

// completeFederatedLogin resumes the app client's authorization flow for a federated user.
func (h *Handler) completeFederatedLogin(c *echo.Context, st *idpState, username string) error {
	client, ok := h.Backend.oauthClient(st.ClientID)
	if !ok {
		return (&authorizeFailure{
			redirect: st.RedirectURI, code: errInvalidRequest, desc: "client_id not found", state: st.AppState,
		}).respond(c)
	}

	return h.completeLogin(c, &authorizeRequest{
		client: client, responseType: st.ResponseType, redirectURI: st.RedirectURI, state: st.AppState,
		codeChallenge: st.CodeChallenge, nonce: st.AppNonce, scopes: st.Scopes,
	}, username)
}

func readRandom(b []byte) (int, error) {
	n, err := rand.Read(b)
	if err != nil {
		return n, fmt.Errorf("random: %w", err)
	}

	return n, nil
}

// completeFederation exchanges the IdP code, validates the id_token and signs the federated user in.
func (h *Handler) completeFederation(c *echo.Context, st *idpState, code string) (string, error) {
	ctx := c.Request().Context()

	idp, ok := h.Backend.federationProvider(st.PoolID, st.ProviderName, "")
	if !ok {
		return "", idpErrorf("identity provider no longer exists")
	}

	cfg, err := loadOIDCConfig(ctx, idp.ProviderType, idp.ProviderDetails)
	if err != nil {
		return "", err
	}

	rawID, access, err := cfg.exchangeCode(ctx, code, st.IDPRedirect)
	if err != nil {
		return "", err
	}

	claims := map[string]any{}

	if cfg.hasIDToken {
		if claims, err = cfg.verifyIDToken(ctx, rawID, st.IDPNonce); err != nil {
			return "", err
		}
	}

	if err = cfg.fetchAttributes(ctx, access, claims); err != nil {
		return "", err
	}

	username, err := h.Backend.federatedSignIn(st.PoolID, st.ClientID, idp, claims, cfg.issuer)
	if err != nil {
		return "", idpErrorf("%s", strings.TrimSpace(err.Error()))
	}

	return username, nil
}
