package cognitoidp

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"html/template"
	"maps"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	csrfCookie       = "XSRF-TOKEN"
	csrfBytes        = 18
	pkceChallengeLen = 43
	responseCode     = "code"
	responseToken    = "token"
	pkceMethodS256   = "S256"
	promptNone       = "none"
)

// authorizeFailure is an invalid authorize request. redirect is set only when the redirect_uri was
// validated, so the error may be sent back to the app; otherwise an error page is rendered.
type authorizeFailure struct {
	redirect string
	code     string
	desc     string
	state    string
}

type authorizeRequest struct {
	client        *UserPoolClient
	responseType  string
	redirectURI   string
	state         string
	codeChallenge string
	nonce         string
	scopes        []string
}

//nolint:gochecknoglobals // parsed once
var loginTemplate = template.Must(template.New("login").Parse(`<!doctype html>
<html lang="en"><head><meta charset="utf-8"><title>Sign in</title></head>
<body><h1>Sign in</h1>
{{if .Error}}<p role="alert">{{.Error}}</p>{{end}}
<form method="post" action="{{.Action}}">
<input type="hidden" name="_csrf" value="{{.CSRF}}">
<label>Username <input name="username" value="{{.Username}}" autocomplete="username"></label><br>
<label>Password <input type="password" name="password" autocomplete="current-password"></label><br>
<button type="submit">Sign in</button>
</form></body></html>`))

//nolint:gochecknoglobals // parsed once
var errorTemplate = template.Must(template.New("err").Parse(`<!doctype html>
<html lang="en"><head><meta charset="utf-8"><title>Error</title></head>
<body><h1>{{.Code}}</h1><p>{{.Desc}}</p></body></html>`))

func setHTMLHeaders(c *echo.Context) {
	hdr := c.Response().Header()
	hdr.Set("Cache-Control", "no-store")
	hdr.Set("X-Frame-Options", "DENY")
	hdr.Set("X-Content-Type-Options", "nosniff")
}

func renderErrorPage(c *echo.Context, status int, code, desc string) error {
	setHTMLHeaders(c)
	c.Response().Header().Set("Content-Type", "text/html; charset=utf-8")
	c.Response().WriteHeader(status)

	return errorTemplate.Execute(c.Response(), map[string]string{"Code": code, "Desc": desc})
}

// redirectWith builds base with extra query or fragment values merged in.
func redirectWith(base string, query, fragment url.Values) (string, error) {
	u, err := url.Parse(base)
	if err != nil {
		return "", err //nolint:wrapcheck // caller only tests for failure
	}

	if len(query) > 0 {
		q := u.Query()
		maps.Copy(q, query)

		u.RawQuery = q.Encode()
	}

	if len(fragment) > 0 {
		u.Fragment = ""
		u.RawFragment = ""

		return u.String() + "#" + fragment.Encode(), nil
	}

	return u.String(), nil
}

// registeredURL returns the entry of list equal to uri, so callers redirect to the stored value.
func registeredURL(list []string, uri string) (string, bool) {
	for _, registered := range list {
		if registered == uri {
			return registered, true
		}
	}

	return "", false
}

// registeredRedirectURI returns the client's registered callback URL matching uri.
func registeredRedirectURI(client *UserPoolClient, uri string) (string, bool) {
	if uri == "" || strings.Contains(uri, "#") {
		return "", false
	}

	registered, ok := registeredURL(client.CallbackURLs, uri)
	if !ok {
		return "", false
	}

	u, err := url.Parse(registered)

	return registered, err == nil && u.Scheme != ""
}

func (f *authorizeFailure) respond(c *echo.Context) error {
	if f.redirect == "" {
		return renderErrorPage(c, http.StatusBadRequest, f.code, f.desc)
	}

	q := url.Values{"error": {f.code}}
	if f.desc != "" {
		q.Set("error_description", f.desc)
	}

	if f.state != "" {
		q.Set("state", f.state)
	}

	loc, err := redirectWith(f.redirect, q, nil)
	if err != nil {
		return renderErrorPage(c, http.StatusBadRequest, errInvalidRequest, "")
	}

	return c.Redirect(http.StatusFound, loc)
}

// validateAuthorize applies the authorize-endpoint rules to q. hostPool, when non-empty, is the pool
// owning the request's domain host and the client must belong to it.
func (h *Handler) validateAuthorize(q url.Values, hostPool string) (*authorizeRequest, *authorizeFailure) {
	client, ok := h.Backend.oauthClient(q.Get("client_id"))
	if !ok || (hostPool != "" && client.UserPoolID != hostPool) {
		return nil, &authorizeFailure{code: errInvalidRequest, desc: "client_id not found"}
	}

	redirect, registered := registeredRedirectURI(client, q.Get("redirect_uri"))
	if !registered {
		return nil, &authorizeFailure{code: "redirect_mismatch", desc: "redirect_uri is not registered for this client"}
	}

	req := &authorizeRequest{
		client: client, redirectURI: redirect, state: q.Get("state"),
		responseType: q.Get("response_type"), codeChallenge: q.Get("code_challenge"), nonce: q.Get("nonce"),
	}
	fail := func(code string) (*authorizeRequest, *authorizeFailure) {
		return nil, &authorizeFailure{redirect: redirect, code: code, state: req.state}
	}

	if code := h.checkGrant(client, req.responseType); code != "" {
		return fail(code)
	}

	if !validPKCE(q) {
		return fail(errInvalidRequest)
	}

	scopes, ok := h.resolveScopes(client, q.Get("scope"))
	if !ok {
		return fail(errInvalidScope)
	}

	req.scopes = scopes

	if none, login := promptFlags(q); none && login {
		return fail(errInvalidRequest)
	}

	return req, nil
}

// promptFlags reports whether the space-delimited prompt parameter holds none and/or login.
func promptFlags(q url.Values) (bool, bool) {
	values := strings.Fields(q.Get("prompt"))

	return slices.Contains(values, promptNone), slices.Contains(values, "login")
}

func (r *authorizeRequest) loginRequired() *authorizeFailure {
	return &authorizeFailure{redirect: r.redirectURI, code: "login_required", state: r.state}
}

func (h *Handler) checkGrant(client *UserPoolClient, responseType string) string {
	switch responseType {
	case responseCode:
		if !clientAllowsFlow(client, flowCode) {
			return errUnauthorizedClient
		}
	case responseToken:
		if !clientAllowsFlow(client, flowImplicit) {
			return errUnauthorizedClient
		}
	default:
		return errInvalidRequest
	}

	return ""
}

// validPKCE accepts no PKCE or S256 with a well-formed 43-char challenge; Cognito supports only S256.
func validPKCE(q url.Values) bool {
	challenge, method := q.Get("code_challenge"), q.Get("code_challenge_method")
	if challenge == "" && method == "" {
		return true
	}

	return method == pkceMethodS256 && len(challenge) == pkceChallengeLen && isUnreserved(challenge)
}

// resolveScopes returns the requested scopes (all client scopes when omitted); each must be allowed.
func (h *Handler) resolveScopes(client *UserPoolClient, raw string) ([]string, bool) {
	scopes := strings.Fields(raw)
	if len(scopes) == 0 {
		return slices.Clone(client.AllowedOAuthScopes), len(client.AllowedOAuthScopes) > 0
	}

	for _, s := range scopes {
		if !slices.Contains(client.AllowedOAuthScopes, s) {
			return nil, false
		}
	}

	if !slices.Contains(scopes, scopeOpenID) {
		for _, s := range []string{scopeProfile, scopeEmail, scopePhone} {
			if slices.Contains(scopes, s) {
				return nil, false
			}
		}
	}

	return scopes, true
}

func (h *Handler) hostPool(c *echo.Context) string {
	pool, _ := h.Backend.domainPoolID(c.Request().Host)

	return pool
}

func (h *Handler) handleOAuthAuthorize(c *echo.Context) error {
	q := c.Request().URL.Query()

	req, fail := h.validateAuthorize(q, h.hostPool(c))
	if fail != nil {
		return fail.respond(c)
	}

	none, login := promptFlags(q)
	if !login {
		if username, ok := h.sessionUser(c, req); ok {
			return h.completeLogin(c, req, username)
		}
	}

	if none {
		return req.loginRequired().respond(c)
	}

	return c.Redirect(http.StatusFound, pathLogin+"?"+c.Request().URL.RawQuery)
}

// sessionUser returns the user of the request's managed-login session cookie, if it is live for the client's pool.
func (h *Handler) sessionUser(c *echo.Context, req *authorizeRequest) (string, bool) {
	cookie, err := c.Request().Cookie(sessionCookie)
	if err != nil || cookie.Value == "" {
		return "", false
	}

	return h.Backend.hostedSessionUser(cookie.Value, req.client.ClientID)
}

// finishLogin starts a fresh managed-login session (replacing any old one) and completes the flow.
func (h *Handler) finishLogin(c *echo.Context, req *authorizeRequest, username string) error {
	if old, err := c.Request().Cookie(sessionCookie); err == nil {
		h.Backend.deleteHostedSession(old.Value)
	}

	id, err := h.Backend.createHostedSession(req.client.UserPoolID, username)
	if err != nil {
		return (&authorizeFailure{redirect: req.redirectURI, code: errServerError, state: req.state}).respond(c)
	}

	setSessionCookie(c.Response(), c.Request(), id, hostedSessionTTL)

	return h.completeLogin(c, req, username)
}

func (h *Handler) endSession(c *echo.Context) {
	if cookie, err := c.Request().Cookie(sessionCookie); err == nil {
		h.Backend.deleteHostedSession(cookie.Value)
	}

	setSessionCookie(c.Response(), c.Request(), "", 0)
}

func (h *Handler) handleHostedLogin(c *echo.Context) error {
	r := c.Request()
	q := r.URL.Query()

	req, fail := h.validateAuthorize(q, h.hostPool(c))
	if fail != nil {
		return fail.respond(c)
	}

	if none, _ := promptFlags(q); none {
		return req.loginRequired().respond(c)
	}

	if r.Method == http.MethodGet {
		return renderLogin(c, q, http.StatusOK, "")
	}

	r.Body = http.MaxBytesReader(c.Response(), r.Body, maxOAuthBodyBytes)
	if err := r.ParseForm(); err != nil {
		return renderErrorPage(c, http.StatusBadRequest, errInvalidRequest, "malformed form body")
	}

	if !csrfValid(r) {
		return renderLogin(c, q, http.StatusForbidden, "Your session expired. Please try again.")
	}

	if r.PostForm.Get("challenge") != "" {
		return h.answerHostedChallenge(c, req, q)
	}

	res, err := h.Backend.oauthLogin(req.client.ClientID, r.PostForm.Get("username"), r.PostForm.Get("password"))
	if err != nil {
		return renderLogin(c, q, http.StatusOK, loginMessage(err))
	}

	if res.ChallengeName != "" {
		return renderChallenge(c, q, http.StatusOK, res, "")
	}

	return h.finishLogin(c, req, res.Username)
}

func loginMessage(err error) string {
	switch {
	case errors.Is(err, errHostedChallenge):
		return "This account needs a sign-in step that this page does not support."
	case errors.Is(err, ErrInvalidPassword):
		return "The new password does not meet the password policy."
	case errors.Is(err, ErrUserNotConfirmed):
		return "User is not confirmed."
	default:
		return "Incorrect username or password."
	}
}

func csrfValid(r *http.Request) bool {
	cookie, err := r.Cookie(csrfCookie)
	if err != nil || cookie.Value == "" {
		return false
	}

	return subtle.ConstantTimeCompare([]byte(cookie.Value), []byte(r.PostForm.Get("_csrf"))) == 1
}

// cookieSecure is true over TLS and on loopback hosts (a secure context); other plain-http hosts would drop it.
func cookieSecure(r *http.Request) bool {
	if r.TLS != nil {
		return true
	}

	host := r.Host
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}

	return host == "localhost" || host == "127.0.0.1" || host == "::1" || host == "[::1]"
}

// newCSRFCookie sets a fresh double-submit token cookie and returns the token for the form field.
func newCSRFCookie(c *echo.Context) (string, error) {
	raw := make([]byte, csrfBytes)
	if _, err := rand.Read(raw); err != nil {
		return "", err //nolint:wrapcheck // caller only tests for failure
	}

	token := base64.RawURLEncoding.EncodeToString(raw)
	cookie := &http.Cookie{
		Name: csrfCookie, Value: token, Path: "/", HttpOnly: true, SameSite: http.SameSiteLaxMode, Secure: true,
	}
	cookie.Secure = cookieSecure(c.Request())
	http.SetCookie(c.Response(), cookie)

	return token, nil
}

func beginHTML(c *echo.Context, status int) {
	setHTMLHeaders(c)
	c.Response().Header().Set("Content-Type", "text/html; charset=utf-8")
	c.Response().WriteHeader(status)
}

func renderLogin(c *echo.Context, q url.Values, status int, msg string) error {
	token, err := newCSRFCookie(c)
	if err != nil {
		return renderErrorPage(c, http.StatusInternalServerError, errServerError, "")
	}

	beginHTML(c, status)

	return loginTemplate.Execute(c.Response(), map[string]string{
		"Error": msg, "CSRF": token, "Username": q.Get("login_hint"),
		"Action": pathLogin + "?" + q.Encode(),
	})
}

// completeLogin issues the code (or implicit tokens) and redirects to the validated redirect_uri.
func (h *Handler) completeLogin(c *echo.Context, req *authorizeRequest, username string) error {
	query, fragment := url.Values{}, url.Values{}

	if req.responseType == responseCode {
		code, err := h.Backend.storeAuthCode(&authCodeEntry{
			PoolID: req.client.UserPoolID, ClientID: req.client.ClientID, Username: username,
			RedirectURI: req.redirectURI, CodeChallenge: req.codeChallenge, Nonce: req.nonce, Scopes: req.scopes,
		})
		if err != nil {
			return (&authorizeFailure{redirect: req.redirectURI, code: errServerError, state: req.state}).respond(c)
		}

		query.Set("code", code)
	} else {
		tokens, err := h.Backend.issueOAuthTokens(
			req.client.UserPoolID, username, req.client.ClientID, tokenGrant{scopes: req.scopes, nonce: req.nonce},
		)
		if err != nil {
			return (&authorizeFailure{redirect: req.redirectURI, code: errServerError, state: req.state}).respond(c)
		}

		fragment.Set("access_token", tokens.AccessToken)

		if slices.Contains(req.scopes, scopeOpenID) {
			fragment.Set("id_token", tokens.IDToken)
		}

		fragment.Set("token_type", "bearer")
		fragment.Set("expires_in", strconv.Itoa(int(tokens.ExpiresIn)))
	}

	if req.state != "" && req.responseType == responseCode {
		query.Set("state", req.state)
	} else if req.state != "" {
		fragment.Set("state", req.state)
	}

	loc, err := redirectWith(req.redirectURI, query, fragment)
	if err != nil {
		return renderErrorPage(c, http.StatusBadRequest, errInvalidRequest, "")
	}

	c.Response().Header().Set("Cache-Control", "no-store")

	return c.Redirect(http.StatusFound, loc)
}

func (h *Handler) handleHostedLogout(c *echo.Context) error {
	q := c.Request().URL.Query()

	client, ok := h.Backend.oauthClient(q.Get("client_id"))
	if hp := h.hostPool(c); !ok || (hp != "" && client.UserPoolID != hp) {
		return renderErrorPage(c, http.StatusBadRequest, errInvalidRequest, "client_id not found")
	}

	h.endSession(c)

	if logout := q.Get("logout_uri"); logout != "" {
		registered, found := registeredURL(client.LogoutURLs, logout)
		if !found {
			return renderErrorPage(c, http.StatusBadRequest, errInvalidRequest, "logout_uri is not registered")
		}

		return c.Redirect(http.StatusFound, registered)
	}

	if q.Get("redirect_uri") == "" {
		return renderErrorPage(c, http.StatusBadRequest, errInvalidRequest, "logout_uri or redirect_uri is required")
	}

	if _, fail := h.validateAuthorize(q, h.hostPool(c)); fail != nil {
		return fail.respond(c)
	}

	return c.Redirect(http.StatusFound, pathLogin+"?"+c.Request().URL.RawQuery)
}
