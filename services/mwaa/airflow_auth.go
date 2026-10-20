package mwaa

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strings"
)

const (
	airflowAdminUser  = "admin"
	adminPassBytes    = 24
	airflowV3Prefix   = "3."
	headerContentType = "Content-Type"
)

var (
	errAirflowLogin = errors.New("airflow login failed")
	csrfTagPattern  = regexp.MustCompile(`<input[^>]*name="csrf_token"[^>]*>`)
	csrfValPattern  = regexp.MustCompile(`value="([^"]+)"`)
)

// airflowTarget addresses one running webserver and holds the server-side admin credential used to reach it.
type airflowTarget struct {
	base    string
	version string
	pass    string
}

// WebSession is a logged-in webserver session handed to the browser by the SSO endpoint.
type WebSession struct {
	Target  string
	Cookies []*http.Cookie
}

func newAdminPassword() string {
	raw := make([]byte, adminPassBytes)
	_, _ = rand.Read(raw)

	return base64.RawURLEncoding.EncodeToString(raw)
}

func isAirflowV3(version string) bool {
	return strings.HasPrefix(strings.TrimSpace(version), airflowV3Prefix)
}

// login performs a server-side login as the admin user and returns the browser session cookies.
func (e *airflowEngine) login(ctx context.Context, t airflowTarget) ([]*http.Cookie, error) {
	if isAirflowV3(t.version) {
		return e.loginV3(ctx, t)
	}

	return e.loginV2(ctx, t)
}

func (e *airflowEngine) loginV2(ctx context.Context, t airflowTarget) ([]*http.Cookie, error) {
	page, err := e.request(ctx, http.MethodGet, t.base+"/login/", nil, nil)
	if err != nil {
		return nil, err
	}

	csrf := csrfToken(page.body)
	if csrf == "" {
		return nil, fmt.Errorf("%w: no csrf token on login page", errAirflowLogin)
	}

	form := url.Values{"username": {airflowAdminUser}, "password": {t.pass}, "csrf_token": {csrf}}
	hdr := http.Header{headerContentType: {"application/x-www-form-urlencoded"}}

	for _, c := range page.cookies {
		hdr.Add("Cookie", c.Name+"="+c.Value)
	}

	res, err := e.request(ctx, http.MethodPost, t.base+"/login/", []byte(form.Encode()), hdr)
	if err != nil {
		return nil, err
	}

	if res.status != http.StatusFound || strings.Contains(res.header.Get("Location"), "/login") ||
		len(res.cookies) == 0 {
		return nil, fmt.Errorf("%w: status %d", errAirflowLogin, res.status)
	}

	return res.cookies, nil
}

func csrfToken(page []byte) string {
	tag := csrfTagPattern.Find(page)
	if tag == nil {
		return ""
	}

	m := csrfValPattern.FindSubmatch(tag)
	if m == nil {
		return ""
	}

	return string(m[1])
}

func (e *airflowEngine) loginV3(ctx context.Context, t airflowTarget) ([]*http.Cookie, error) {
	tok, err := e.v3Token(ctx, t)
	if err != nil {
		return nil, err
	}

	//nolint:gosec // loopback http webserver; the SSO handler sets the final attributes
	return []*http.Cookie{{Name: "_token", Value: tok, Path: "/", HttpOnly: true, SameSite: http.SameSiteLaxMode}}, nil
}

func (e *airflowEngine) v3Token(ctx context.Context, t airflowTarget) (string, error) {
	body, _ := json.Marshal(map[string]string{"username": airflowAdminUser, "password": t.pass})

	res, err := e.request(ctx, http.MethodPost, t.base+"/auth/token", body, http.Header{
		headerContentType: {"application/json"},
	})
	if err != nil {
		return "", err
	}

	var out struct {
		Token string `json:"access_token"`
	}

	if res.status >= http.StatusMultipleChoices || json.Unmarshal(res.body, &out) != nil || out.Token == "" {
		return "", fmt.Errorf("%w: status %d", errAirflowLogin, res.status)
	}

	return out.Token, nil
}

// restHeaders authenticates one REST API call: basic auth on 2.x, a bearer JWT on 3.x.
func (e *airflowEngine) restHeaders(ctx context.Context, t airflowTarget) (http.Header, error) {
	hdr := http.Header{}

	if !isAirflowV3(t.version) {
		creds := base64.StdEncoding.EncodeToString([]byte(airflowAdminUser + ":" + t.pass))
		hdr.Set("Authorization", "Basic "+creds)

		return hdr, nil
	}

	tok, err := e.v3Token(ctx, t)
	if err != nil {
		return nil, err
	}

	hdr.Set("Authorization", "Bearer "+tok)

	return hdr, nil
}

func (ae *airflowEnv) target() airflowTarget {
	return airflowTarget{base: ae.base, version: ae.version, pass: ae.adminPass}
}

// WebLogin validates a web login token, logs in server-side and returns the session for the browser.
func (b *InMemoryBackend) WebLogin(ctx context.Context, token string) (*WebSession, error) {
	e, ae, err := b.resolveToken(token, tokenKindWeb)
	if err != nil {
		return nil, err
	}

	b.mu.RLock("WebLogin")
	t := ae.target()
	b.mu.RUnlock()

	cookies, err := e.login(ctx, t)
	if err != nil {
		return nil, err
	}

	return &WebSession{Target: t.base, Cookies: cookies}, nil
}
