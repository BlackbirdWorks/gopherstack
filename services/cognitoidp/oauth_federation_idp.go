package cognitoidp

import (
	"context"
	"crypto/rsa"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math/big"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const (
	idpHTTPTimeout   = 10 * time.Second
	maxIDPDocBytes   = 1 << 20
	idpDefaultScopes = "openid"
	grantAuthCode    = "authorization_code"
	algRS256         = "RS256"
	schemeHTTPS      = "https"
	detailClientID   = "client_id"
	detailSecret     = "client_secret"
	detailIssuer     = "oidc_issuer"
	detailAuthorize  = "authorize_url"
	detailToken      = "token_url"
	detailAttributes = "attributes_url"
	detailJWKS       = "jwks_uri"
	detailScopes     = "authorize_scopes"
	detailAttrMethod = "attributes_request_method"
)

var errIDP = errors.New("identity provider error")

// oidcConfig is the resolved OIDC configuration of an identity provider.
type oidcConfig struct {
	clientID      string
	secret        string
	issuer        string
	authorizeURL  string
	tokenURL      string
	attributesURL string
	jwksURI       string
	attrMethod    string
	scopes        string
}

type discoveryDoc struct {
	Issuer                string `json:"issuer"`
	AuthorizationEndpoint string `json:"authorization_endpoint"`
	TokenEndpoint         string `json:"token_endpoint"`
	UserinfoEndpoint      string `json:"userinfo_endpoint"`
	JWKSURI               string `json:"jwks_uri"`
}

type jwksDoc struct {
	Keys []struct {
		Kty string `json:"kty"`
		Kid string `json:"kid"`
		N   string `json:"n"`
		E   string `json:"e"`
	} `json:"keys"`
}

func idpErrorf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", errIDP, fmt.Sprintf(format, args...))
}

func httpURL(raw string) bool {
	u, err := url.Parse(raw)

	return err == nil && (u.Scheme == "http" || u.Scheme == schemeHTTPS) && u.Host != ""
}

// loadOIDCConfig reads ProviderDetails and, for any missing endpoint, discovers it from the issuer.
func loadOIDCConfig(ctx context.Context, details map[string]string) (*oidcConfig, error) {
	cfg := &oidcConfig{
		clientID: details[detailClientID], secret: details[detailSecret], issuer: details[detailIssuer],
		authorizeURL: details[detailAuthorize], tokenURL: details[detailToken],
		attributesURL: details[detailAttributes], jwksURI: details[detailJWKS],
		attrMethod: strings.ToUpper(details[detailAttrMethod]), scopes: details[detailScopes],
	}

	if cfg.clientID == "" || cfg.issuer == "" {
		return nil, idpErrorf("provider requires client_id and oidc_issuer")
	}

	if cfg.scopes == "" {
		cfg.scopes = idpDefaultScopes
	}

	if cfg.authorizeURL == "" || cfg.tokenURL == "" || cfg.jwksURI == "" {
		if err := cfg.discover(ctx); err != nil {
			return nil, err
		}
	}

	for _, u := range []string{cfg.authorizeURL, cfg.tokenURL, cfg.jwksURI} {
		if !httpURL(u) {
			return nil, idpErrorf("endpoint %q is not an http(s) URL", u)
		}
	}

	if cfg.attributesURL != "" && !httpURL(cfg.attributesURL) {
		return nil, idpErrorf("attributes_url %q is not an http(s) URL", cfg.attributesURL)
	}

	return cfg, nil
}

func (c *oidcConfig) discover(ctx context.Context) error {
	var doc discoveryDoc

	target := strings.TrimSuffix(c.issuer, "/") + discoverySuffix

	if err := idpFetchJSON(ctx, http.MethodGet, target, nil, "", &doc); err != nil {
		return err
	}

	if doc.Issuer != c.issuer {
		return idpErrorf("discovery issuer %q does not match oidc_issuer", doc.Issuer)
	}

	fill := func(dst *string, v string) {
		if *dst == "" {
			*dst = v
		}
	}

	fill(&c.authorizeURL, doc.AuthorizationEndpoint)
	fill(&c.tokenURL, doc.TokenEndpoint)
	fill(&c.attributesURL, doc.UserinfoEndpoint)
	fill(&c.jwksURI, doc.JWKSURI)

	return nil
}

// idpFetchJSON performs a bounded request and decodes a JSON object; the response is untrusted data.
func idpFetchJSON(
	ctx context.Context, method, target string, form url.Values, bearer string, out any,
) error {
	var body io.Reader
	if form != nil {
		body = strings.NewReader(form.Encode())
	}

	req, err := http.NewRequestWithContext(ctx, method, target, body)
	if err != nil {
		return idpErrorf("building request: %v", err)
	}

	req.Header.Set("Accept", "application/json")

	if form != nil {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}

	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}

	client := &http.Client{
		Timeout:       idpHTTPTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}

	resp, err := client.Do(req)
	if err != nil {
		return idpErrorf("request to %s failed: %v", req.URL.Host, err)
	}

	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return idpErrorf("%s returned status %d", req.URL.Host, resp.StatusCode)
	}

	if err = json.NewDecoder(io.LimitReader(resp.Body, maxIDPDocBytes)).Decode(out); err != nil {
		return idpErrorf("invalid JSON from %s: %v", req.URL.Host, err)
	}

	return nil
}

// exchangeCode redeems the IdP authorization code and returns the raw id_token and access_token.
func (c *oidcConfig) exchangeCode(ctx context.Context, code, redirectURI string) (string, string, error) {
	form := url.Values{
		"grant_type": {grantAuthCode}, "code": {code}, "redirect_uri": {redirectURI},
		"client_id": {c.clientID},
	}
	if c.secret != "" {
		form.Set("client_secret", c.secret)
	}

	var tok struct {
		IDToken     string `json:"id_token"`
		AccessToken string `json:"access_token"`
	}

	if err := idpFetchJSON(ctx, http.MethodPost, c.tokenURL, form, "", &tok); err != nil {
		return "", "", err
	}

	if tok.IDToken == "" {
		return "", "", idpErrorf("token response has no id_token")
	}

	return tok.IDToken, tok.AccessToken, nil
}

// verifyIDToken checks signature (against jwks_uri), iss, aud, exp and nonce, returning the claims.
func (c *oidcConfig) verifyIDToken(ctx context.Context, raw, nonce string) (map[string]any, error) {
	var set jwksDoc

	if err := idpFetchJSON(ctx, http.MethodGet, c.jwksURI, nil, "", &set); err != nil {
		return nil, err
	}

	keyFunc := func(tk *jwt.Token) (any, error) {
		kid, _ := tk.Header["kid"].(string)

		for _, k := range set.Keys {
			if k.Kty != "RSA" || (kid != "" && k.Kid != kid) {
				continue
			}

			return rsaKey(k.N, k.E)
		}

		return nil, jwt.ErrTokenUnverifiable
	}

	claims := jwt.MapClaims{}
	parser := jwt.NewParser(
		jwt.WithValidMethods([]string{algRS256, "RS384", "RS512"}),
		jwt.WithIssuer(c.issuer), jwt.WithAudience(c.clientID), jwt.WithExpirationRequired(),
	)

	if _, err := parser.ParseWithClaims(raw, claims, keyFunc); err != nil {
		return nil, idpErrorf("id_token validation failed: %v", err)
	}

	got, _ := claims[claimNonce].(string)
	if subtle.ConstantTimeCompare([]byte(got), []byte(nonce)) != 1 {
		return nil, idpErrorf("id_token nonce does not match")
	}

	return claims, nil
}

func rsaKey(n, e string) (*rsa.PublicKey, error) {
	nb, err := base64.RawURLEncoding.DecodeString(n)
	if err != nil {
		return nil, fmt.Errorf("decoding modulus: %w", err)
	}

	eb, err := base64.RawURLEncoding.DecodeString(e)
	if err != nil {
		return nil, fmt.Errorf("decoding exponent: %w", err)
	}

	exp := new(big.Int).SetBytes(eb)
	if !exp.IsInt64() || exp.Int64() < 3 || exp.Int64() > 1<<31-1 {
		return nil, jwt.ErrTokenUnverifiable
	}

	return &rsa.PublicKey{N: new(big.Int).SetBytes(nb), E: int(exp.Int64())}, nil
}

// fetchAttributes overlays userinfo claims; its sub must match the id_token subject.
func (c *oidcConfig) fetchAttributes(ctx context.Context, accessToken string, claims map[string]any) error {
	if c.attributesURL == "" || accessToken == "" {
		return nil
	}

	method := http.MethodGet
	if c.attrMethod == http.MethodPost {
		method = http.MethodPost
	}

	var info map[string]any

	var form url.Values
	if method == http.MethodPost {
		form = url.Values{}
	}

	if err := idpFetchJSON(ctx, method, c.attributesURL, form, accessToken, &info); err != nil {
		return err
	}

	if sub, ok := info[claimSub]; ok && claimString(sub) != claimString(claims[claimSub]) {
		return idpErrorf("userinfo subject does not match id_token")
	}

	maps.Copy(claims, info)

	return nil
}
