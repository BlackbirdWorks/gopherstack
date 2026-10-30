package ociapigateway

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	jwksMaxBytes   = 1 << 20
	jwksFetchLimit = 10 * time.Second
	defaultCacheHr = 1
	defaultScheme  = "Bearer"
	defaultHeader  = "Authorization"
)

var (
	errNoKey      = errors.New("no matching signing key")
	errClaim      = errors.New("claim validation failed")
	errKeyFormat  = errors.New("unsupported key")
	errJWKSStatus = errors.New("jwks endpoint answered")
)

// authResult is the outcome of authenticating a data-plane request.
type authResult struct {
	wwwAuth string
	scopes  []string
	ok      bool
}

func parseStaticKey(k *staticKeyDoc) (crypto.PublicKey, error) {
	if k.Kid == "" {
		return nil, fmt.Errorf("%w: kid is required", errKeyFormat)
	}

	switch k.Format {
	case "JSON_WEB_KEY":
		return rsaFromJWK(k.Kty, k.N, k.E)
	case "PEM":
		blk, _ := pem.Decode([]byte(k.Key))
		if blk == nil {
			return nil, fmt.Errorf("%w: key is not PEM", errKeyFormat)
		}

		if pub, err := x509.ParsePKIXPublicKey(blk.Bytes); err == nil {
			return pub, nil
		}

		cert, err := x509.ParseCertificate(blk.Bytes)
		if err != nil {
			return nil, fmt.Errorf("%w: key is neither a public key nor a certificate", errKeyFormat)
		}

		return cert.PublicKey, nil
	default:
		return nil, fmt.Errorf("%w: format must be JSON_WEB_KEY or PEM", errKeyFormat)
	}
}

func rsaFromJWK(kty, n, e string) (*rsa.PublicKey, error) {
	if kty != "RSA" {
		return nil, fmt.Errorf("%w: only RSA web keys are supported", errKeyFormat)
	}

	nb, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(n, "="))
	if err != nil || len(nb) == 0 {
		return nil, fmt.Errorf("%w: invalid modulus", errKeyFormat)
	}

	eb, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(e, "="))
	if err != nil || len(eb) == 0 {
		return nil, fmt.Errorf("%w: invalid exponent", errKeyFormat)
	}

	exp := new(big.Int).SetBytes(eb)
	if !exp.IsInt64() || exp.Int64() < 3 || exp.Int64() > 1<<31-1 {
		return nil, fmt.Errorf("%w: invalid exponent", errKeyFormat)
	}

	return &rsa.PublicKey{N: new(big.Int).SetBytes(nb), E: int(exp.Int64())}, nil
}

type jwksEntry struct {
	expires time.Time
	keys    map[string]crypto.PublicKey
}

// jwksCache holds remote key sets until their maxCacheDurationInHours elapses on the backend clock.
type jwksCache struct {
	mu      *lockmetrics.RWMutex
	entries map[string]*jwksEntry
}

func newJWKSCache() *jwksCache {
	return &jwksCache{mu: lockmetrics.New("ociapigateway.jwks"), entries: map[string]*jwksEntry{}}
}

func (c *jwksCache) reset() {
	c.mu.Lock("reset")
	c.entries = map[string]*jwksEntry{}
	c.mu.Unlock()
}

func (c *jwksCache) get(uri string, now time.Time) (map[string]crypto.PublicKey, bool) {
	c.mu.RLock("get")
	defer c.mu.RUnlock()

	e, ok := c.entries[uri]
	if !ok || !e.expires.After(now) {
		return nil, false
	}

	return e.keys, true
}

func (c *jwksCache) put(uri string, keys map[string]crypto.PublicKey, expires time.Time) {
	c.mu.Lock("put")
	c.entries[uri] = &jwksEntry{keys: keys, expires: expires}
	c.mu.Unlock()
}

func (b *InMemoryBackend) remoteKeys(ctx context.Context, uri string, hours *int) (map[string]crypto.PublicKey, error) {
	now := b.clock()
	if keys, ok := b.jwks.get(uri, now); ok {
		return keys, nil
	}

	ctx, cancel := context.WithTimeout(ctx, jwksFetchLimit)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, uri, nil)
	if err != nil {
		return nil, err
	}

	resp, err := b.client.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: %d", errJWKSStatus, resp.StatusCode)
	}

	var doc struct {
		Keys []struct {
			Kid string `json:"kid"`
			Kty string `json:"kty"`
			N   string `json:"n"`
			E   string `json:"e"`
		} `json:"keys"`
	}

	if err = json.NewDecoder(io.LimitReader(resp.Body, jwksMaxBytes)).Decode(&doc); err != nil {
		return nil, err
	}

	keys := map[string]crypto.PublicKey{}

	for _, k := range doc.Keys {
		if pub, kerr := rsaFromJWK(k.Kty, k.N, k.E); kerr == nil {
			keys[k.Kid] = pub
		}
	}

	h := defaultCacheHr
	if hours != nil && *hours > 0 {
		h = *hours
	}

	b.jwks.put(uri, keys, now.Add(time.Duration(h)*time.Hour))

	return keys, nil
}

// signingKeys resolves the key set of a JWT or token policy.
func (b *InMemoryBackend) signingKeys(ctx context.Context, a *authDoc) (map[string]crypto.PublicKey, error) {
	ks := keySource(a)
	if ks.kind == keysRemote {
		return b.remoteKeys(ctx, ks.uri, ks.hours)
	}

	static := ks.keys

	keys := map[string]crypto.PublicKey{}

	for i := range static {
		pub, err := parseStaticKey(&static[i])
		if err != nil {
			return nil, err
		}

		keys[static[i].Kid] = pub
	}

	return keys, nil
}

type keySet struct {
	hours *int
	kind  string
	uri   string
	keys  []staticKeyDoc
}

func keySource(a *authDoc) keySet {
	if a.PublicKeys != nil {
		k := a.PublicKeys

		return keySet{kind: k.Type, uri: k.URI, hours: k.MaxCacheHours, keys: k.Keys}
	}

	if a.ValidationPolicy != nil {
		v := a.ValidationPolicy

		return keySet{kind: v.Type, uri: v.URI, hours: v.MaxCacheHours, keys: v.Keys}
	}

	return keySet{}
}

// effectiveChecks returns the issuers, audiences and claims the token must satisfy.
func effectiveChecks(a *authDoc) additionalDoc {
	if a.ValidationPolicy != nil && a.ValidationPolicy.Additional != nil {
		return *a.ValidationPolicy.Additional
	}

	return additionalDoc{Issuers: a.Issuers, Audiences: a.Audiences, VerifyClaims: a.VerifyClaims}
}

// tokenOf reads the bearer token from the configured header (after the scheme) or query parameter.
func tokenOf(a *authDoc, r *http.Request) string {
	header, query := "", ""
	if a.TokenHeader != nil {
		header = *a.TokenHeader
	}

	if a.TokenQueryParam != nil {
		query = *a.TokenQueryParam
	}

	if header == "" && query == "" {
		header = defaultHeader
	}

	if header != "" {
		v := strings.TrimSpace(r.Header.Get(header))

		scheme := defaultScheme
		if a.TokenAuthScheme != nil && *a.TokenAuthScheme != "" {
			scheme = *a.TokenAuthScheme
		}

		if a.Type == authCustom {
			return v
		}

		if len(v) > len(scheme)+1 && strings.EqualFold(v[:len(scheme)], scheme) && v[len(scheme)] == ' ' {
			return strings.TrimSpace(v[len(scheme)+1:])
		}

		if v != "" && header != defaultHeader {
			return v
		}
	}

	if query != "" {
		return r.URL.Query().Get(query)
	}

	return ""
}

func (b *InMemoryBackend) verifyJWT(ctx context.Context, a *authDoc, token string) authResult {
	keys, err := b.signingKeys(ctx, a)
	if err != nil {
		return authResult{}
	}

	skew := time.Duration(0)
	if a.MaxClockSkew != nil && *a.MaxClockSkew > 0 {
		skew = time.Duration(*a.MaxClockSkew * float64(time.Second))
	}

	claims := jwt.MapClaims{}
	parser := jwt.NewParser(
		jwt.WithValidMethods([]string{"RS256", "RS384", "RS512", "PS256", "PS384", "PS512", "ES256", "ES384", "ES512"}),
		jwt.WithTimeFunc(b.clock), jwt.WithLeeway(skew), jwt.WithExpirationRequired(),
	)

	_, err = parser.ParseWithClaims(token, claims, func(t *jwt.Token) (any, error) {
		kid, _ := t.Header["kid"].(string)
		if k, ok := keys[kid]; ok {
			return checkKeyAlg(k, t)
		}

		if kid == "" && len(keys) == 1 {
			for _, k := range keys {
				return checkKeyAlg(k, t)
			}
		}

		return nil, errNoKey
	})
	if err != nil {
		return authResult{}
	}

	if checkClaims(claims, effectiveChecks(a)) != nil {
		return authResult{}
	}

	return authResult{ok: true, scopes: scopesOf(claims)}
}

// checkKeyAlg refuses a key whose type does not fit the token's signing algorithm family.
func checkKeyAlg(k crypto.PublicKey, t *jwt.Token) (crypto.PublicKey, error) {
	alg := t.Method.Alg()

	switch k.(type) {
	case *rsa.PublicKey:
		if strings.HasPrefix(alg, "RS") || strings.HasPrefix(alg, "PS") {
			return k, nil
		}
	case *ecdsa.PublicKey:
		if strings.HasPrefix(alg, "ES") {
			return k, nil
		}
	}

	return nil, errKeyFormat
}

func claimStrings(v any) []string {
	switch t := v.(type) {
	case string:
		return []string{t}
	case []any:
		out := make([]string, 0, len(t))

		for _, e := range t {
			if s, ok := e.(string); ok {
				out = append(out, s)
			}
		}

		return out
	default:
		return nil
	}
}

func checkClaims(claims jwt.MapClaims, want additionalDoc) error {
	issuers, audiences, extra := want.Issuers, want.Audiences, want.VerifyClaims

	if len(issuers) > 0 {
		iss, _ := claims["iss"].(string)
		if !slices.Contains(issuers, iss) {
			return errClaim
		}
	}

	if len(audiences) > 0 {
		have := claimStrings(claims["aud"])
		if !slices.ContainsFunc(have, func(s string) bool { return slices.Contains(audiences, s) }) {
			return errClaim
		}
	}

	for _, c := range extra {
		v, present := claims[c.Key]
		required := c.IsRequired == nil || *c.IsRequired

		if !present {
			if required {
				return errClaim
			}

			continue
		}

		if len(c.Values) > 0 &&
			!slices.ContainsFunc(claimStrings(v), func(s string) bool { return slices.Contains(c.Values, s) }) {
			return errClaim
		}
	}

	return nil
}

func scopesOf(claims jwt.MapClaims) []string {
	var out []string

	for _, k := range []string{"scope", "scp"} {
		for _, s := range claimStrings(claims[k]) {
			out = append(out, strings.Fields(s)...)
		}
	}

	return out
}

// authenticate runs the deployment authentication policy; anonymous reports a request that carried no token.
func (b *InMemoryBackend) authenticate(
	ctx context.Context, a *authDoc, r *http.Request, rv *reqView,
) (authResult, bool) {
	token := tokenOf(a, r)
	if token == "" {
		if a.IsAnonymousAllowed != nil && *a.IsAnonymousAllowed {
			return authResult{ok: true}, true
		}

		return authResult{}, false
	}

	if a.Type == authCustom {
		return b.customAuth(ctx, a, token, rv), false
	}

	return b.verifyJWT(ctx, a, token), false
}

// customAuth asks the authorizer function to validate the token or the configured request parameters.
func (b *InMemoryBackend) customAuth(ctx context.Context, a *authDoc, token string, rv *reqView) authResult {
	b.mu.RLock("customAuth")
	inv := b.functions
	b.mu.RUnlock()

	if inv == nil {
		return authResult{}
	}

	payload := map[string]any{"type": "TOKEN", "token": token}

	if len(a.Parameters) > 0 {
		data := make(map[string]string, len(a.Parameters))
		for name, expr := range a.Parameters {
			data[name] = rv.lookup(expr)
		}

		payload = map[string]any{"type": "USER_DEFINED", "data": data}
	}

	body, _ := json.Marshal(payload) //nolint:errchkjson // static shape

	out, err := inv.InvokeFunction(ctx, a.FunctionID, FunctionRequest{
		Method: http.MethodPost, Header: http.Header{"Content-Type": {"application/json"}}, Body: body,
	})
	if err != nil || out.Status != http.StatusOK {
		return authResult{}
	}

	var verdict struct {
		WWWAuth string   `json:"wwwAuthenticate"`
		Scope   []string `json:"scope"`
		Active  bool     `json:"active"`
	}

	if json.Unmarshal(out.Body, &verdict) != nil || !verdict.Active {
		return authResult{wwwAuth: verdict.WWWAuth}
	}

	return authResult{ok: true, scopes: verdict.Scope}
}
