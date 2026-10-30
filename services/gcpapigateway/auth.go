package gcpapigateway

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
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
	jwksTTL        = 5 * time.Minute
	bearerPrefix   = "Bearer "
	msgJWTFails    = "Jwt verification fails"
	msgJWTFormat   = "Jwt is not in the form of Header.Payload.Signature with two dots and 3 sections"
	msgJWKSFetch   = "Jwks remote fetch is failed"
	msgJWKSNoKeys  = "Jwks doesn't have any valid public key"
	iapHeader      = "X-Goog-Iap-Jwt-Assertion"
	jwtParts       = 3
)

var (
	errNoKey     = errors.New("no matching signing key")
	errKeyFormat = errors.New("unsupported key")
	errJWKS      = errors.New("jwks endpoint answered")
)

// authError is a rejected data-plane request.
type authError struct {
	msg    string
	status int
}

type jwksEntry struct {
	expires time.Time
	keys    map[string]crypto.PublicKey
}

type jwksCache struct {
	mu      *lockmetrics.RWMutex
	entries map[string]*jwksEntry
}

func newJWKSCache() *jwksCache {
	return &jwksCache{mu: lockmetrics.New("gcpapigateway.jwks"), entries: map[string]*jwksEntry{}}
}

func (c *jwksCache) reset() {
	c.mu.Lock("reset")
	defer c.mu.Unlock()

	c.entries = map[string]*jwksEntry{}
}

func (c *jwksCache) get(uri string, now time.Time) (map[string]crypto.PublicKey, bool) {
	c.mu.RLock("get")
	defer c.mu.RUnlock()

	e, ok := c.entries[uri]
	if !ok || !now.Before(e.expires) {
		return nil, false
	}

	return e.keys, true
}

func (c *jwksCache) put(uri string, keys map[string]crypto.PublicKey, expires time.Time) {
	c.mu.Lock("put")
	defer c.mu.Unlock()

	c.entries[uri] = &jwksEntry{keys: keys, expires: expires}
}

func (b *InMemoryBackend) fetchKeys(ctx context.Context, uri string) (map[string]crypto.PublicKey, error) {
	now := b.now()
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
		return nil, fmt.Errorf("%w: %d", errJWKS, resp.StatusCode)
	}

	raw, err := io.ReadAll(io.LimitReader(resp.Body, jwksMaxBytes))
	if err != nil {
		return nil, err
	}

	keys := parseKeySet(raw)
	b.jwks.put(uri, keys, now.Add(jwksTTL))

	return keys, nil
}

type jwk struct {
	Kid string `json:"kid"`
	Kty string `json:"kty"`
	N   string `json:"n"`
	E   string `json:"e"`
	Crv string `json:"crv"`
	X   string `json:"x"`
	Y   string `json:"y"`
}

// parseKeySet reads a JWK set or a map of key id to X.509 PEM certificate.
func parseKeySet(raw []byte) map[string]crypto.PublicKey {
	keys := map[string]crypto.PublicKey{}

	var set struct {
		Keys []jwk `json:"keys"`
	}

	if json.Unmarshal(raw, &set) == nil && len(set.Keys) > 0 {
		for _, k := range set.Keys {
			if pub, err := k.public(); err == nil {
				keys[k.Kid] = pub
			}
		}

		return keys
	}

	var certs map[string]string
	if json.Unmarshal(raw, &certs) != nil {
		return keys
	}

	for kid, p := range certs {
		blk, _ := pem.Decode([]byte(p))
		if blk == nil {
			continue
		}

		if cert, err := x509.ParseCertificate(blk.Bytes); err == nil {
			keys[kid] = cert.PublicKey
		}
	}

	return keys
}

func b64(s string) (*big.Int, error) {
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(s, "="))
	if err != nil {
		return nil, err
	}

	return new(big.Int).SetBytes(raw), nil
}

func (k jwk) public() (crypto.PublicKey, error) {
	switch k.Kty {
	case "RSA":
		return k.rsaKey()
	case "EC":
		return k.ecKey()
	default:
		return nil, errKeyFormat
	}
}

func (k jwk) rsaKey() (crypto.PublicKey, error) {
	n, err := b64(k.N)
	if err != nil {
		return nil, err
	}

	e, err := b64(k.E)
	if err != nil || !e.IsInt64() || e.Int64() < 3 || e.Int64() > 1<<31-1 {
		return nil, errKeyFormat
	}

	return &rsa.PublicKey{N: n, E: int(e.Int64())}, nil
}

func (k jwk) ecKey() (crypto.PublicKey, error) {
	var curve elliptic.Curve

	switch k.Crv {
	case "P-256":
		curve = elliptic.P256()
	case "P-384":
		curve = elliptic.P384()
	case "P-521":
		curve = elliptic.P521()
	default:
		return nil, errKeyFormat
	}

	x, err := b64(k.X)
	if err != nil {
		return nil, err
	}

	y, err := b64(k.Y)
	if err != nil {
		return nil, err
	}

	size := (curve.Params().BitSize + 7) / 8 //nolint:mnd // bits to bytes
	if x.BitLen() > size*8 || y.BitLen() > size*8 {
		return nil, errKeyFormat
	}

	point := make([]byte, 1+2*size)
	point[0] = 4
	x.FillBytes(point[1 : 1+size])
	y.FillBytes(point[1+size:])

	return ecdsa.ParseUncompressedPublicKey(curve, point)
}

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

// tokenOf reads the JWT from the definitions' jwtLocations; a definition without any uses the defaults
// (Authorization Bearer, X-Goog-Iap-Jwt-Assertion, access_token).
func tokenOf(r *http.Request, defs []*secDef) string {
	for _, d := range defs {
		if len(d.Locations) == 0 {
			if t := defaultToken(r); t != "" {
				return t
			}

			continue
		}

		for _, l := range d.Locations {
			if t := locationToken(r, l); t != "" {
				return t
			}
		}
	}

	return ""
}

func defaultToken(r *http.Request) string {
	if v, ok := strings.CutPrefix(r.Header.Get("Authorization"), bearerPrefix); ok {
		return strings.TrimSpace(v)
	}

	if v := r.Header.Get(iapHeader); v != "" {
		return v
	}

	return r.URL.Query().Get("access_token")
}

func locationToken(r *http.Request, l jwtLocation) string {
	if l.Query != "" {
		return r.URL.Query().Get(l.Query)
	}

	v, ok := strings.CutPrefix(r.Header.Get(l.Header), l.Prefix)
	if !ok {
		return ""
	}

	return strings.TrimSpace(v)
}

func apiKeyOf(r *http.Request, d *secDef) string {
	q := r.URL.Query()

	candidates := []string{q.Get("key"), r.Header.Get("X-Api-Key")}
	if d.KeyIn == inQuery {
		candidates = append([]string{q.Get(d.KeyName)}, candidates...)
	} else {
		candidates = append([]string{r.Header.Get(d.KeyName)}, candidates...)
	}

	for _, c := range candidates {
		if c != "" {
			return c
		}
	}

	return ""
}

func claimAudiences(v any) []string {
	switch t := v.(type) {
	case string:
		return []string{t}
	case []any:
		var out []string

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

//nolint:gochecknoglobals // immutable allow-list
var allowedAlgs = []string{"RS256", "RS384", "RS512", "PS256", "PS384", "PS512", "ES256", "ES384", "ES512"}

// issuerDef picks the definition matching the token's issuer; a definition without an issuer matches any.
func issuerDef(defs []*secDef, token string) (*secDef, *authError) {
	unverified := jwt.MapClaims{}
	if _, _, err := jwt.NewParser().ParseUnverified(token, unverified); err != nil {
		msg := msgJWTFails
		if strings.Count(token, ".") != jwtParts-1 {
			msg = msgJWTFormat
		}

		return nil, &authError{status: http.StatusUnauthorized, msg: msg}
	}

	iss, _ := unverified["iss"].(string)

	idx := slices.IndexFunc(defs, func(d *secDef) bool { return d.Issuer == iss })
	if idx < 0 {
		idx = slices.IndexFunc(defs, func(d *secDef) bool { return d.Issuer == "" })
	}

	if idx < 0 {
		return nil, &authError{status: http.StatusUnauthorized, msg: "Jwt issuer is not configured"}
	}

	return defs[idx], nil
}

// verifyJWT checks token against the JWT definitions of one security requirement.
func (b *InMemoryBackend) verifyJWT(
	ctx context.Context, defs []*secDef, managed, token string,
) (jwt.MapClaims, *authError) {
	if token == "" {
		return nil, &authError{status: http.StatusUnauthorized, msg: "Jwt is missing"}
	}

	def, aerr := issuerDef(defs, token)
	if aerr != nil {
		return nil, aerr
	}

	keys, err := b.fetchKeys(ctx, def.JWKSURI)
	if err != nil {
		return nil, &authError{status: http.StatusUnauthorized, msg: msgJWKSFetch}
	}

	if len(keys) == 0 {
		return nil, &authError{status: http.StatusUnauthorized, msg: msgJWKSNoKeys}
	}

	claims := jwt.MapClaims{}
	opts := []jwt.ParserOption{jwt.WithValidMethods(allowedAlgs), jwt.WithTimeFunc(b.now), jwt.WithExpirationRequired()}

	if def.Issuer != "" {
		opts = append(opts, jwt.WithIssuer(def.Issuer))
	}

	parser := jwt.NewParser(opts...)

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
		if errors.Is(err, jwt.ErrTokenExpired) {
			return nil, &authError{status: http.StatusUnauthorized, msg: "Jwt is expired"}
		}

		if errors.Is(err, jwt.ErrTokenNotValidYet) {
			return nil, &authError{status: http.StatusUnauthorized, msg: "Jwt not yet valid"}
		}

		return nil, &authError{status: http.StatusUnauthorized, msg: msgJWTFails}
	}

	want := def.Audiences
	if len(want) == 0 {
		want = []string{"https://" + managed}
	}

	if !slices.ContainsFunc(claimAudiences(claims["aud"]), func(a string) bool { return slices.Contains(want, a) }) {
		return nil, &authError{status: http.StatusForbidden, msg: "Audiences in Jwt are not allowed"}
	}

	return claims, nil
}

const missingKeyMsg = "UNAUTHENTICATED: Method doesn't allow unregistered callers (callers without established " +
	"identity). Please use API Key or other form of API consumer identity to call this API."

// checkRequirement verifies every definition of one security requirement.
func (b *InMemoryBackend) checkRequirement(
	r *http.Request, sv *served, names []string,
) (jwt.MapClaims, *authError) {
	var (
		jwtDefs []*secDef
		claims  jwt.MapClaims
	)

	for _, n := range names {
		d := sv.spec.Defs[n]

		if d.Kind == kindJWT {
			jwtDefs = append(jwtDefs, d)

			continue
		}

		if apiKeyOf(r, d) == "" {
			return nil, &authError{status: http.StatusUnauthorized, msg: missingKeyMsg}
		}
	}

	if len(jwtDefs) > 0 {
		var aerr *authError

		claims, aerr = b.verifyJWT(r.Context(), jwtDefs, sv.managed, tokenOf(r, jwtDefs))
		if aerr != nil {
			return nil, aerr
		}
	}

	return claims, nil
}

// authorize applies a route's security: any one requirement must hold; an empty requirement allows anonymous calls.
func (b *InMemoryBackend) authorize(r *http.Request, sv *served, rt *route) (jwt.MapClaims, *authError) {
	if len(rt.Security) == 0 {
		return nil, nil
	}

	var firstErr *authError

	for _, names := range rt.Security {
		claims, aerr := b.checkRequirement(r, sv, names)
		if aerr == nil {
			return claims, nil
		}

		if firstErr == nil {
			firstErr = aerr
		}
	}

	return nil, firstErr
}
