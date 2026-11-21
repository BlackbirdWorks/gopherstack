package azurearm

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"maps"
	"math/big"
	"net/http"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	apimJWTNone           = "none"
	apimMatchAny          = "any"
	apimCValidateJWT      = "validate-jwt"
	apimCOpenIDConfig     = "openid-config"
	apimCSigningKeys      = "issuer-signing-keys"
	apimCDecryptionKeys   = "decryption-keys"
	apimCAudiences        = "audiences"
	apimCIssuers          = "issuers"
	apimCRequiredClaims   = "required-claims"
	apimJWKSTTL           = time.Hour
	apimJWKSRetry         = 5 * time.Minute
	apimJWKSMaxEntries    = 256
	apimJWKSMaxKeys       = 64
	apimJWKSFetchTimeout  = 10 * time.Second
	apimJWKSMaxBytes      = 1 << 20
	apimJWTMaxErrText     = 200
	apimJWTMaxClockSkew   = 24 * 60 * 60
	apimJWTMaxClaimValues = 256
)

var errAPIMJWKS = errors.New("openid configuration could not be retrieved")

type gwJWTKey struct {
	rsa *rsa.PublicKey
	id  string
	sym []byte
}

type apimJWKSEntry struct {
	fetched, attempted time.Time
	issuer             string
	keys               []gwJWTKey
}

// apimJWKS caches OpenID configuration and signing keys per service and URL.
type apimJWKS struct {
	mu *lockmetrics.RWMutex
	m  map[string]*apimJWKSEntry
}

func newAPIMJWKS() *apimJWKS {
	return &apimJWKS{mu: lockmetrics.New("azurearm.apim.jwks"), m: map[string]*apimJWKSEntry{}}
}

func (j *apimJWKS) reset() {
	j.mu.Lock("reset")
	j.m = map[string]*apimJWKSEntry{}
	j.mu.Unlock()
}

func (j *apimJWKS) get(key string) (apimJWKSEntry, bool) {
	j.mu.RLock("lookup")
	defer j.mu.RUnlock()

	e := j.m[key]
	if e == nil {
		return apimJWKSEntry{}, false
	}

	return *e, true
}

func (j *apimJWKS) put(key string, e apimJWKSEntry) {
	j.mu.Lock("put")
	defer j.mu.Unlock()

	if _, ok := j.m[key]; !ok && len(j.m) >= apimJWKSMaxEntries {
		for k := range j.m {
			delete(j.m, k)

			break
		}
	}

	j.m[key] = &e
}

type gwJWKDoc struct {
	Keys []struct {
		Kty string `json:"kty"`
		Kid string `json:"kid"`
		N   string `json:"n"`
		E   string `json:"e"`
	} `json:"keys"`
}

func (g *gwState) fetchJSON(ctx context.Context, target string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return err
	}

	resp, err := g.p.client.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%w: status %d", errAPIMJWKS, resp.StatusCode)
	}

	b, err := io.ReadAll(io.LimitReader(resp.Body, apimJWKSMaxBytes+1))
	if err != nil || len(b) > apimJWKSMaxBytes {
		return errAPIMJWKS
	}

	return json.Unmarshal(b, out, jsonx.V1())
}

func (g *gwState) loadJWKS(target string) (apimJWKSEntry, error) {
	ctx, cancel := context.WithTimeout(g.r.Context(), apimJWKSFetchTimeout)
	defer cancel()

	var cfg struct {
		Issuer  string `json:"issuer"`
		JWKSURI string `json:"jwks_uri"`
	}

	if err := g.fetchJSON(ctx, target, &cfg); err != nil || cfg.JWKSURI == "" {
		return apimJWKSEntry{}, errAPIMJWKS
	}

	var doc gwJWKDoc
	if err := g.fetchJSON(ctx, cfg.JWKSURI, &doc); err != nil {
		return apimJWKSEntry{}, errAPIMJWKS
	}

	e := apimJWKSEntry{issuer: cfg.Issuer, fetched: g.now(), attempted: g.now()}

	for _, k := range doc.Keys {
		if k.Kty != "RSA" || len(e.keys) >= apimJWKSMaxKeys {
			continue
		}

		if pk, ok := apimRSAKey(k.N, k.E); ok {
			e.keys = append(e.keys, gwJWTKey{id: k.Kid, rsa: pk})
		}
	}

	return e, nil
}

func apimRSAKey(nb64, eb64 string) (*rsa.PublicKey, bool) {
	nb, err1 := base64.RawURLEncoding.DecodeString(strings.TrimRight(nb64, "="))
	eb, err2 := base64.RawURLEncoding.DecodeString(strings.TrimRight(eb64, "="))

	if err1 != nil || err2 != nil || len(nb) == 0 || len(eb) == 0 || len(eb) > 4 || len(nb) > 1024 {
		return nil, false
	}

	e := new(big.Int).SetBytes(eb)
	if !e.IsInt64() || e.Int64() < 3 {
		return nil, false
	}

	return &rsa.PublicKey{N: new(big.Int).SetBytes(nb), E: int(e.Int64())}, true
}

// openIDKeys returns the cached keys of an openid-config URL; a kid that the cache lacks forces a refresh at most
// every five minutes.
func (g *gwState) openIDKeys(target, kid string) (apimJWKSEntry, error) {
	ck := apimKeyOf(g.plan.svcKey, target)
	now := g.now()

	e, ok := g.p.jwks.get(ck)
	if ok && now.Sub(e.fetched) < apimJWKSTTL &&
		(kid == "" || jwkHas(e.keys, kid) || now.Sub(e.attempted) < apimJWKSRetry) {
		return e, nil
	}

	if ok && now.Sub(e.attempted) < apimJWKSRetry {
		return e, nil
	}

	fresh, err := g.loadJWKS(target)
	if err != nil {
		if ok {
			e.attempted = now
			g.p.jwks.put(ck, e)

			return e, nil
		}

		return apimJWKSEntry{}, err
	}

	g.p.jwks.put(ck, fresh)

	return fresh, nil
}

func jwkHas(keys []gwJWTKey, kid string) bool {
	for _, k := range keys {
		if k.id == kid {
			return true
		}
	}

	return false
}

func (g *gwState) jwtFail(n *polNode, reason, msg string) {
	status := int(g.intAttr(n, "failed-validation-httpcode", http.StatusUnauthorized))
	if m, ok := n.Attrs["failed-validation-error-message"]; ok {
		msg = g.sub(m)
	}

	g.failR(status, reason, msg)
}

func (g *gwState) jwtToken(n *polNode) (string, bool) {
	switch {
	case n.Attrs["header-name"] != "":
		name := g.sub(n.Attrs["header-name"])
		v := strings.TrimSpace(g.header.Get(name))

		if scheme := g.sub(n.Attrs["require-scheme"]); scheme != "" && strings.EqualFold(name, apimCAuthorizationX) {
			pre, rest, _ := strings.Cut(v, " ")
			if !strings.EqualFold(pre, scheme) {
				return "", false
			}

			return strings.TrimSpace(rest), true
		}

		if strings.EqualFold(name, apimCAuthorizationX) {
			if pre, rest, ok := strings.Cut(v, " "); ok && !strings.Contains(pre, ".") {
				v = strings.TrimSpace(rest)
			}
		}

		return v, v != ""
	case n.Attrs["query-parameter-name"] != "":
		v := g.query.Get(g.sub(n.Attrs["query-parameter-name"]))

		return v, v != ""
	}

	v, _ := g.subVal(n.Attrs["token-value"]).(string)

	return v, v != ""
}

func (g *gwState) jwtKeys(n *polNode, kid string) ([]gwJWTKey, string, error) {
	var (
		keys   []gwJWTKey
		issuer string
	)

	if sk := n.kid(apimCSigningKeys); sk != nil {
		for _, k := range sk.Kids {
			if jk, ok := g.policyKey(k); ok {
				keys = append(keys, jk)
			}
		}
	}

	for _, oc := range n.Kids {
		if oc.Name != apimCOpenIDConfig {
			continue
		}

		e, err := g.openIDKeys(g.sub(oc.Attrs[apimCUrl]), kid)
		if err != nil {
			return nil, "", err
		}

		keys, issuer = append(keys, e.keys...), e.issuer
	}

	return keys, issuer, nil
}

func (g *gwState) policyKey(k *polNode) (gwJWTKey, bool) {
	id := g.sub(k.Attrs[apimCID])

	if cid := k.Attrs[apimCCertID]; cid != "" {
		c, found := g.plan.certs[strings.ToLower(g.sub(cid))]
		if !found {
			return gwJWTKey{}, false
		}

		der, err := base64.StdEncoding.DecodeString(c.spki)
		if err != nil {
			return gwJWTKey{}, false
		}

		pub, perr := x509.ParsePKIXPublicKey(der)
		rk, isRSA := pub.(*rsa.PublicKey)

		return gwJWTKey{id: id, rsa: rk}, perr == nil && isRSA
	}

	if k.Attrs["n"] != "" {
		pk, ok := apimRSAKey(g.sub(k.Attrs["n"]), g.sub(k.Attrs["e"]))

		return gwJWTKey{id: id, rsa: pk}, ok
	}

	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(g.sub(k.Text)))
	if err != nil || len(raw) == 0 {
		return gwJWTKey{}, false
	}

	return gwJWTKey{id: id, sym: raw}, true
}

func jwtMethods(unsigned bool) []string {
	m := []string{"HS256", "HS384", "HS512", "RS256", "RS384", "RS512", "PS256", "PS384", "PS512"}
	if unsigned {
		m = append(m, apimJWTNone)
	}

	return m
}

func (g *gwState) validateJWT(n *polNode) {
	raw, ok := g.jwtToken(n)
	if !ok {
		g.jwtFail(n, "TokenNotPresent", "JWT not present.")

		return
	}

	signed := g.boolAttr(n, "require-signed-tokens", true)
	skew := g.intAttr(n, "clock-skew", 0)

	if g.evalErr != nil || skew < 0 || skew > apimJWTMaxClockSkew {
		g.evalErr = apimExprFail("clock-skew must be 0 to %d seconds", apimJWTMaxClockSkew)

		return
	}

	opts := []jwt.ParserOption{
		jwt.WithValidMethods(jwtMethods(!signed)), jwt.WithLeeway(time.Duration(skew) * time.Second),
		jwt.WithTimeFunc(g.now),
	}
	if g.boolAttr(n, "require-expiration-time", true) {
		opts = append(opts, jwt.WithExpirationRequired())
	}

	parser := jwt.NewParser(opts...)

	var hdr struct{ Kid string }

	if tok, _, err := jwt.NewParser().ParseUnverified(raw, jwt.MapClaims{}); err == nil {
		hdr.Kid, _ = tok.Header["kid"].(string)
	}

	keys, issuer, err := g.jwtKeys(n, hdr.Kid)
	if err != nil {
		g.jwtFail(n, "JwtInvalid", "The OpenID configuration could not be retrieved. Access denied.")

		return
	}

	claims := jwt.MapClaims{}
	tok, perr := g.parseWithKeys(parser, raw, claims, keys, hdr.Kid, signed)

	if perr != nil {
		g.jwtParseFail(n, perr)

		return
	}

	if !g.jwtClaims(n, claims, issuer) {
		return
	}

	if v := n.Attrs["output-token-variable-name"]; v != "" {
		g.vars[v] = &apimExprJWT{claims: claims, alg: tok.Method.Alg()}
	}
}

func (g *gwState) parseWithKeys(
	p *jwt.Parser, raw string, claims jwt.MapClaims, keys []gwJWTKey, kid string, signed bool,
) (*jwt.Token, error) {
	cands := make([]gwJWTKey, 0, len(keys))

	for _, k := range keys {
		if kid == "" || k.id == "" || k.id == kid {
			cands = append(cands, k)
		}
	}

	if len(cands) == 0 {
		cands = keys
	}

	last := jwt.ErrTokenUnverifiable

	for _, k := range cands {
		c := jwt.MapClaims{}

		tok, err := p.ParseWithClaims(raw, c, func(t *jwt.Token) (any, error) { return jwtKeyFor(t, k, signed) })
		if err == nil {
			maps.Copy(claims, c)

			return tok, nil
		}

		if !errors.Is(err, jwt.ErrTokenSignatureInvalid) && !errors.Is(err, jwt.ErrTokenUnverifiable) {
			return nil, err
		}

		last = err
	}

	if !signed && len(cands) == 0 {
		c := jwt.MapClaims{}

		if tok, err := p.ParseWithClaims(
			raw,
			c,
			func(*jwt.Token) (any, error) { return jwt.UnsafeAllowNoneSignatureType, nil },
		); err == nil {
			maps.Copy(claims, c)

			return tok, nil
		}
	}

	return nil, last
}

// jwtKeyFor binds an algorithm family to a key type, so an HS* token can never verify against an RSA key.
func jwtKeyFor(t *jwt.Token, k gwJWTKey, signed bool) (any, error) {
	switch t.Method.(type) {
	case *jwt.SigningMethodHMAC:
		if k.sym != nil {
			return k.sym, nil
		}
	case *jwt.SigningMethodRSA, *jwt.SigningMethodRSAPSS:
		if k.rsa != nil {
			return k.rsa, nil
		}
	}

	if t.Method.Alg() == apimJWTNone && !signed {
		return jwt.UnsafeAllowNoneSignatureType, nil
	}

	return nil, jwt.ErrTokenUnverifiable
}

func (g *gwState) jwtParseFail(n *polNode, err error) {
	switch {
	case errors.Is(err, jwt.ErrTokenExpired):
		g.jwtFail(n, "TokenExpired", "IDX10223: Lifetime validation failed. The token is expired. Access denied.")
	case errors.Is(err, jwt.ErrTokenSignatureInvalid):
		g.jwtFail(n, "TokenSignatureInvalid", "IDX10503: Signature validation failed. Access denied.")
	case errors.Is(err, jwt.ErrTokenUnverifiable):
		g.jwtFail(
			n,
			"TokenSignatureKeyNotFound",
			"IDX10500: Signature validation failed. No security keys were provided. Access denied.",
		)
	case errors.Is(err, jwt.ErrTokenRequiredClaimMissing):
		g.jwtFail(n, "JwtInvalid", "IDX10225: The token has no expiration time.")
	default:
		g.jwtFail(n, "JwtInvalid", "The token could not be validated: "+jwtShortErr(err))
	}
}

func jwtShortErr(err error) string {
	s := err.Error()
	if len(s) > apimJWTMaxErrText {
		s = s[:apimJWTMaxErrText]
	}

	return s
}

func apimClaimValues(v any, sep string) []string {
	switch t := v.(type) {
	case string:
		if sep != "" {
			return strings.Split(t, sep)
		}

		return []string{t}
	case []any:
		out := make([]string, 0, len(t))

		for _, e := range t {
			out = append(out, apimExprString(apimClaimScalar(e)))
		}

		return out
	}

	return []string{apimExprString(apimClaimScalar(v))}
}

func apimClaimScalar(v any) any {
	if f, ok := v.(float64); ok && f == float64(int64(f)) {
		return int64(f)
	}

	return v
}

func (g *gwState) jwtClaims(n *polNode, claims jwt.MapClaims, openIDIssuer string) bool {
	if a := n.kid(apimCAudiences); a != nil {
		allowed, have := g.subAll(a.values(apimCAudience)), apimClaimValues(claims["aud"], "")
		if !anyIn(have, allowed) {
			g.jwtFail(n, "TokenAudienceNotAllowed", "IDX10214: Audience validation failed. Audiences: '"+
				strings.Join(have, ", ")+"'. Did not match one of the valid audiences. Access denied.")

			return false
		}
	}

	iss, _ := claims["iss"].(string)
	issuers := []string(nil)

	if i := n.kid(apimCIssuers); i != nil {
		issuers = g.subAll(i.values(apimCIssuer))
	} else if openIDIssuer != "" {
		issuers = []string{openIDIssuer}
	}

	if issuers != nil && !anyIn([]string{iss}, issuers) {
		g.jwtFail(n, "TokenIssuerNotAllowed", "IDX10205: Issuer validation failed. Issuer: '"+iss+
			"'. Did not match one of the valid issuers. Access denied.")

		return false
	}

	return g.requiredClaims(n, claims)
}

func anyIn(have, allowed []string) bool {
	for _, h := range have {
		if slices.Contains(allowed, h) {
			return true
		}
	}

	return false
}

func (g *gwState) requiredClaims(n *polNode, claims jwt.MapClaims) bool {
	rc := n.kid(apimCRequiredClaims)
	if rc == nil {
		return true
	}

	var missing []string

	for _, c := range rc.Kids {
		if _, ok := claims[c.Attrs[fieldName]]; !ok {
			missing = append(missing, c.Attrs[fieldName])
		}
	}

	if len(missing) > 0 {
		sort.Strings(missing)
		g.jwtFail(
			n,
			"TokenClaimNotFound",
			"JWT is missing the following claims: "+strings.Join(missing, ", ")+". Access denied.",
		)

		return false
	}

	for _, c := range rc.Kids {
		want := g.subAll(c.values(fieldValue))
		if len(want) == 0 || len(want) > apimJWTMaxClaimValues {
			continue
		}

		have := apimClaimValues(claims[c.Attrs[fieldName]], c.Attrs["separator"])

		okAll := true
		for _, w := range want {
			okAll = okAll && anyIn([]string{w}, have)
		}

		if (c.Attrs["match"] == apimMatchAny && anyIn(have, want)) || (c.Attrs["match"] != apimMatchAny && okAll) {
			continue
		}

		g.jwtFail(n, "TokenClaimValueNotAllowed", "Claim "+c.Attrs[fieldName]+" value of "+strings.Join(have, ",")+
			" is not allowed. Access denied.")

		return false
	}

	return true
}

// apimExprJWT is the Jwt object of the policy expressions reference.
type apimExprJWT struct {
	claims jwt.MapClaims
	alg    string
}

func (j *apimExprJWT) member(name string) (any, error) {
	switch name {
	case "Claims":
		return &apimExprDict{get: func(k string) (any, bool) {
			v, ok := j.claims[k]
			if !ok {
				return nil, false
			}

			return apimClaimValues(v, ""), true
		}}, nil
	case "Issuer":
		s, _ := j.claims["iss"].(string)

		return s, nil
	case "Subject":
		s, _ := j.claims["sub"].(string)

		return s, nil
	case "Algorithm":
		return j.alg, nil
	case apxID:
		s, _ := j.claims["jti"].(string)

		return s, nil
	}

	return nil, apimExprFail("Jwt.%s is not supported", name)
}
