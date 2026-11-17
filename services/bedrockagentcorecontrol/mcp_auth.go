package bedrockagentcorecontrol

import (
	"context"
	"crypto/rsa"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"

	"github.com/golang-jwt/jwt/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

const (
	discoverySuffixOIDC  = "/.well-known/openid-configuration"
	discoverySuffixOAuth = "/.well-known/oauth-authorization-server"
	authIAM              = "AWS_IAM"
	authNone             = "NONE"
	authAuthenticateOnly = "AUTHENTICATE_ONLY"
	opContains           = "CONTAINS"
	opContainsAny        = "CONTAINS_ANY"
	valueTypeStringArray = "STRING_ARRAY"
)

var (
	errSigningMethod   = errors.New("unexpected JWT signing method")
	errNoSigningKey    = errors.New("no signing key for token")
	errJWKSUnavailable = errors.New("issuer key set recently failed to load")
)

// JWKSProvider resolves RSA signing keys of issuers hosted by the emulator (Cognito user pools).
type JWKSProvider interface {
	GetJWTPublicKey(issuerURL, kid string) (*rsa.PublicKey, error)
}

// authFailure is a rejected inbound request.
type authFailure struct {
	challenge string
	message   string
	code      string
	status    int
}

func bearerFailure(status int, code, msg string) *authFailure {
	return &authFailure{status: status, message: msg, code: code}
}

const (
	protectedResourceSuffix = "/.well-known/oauth-protected-resource"
	schemeHTTP              = "http"
	schemeHTTPS             = "https"
)

// protectedResourceURLs returns the resource a request addresses and the metadata URL a challenge advertises
// for it: the gateway root or its path-addressed form, or the runtime's invocation URL with its qualifier.
func protectedResourceURLs(r *http.Request) (string, string) {
	scheme := schemeHTTP
	if r.TLS != nil {
		scheme = schemeHTTPS
	}

	base := scheme + "://" + r.Host

	path := r.URL.EscapedPath()
	if rest, ok := strings.CutPrefix(path, "/runtimes/"); ok {
		arn, _, _ := strings.Cut(rest, "/")
		resource := base + "/runtimes/" + arn + "/invocations"
		meta := resource + protectedResourceSuffix

		if q := r.URL.Query().Get("qualifier"); q != "" {
			meta += "?qualifier=" + q
		}

		return resource, meta
	}

	if rest, ok := strings.CutPrefix(path, pathGatewayPrefix); ok {
		id, _, _ := strings.Cut(rest, "/")
		root := base + pathGatewayPrefix + id

		return root + pathMCP, root + protectedResourceSuffix
	}

	return base + pathMCP, base + protectedResourceSuffix
}

func protectedResourceURL(r *http.Request) string {
	_, meta := protectedResourceURLs(r)

	return meta
}

// protectedResourceMetadata is the RFC 9728 document (section 2) for a resource guarded by a JWT authorizer.
func protectedResourceMetadata(r *http.Request, cfg map[string]any) map[string]any {
	resource, _ := protectedResourceURLs(r)
	discovery := strOf(cfg, "discoveryUrl")
	issuer := strings.TrimSuffix(strings.TrimSuffix(discovery, discoverySuffixOIDC), discoverySuffixOAuth)

	doc := map[string]any{
		"resource":                 resource,
		"authorization_servers":    []string{issuer},
		"bearer_methods_supported": []string{"header"},
	}

	if sc := strings.Fields(advertisedScopes(cfg)); len(sc) > 0 {
		doc["scopes_supported"] = sc
	}

	return doc
}

func advertisedScopes(cfg map[string]any) string {
	mapping := objOf(cfg, "advertisedScopeMapping")
	out := stringsOf(cfg["allowedScopes"])

	for i, sc := range out {
		if v, ok := mapping[sc].(string); ok && v != "" {
			out[i] = v
		}
	}

	return strings.Join(out, " ")
}

// jwtChallenge builds the WWW-Authenticate value: gateways advertise resource_metadata and scope on 401,
// plus error="insufficient_scope" on 403; runtimes advertise resource_metadata only.
func jwtChallenge(r *http.Request, f *authFailure, cfg map[string]any) string {
	resource := fmt.Sprintf("resource_metadata=%q", protectedResourceURL(r))

	var scope string
	if sc := advertisedScopes(cfg); sc != "" && !strings.HasPrefix(r.URL.Path, "/runtimes/") {
		scope = fmt.Sprintf("scope=%q", sc)
	}

	parts := []string{resource, scope}
	if f.status == http.StatusForbidden && f.code == "insufficient_scope" {
		parts = []string{`error="insufficient_scope"`, scope, resource}
	}

	return "Bearer " + strings.Join(slices.DeleteFunc(parts, func(p string) bool { return p == "" }), ", ")
}

// authorize enforces the gateway's inbound authorizer.
func (b *InMemoryBackend) authorize(ctx context.Context, r *http.Request, gw *gatewayView) *authFailure {
	switch strOf(gw.doc, "authorizerType") {
	case authIAM:
		return b.authorizeIAM(r)
	case authCustomJWT, authAuthenticateOnly:
		cfg := objOf(objOf(gw.doc, "authorizerConfiguration"), "customJWTAuthorizer")
		if cfg == nil {
			if strOf(gw.doc, "authorizerType") == authAuthenticateOnly {
				return nil
			}

			f := bearerFailure(http.StatusUnauthorized, "invalid_token", "gateway has no JWT authorizer configuration")
			f.challenge = jwtChallenge(r, f, nil)

			return f
		}

		return b.authorizeJWT(ctx, r, cfg)
	default:
		return nil
	}
}

func (b *InMemoryBackend) authorizeIAM(r *http.Request) *authFailure {
	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, "AWS4-HMAC-SHA256") {
		return &authFailure{status: http.StatusForbidden, message: "Missing Authentication Token"}
	}

	if !strings.Contains(auth, "/bedrock-agentcore/aws4_request") {
		return &authFailure{status: http.StatusForbidden, message: "The request was not signed for bedrock-agentcore"}
	}

	b.mu.RLock("authorizeIAM")
	secret := b.sigSecret
	b.mu.RUnlock()

	if serr := httputils.NewSigV4Validator(secret).Verify(r); serr != nil {
		return &authFailure{status: http.StatusForbidden, message: serr.Message}
	}

	return nil
}

func (b *InMemoryBackend) authorizeJWT(ctx context.Context, r *http.Request, cfg map[string]any) *authFailure {
	f := b.verifyJWT(ctx, r, cfg)
	if f != nil {
		f.challenge = jwtChallenge(r, f, cfg)
	}

	return f
}

func (b *InMemoryBackend) verifyJWT(ctx context.Context, r *http.Request, cfg map[string]any) *authFailure {
	h := r.Header.Get("Authorization")

	token, found := strings.CutPrefix(h, "Bearer ")
	if !found {
		token, found = strings.CutPrefix(h, "bearer ")
	}

	if !found || strings.TrimSpace(token) == "" {
		return bearerFailure(http.StatusUnauthorized, "invalid_request", "Missing bearer token")
	}

	_, f := b.verifyBearerToken(ctx, token, cfg)

	return f
}

// verifyBearerToken verifies the token against the authorizer cfg and returns its claims.
func (b *InMemoryBackend) verifyBearerToken(
	ctx context.Context, token string, cfg map[string]any,
) (jwt.MapClaims, *authFailure) {
	discovery := strOf(cfg, "discoveryUrl")
	issuer := strings.TrimSuffix(strings.TrimSuffix(discovery, discoverySuffixOIDC), discoverySuffixOAuth)

	keyfunc := func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodRSA); !ok {
			return nil, errSigningMethod
		}

		kid, _ := t.Header["kid"].(string)
		key, iss, err := b.signingKey(ctx, discovery, issuer, kid)
		issuer = iss

		return key, err
	}

	claims := jwt.MapClaims{}

	tok, err := jwt.ParseWithClaims(strings.TrimSpace(token), claims, keyfunc, jwt.WithExpirationRequired())
	if err != nil || !tok.Valid {
		return nil, bearerFailure(http.StatusUnauthorized, "invalid_token", "Invalid or expired token")
	}

	if iss, _ := claims["iss"].(string); iss != issuer {
		return nil, bearerFailure(
			http.StatusUnauthorized, "invalid_token", "Token issuer does not match the authorizer",
		)
	}

	return claims, checkClaims(claims, cfg)
}

// signingKey resolves the verification key from an emulator-hosted issuer, else from the issuer's JWKS.
func (b *InMemoryBackend) signingKey(
	ctx context.Context, discovery, issuer, kid string,
) (*rsa.PublicKey, string, error) {
	b.mu.RLock("signingKey")
	p := b.jwks
	b.mu.RUnlock()

	if p != nil {
		if key, err := p.GetJWTPublicKey(issuer, kid); err == nil && key != nil {
			return key, issuer, nil
		}
	}

	return b.jwksKey(ctx, discovery, kid)
}

func claimStrings(v any) []string {
	switch t := v.(type) {
	case string:
		return []string{t}
	case []any:
		return stringsOf(t)
	default:
		return nil
	}
}

func intersects(have, want []string) bool {
	return slices.ContainsFunc(want, func(w string) bool { return slices.Contains(have, w) })
}

func checkClaims(claims jwt.MapClaims, cfg map[string]any) *authFailure {
	if want := stringsOf(cfg["allowedAudience"]); len(want) > 0 && !intersects(claimStrings(claims["aud"]), want) {
		return bearerFailure(http.StatusUnauthorized, "invalid_token", "Token audience is not allowed")
	}

	if want := stringsOf(cfg["allowedClients"]); len(want) > 0 {
		have := claimStrings(claims["client_id"])
		if len(have) == 0 {
			have = claimStrings(claims["azp"])
		}

		if !intersects(have, want) {
			return bearerFailure(http.StatusUnauthorized, "invalid_token", "Token client is not allowed")
		}
	}

	if want := stringsOf(cfg["allowedScopes"]); len(want) > 0 {
		have := strings.Fields(strings.Join(claimStrings(claims["scope"]), " "))
		have = append(have, claimStrings(claims["scp"])...)

		if !intersects(have, want) {
			return bearerFailure(http.StatusForbidden, "insufficient_scope", "Token lacks a required scope")
		}
	}

	for _, c := range listOf(cfg, "customClaims") {
		if !customClaimMatches(claims, mapOf(c)) {
			return bearerFailure(http.StatusUnauthorized, "invalid_token", "Token custom claim validation failed")
		}
	}

	return nil
}

func customClaimMatches(claims jwt.MapClaims, c map[string]any) bool {
	rule := objOf(c, "authorizingClaimMatchValue")
	val := objOf(rule, "claimMatchValue")

	var want []string
	if s := strOf(val, "matchValueString"); s != "" {
		want = []string{s}
	} else {
		want = stringsOf(val["matchValueStringList"])
	}

	have := claimStrings(claims[strOf(c, "inboundTokenClaimName")])
	if strOf(c, "inboundTokenClaimValueType") != valueTypeStringArray && len(have) == 1 {
		have = strings.Fields(have[0])
		if len(have) == 0 {
			have = claimStrings(claims[strOf(c, "inboundTokenClaimName")])
		}
	}

	switch strOf(rule, "claimMatchOperator") {
	case opContains:
		return len(want) > 0 && !slices.ContainsFunc(want, func(w string) bool { return !slices.Contains(have, w) })
	case opContainsAny:
		return intersects(have, want)
	default:
		raw := claimStrings(claims[strOf(c, "inboundTokenClaimName")])

		return len(want) == 1 && len(raw) == 1 && raw[0] == want[0]
	}
}
