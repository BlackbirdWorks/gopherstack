package appsync

import (
	"context"
	"maps"
	"net"
	"net/http"
	"strings"

	"github.com/golang-jwt/jwt/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
)

// authTypeLabel is the string $util.authType() returns per auth mode.
func authTypeLabel(t AuthenticationType) string {
	switch t {
	case AuthTypeIAM:
		return "IAM Authorization"
	case AuthTypeCognito:
		return "User Pool Authorization"
	case AuthTypeOIDC:
		return "Open ID Connect Authorization"
	case AuthTypeLambda:
		return "Lambda Authorization"
	case AuthTypeAPIKey:
		return "API Key Authorization"
	default:
		return ""
	}
}

// sourceIPs is the identity sourceIp list: the X-Forwarded-For chain, else the peer address.
func sourceIPs(r *http.Request) []any {
	if r == nil {
		return []any{}
	}

	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		var out []any

		for p := range strings.SplitSeq(xff, ",") {
			if p = strings.TrimSpace(p); p != "" {
				out = append(out, p)
			}
		}

		return out
	}

	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}

	if host == "" {
		return []any{}
	}

	return []any{host}
}

// iamIdentity builds the AWS_IAM identity from the SigV4 caller.
func (b *InMemoryBackend) iamIdentity(ctx context.Context, auth GraphQLAuth) map[string]any {
	account, username, userArn := b.accountID, "", ""

	if p := awsmeta.GetPrincipal(ctx); p != nil {
		account, username, userArn = firstNonEmpty(p.AccountID, account), p.UserName, p.Arn
	}

	if auth.Request != nil {
		m := awsmeta.FromRequest(auth.Request, b.region)
		account = firstNonEmpty(m.Account, account)
		username = firstNonEmpty(username, m.AccessKeyID)
	}

	if userArn == "" && username != "" {
		userArn = arn.Build("iam", "", account, "user/"+username)
	}

	return map[string]any{
		"accountId":                   account,
		"cognitoIdentityPoolId":       nil,
		"cognitoIdentityId":           nil,
		"sourceIp":                    sourceIPs(auth.Request),
		"username":                    nilIfEmpty(username),
		"userArn":                     nilIfEmpty(userArn),
		"cognitoIdentityAuthType":     nil,
		"cognitoIdentityAuthProvider": nil,
	}
}

func nilIfEmpty(s string) any {
	if s == "" {
		return nil
	}

	return s
}

func claimsMap(c jwt.MapClaims) map[string]any {
	out := make(map[string]any, len(c))
	maps.Copy(out, c)

	return out
}

// cognitoIdentity builds the AMAZON_COGNITO_USER_POOLS identity from verified claims.
func cognitoIdentity(c jwt.MapClaims, r *http.Request) map[string]any {
	var groups any

	if g := jwtClaimStrings(c, "cognito:groups"); g != nil {
		groups = toAnySlice(g)
	}

	username, _ := c["cognito:username"].(string)
	username = firstNonEmpty(username, claimString(c, "username"))

	return map[string]any{
		"sub":                 nilIfEmpty(claimString(c, "sub")),
		"issuer":              nilIfEmpty(claimString(c, "iss")),
		"username":            nilIfEmpty(username),
		"claims":              claimsMap(c),
		"sourceIp":            sourceIPs(r),
		"defaultAuthStrategy": "ALLOW",
		"groups":              groups,
	}
}

// oidcIdentity builds the OPENID_CONNECT identity from verified claims.
func oidcIdentity(c jwt.MapClaims) map[string]any {
	return map[string]any{
		"sub":    nilIfEmpty(claimString(c, "sub")),
		"issuer": nilIfEmpty(claimString(c, "iss")),
		"claims": claimsMap(c),
	}
}

// lambdaIdentity wraps the authorizer's resolverContext; an absent one is an empty map.
func lambdaIdentity(rc map[string]any) map[string]any {
	if rc == nil {
		rc = map[string]any{}
	}

	return map[string]any{"resolverContext": rc}
}

func claimString(c jwt.MapClaims, key string) string {
	s, _ := c[key].(string)

	return s
}

func toAnySlice(in []string) []any {
	out := make([]any, len(in))
	for i, s := range in {
		out[i] = s
	}

	return out
}

// unverifiedClaims decodes a bearer token's claims without checking them, for
// backends with no JWKS provider (auth passes through there).
func unverifiedClaims(authToken string) jwt.MapClaims {
	tok := strings.TrimPrefix(strings.TrimPrefix(authToken, "Bearer "), "bearer ")
	claims := jwt.MapClaims{}

	if _, _, err := jwt.NewParser().ParseUnverified(tok, claims); err != nil {
		return jwt.MapClaims{}
	}

	return claims
}

// requestIdentity is the per-request state every resolver context shares.
type requestIdentity struct {
	identity   any
	domainName any
	authType   AuthenticationType
}

// customDomainOf returns the registered custom domain r was addressed to, or nil.
// Caller holds b.mu.
func (b *InMemoryBackend) customDomainOf(r *http.Request) any {
	if r == nil {
		return nil
	}

	host := r.Host
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}

	if host != "" && b.domainNames.Has(host) {
		return host
	}

	return nil
}
