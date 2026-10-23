package azurecontainerregistry

import (
	"crypto/subtle"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	tokenIssuer  = "Azure Container Registry"
	refreshTTL   = 3 * time.Hour
	accessTTL    = time.Hour
	kindRefresh  = "refresh"
	kindAccess   = "access"
	tokenJTIHex  = 24
	scopeParts   = 3
	scopeCatalog = "registry"
)

// NullUsername is the username registry clients send with a refresh token as the password.
const NullUsername = "00000000-0000-0000-0000-000000000000"

type tokenAccess struct {
	Type    string   `json:"type"`
	Name    string   `json:"name"`
	Actions []string `json:"actions"`
}

type tokenClaims struct {
	jwt.RegisteredClaims
	Kind     string        `json:"kind"`
	Registry string        `json:"registry"`
	Access   []tokenAccess `json:"access,omitempty"`
}

func (b *InMemoryBackend) issue(kind, reg, audience string, access []tokenAccess, ttl time.Duration) (string, error) {
	now := b.now()
	c := tokenClaims{
		Kind: kind, Registry: strs.Fold(reg), Access: access,
		Issuer: tokenIssuer, Audience: jwt.ClaimStrings{audience}, Subject: reg,
		ID:        awsid.NewN("", tokenJTIHex),
		IssuedAt:  jwt.NewNumericDate(now),
		NotBefore: jwt.NewNumericDate(now.Add(-time.Minute)),
		ExpiresAt: jwt.NewNumericDate(now.Add(ttl)),
	}

	return jwt.NewWithClaims(jwt.SigningMethodHS256, c).SignedString(b.secret)
}

// IssueRefreshToken mints the refresh token az acr login exchanges an Entra ID token for.
func (b *InMemoryBackend) IssueRefreshToken(reg, audience string) (string, error) {
	return b.issue(kindRefresh, reg, audience, nil, refreshTTL)
}

// IssueAccessToken mints a registry access token for the requested scopes.
func (b *InMemoryBackend) IssueAccessToken(reg, audience string, scopes []string) (string, error) {
	return b.issue(kindAccess, reg, audience, parseScopes(scopes), accessTTL)
}

// parseScopes reads "type:name:action,action" scopes; malformed entries are ignored.
func parseScopes(raw []string) []tokenAccess {
	var out []tokenAccess

	for _, group := range raw {
		for s := range strings.FieldsSeq(group) {
			parts := strings.SplitN(s, ":", scopeParts)
			if len(parts) != scopeParts {
				continue
			}

			out = append(out, tokenAccess{Type: parts[0], Name: parts[1], Actions: strings.Split(parts[2], ",")})
		}
	}

	return out
}

func (b *InMemoryBackend) verify(tok, kind, reg string) bool {
	var c tokenClaims

	parsed, err := jwt.ParseWithClaims(tok, &c, func(*jwt.Token) (any, error) { return b.secret, nil },
		jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}),
		jwt.WithIssuer(tokenIssuer), jwt.WithTimeFunc(b.now), jwt.WithExpirationRequired())
	if err != nil || !parsed.Valid {
		return false
	}

	return c.Kind == kind && c.Registry == strs.Fold(reg)
}

// VerifyRefresh reports whether tok is an unexpired refresh token of reg.
func (b *InMemoryBackend) VerifyRefresh(tok, reg string) bool { return b.verify(tok, kindRefresh, reg) }

// VerifyAccess reports whether tok is an unexpired access token of reg.
func (b *InMemoryBackend) VerifyAccess(tok, reg string) bool { return b.verify(tok, kindAccess, reg) }

// VerifyAdmin reports whether user and password are the registry's enabled admin credentials.
func (b *InMemoryBackend) VerifyAdmin(reg, user, password string) bool {
	b.mu.RLock("VerifyAdmin")
	defer b.mu.RUnlock()

	r, ok := b.regs[strs.Fold(reg)]
	if !ok || !r.Admin || !strs.Equal(user, r.Name) {
		return false
	}

	return password != "" && (constEq(password, r.Password1) || constEq(password, r.Password2))
}

func constEq(a, b string) bool { return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1 }
