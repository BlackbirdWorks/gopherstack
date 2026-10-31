package verifiedpermissions

import (
	"crypto/rsa"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
	"github.com/blackbirdworks/gopherstack/pkgs/safemap"
)

// Settings configures JWT verification for IsAuthorizedWithToken.
type Settings struct {
	DisableJWTVerification bool   `json:"disable_jwt_verification" env:"VERIFIEDPERMISSIONS_DISABLE_JWT_VERIFICATION" help:"Skip JWT signature, expiry and issuer checks in IsAuthorizedWithToken (claims are still parsed)."`   //nolint:lll // Kong struct tag
	OIDCCAFile             string `json:"oidc_ca_file"            env:"GOPHERSTACK_VERIFIEDPERMISSIONS_OIDC_CA_FILE"      help:"PEM bundle of extra CAs trusted when fetching OIDC discovery/JWKS (self-signed local issuers)."` //nolint:lll // Kong struct tag
}

// ConfigProvider extracts Settings from the abstract AppContext config.
type ConfigProvider interface {
	GetVerifiedPermissionsSettings() Settings
}

// CognitoKeyResolver resolves a user pool's signing key and token issuer in-process.
type CognitoKeyResolver interface {
	UserPoolSigningKey(userPoolID, kid string) (*rsa.PublicKey, string, error)
}

var (
	// ErrTokenInvalid is returned when a submitted JWT fails validation.
	ErrTokenInvalid  = ErrValidation
	errNoKeyResolver = errors.New("no key source available")
)

const (
	claimIss        = "iss"
	claimTokenUse   = "token_use"
	tokenUseID      = "id"
	tokenUseAccess  = "access"
	oidcDiscoveryTL = "/.well-known/openid-configuration"
	jwksCacheTTL    = time.Hour
	jwksMinRefresh  = 30 * time.Second
	oidcHTTPTimeout = 10 * time.Second
	maxOIDCBody     = 1 << 20
)

type cognitoHolder struct{ r CognitoKeyResolver }

type clockHolder struct{ fn func() time.Time }

// TokenVerifier validates identity-source JWTs. One instance is shared by every region.
type TokenVerifier struct {
	cognito  atomic.Pointer[cognitoHolder]
	clock    atomic.Pointer[clockHolder]
	client   atomic.Pointer[http.Client]
	cache    *safemap.Map[string, jwksEntry]
	disabled bool
}

// NewTokenVerifier builds a verifier. client may be nil for a default httpclient.
func NewTokenVerifier(disabled bool, client *http.Client) *TokenVerifier {
	v := &TokenVerifier{disabled: disabled, cache: safemap.New[string, jwksEntry]("verifiedpermissions.jwks")}
	if client == nil {
		client = httpclient.New(oidcHTTPTimeout)
	}

	v.client.Store(client)

	return v
}

// SetCognitoKeys wires the in-process Cognito user pool key source.
func (v *TokenVerifier) SetCognitoKeys(r CognitoKeyResolver) { v.cognito.Store(&cognitoHolder{r: r}) }

// SetClock overrides the clock used for exp/nbf checks and JWKS cache aging.
func (v *TokenVerifier) SetClock(fn func() time.Time) { v.clock.Store(&clockHolder{fn: fn}) }

// SetHTTPClient overrides the client used for OIDC discovery and JWKS fetches.
func (v *TokenVerifier) SetHTTPClient(c *http.Client) { v.client.Store(c) }

func (v *TokenVerifier) now() time.Time {
	if c := v.clock.Load(); c != nil && c.fn != nil {
		return c.fn()
	}

	return time.Now()
}

func tokenErr(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrTokenInvalid}, args...)...)
}

// Verify validates token against identity source is and returns its claims.
// access selects the token flavour submitted (accessToken vs identityToken).
func (v *TokenVerifier) Verify(is IdentitySource, token string, access bool) (map[string]any, error) {
	if v.disabled {
		return parseJWTClaims(token)
	}

	if is.UserPoolArn == "" && is.OIDCTokenSelection != nil {
		if want := is.OIDCTokenSelection.TokenType; (want == tokenTypeAccess) != access && want != "" {
			return nil, tokenErr("identity source accepts %s tokens only", strings.ToLower(want))
		}
	}

	var issuer string

	parsed, err := jwt.Parse(token, func(t *jwt.Token) (any, error) {
		kid, _ := t.Header["kid"].(string)

		key, iss, kerr := v.signingKey(is, t, kid)
		issuer = iss

		return key, kerr
	},
		jwt.WithValidMethods([]string{jwt.SigningMethodRS256.Alg()}),
		jwt.WithTimeFunc(v.now),
		jwt.WithExpirationRequired(),
	)
	if err != nil {
		return nil, tokenErr("token validation failed: %v", err)
	}

	claims, _ := parsed.Claims.(jwt.MapClaims)
	if got, _ := claims[claimIss].(string); !issuerAccepted(is, issuer, got) {
		return nil, tokenErr("token issuer %q does not match the identity source", got)
	}

	if is.UserPoolArn != "" {
		want := tokenUseID
		if access {
			want = tokenUseAccess
		}

		if tu, _ := claims[claimTokenUse].(string); tu != want {
			return nil, tokenErr("token_use must be %q", want)
		}
	}

	if !matchesConfiguredAudience(is, claims, access) {
		return nil, tokenErr("token audience/client does not match the identity source")
	}

	return map[string]any(claims), nil
}

func issuerAccepted(is IdentitySource, poolIssuer, got string) bool {
	if got == "" {
		return false
	}

	if is.UserPoolArn != "" {
		return got == poolIssuer || got == cognitoIssuerFromUserPoolArn(is.UserPoolArn)
	}

	return got == is.OpenIDIssuer
}

func (v *TokenVerifier) signingKey(is IdentitySource, _ *jwt.Token, kid string) (*rsa.PublicKey, string, error) {
	if is.UserPoolArn != "" {
		h := v.cognito.Load()
		if h == nil || h.r == nil {
			return nil, "", fmt.Errorf("cognito: %w", errNoKeyResolver)
		}

		key, iss, err := h.r.UserPoolSigningKey(cognitoPoolID(is.UserPoolArn), kid)
		if err != nil {
			return nil, "", fmt.Errorf("cognito key lookup: %w", err)
		}

		return key, iss, nil
	}

	key, err := v.oidcKey(is.OpenIDIssuer, kid)

	return key, is.OpenIDIssuer, err
}
