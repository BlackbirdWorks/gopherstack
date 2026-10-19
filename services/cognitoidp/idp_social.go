package cognitoidp

import (
	"crypto/ecdsa"
	"crypto/x509"
	"encoding/pem"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const (
	providerTypeSAML     = "SAML"
	providerTypeGoogle   = "Google"
	providerTypeFacebook = "Facebook"
	providerTypeAmazon   = "LoginWithAmazon"
	providerTypeApple    = "SignInWithApple"

	detailApplePrivateKey = "private_key"
	detailAppleTeamID     = "team_id"
	detailAppleKeyID      = "key_id"

	appleAudience     = "https://appleid.apple.com"
	appleSecretTTL    = 5 * time.Minute
	facebookGraphBase = "https://graph.facebook.com/v17.0"
)

type socialDefaults struct {
	issuer, authorize, codeURL, attributes, jwks, scopes, subject string
	idToken                                                       bool
}

var socialProviders = map[string]socialDefaults{ //nolint:gochecknoglobals // immutable provider endpoint table
	providerTypeGoogle: {
		issuer: "https://accounts.google.com", authorize: "https://accounts.google.com/o/oauth2/v2/auth",
		codeURL: "https://oauth2.googleapis.com/token", jwks: "https://www.googleapis.com/oauth2/v3/certs",
		scopes: "openid email profile", subject: claimSub, idToken: true,
	},
	providerTypeApple: {
		issuer: appleAudience, authorize: appleAudience + "/auth/authorize", codeURL: appleAudience + "/auth/token",
		jwks: appleAudience + "/auth/keys", scopes: "email name", subject: claimSub, idToken: true,
	},
	providerTypeFacebook: {
		authorize: "https://www.facebook.com/v17.0/dialog/oauth", codeURL: facebookGraphBase + "/oauth/access_token",
		attributes: facebookGraphBase + "/me?fields=id,name,email", scopes: "public_profile,email", subject: "id",
	},
	providerTypeAmazon: {
		authorize: "https://www.amazon.com/ap/oa", codeURL: "https://api.amazon.com/auth/o2/token",
		attributes: "https://api.amazon.com/user/profile", scopes: "profile", subject: "user_id",
	},
}

// applySocialDefaults fills the well-known endpoints of Google/Apple/Facebook/Amazon; explicit ProviderDetails win.
func applySocialDefaults(cfg *oidcConfig) {
	d, ok := socialProviders[cfg.providerType]
	if !ok {
		return
	}

	fill := func(dst *string, v string) {
		if *dst == "" {
			*dst = v
		}
	}

	fill(&cfg.issuer, d.issuer)
	fill(&cfg.authorizeURL, d.authorize)
	fill(&cfg.tokenURL, d.codeURL)
	fill(&cfg.attributesURL, d.attributes)
	fill(&cfg.jwksURI, d.jwks)
	fill(&cfg.scopes, d.scopes)

	cfg.subjectClaim = d.subject
	cfg.hasIDToken = d.idToken
}

// clientSecret returns the static secret, or for Sign in with Apple a freshly signed ES256 client-secret JWT.
func (c *oidcConfig) clientSecret() (string, error) {
	if c.providerType != providerTypeApple || c.appleKeyPEM == "" {
		return c.secret, nil
	}

	block, _ := pem.Decode([]byte(c.appleKeyPEM))
	if block == nil {
		return "", idpErrorf("private_key is not valid PEM")
	}

	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return "", idpErrorf("private_key is not a PKCS8 key: %v", err)
	}

	key, ok := parsed.(*ecdsa.PrivateKey)
	if !ok {
		return "", idpErrorf("private_key is not an EC key")
	}

	now := time.Now()
	tok := jwt.NewWithClaims(jwt.SigningMethodES256, jwt.MapClaims{
		"iss": c.appleTeamID, "iat": now.Unix(), "exp": now.Add(appleSecretTTL).Unix(),
		"aud": appleAudience, "sub": c.clientID,
	})
	tok.Header["kid"] = c.appleKeyID

	signed, err := tok.SignedString(key)
	if err != nil {
		return "", idpErrorf("signing client secret: %v", err)
	}

	return signed, nil
}
