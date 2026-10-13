package cognitoidentity

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"
)

const (
	openIDTokenIssuer = "cognito-identity.amazonaws.com"
	// getOpenIDTokenTTL is GetOpenIdToken's documented lifetime ("valid for 10 minutes").
	getOpenIDTokenTTL = 10 * time.Minute
	// defaultDeveloperTokenTTL is GetOpenIdTokenForDeveloperIdentity's default when
	// TokenDuration is omitted ("valid for 15 minutes").
	defaultDeveloperTokenTTL = 15 * time.Minute
	// openIDTagsClaim carries principal tags in the shape services/sts reads from web-identity tokens.
	openIDTagsClaim = "https://aws.amazon.com/tags"
)

// DeveloperTokenOptions carries the optional GetOpenIdTokenForDeveloperIdentity members.
type DeveloperTokenOptions struct {
	PrincipalTags map[string]string
}

// issueOpenIDToken builds a JWT-shaped OpenID token whose exp claim honours ttl, so a
// consumer such as STS AssumeRoleWithWebIdentity rejects it once expired.
func issueOpenIDToken(
	poolID, identityID string,
	authenticated bool,
	ttl time.Duration,
	tags map[string]string,
) (string, error) {
	amr := "unauthenticated"
	if authenticated {
		amr = "authenticated"
	}

	now := time.Now().UTC()
	claims := map[string]any{
		"iss": openIDTokenIssuer,
		"aud": poolID,
		"sub": identityID,
		"amr": []string{amr},
		"iat": now.Unix(),
		"exp": now.Add(ttl).Unix(),
	}

	if len(tags) > 0 {
		claims[openIDTagsClaim] = tags
	}

	header, err := json.Marshal(map[string]string{"alg": "RS256", "typ": "JWT"})
	if err != nil {
		return "", fmt.Errorf("encode token header: %w", err)
	}

	payload, err := json.Marshal(claims)
	if err != nil {
		return "", fmt.Errorf("encode token claims: %w", err)
	}

	enc := base64.RawURLEncoding

	return enc.EncodeToString(header) + "." + enc.EncodeToString(payload) + ".mock-signature", nil
}
