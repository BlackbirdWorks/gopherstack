package cognitoidentity

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const jwtSegments = 3

// parseLoginClaims decodes the payload of a JWT login token; ok is false for opaque tokens.
func parseLoginClaims(token string) (map[string]any, bool) {
	parts := strings.Split(token, ".")
	if len(parts) != jwtSegments {
		return nil, false
	}

	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(parts[1], "="))
	if err != nil {
		return nil, false
	}

	var claims map[string]any
	if json.Unmarshal(raw, &claims) != nil {
		return nil, false
	}

	return claims, true
}

// loginSubject identifies the end user behind a login token. A JWT names its user by iss+sub, so
// a fresh token for the same user maps to the same identity; opaque tokens identify themselves.
func loginSubject(token string) string {
	claims, ok := parseLoginClaims(token)
	if !ok {
		return token
	}

	sub, _ := claims["sub"].(string)
	if sub == "" {
		return token
	}

	iss, _ := claims["iss"].(string)

	return "jwt|" + iss + "|" + sub
}

func sameLogin(a, b string) bool {
	return a != "" && b != "" && loginSubject(a) == loginSubject(b)
}

// validateLoginToken rejects JWT login tokens that are expired or issued to a client the pool
// does not list for that provider. Opaque tokens are accepted as-is.
func validateLoginToken(pool *IdentityPool, provider, token string, now time.Time) error {
	claims, ok := parseLoginClaims(token)
	if !ok {
		return nil
	}

	if exp, isNum := claims["exp"].(float64); isNum && int64(exp) <= now.Unix() {
		return fmt.Errorf("%w: invalid login token, token expired: %d >= %d", ErrNotAuthorized, int64(exp), now.Unix())
	}

	for _, p := range pool.IdentityProviders {
		if p.ProviderName != provider || p.ClientID == "" {
			continue
		}

		aud, _ := claims["aud"].(string)
		clientID, _ := claims["client_id"].(string)

		if aud != p.ClientID && clientID != p.ClientID {
			return fmt.Errorf("%w: invalid login token, audience not allowed for this identity pool", ErrNotAuthorized)
		}
	}

	return nil
}

func validateLoginTokens(pool *IdentityPool, logins map[string]string, now time.Time) error {
	for provider, token := range logins {
		if err := validateLoginToken(pool, provider, token, now); err != nil {
			return err
		}
	}

	return nil
}
