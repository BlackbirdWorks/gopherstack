package cognitoidentity

import (
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const jwtSegments = 3

var errBadSignature = fmt.Errorf("%w: invalid login token, signature verification failed", ErrNotAuthorized)

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
func (b *InMemoryBackend) validateLoginToken(pool *IdentityPool, provider, token string, now time.Time) error {
	claims, ok := parseLoginClaims(token)
	if !ok {
		return nil
	}

	if err := b.verifyLoginSignature(token, claims); err != nil {
		return err
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

func (b *InMemoryBackend) validateLoginTokens(pool *IdentityPool, logins map[string]string, now time.Time) error {
	for provider, token := range logins {
		if err := b.validateLoginToken(pool, provider, token, now); err != nil {
			return err
		}
	}

	return nil
}

// verifyLoginSignature checks the RS256 signature of a token issued by an emulator-hosted user pool.
// Tokens from issuers the emulator does not host are accepted unverified.
func (b *InMemoryBackend) verifyLoginSignature(token string, claims map[string]any) error {
	if b.keyResolver == nil {
		return nil
	}

	iss, _ := claims["iss"].(string)
	if iss == "" {
		return nil
	}

	parts := strings.Split(token, ".")

	rawHdr, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return errBadSignature
	}

	var hdr struct {
		Alg string `json:"alg"`
		Kid string `json:"kid"`
	}

	if json.Unmarshal(rawHdr, &hdr) != nil {
		return errBadSignature
	}

	key, hosted, err := b.keyResolver.ResolveTokenKey(iss, hdr.Kid)
	if !hosted {
		return nil
	}

	if err != nil || key == nil || hdr.Alg != "RS256" {
		return errBadSignature
	}

	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return errBadSignature
	}

	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if rsa.VerifyPKCS1v15(key, crypto.SHA256, digest[:], sig) != nil {
		return errBadSignature
	}

	return nil
}
