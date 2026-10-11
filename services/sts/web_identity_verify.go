package sts

import (
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
)

const jwtAlgRS256 = "RS256"

// isEKSIssuerShape reports whether issuer looks like an EKS cluster OIDC issuer.
func isEKSIssuerShape(issuer string) bool {
	host, path, ok := strings.Cut(strings.TrimPrefix(issuer, "https://"), "/")

	return ok && strings.HasPrefix(host, "oidc.eks.") && strings.HasSuffix(host, ".amazonaws.com") &&
		strings.HasPrefix(path, "id/")
}

// verifyRS256 checks token's RS256 signature against any of keys.
func verifyRS256(token string, keys []*rsa.PublicKey) error {
	parts := strings.Split(token, ".")
	if len(parts) != jwtFullPartCount {
		return fmt.Errorf("%w: token is not a JWT", ErrInvalidIdentityToken)
	}

	rawHdr, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return fmt.Errorf("%w: JWT header could not be decoded", ErrInvalidIdentityToken)
	}

	var hdr struct {
		Alg string `json:"alg"`
	}

	if err = json.Unmarshal(rawHdr, &hdr); err != nil || hdr.Alg != jwtAlgRS256 {
		return fmt.Errorf("%w: unsupported JWT signing algorithm", ErrInvalidIdentityToken)
	}

	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return fmt.Errorf("%w: JWT signature could not be decoded", ErrInvalidIdentityToken)
	}

	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))

	for _, k := range keys {
		if rsa.VerifyPKCS1v15(k, crypto.SHA256, digest[:], sig) == nil {
			return nil
		}
	}

	return fmt.Errorf(
		"%w: Couldn't retrieve verification key from your identity provider, "+
			"or the token signature is invalid", ErrInvalidIdentityToken)
}

// verifyManagedIssuerSignature verifies tokens issued by emulator-hosted issuers.
// Issuers the emulator does not host are left unverified.
func (b *InMemoryBackend) verifyManagedIssuerSignature(token, issuer string) error {
	b.mu.RLock("verifyManagedIssuerSignature")
	r := b.issuerKeys
	b.mu.RUnlock()

	if r == nil || issuer == "" {
		return nil
	}

	keys, managed := r.OIDCIssuerKeys(issuer)
	if !managed {
		if isEKSIssuerShape(issuer) {
			return fmt.Errorf("%w: no signing keys for issuer %q", ErrInvalidIdentityToken, issuer)
		}

		return nil
	}

	return verifyRS256(token, keys)
}

// webIdentityAudiences returns the token's aud claim as a list.
func webIdentityAudiences(token string) []string {
	claims := parseJWTPayloadClaims(token)
	if claims == nil {
		return nil
	}

	switch v := claims[jwtClaimAud].(type) {
	case string:
		return []string{v}
	case []any:
		out := make([]string, 0, len(v))

		for _, a := range v {
			if s, ok := a.(string); ok {
				out = append(out, s)
			}
		}

		return out
	}

	return nil
}

// matchedClientID returns the provider client ID matched by the token's audience.
// ok is false when the provider's client-ID list rejects every audience.
func (b *InMemoryBackend) matchedClientID(token, issuer string) (string, bool) {
	b.mu.RLock("matchedClientID")
	cl := b.oidcClientIDs
	b.mu.RUnlock()

	auds := webIdentityAudiences(token)
	if cl == nil || len(auds) == 0 {
		return "", true
	}

	ids, found := cl.OIDCProviderClientIDs(issuer)
	if !found || len(ids) == 0 {
		return "", true
	}

	for _, a := range auds {
		if slices.Contains(ids, a) {
			return a, true
		}
	}

	return "", false
}
