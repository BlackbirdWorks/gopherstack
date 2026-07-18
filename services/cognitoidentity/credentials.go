package cognitoidentity

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"time"
)

// GetCredentialsForIdentity returns synthetic temporary AWS credentials for an identity.
func (b *InMemoryBackend) GetCredentialsForIdentity(
	ctx context.Context,
	identityID string,
	logins map[string]string,
) (*Credentials, error) {
	if identityID == "" {
		return nil, fmt.Errorf("%w: IdentityId is required", ErrInvalidParameter)
	}

	region := getRegion(ctx, b.region)

	b.mu.RLock("GetCredentialsForIdentity")
	defer b.mu.RUnlock()

	identity, ok := b.identityGet(region, identityID)
	if !ok {
		return nil, fmt.Errorf("%w: identity %q not found", ErrIdentityPoolNotFound, identityID)
	}

	// An authenticated identity (one that has logins on record) must present a
	// matching login token. An empty request Logins map would otherwise skip
	// the validation loop entirely and hand out credentials with no token,
	// bypassing authentication.
	if len(logins) == 0 && len(identity.Logins) > 0 {
		return nil, fmt.Errorf(
			"%w: Logins is required for an authenticated identity",
			ErrNotAuthorized,
		)
	}

	for provider, token := range logins {
		stored, exists := identity.Logins[provider]
		if !exists || stored != token {
			return nil, fmt.Errorf(
				"%w: login token for provider %q does not match",
				ErrNotAuthorized,
				provider,
			)
		}
	}

	expiry := time.Now().Add(credentialsExpirySeconds * time.Second)

	keyID, err := randomAlphanumeric(accessKeyIDLen)
	if err != nil {
		return nil, fmt.Errorf("generate access key ID: %w", err)
	}

	secretKey, err := randomAlphanumeric(secretKeyLen)
	if err != nil {
		return nil, fmt.Errorf("generate secret key: %w", err)
	}

	sessionToken, err := randomAlphanumeric(tokenLen)
	if err != nil {
		return nil, fmt.Errorf("generate session token: %w", err)
	}

	return &Credentials{
		AccessKeyID:     "ASIA" + keyID,
		SecretAccessKey: secretKey,
		SessionToken:    sessionToken,
		Expiration:      expiry,
		IdentityID:      identityID,
	}, nil
}

// GetOpenIDToken returns a synthetic OpenID Connect token for an identity.
func (b *InMemoryBackend) GetOpenIDToken(
	ctx context.Context,
	identityID string,
	_ map[string]string,
) (*OpenIDToken, error) {
	if identityID == "" {
		return nil, fmt.Errorf("%w: IdentityId is required", ErrInvalidParameter)
	}

	region := getRegion(ctx, b.region)

	b.mu.RLock("GetOpenIDToken")
	defer b.mu.RUnlock()

	if _, ok := b.identityGet(region, identityID); !ok {
		return nil, fmt.Errorf("%w: identity %q not found", ErrIdentityPoolNotFound, identityID)
	}

	// Return a synthetic token.
	payload, err := randomAlphanumeric(tokenLen)
	if err != nil {
		return nil, fmt.Errorf("generate token: %w", err)
	}

	token := fmt.Sprintf("eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.%s.signature", payload)

	return &OpenIDToken{
		IdentityID: identityID,
		Token:      token,
	}, nil
}

// GetOpenIDTokenForDeveloperIdentity registers or retrieves an identity for a developer
// authenticated user, then returns a synthetic OpenID token.
func (b *InMemoryBackend) GetOpenIDTokenForDeveloperIdentity(
	ctx context.Context,
	poolID string,
	identityID string,
	logins map[string]string,
	tokenDuration int64,
) (*DeveloperOpenIDToken, error) {
	const maxTokenDuration = 86400

	if tokenDuration < 0 || tokenDuration > maxTokenDuration {
		return nil, fmt.Errorf(
			"%w: TokenDuration must be between 0 and %d seconds",
			ErrInvalidParameter, maxTokenDuration,
		)
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("GetOpenIDTokenForDeveloperIdentity")
	defer b.mu.Unlock()

	if _, ok := b.poolGet(region, poolID); !ok {
		return nil, fmt.Errorf("%w: identity pool %q not found", ErrIdentityPoolNotFound, poolID)
	}

	if identityID != "" {
		if _, ok := b.identityGet(region, identityID); !ok {
			return nil, fmt.Errorf("%w: identity %q not found", ErrIdentityPoolNotFound, identityID)
		}
	}

	if identityID == "" {
		identityID = b.lookupOrCreateDeveloperIdentity(region, poolID, logins)
	}

	payload, err := randomAlphanumeric(tokenLen)
	if err != nil {
		return nil, fmt.Errorf("generate token: %w", err)
	}

	token := fmt.Sprintf("eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.%s.signature", payload)

	return &DeveloperOpenIDToken{
		IdentityID: identityID,
		Token:      token,
	}, nil
}

// randomAlphanumeric returns a random alphanumeric string of length n.
// It reads a single batch of random bytes from crypto/rand and uses rejection
// sampling to avoid modulo bias, falling back to additional reads only when
// the initial batch has insufficient usable bytes (~3 % rejection rate).
func randomAlphanumeric(n int) (string, error) {
	const (
		charsLen = len(alphanumChars) // 62
		byteMax  = 256                // total number of distinct byte values
		// cutoff is the largest multiple of charsLen that fits in a byte (0–255).
		// Bytes in [0, cutoff) map uniformly; bytes in [cutoff, byteMax) are rejected.
		cutoff = (byteMax / charsLen) * charsLen // 248
		// batchOversize is extra bytes read per iteration to reduce retry probability.
		batchOversize = 16
	)

	result := make([]byte, 0, n)

	for len(result) < n {
		// Over-read to reduce the chance of needing more than one batch.
		raw := make([]byte, n+batchOversize)
		if _, err := io.ReadFull(rand.Reader, raw); err != nil {
			return "", fmt.Errorf("crypto/rand failure: %w", err)
		}

		for _, b := range raw {
			if int(b) < cutoff {
				result = append(result, alphanumChars[int(b)%charsLen])
				if len(result) == n {
					break
				}
			}
		}
	}

	return string(result), nil
}
