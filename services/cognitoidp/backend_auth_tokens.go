package cognitoidp

import (
	"crypto/rsa"
	"fmt"
	"time"
)

// findUserByAccessTokenLocked finds the live *User for a given access token.
// It uses the usersBySub secondary index for O(1) lookup after JWT parsing.
// The caller must hold b.mu (either read or write lock).
func (b *InMemoryBackend) findUserByAccessTokenLocked(accessToken string) (*User, error) {
	for _, pool := range b.pools.All() {
		claims, err := pool.issuer.ParseAccessToken(accessToken)
		if err != nil {
			continue
		}

		sub, _ := claims["sub"].(string)
		if sub == "" {
			continue
		}

		// O(1) lookup via secondary index.
		u, found := b.userBySub(pool.ID, sub)
		if !found {
			continue
		}

		// Check per-user token revocation: reject tokens issued before GlobalSignOut.
		if revokedBefore, ok2 := b.tokenRevokedBefore[pool.ID+":"+u.Username]; ok2 {
			authTime, _ := claims["auth_time"].(float64)
			if time.Unix(int64(authTime), 0).Before(revokedBefore) {
				continue
			}
		}

		return u, nil
	}

	return nil, fmt.Errorf("%w: access token is invalid or expired", ErrNotAuthorized)
}

// GetSigningCertificate returns a deterministic, PEM-encoded self-signed X.509
// certificate for the user pool's JWT signing key. The certificate is cached on the
// pool's token issuer, so repeated calls for the same pool return a stable PEM.
func (b *InMemoryBackend) GetSigningCertificate(userPoolID string) (string, error) {
	b.mu.RLock("GetSigningCertificate")
	defer b.mu.RUnlock()

	pool, ok := b.pools.Get(userPoolID)
	if !ok {
		return "", fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	cert, err := pool.issuer.SigningCertificatePEM()
	if err != nil {
		return "", fmt.Errorf("signing certificate for pool %q: %w", userPoolID, err)
	}

	return cert, nil
}

// GetUserPoolJWKS returns the JSON Web Key Set for the given user pool.
func (b *InMemoryBackend) GetUserPoolJWKS(userPoolID string) (*JWKSResponse, error) {
	b.mu.RLock("GetUserPoolJWKS")
	defer b.mu.RUnlock()

	pool, ok := b.pools.Get(userPoolID)
	if !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	jwks := pool.issuer.JWKS()

	return &jwks, nil
}

// GetJWTPublicKey returns the RSA public key for the user pool whose issuerURL
// matches and whose key ID equals kid. Returns nil, nil when no pool matches
// (caller should reject the token as unauthorized).
func (b *InMemoryBackend) GetJWTPublicKey(issuerURL, kid string) (*rsa.PublicKey, error) {
	b.mu.RLock("GetJWTPublicKey")
	defer b.mu.RUnlock()

	for _, pool := range b.pools.All() {
		if pool.issuer == nil || pool.issuer.issuerURL != issuerURL {
			continue
		}

		key, ok := pool.issuer.PublicKeyForKID(kid)
		if !ok {
			return nil, fmt.Errorf("%w: %q for issuer %q", ErrJWTKeyNotFound, kid, issuerURL)
		}

		return key, nil
	}

	return nil, ErrJWTIssuerUnknown
}

// resolveClientTokenSettings looks up clientID and returns its token issuance
// settings, falling back to AWS defaults (no custom scopes, default token
// lifetimes, defaultRefreshTokenTTL) when the client is not found -- token
// issuance never fails solely because the client lookup misses here, matching
// existing behavior in both of this helper's callers.
func (b *InMemoryBackend) resolveClientTokenSettings(clientID string) clientTokenSettings {
	settings := clientTokenSettings{refreshTokenTTL: defaultRefreshTokenTTL}

	client, ok := b.clients.Get(clientID)
	if !ok {
		return settings
	}

	settings.scopes = client.AllowedOAuthScopes
	if d := tokenExpiryFor(client, "AccessToken"); d > 0 {
		settings.accessTokenExpiry = d
	}

	if d := tokenExpiryFor(client, "IdToken"); d > 0 {
		settings.idTokenExpiry = d
	}

	if d := tokenExpiryFor(client, "RefreshToken"); d > 0 {
		settings.refreshTokenTTL = d
	}

	return settings
}

// issueTokensLocked issues tokens for a confirmed user. Caller must hold the write
// lock. triggerSource identifies which authentication path is issuing tokens
// (TokenGeneration_Authentication, TokenGeneration_NewPasswordChallenge, ...) for
// the PreTokenGeneration Lambda trigger's event envelope.
func (b *InMemoryBackend) issueTokensLocked(
	pool *UserPool, clientID string, user *User, triggerSource string,
) (*AuthResult, error) {
	now := time.Now()
	user.LastAuthTime = now

	groups := b.userGroupsLocked(pool.ID, user.Username)
	settings := b.resolveClientTokenSettings(clientID)

	claimsToAdd, claimsToSuppress, err := b.preTokenGenerationOverride(pool, clientID, user, groups, triggerSource)
	if err != nil {
		return nil, err
	}

	tokens, err := pool.issuer.Issue(TokenParams{
		ClientID:              clientID,
		Username:              user.Username,
		UserSub:               user.Sub,
		Groups:                groups,
		AuthTime:              now.Unix(),
		Scopes:                settings.scopes,
		Attributes:            user.Attributes,
		AccessTokenExpiry:     settings.accessTokenExpiry,
		IDTokenExpiry:         settings.idTokenExpiry,
		ClaimsToAddOrOverride: claimsToAdd,
		ClaimsToSuppress:      claimsToSuppress,
	})
	if err != nil {
		return nil, fmt.Errorf("issuing tokens: %w", err)
	}

	// Store the refresh token so REFRESH_TOKEN_AUTH can validate it.
	b.storeRefreshTokenLocked(tokens.RefreshToken, &refreshTokenEntry{
		PoolID:    pool.ID,
		ClientID:  clientID,
		Username:  user.Username,
		AuthTime:  now.Unix(),
		ExpiresAt: now.UTC().Add(settings.refreshTokenTTL),
	})

	return &AuthResult{Tokens: tokens}, nil
}

// InitiateAuthRefreshToken exchanges a valid refresh token for new ID/Access tokens.
func (b *InMemoryBackend) InitiateAuthRefreshToken(clientID, refreshToken string) (*TokenResult, error) {
	b.mu.Lock("InitiateAuthRefreshToken")
	defer b.mu.Unlock()

	if refreshToken == "" {
		return nil, fmt.Errorf("%w: Missing required parameter REFRESH_TOKEN", ErrInvalidParameter)
	}

	entry, ok := b.refreshTokens[refreshToken]
	if !ok {
		return nil, fmt.Errorf("%w: refresh token not found or expired", ErrNotAuthorized)
	}
	if !entry.ExpiresAt.IsZero() && !entry.ExpiresAt.After(time.Now().UTC()) {
		b.deleteRefreshTokenLocked(refreshToken)

		return nil, fmt.Errorf("%w: refresh token not found or expired", ErrNotAuthorized)
	}

	if entry.ClientID != clientID {
		return nil, fmt.Errorf("%w: refresh token was issued for a different client", ErrNotAuthorized)
	}

	pool, ok := b.pools.Get(entry.PoolID)
	if !ok {
		return nil, fmt.Errorf("%w: user pool %q not found", ErrUserPoolNotFound, entry.PoolID)
	}

	user, ok := b.users.Get(userKey(entry.PoolID, entry.Username))
	if !ok {
		return nil, fmt.Errorf("%w: user %q not found", ErrUserNotFound, entry.Username)
	}

	if !user.Enabled {
		return nil, fmt.Errorf("%w: user %q account is disabled", ErrNotAuthorized, entry.Username)
	}

	now := time.Now()
	groups := b.userGroupsLocked(entry.PoolID, user.Username)
	settings := b.resolveClientTokenSettings(clientID)

	// Preserve the original authentication time across refresh; AWS Cognito
	// does not reset auth_time on REFRESH_TOKEN_AUTH. Legacy entries minted
	// before AuthTime was tracked fall back to the refresh moment.
	authTime := entry.AuthTime
	if authTime == 0 {
		authTime = now.Unix()
		entry.AuthTime = authTime
	}

	claimsToAdd, claimsToSuppress, err := b.preTokenGenerationOverride(
		pool, clientID, user, groups, triggerSourceTokenGenRefreshTokens,
	)
	if err != nil {
		return nil, err
	}

	tokens, err := pool.issuer.Issue(TokenParams{
		ClientID:              clientID,
		Username:              user.Username,
		UserSub:               user.Sub,
		Groups:                groups,
		AuthTime:              authTime,
		Scopes:                settings.scopes,
		AccessTokenExpiry:     settings.accessTokenExpiry,
		IDTokenExpiry:         settings.idTokenExpiry,
		ClaimsToAddOrOverride: claimsToAdd,
		ClaimsToSuppress:      claimsToSuppress,
	})
	if err != nil {
		return nil, fmt.Errorf("issuing tokens: %w", err)
	}

	// Rotate the refresh token: invalidate old, store new.
	b.deleteRefreshTokenLocked(refreshToken)
	entry.ExpiresAt = now.UTC().Add(settings.refreshTokenTTL)
	b.storeRefreshTokenLocked(tokens.RefreshToken, entry)

	return tokens, nil
}

// RevokeToken revokes a refresh token, preventing further use.
func (b *InMemoryBackend) RevokeToken(token, clientID string) error {
	b.mu.Lock("RevokeToken")
	defer b.mu.Unlock()

	entry, ok := b.refreshTokens[token]
	if !ok {
		// AWS Cognito silently succeeds when revoking an already-revoked/unknown token.
		return nil
	}

	if entry.ClientID != clientID {
		return fmt.Errorf("%w: token was issued for a different client", ErrNotAuthorized)
	}

	b.deleteRefreshTokenLocked(token)

	return nil
}

// ValidateAccessToken verifies that the supplied access token is valid and resolves to a
// live user, returning NotAuthorizedException otherwise. It is used by access-token-scoped
// operations that have no persistent state to mutate but must still authenticate the token.
func (b *InMemoryBackend) ValidateAccessToken(accessToken string) error {
	b.mu.RLock("ValidateAccessToken")
	defer b.mu.RUnlock()

	if _, err := b.findUserByAccessTokenLocked(accessToken); err != nil {
		return err
	}

	return nil
}

// AdminUserGlobalSignOut signs out a user from all sessions by revoking their refresh tokens
// and setting a per-user revocation timestamp so previously-issued access tokens are invalidated.
func (b *InMemoryBackend) AdminUserGlobalSignOut(userPoolID, username string) error {
	b.mu.Lock("AdminUserGlobalSignOut")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if _, ok := b.users.Get(userKey(userPoolID, username)); !ok {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, username)
	}

	b.deleteRefreshTokensForUserLocked(userPoolID, username)
	b.tokenRevokedBefore[userPoolID+":"+username] = time.Now().UTC()

	return nil
}

// GlobalSignOut signs out the authenticated user by revoking their refresh tokens
// and setting a per-user revocation timestamp so previously-issued access tokens are invalidated.
func (b *InMemoryBackend) GlobalSignOut(accessToken string) error {
	b.mu.Lock("GlobalSignOut")
	defer b.mu.Unlock()

	user, err := b.findUserByAccessTokenLocked(accessToken)
	if err != nil {
		return err
	}

	b.deleteRefreshTokensForUserLocked(user.UserPoolID, user.Username)
	b.tokenRevokedBefore[user.UserPoolID+":"+user.Username] = time.Now().UTC()

	return nil
}

// deleteRefreshTokenLocked deletes a refresh token and updates secondary indexes.
// Caller must hold b.mu in write mode.
func (b *InMemoryBackend) deleteRefreshTokenLocked(token string) {
	entry, ok := b.refreshTokens[token]
	if !ok {
		return
	}

	delete(b.refreshTokens, token)

	clientTokens, cok := b.refreshTokensByClient[entry.ClientID]
	if cok {
		delete(clientTokens, token)
		if len(clientTokens) == 0 {
			delete(b.refreshTokensByClient, entry.ClientID)
		}
	}

	userKey := entry.PoolID + ":" + entry.Username
	userTokens, foundUserTokens := b.refreshTokensByUser[userKey]
	if foundUserTokens {
		delete(userTokens, token)
		if len(userTokens) == 0 {
			delete(b.refreshTokensByUser, userKey)
		}
	}
}

// deleteRefreshTokensForClientAndUserIndexLocked deletes all refresh tokens issued for a client
// and keeps both secondary indexes (refreshTokensByClient, refreshTokensByUser) consistent.
// Caller must hold b.mu in write mode.
func (b *InMemoryBackend) deleteRefreshTokensForClientAndUserIndexLocked(clientID string) {
	clientTokens, ok := b.refreshTokensByClient[clientID]
	if !ok {
		return
	}

	for token := range clientTokens {
		entry, exists := b.refreshTokens[token]
		if !exists {
			continue
		}

		// Also clean up the user index to prevent memory leaks.
		userKey := entry.PoolID + ":" + entry.Username
		userTokens, foundUserTokens := b.refreshTokensByUser[userKey]
		if foundUserTokens {
			delete(userTokens, token)
			if len(userTokens) == 0 {
				delete(b.refreshTokensByUser, userKey)
			}
		}

		delete(b.refreshTokens, token)
	}

	delete(b.refreshTokensByClient, clientID)
}

// deleteRefreshTokensForUserLocked deletes all refresh tokens for a user in a pool.
// Caller must hold b.mu in write mode.
func (b *InMemoryBackend) deleteRefreshTokensForUserLocked(poolID, username string) {
	userKey := poolID + ":" + username
	userTokens, ok := b.refreshTokensByUser[userKey]
	if !ok {
		return
	}

	for token := range userTokens {
		entry, exists := b.refreshTokens[token]
		if !exists {
			continue
		}
		clientTokens, cok := b.refreshTokensByClient[entry.ClientID]
		if cok {
			delete(clientTokens, token)
			if len(clientTokens) == 0 {
				delete(b.refreshTokensByClient, entry.ClientID)
			}
		}
		delete(b.refreshTokens, token)
	}

	delete(b.refreshTokensByUser, userKey)
}

// storeRefreshTokenLocked stores a refresh token and updates secondary indexes.
// Caller must hold b.mu in write mode.
func (b *InMemoryBackend) storeRefreshTokenLocked(token string, entry *refreshTokenEntry) {
	b.refreshTokens[token] = entry

	if b.refreshTokensByClient[entry.ClientID] == nil {
		b.refreshTokensByClient[entry.ClientID] = make(map[string]struct{})
	}

	b.refreshTokensByClient[entry.ClientID][token] = struct{}{}

	userKey := entry.PoolID + ":" + entry.Username
	if b.refreshTokensByUser[userKey] == nil {
		b.refreshTokensByUser[userKey] = make(map[string]struct{})
	}
	b.refreshTokensByUser[userKey][token] = struct{}{}
}

// tokenExpiryFor returns the configured token expiry duration for the given token type
// ("AccessToken", "IdToken", "RefreshToken"). Returns 0 when not configured (use default).
func tokenExpiryFor(client *UserPoolClient, tokenType string) time.Duration {
	var validity int32
	switch tokenType {
	case "AccessToken":
		validity = client.AccessTokenValidity
	case "IdToken":
		validity = client.IDTokenValidity
	case "RefreshToken":
		validity = client.RefreshTokenValidity
	}
	if validity <= 0 {
		return 0
	}

	unit := "minutes"
	if tokenType == "RefreshToken" {
		unit = "days"
	}
	if client.TokenValidityUnits != nil {
		if u, ok := client.TokenValidityUnits[tokenType]; ok && u != "" {
			unit = u
		}
	}

	switch unit {
	case "seconds":
		return time.Duration(validity) * time.Second
	case "minutes":
		return time.Duration(validity) * time.Minute
	case "hours":
		return time.Duration(validity) * time.Hour
	case "days":
		return time.Duration(validity) * 24 * time.Hour
	default:
		return time.Duration(validity) * time.Minute
	}
}
