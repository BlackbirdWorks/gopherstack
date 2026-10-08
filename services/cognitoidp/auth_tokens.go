package cognitoidp

import (
	"crypto/rsa"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"strings"
	"time"

	"github.com/google/uuid"
)

// findUserByAccessTokenLocked finds the live *User for a given access token.
// It uses the usersBySub secondary index for O(1) lookup after JWT parsing.
// The caller must hold b.mu (either read or write lock).
func (b *InMemoryBackend) findUserByAccessTokenLocked(accessToken string) (*User, error) {
	pools := b.pools.All()
	kid := tokenHeaderKID(accessToken)

	// Pools whose key ID matches the token header go first so a valid token costs one RSA verify.
	if kid != "" {
		for _, pool := range pools {
			if pool.issuer.keyID != kid {
				continue
			}

			if u, ok := b.userForAccessToken(pool, accessToken); ok {
				return u, nil
			}
		}
	}

	for _, pool := range pools {
		if kid != "" && pool.issuer.keyID == kid {
			continue
		}

		if u, ok := b.userForAccessToken(pool, accessToken); ok {
			return u, nil
		}
	}

	return nil, fmt.Errorf("%w: access token is invalid or expired", ErrNotAuthorized)
}

// userForAccessToken verifies accessToken against pool's key and returns the live, non-revoked user.
func (b *InMemoryBackend) userForAccessToken(pool *UserPool, accessToken string) (*User, bool) {
	claims, err := pool.issuer.ParseAccessToken(accessToken)
	if err != nil {
		return nil, false
	}

	sub, _ := claims["sub"].(string)
	if sub == "" {
		return nil, false
	}

	// O(1) lookup via secondary index.
	u, found := b.userBySub(pool.ID, sub)
	if !found {
		return nil, false
	}

	// Reject tokens minted at or before GlobalSignOut; authSeq is exact, auth_time is the
	// fallback for pre-authSeq snapshots.
	key := pool.ID + ":" + u.Username
	if origin, _ := claims[claimOriginJTI].(string); origin != "" {
		if _, revoked := b.revokedChains[origin]; revoked {
			return nil, false
		}
	}

	if revokedSeq := b.tokenRevokedBeforeSeq[key]; revokedSeq > 0 {
		authSeq, _ := claims[claimAuthSeq].(float64)
		if int64(authSeq) <= revokedSeq {
			return nil, false
		}
	} else if revokedBefore, ok2 := b.tokenRevokedBefore[key]; ok2 {
		authTime, _ := claims[claimAuthTime].(float64)
		if time.Unix(int64(authTime), 0).Before(revokedBefore) {
			return nil, false
		}
	}

	return u, true
}

// tokenHeaderKID returns the unverified "kid" JOSE header of a JWT, or "" if absent or malformed.
func tokenHeaderKID(token string) string {
	seg, _, ok := strings.Cut(token, ".")
	if !ok {
		return ""
	}

	raw, err := base64.RawURLEncoding.DecodeString(seg)
	if err != nil {
		return ""
	}

	var hdr struct {
		Kid string `json:"kid"`
	}
	if json.Unmarshal(raw, &hdr) != nil {
		return ""
	}

	return hdr.Kid
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

// issueTokensLocked issues tokens for a confirmed user; it releases the caller's write lock
// around triggers and signing, so state read before the call may be stale.
func (b *InMemoryBackend) issueTokensLocked(
	pool *UserPool, clientID string, user *User, triggerSource string, cm map[string]string,
) (*AuthResult, error) {
	return b.issueScopedTokensLocked(
		pool, clientID, user, triggerSource, tokenGrant{storeRefresh: true, clientMetadata: cm},
	)
}

// grantScopes is the scope set a grant will issue: explicit scopes, else the client's allowed scopes.
func (b *InMemoryBackend) grantScopes(clientID string, scopes []string) []string {
	if scopes != nil {
		return scopes
	}

	return b.resolveClientTokenSettings(clientID).scopes
}

// tokenGrant carries the OAuth specifics of a token issuance.
type tokenGrant struct {
	clientMetadata map[string]string
	nonce          string
	scopes         []string
	storeRefresh   bool
}

// issueScopedTokensLocked is issueTokensLocked with an explicit OAuth scope set (nil means the
// client's AllowedOAuthScopes) and an option to skip registering the refresh token.
func (b *InMemoryBackend) issueScopedTokensLocked(
	pool *UserPool, clientID string, user *User, triggerSource string, grant tokenGrant,
) (*AuthResult, error) {
	scopes, storeRefresh := grant.scopes, grant.storeRefresh
	groups := b.userGroupsLocked(pool.ID, user.Username)

	eventScopes := strings.Fields(resolveAccessScope(b.grantScopes(clientID, scopes)))

	overrides, err := b.preTokenGenerationOverrideAuth(
		pool, clientID, user, groups, eventScopes, triggerSource, grant.clientMetadata,
	)
	if err != nil {
		return nil, err
	}

	// PostAuthentication fires after PreTokenGeneration and never on token refresh
	// (InitiateAuthRefreshToken does not come through here), matching AWS.
	if postAuthErr := b.postAuthenticationNotify(pool, clientID, user, grant.clientMetadata); postAuthErr != nil {
		return nil, postAuthErr
	}

	if curErr := b.authUserCurrentLocked(pool, user); curErr != nil {
		return nil, curErr
	}

	now := time.Now()
	user.LastAuthTime = now
	b.tokenSeq++

	seq := b.tokenSeq
	revokeKey := pool.ID + ":" + user.Username
	settings := b.resolveClientTokenSettings(clientID)
	if scopes != nil {
		settings.scopes = scopes
	}

	params := TokenParams{
		ClientID:          clientID,
		Username:          user.Username,
		UserSub:           user.Sub,
		Groups:            b.userGroupsLocked(pool.ID, user.Username),
		AuthTime:          now.Unix(),
		AuthSeq:           seq,
		Scopes:            settings.scopes,
		Attributes:        maps.Clone(user.Attributes),
		AccessTokenExpiry: settings.accessTokenExpiry,
		IDTokenExpiry:     settings.idTokenExpiry,
		Nonce:             grant.nonce,
	}
	if storeRefresh {
		params.OriginJTI = uuid.NewString()
	}

	overrides.useTo(&params)

	var (
		tokens  *TokenResult
		signErr error
	)

	b.releaseLocked("IssueTokens", func() { tokens, signErr = pool.issuer.Issue(params) })

	if signErr != nil {
		return nil, fmt.Errorf("issuing tokens: %w", signErr)
	}

	if curErr := b.authUserCurrentLocked(pool, user); curErr != nil {
		return nil, curErr
	}

	// A sign-out that landed while signing already covers seq; storing the refresh
	// token now would let it outlive that sign-out.
	if revoked, ok := b.tokenRevokedBeforeSeq[revokeKey]; ok && seq <= revoked {
		return nil, fmt.Errorf("%w: user %q was signed out during authentication", ErrNotAuthorized, user.Username)
	}

	if !storeRefresh {
		tokens.RefreshToken = ""

		return &AuthResult{Tokens: tokens}, nil
	}

	b.storeRefreshTokenLocked(tokens.RefreshToken, &refreshTokenEntry{
		ChainID:   params.OriginJTI,
		PoolID:    pool.ID,
		ClientID:  clientID,
		Username:  user.Username,
		Scopes:    scopes,
		AuthTime:  now.Unix(),
		ExpiresAt: now.UTC().Add(settings.refreshTokenTTL),
	})

	return &AuthResult{Tokens: tokens}, nil
}

// InitiateAuthRefreshToken exchanges a valid refresh token for new ID/Access tokens.
func (b *InMemoryBackend) InitiateAuthRefreshToken(
	clientID, refreshToken string, meta ...ClientMetadata,
) (*TokenResult, error) {
	return b.exchangeRefreshToken(clientID, refreshToken, refreshOpts{clientMetadata: firstMetadata(meta)})
}

// GetTokensFromRefreshToken mirrors the GetTokensFromRefreshToken API: it checks the client
// secret and rotates the refresh token only when the client enables RefreshTokenRotation.
func (b *InMemoryBackend) GetTokensFromRefreshToken(
	clientID, refreshToken, clientSecret string, meta ...ClientMetadata,
) (*TokenResult, error) {
	return b.exchangeRefreshToken(clientID, refreshToken, refreshOpts{
		verifySecret:   true,
		clientSecret:   clientSecret,
		honorRotation:  true,
		clientMetadata: firstMetadata(meta),
	})
}

type refreshOpts struct {
	clientMetadata map[string]string
	clientSecret   string
	verifySecret   bool
	honorRotation  bool
}

// liveRefreshEntryLocked returns the unexpired refresh-token entry issued to clientID, evicting an expired one.
func (b *InMemoryBackend) liveRefreshEntryLocked(refreshToken, clientID string) (*refreshTokenEntry, error) {
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

	if !entry.RetiredUntil.IsZero() && !entry.RetiredUntil.After(time.Now().UTC()) {
		return nil, fmt.Errorf("%w: refresh token was invalidated by refresh token rotation", ErrRefreshTokenReuse)
	}

	return entry, nil
}

// finishRefreshLocked rotates the refresh token, or withholds a new one when rotation is off.
func (b *InMemoryBackend) finishRefreshLocked(
	tokens *TokenResult, oldToken string, entry *refreshTokenEntry, expiresAt time.Time, rotate bool,
	grace time.Duration, retire bool,
) {
	if !rotate {
		tokens.RefreshToken = ""

		return
	}

	if !entry.RetiredUntil.IsZero() || retire {
		next := *entry
		next.ExpiresAt = expiresAt
		next.RetiredUntil = time.Time{}
		b.storeRefreshTokenLocked(tokens.RefreshToken, &next)

		if entry.RetiredUntil.IsZero() {
			entry.RetiredUntil = time.Now().UTC().Add(grace)
		}

		return
	}

	b.deleteRefreshTokenLocked(oldToken)
	entry.ExpiresAt = expiresAt
	b.storeRefreshTokenLocked(tokens.RefreshToken, entry)
}

// refreshRetryGrace is the client's refresh-token-rotation RetryGracePeriodSeconds, or zero.
func refreshRetryGrace(client *UserPoolClient) time.Duration {
	switch v := client.RefreshTokenRotation["RetryGracePeriodSeconds"].(type) {
	case float64:
		return time.Duration(v) * time.Second
	case int:
		return time.Duration(v) * time.Second
	case int32:
		return time.Duration(v) * time.Second
	case int64:
		return time.Duration(v) * time.Second
	default:
		return 0
	}
}

// refreshRotationLocked verifies the client secret when asked and reports whether the refresh token rotates.
func (b *InMemoryBackend) refreshRotationLocked(clientID string, opts refreshOpts) (bool, error) {
	if !opts.verifySecret {
		return true, nil
	}

	client, err := b.checkClientSecretLocked(clientID, opts.clientSecret, ErrNotAuthorized)
	if err != nil {
		return false, err
	}

	return !opts.honorRotation || rotationEnabled(client), nil
}

// checkClientSecretLocked returns the client when secret matches its client secret (or it has none).
func (b *InMemoryBackend) checkClientSecretLocked(clientID, secret string, mismatch error) (*UserPoolClient, error) {
	client, found := b.clients.Get(clientID)
	if !found {
		return nil, fmt.Errorf("%w: client %q not found", ErrClientNotFound, clientID)
	}

	if client.ClientSecret != "" && subtle.ConstantTimeCompare([]byte(client.ClientSecret), []byte(secret)) != 1 {
		return nil, fmt.Errorf("%w: unable to verify client secret", mismatch)
	}

	return client, nil
}

func rotationEnabled(client *UserPoolClient) bool {
	feature, _ := client.RefreshTokenRotation["Feature"].(string)

	return feature == "ENABLED"
}

// refreshSubjectLocked resolves the pool and enabled user a refresh token belongs to.
func (b *InMemoryBackend) refreshSubjectLocked(entry *refreshTokenEntry) (*UserPool, *User, error) {
	pool, ok := b.pools.Get(entry.PoolID)
	if !ok {
		return nil, nil, fmt.Errorf("%w: user pool %q not found", ErrUserPoolNotFound, entry.PoolID)
	}

	user, ok := b.users.Get(userKey(entry.PoolID, entry.Username))
	if !ok {
		return nil, nil, fmt.Errorf("%w: user %q not found", ErrUserNotFound, entry.Username)
	}

	if !user.Enabled {
		return nil, nil, fmt.Errorf("%w: user %q account is disabled", ErrNotAuthorized, entry.Username)
	}

	return pool, user, nil
}

// retryGraceLocked is the client's refresh-token retry grace period when the exchange retires the old token.
func (b *InMemoryBackend) retryGraceLocked(clientID string, retire bool) time.Duration {
	if !retire {
		return 0
	}

	if client, found := b.clients.Get(clientID); found {
		return refreshRetryGrace(client)
	}

	return 0
}

func (b *InMemoryBackend) exchangeRefreshToken(
	clientID, refreshToken string,
	opts refreshOpts,
) (*TokenResult, error) {
	b.mu.Lock("InitiateAuthRefreshToken")
	defer b.mu.Unlock()

	if refreshToken == "" {
		return nil, fmt.Errorf("%w: Missing required parameter REFRESH_TOKEN", ErrInvalidParameter)
	}

	rotate, err := b.refreshRotationLocked(clientID, opts)
	if err != nil {
		return nil, err
	}

	entry, err := b.liveRefreshEntryLocked(refreshToken, clientID)
	if err != nil {
		if !opts.honorRotation && errors.Is(err, ErrRefreshTokenReuse) {
			return nil, fmt.Errorf("%w: Invalid Refresh Token", ErrNotAuthorized)
		}

		return nil, err
	}

	pool, user, err := b.refreshSubjectLocked(entry)
	if err != nil {
		return nil, err
	}

	now := time.Now()
	groups := b.userGroupsLocked(entry.PoolID, user.Username)
	settings := b.resolveClientTokenSettings(clientID)
	if len(entry.Scopes) > 0 {
		settings.scopes = entry.Scopes
	}

	// Preserve the original authentication time across refresh; AWS Cognito
	// does not reset auth_time on REFRESH_TOKEN_AUTH. Legacy entries minted
	// before AuthTime was tracked fall back to the refresh moment.
	authTime := entry.AuthTime
	if authTime == 0 {
		authTime = now.Unix()
		entry.AuthTime = authTime
	}

	overrides, err := b.preTokenGenerationOverrideAuth(
		pool, clientID, user, groups, strings.Fields(resolveAccessScope(settings.scopes)),
		triggerSourceTokenGenRefreshTokens, opts.clientMetadata,
	)
	if err != nil {
		return nil, err
	}

	if curErr := b.refreshStillValidLocked(pool, user, refreshToken, entry); curErr != nil {
		return nil, curErr
	}

	b.tokenSeq++

	seq := b.tokenSeq
	params := TokenParams{
		ClientID:          clientID,
		Username:          user.Username,
		UserSub:           user.Sub,
		Groups:            b.userGroupsLocked(entry.PoolID, user.Username),
		AuthTime:          authTime,
		AuthSeq:           seq,
		Scopes:            settings.scopes,
		AccessTokenExpiry: settings.accessTokenExpiry,
		IDTokenExpiry:     settings.idTokenExpiry,
		OriginJTI:         entry.ChainID,
	}
	overrides.useTo(&params)

	var (
		tokens  *TokenResult
		signErr error
	)

	b.releaseLocked("RefreshIssue", func() { tokens, signErr = pool.issuer.Issue(params) })

	if signErr != nil {
		return nil, fmt.Errorf("issuing tokens: %w", signErr)
	}

	if commitErr := b.commitRefreshLocked(pool, user, refreshToken, entry, seq); commitErr != nil {
		return nil, commitErr
	}

	retire := opts.honorRotation && rotate

	b.finishRefreshLocked(
		tokens, refreshToken, entry, now.UTC().Add(settings.refreshTokenTTL), rotate,
		b.retryGraceLocked(clientID, retire), retire,
	)

	return tokens, nil
}

// commitRefreshLocked re-validates after signing and rejects a refresh that a
// sign-out (seq already revoked) or token revocation overtook.
func (b *InMemoryBackend) commitRefreshLocked(
	pool *UserPool, user *User, token string, entry *refreshTokenEntry, seq int64,
) error {
	if err := b.refreshStillValidLocked(pool, user, token, entry); err != nil {
		return err
	}

	if revoked, found := b.tokenRevokedBeforeSeq[pool.ID+":"+user.Username]; found && seq <= revoked {
		return fmt.Errorf("%w: user %q was signed out during refresh", ErrNotAuthorized, user.Username)
	}

	return nil
}

// refreshStillValidLocked re-checks, after b.mu was released, that the refresh token
// is still the live entry and its user is still allowed to sign in.
func (b *InMemoryBackend) refreshStillValidLocked(
	pool *UserPool, user *User, token string, entry *refreshTokenEntry,
) error {
	if err := b.authUserCurrentLocked(pool, user); err != nil {
		return err
	}

	if cur, ok := b.refreshTokens[token]; !ok || cur != entry {
		return fmt.Errorf("%w: refresh token not found or expired", ErrNotAuthorized)
	}

	return nil
}

// RevokeToken revokes a refresh token, preventing further use.
func (b *InMemoryBackend) RevokeToken(token, clientID string) error {
	return b.revokeToken(token, clientID, nil)
}

// RevokeTokenWithSecret is RevokeToken plus the client-secret check the RevokeToken API performs.
func (b *InMemoryBackend) RevokeTokenWithSecret(token, clientID, clientSecret string) error {
	return b.revokeToken(token, clientID, &clientSecret)
}

func (b *InMemoryBackend) revokeToken(token, clientID string, clientSecret *string) error {
	b.mu.Lock("RevokeToken")
	defer b.mu.Unlock()

	if clientSecret != nil {
		if _, err := b.checkClientSecretLocked(clientID, *clientSecret, ErrTokenUnauthorized); err != nil {
			return err
		}
	}

	entry, ok := b.refreshTokens[token]
	if !ok {
		// AWS Cognito silently succeeds when revoking an already-revoked/unknown token.
		return nil
	}

	if entry.ClientID != clientID {
		return fmt.Errorf("%w: token was issued for a different client", ErrTokenUnauthorized)
	}

	b.revokeChainLocked(entry)
	b.deleteRefreshTokenLocked(token)

	return nil
}

// revokeChainLocked invalidates every refresh token and access token minted from entry's chain.
func (b *InMemoryBackend) revokeChainLocked(entry *refreshTokenEntry) {
	if entry.ChainID == "" {
		return
	}

	b.revokedChains[entry.ChainID] = time.Now().UTC().Add(maxAccessTokenLifetime)

	for token := range b.refreshTokensByUser[entry.PoolID+":"+entry.Username] {
		if other := b.refreshTokens[token]; other != nil && other.ChainID == entry.ChainID {
			b.deleteRefreshTokenLocked(token)
		}
	}
}

// maxAccessTokenLifetime is Cognito's longest access token validity (one day).
const maxAccessTokenLifetime = 24 * time.Hour

// EvictExpiredRevokedChains drops revoked-chain markers whose access tokens have all expired.
func (b *InMemoryBackend) EvictExpiredRevokedChains() int {
	b.mu.Lock("EvictExpiredRevokedChains")
	defer b.mu.Unlock()

	now := time.Now().UTC()
	n := 0

	for id, until := range b.revokedChains {
		if !until.After(now) {
			delete(b.revokedChains, id)

			n++
		}
	}

	return n
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

	key := userPoolID + ":" + username
	b.tokenRevokedBeforeSeq[key] = b.tokenSeq
	b.tokenRevokedBefore[key] = time.Now().UTC()
	b.dropHostedSessionsLocked(userPoolID, username)

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

	key := user.UserPoolID + ":" + user.Username
	b.tokenRevokedBeforeSeq[key] = b.tokenSeq
	b.tokenRevokedBefore[key] = time.Now().UTC()
	b.dropHostedSessionsLocked(user.UserPoolID, user.Username)

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

	b.maybeEvictExpiredRefreshTokensLocked()
}

// maybeEvictExpiredRefreshTokensLocked drops expired, never-refreshed tokens
// once the table is large (same pattern as sts). Caller holds b.mu.
func (b *InMemoryBackend) maybeEvictExpiredRefreshTokensLocked() {
	if len(b.refreshTokens) < refreshTokenEvictThreshold {
		return
	}

	b.refreshTokenInsertsSinceSweep++
	if b.refreshTokenInsertsSinceSweep < refreshTokenEvictSweepInterval {
		return
	}

	b.refreshTokenInsertsSinceSweep = 0

	now := time.Now().UTC()
	for token, entry := range b.refreshTokens {
		if !entry.ExpiresAt.IsZero() && !entry.ExpiresAt.After(now) {
			b.deleteRefreshTokenLocked(token)
		}
	}
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
