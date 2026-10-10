package cognitoidp

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const (
	// authCodeTTL: the authorization code is valid for five minutes (authorization-endpoint docs).
	authFlowUserPassword = "USER_PASSWORD_AUTH"
	authCodeTTL          = 5 * time.Minute
	maxAuthCodes         = 10000
	authCodeBytes        = 24
	claimVersion         = "version"
	// clientCredentialsTokenVersion is the "version" claim on M2M access tokens.
	clientCredentialsTokenVersion = 2
	scopeOpenID                   = "openid"
	scopeProfile                  = "profile"
	scopeEmail                    = "email"
	scopePhone                    = "phone"
	// jtiBytes is the random length of a client-credentials token's jti.
	jtiBytes = 16
)

var (
	errHostedChallenge = errors.New("additional authentication challenge required")
	errInsufficient    = errors.New("access token lacks the openid scope")
	errAuthCodeInvalid = errors.New("authorization code invalid")
)

// authCodeEntry is an ephemeral, never-persisted authorization code grant.
type authCodeEntry struct {
	ExpiresAt     time.Time
	PoolID        string
	ClientID      string
	Username      string
	RedirectURI   string
	CodeChallenge string
	Nonce         string
	Scopes        []string
}

func (b *InMemoryBackend) oauthClient(clientID string) (*UserPoolClient, bool) {
	b.mu.RLock("OAuthClient")
	defer b.mu.RUnlock()

	c, ok := b.clients.Get(clientID)
	if !ok {
		return nil, false
	}

	cp := *c

	return &cp, true
}

// oauthLogin verifies credentials for the hosted login page; it is not reachable through any wire
// AuthFlow value, so ExplicitAuthFlows (an API-flow setting) does not gate it.
func (b *InMemoryBackend) oauthLogin(clientID, username, password string) (hostedLogin, error) {
	b.mu.Lock("OAuthLogin")
	defer b.mu.Unlock()

	client, ok := b.clients.Get(clientID)
	if !ok {
		return hostedLogin{}, fmt.Errorf("%w: client %q not found", ErrClientNotFound, clientID)
	}

	pool, ok := b.pools.Get(client.UserPoolID)
	if !ok {
		return hostedLogin{}, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, client.UserPoolID)
	}

	user, finalStatus, err := b.hostedLoginUserLocked(pool, client, username, password)
	if err != nil {
		return hostedLogin{}, err
	}

	if err = b.precheckUserLocked(pool, clientID, user, nil); err != nil {
		return hostedLogin{}, err
	}

	if err = b.verifyPasswordLocked(pool, user, password); err != nil {
		return hostedLogin{}, err
	}

	b.applyPostMigrationFinalStatus(pool.ID, username, finalStatus)

	return b.hostedChallengeLocked(pool, clientID, user)
}

// hostedLoginUserLocked finds the user, falling back to the UserMigration trigger like USER_PASSWORD_AUTH.
func (b *InMemoryBackend) hostedLoginUserLocked(
	pool *UserPool, client *UserPoolClient, username, password string,
) (*User, string, error) {
	if user, ok := b.users.Get(userKey(pool.ID, username)); ok {
		return user, "", nil
	}

	migrated, finalStatus, err := b.tryUserMigration(
		pool, client.ClientID, authFlowUserPassword, username, password, nil,
	)
	if err != nil {
		return nil, "", err
	}

	if migrated == nil {
		return nil, "", unknownUserAuthError(client, username)
	}

	return migrated, finalStatus, nil
}

func (b *InMemoryBackend) storeAuthCode(entry *authCodeEntry) (string, error) {
	raw := make([]byte, authCodeBytes)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generating authorization code: %w", err)
	}

	code := base64.RawURLEncoding.EncodeToString(raw)

	b.mu.Lock("StoreAuthCode")
	defer b.mu.Unlock()

	now := time.Now()
	if len(b.authCodes) >= maxAuthCodes {
		b.sweepAuthCodesLocked(now)
	}

	if len(b.authCodes) >= maxAuthCodes {
		b.evictOldestAuthCodeLocked()
	}

	entry.ExpiresAt = now.Add(authCodeTTL)
	b.authCodes[code] = entry

	return code, nil
}

func (b *InMemoryBackend) sweepAuthCodesLocked(now time.Time) {
	for k, e := range b.authCodes {
		if !e.ExpiresAt.After(now) {
			delete(b.authCodes, k)
		}
	}
}

func (b *InMemoryBackend) evictOldestAuthCodeLocked() {
	var (
		oldestKey string
		oldest    time.Time
	)

	for k, e := range b.authCodes {
		if oldestKey == "" || e.ExpiresAt.Before(oldest) {
			oldestKey, oldest = k, e.ExpiresAt
		}
	}

	delete(b.authCodes, oldestKey)
}

// consumeAuthCode removes the code unconditionally so it can never be redeemed twice.
func (b *InMemoryBackend) consumeAuthCode(code string) (*authCodeEntry, error) {
	b.mu.Lock("ConsumeAuthCode")
	defer b.mu.Unlock()

	e, ok := b.authCodes[code]
	if !ok {
		return nil, errAuthCodeInvalid
	}

	delete(b.authCodes, code)

	if !e.ExpiresAt.After(time.Now()) {
		return nil, errAuthCodeInvalid
	}

	return e, nil
}

func (b *InMemoryBackend) issueOAuthTokens(
	poolID, username, clientID string, grant tokenGrant,
) (*TokenResult, error) {
	b.mu.Lock("IssueOAuthTokens")
	defer b.mu.Unlock()

	pool, ok := b.pools.Get(poolID)
	if !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, poolID)
	}

	user, ok := b.users.Get(userKey(poolID, username))
	if !ok {
		return nil, fmt.Errorf("%w: user %q not found", ErrUserNotFound, username)
	}

	if _, ok = b.clients.Get(clientID); !ok {
		return nil, fmt.Errorf("%w: client %q not found", ErrClientNotFound, clientID)
	}

	grant.scopes = slices.Clone(grant.scopes)

	res, err := b.issueScopedTokensLocked(pool, clientID, user, triggerSourceTokenGenHostedAuth, grant)
	if err != nil {
		return nil, err
	}

	return res.Tokens, nil
}

// oauthRefresh runs the refresh grant and returns the scopes the session was granted.
func (b *InMemoryBackend) oauthRefresh(clientID, refreshToken string) (*TokenResult, []string, error) {
	b.mu.RLock("OAuthRefresh")

	var scopes []string
	if e, ok := b.refreshTokens[refreshToken]; ok {
		scopes = slices.Clone(e.Scopes)
	}

	if len(scopes) == 0 {
		if c, ok := b.clients.Get(clientID); ok {
			scopes = slices.Clone(c.AllowedOAuthScopes)
		}
	}

	b.mu.RUnlock()

	tokens, err := b.InitiateAuthRefreshToken(clientID, refreshToken)
	if err != nil {
		return nil, nil, err
	}

	return tokens, scopes, nil
}

// issueClientCredentialsToken mints the access-token-only M2M token for a client.
func (b *InMemoryBackend) issueClientCredentialsToken(
	clientID string, scopes []string, metadata map[string]string,
) (string, int32, error) {
	b.mu.RLock("IssueClientCredentialsToken")

	client, ok := b.clients.Get(clientID)
	if !ok {
		b.mu.RUnlock()

		return "", 0, fmt.Errorf("%w: client %q not found", ErrClientNotFound, clientID)
	}

	pool, ok := b.pools.Get(client.UserPoolID)
	if !ok {
		b.mu.RUnlock()

		return "", 0, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, client.UserPoolID)
	}

	expiry := time.Duration(tokenExpirySeconds) * time.Second
	if d := tokenExpiryFor(client, "AccessToken"); d > 0 {
		expiry = d
	}

	issuer := pool.issuer
	call := b.prepareM2MTrigger(pool, clientID, scopes, metadata)

	b.mu.RUnlock()

	var override m2mOverride

	if call != nil {
		var trigErr error
		if override, trigErr = runM2MTrigger(call); trigErr != nil {
			return "", 0, trigErr
		}
	}

	scope := ""
	if granted := override.applyScopes(scopes); len(granted) > 0 {
		scope = resolveAccessScope(granted)
	}

	tok, err := issuer.signClientCredentialsToken(clientID, scope, time.Now(), expiry, override)
	if err != nil {
		return "", 0, err
	}

	return tok, int32(expiry.Seconds()), nil
}

func (t *tokenIssuer) signClientCredentialsToken(
	clientID, scope string, now time.Time, expiry time.Duration, override m2mOverride,
) (string, error) {
	jti := make([]byte, jtiBytes)
	if _, err := rand.Read(jti); err != nil {
		return "", fmt.Errorf("generating jti: %w", err)
	}

	claims := jwt.MapClaims{
		claimSub:      clientID,
		claimIss:      t.issuerURL,
		claimClientID: clientID,
		claimTokenUse: tokenUseAccess,
		claimScope:    scope,
		claimVersion:  clientCredentialsTokenVersion,
		claimJTI:      base64.RawURLEncoding.EncodeToString(jti),
		claimIat:      now.Unix(),
		claimExp:      now.Add(expiry).Unix(),
		claimAuthTime: now.Unix(),
	}
	if scope == "" {
		delete(claims, claimScope)
	}

	override.applyClaims(claims)

	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = t.keyID

	signed, err := tok.SignedString(t.privateKey)
	if err != nil {
		return "", fmt.Errorf("signing client credentials token: %w", err)
	}

	return signed, nil
}

// scopeKnown reports whether scope is a reserved scope or a resource-server scope of the pool.
func (b *InMemoryBackend) scopeKnown(poolID, scope string) bool {
	switch scope {
	case scopeOpenID, scopeProfile, scopeEmail, scopePhone, defaultAccessScope:
		return true
	}

	b.mu.RLock("ScopeKnown")
	defer b.mu.RUnlock()

	for _, rs := range b.resourceServersByPool.Get(poolID) {
		for _, s := range rs.Scopes {
			if rs.Identifier+"/"+s.ScopeName == scope {
				return true
			}
		}
	}

	return false
}

// domainPoolID resolves a Host header to the pool owning a registered user pool domain.
func (b *InMemoryBackend) domainPoolID(host string) (string, bool) {
	host = strings.ToLower(host)
	if h, _, found := strings.Cut(host, ":"); found {
		host = h
	}

	b.mu.RLock("DomainPoolID")
	defer b.mu.RUnlock()

	if d, ok := b.domains.Get(host); ok {
		return d.UserPoolID, true
	}

	if !strings.HasSuffix(host, ".amazoncognito.com") && !strings.HasSuffix(host, ".localhost") {
		return "", false
	}

	prefix, _, _ := strings.Cut(host, ".")
	if d, ok := b.domains.Get(prefix); ok {
		return d.UserPoolID, true
	}

	return "", false
}

type oidcConfiguration struct {
	Issuer                string   `json:"issuer"`
	AuthorizationEndpoint string   `json:"authorization_endpoint"`
	TokenEndpoint         string   `json:"token_endpoint"`
	UserinfoEndpoint      string   `json:"userinfo_endpoint"`
	JWKSURI               string   `json:"jwks_uri"`
	ScopesSupported       []string `json:"scopes_supported"`
	ResponseTypes         []string `json:"response_types_supported"`
	ResponseModes         []string `json:"response_modes_supported"`
	GrantTypes            []string `json:"grant_types_supported"`
	AuthMethods           []string `json:"token_endpoint_auth_methods_supported"`
	SubjectTypes          []string `json:"subject_types_supported"`
	IDTokenSigningAlgs    []string `json:"id_token_signing_alg_values_supported"`
	ClaimTypes            []string `json:"claim_types_supported"`
	ClaimsSupported       []string `json:"claims_supported"`
}

// oidcDiscovery builds the discovery document; issuer is the exact iss claim of this pool's tokens.
func (b *InMemoryBackend) oidcDiscovery(poolID string) (*oidcConfiguration, error) {
	b.mu.RLock("OIDCDiscovery")
	defer b.mu.RUnlock()

	pool, ok := b.pools.Get(poolID)
	if !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, poolID)
	}

	base := strings.TrimRight(b.endpoint, "/")

	return &oidcConfiguration{
		Issuer:                pool.issuer.issuerURL,
		AuthorizationEndpoint: base + pathOAuthAuthorize,
		TokenEndpoint:         base + pathOAuthToken,
		UserinfoEndpoint:      base + pathOAuthUserInfo,
		JWKSURI:               pool.issuer.issuerURL + jwksPathSuffix,
		ScopesSupported:       []string{scopePhone, scopeEmail, scopeOpenID, scopeProfile},
		ResponseTypes:         []string{"code", "token"},
		ResponseModes:         []string{"query", "fragment"},
		GrantTypes:            []string{"authorization_code", "implicit", "refresh_token", "client_credentials"},
		AuthMethods:           []string{"client_secret_basic", "client_secret_post"},
		SubjectTypes:          []string{"public"},
		IDTokenSigningAlgs:    []string{"RS256"},
		ClaimTypes:            []string{"normal"},
		ClaimsSupported:       strings.Fields("sub iss aud exp iat email phone_number name preferred_username"),
	}, nil
}

func profileClaims() []string {
	return strings.Fields("name family_name given_name middle_name nickname preferred_username profile " +
		"picture website gender birthdate zoneinfo locale updated_at")
}

func emailClaims() []string { return strings.Fields("email email_verified") }

func phoneClaims() []string { return strings.Fields("phone_number phone_number_verified") }

// oauthUserInfo returns the userInfo claims allowed by the access token's scopes.
func (b *InMemoryBackend) oauthUserInfo(accessToken string) (map[string]string, error) {
	b.mu.RLock("OAuthUserInfo")
	defer b.mu.RUnlock()

	user, err := b.findUserByAccessTokenLocked(accessToken)
	if err != nil {
		return nil, err
	}

	pool, ok := b.pools.Get(user.UserPoolID)
	if !ok || !user.Enabled {
		return nil, fmt.Errorf("%w: access token is invalid", ErrNotAuthorized)
	}

	claims, err := pool.issuer.ParseAccessToken(accessToken)
	if err != nil {
		return nil, err
	}

	scope, _ := claims[claimScope].(string)
	scopes := strings.Fields(scope)

	if !slices.Contains(scopes, scopeOpenID) {
		return nil, errInsufficient
	}

	clientID, _ := claims[claimClientID].(string)
	client, _ := b.clients.Get(clientID)

	return userInfoClaims(user, client, scopes), nil
}

func userInfoClaims(user *User, client *UserPoolClient, scopes []string) map[string]string {
	allowed := func(attr string) bool {
		if attr == claimSub {
			return false
		}

		return client == nil || len(client.ReadAttributes) == 0 || slices.Contains(client.ReadAttributes, attr)
	}

	var want func(string) bool

	if len(scopes) == 1 {
		want = func(string) bool { return true }
	} else {
		want = func(attr string) bool {
			return slices.Contains(profileClaims(), attr) || strings.HasPrefix(attr, "custom:") ||
				(slices.Contains(scopes, scopeEmail) && slices.Contains(emailClaims(), attr)) ||
				(slices.Contains(scopes, scopePhone) && slices.Contains(phoneClaims(), attr))
		}
	}

	out := map[string]string{"sub": user.Sub, "username": user.Username}

	for attr, v := range user.Attributes {
		if allowed(attr) && want(attr) {
			out[attr] = v
		}
	}

	return out
}

// customScopesFor lists every resource-server scope the client may use, sorted.
func (b *InMemoryBackend) customScopesFor(client *UserPoolClient) []string {
	var out []string

	for _, s := range client.AllowedOAuthScopes {
		if b.scopeKnown(client.UserPoolID, s) && strings.Contains(s, "/") {
			out = append(out, s)
		}
	}

	sort.Strings(out)

	return out
}
