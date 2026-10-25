package signin

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

const (
	grantAuthorizationCode = "authorization_code"
	grantRefreshToken      = "refresh_token"
	grantClientCredentials = "client_credentials"
	tokenTypeSigV4         = "aws_sigv4"
	tokenTypeBearer        = "Bearer"
	tokenKindAccess        = "access_token"
	tokenKindRefresh       = "refresh_token"
	refreshTokenPrefix     = "ASOR"
	issuer                 = "signin.amazonaws.com"
	pkceMethodS256         = "S256"

	authCodeTTL    = 10 * time.Minute
	credentialsTTL = 15 * time.Minute
	refreshTTL     = 12 * time.Hour
	iamTokenTTL    = time.Hour

	minVerifierLen = 43
	maxVerifierLen = 128
	keyIDLen       = 16
	secretKeyLen   = 40
	textLen        = 26
)

// TokenRequest is the CreateOAuth2Token body.
type TokenRequest struct {
	ClientID     string
	GrantType    string
	Code         string
	CodeVerifier string
	RedirectURI  string
	RefreshToken string
}

// TokenResult is the CreateOAuth2Token result.
type TokenResult struct {
	AccessKeyID  string
	SecretKey    string
	SessionToken string
	RefreshToken string
	IDToken      string
	ExpiresIn    int32
}

// IAMTokenResult is the CreateOAuth2TokenWithIAM result.
type IAMTokenResult struct {
	AccessToken string
	ExpiresIn   int32
}

// Introspection is the RFC 7662 result; only Active is set for an inactive token.
type Introspection struct {
	Active    bool
	Account   string
	Aud       string
	ClientID  string
	Exp       int64
	Iat       int64
	Iss       string
	Jti       string
	Nbf       int64
	Resource  string
	Session   string
	Sub       string
	TokenType string
	UserID    string
}

func randText(n int) string {
	var sb strings.Builder

	for sb.Len() < n {
		sb.WriteString(rand.Text())
	}

	return sb.String()[:n]
}

func (b *InMemoryBackend) sign(payload string) string {
	m := hmac.New(sha256.New, b.signingKey)
	m.Write([]byte(payload))

	return base64.RawURLEncoding.EncodeToString(m.Sum(nil))
}

func (b *InMemoryBackend) makeJWT(claims map[string]any) string {
	enc := base64.RawURLEncoding
	head := enc.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	raw, _ := json.Marshal(claims)
	body := enc.EncodeToString(raw)

	return head + "." + body + "." + b.sign(head+"."+body)
}

// parseJWT verifies the signature and returns the jti claim.
func (b *InMemoryBackend) parseJWT(tok string) (string, bool) {
	parts := strings.Split(tok, ".")
	if len(parts) != 3 { //nolint:mnd // header.payload.signature
		return "", false
	}

	if subtle.ConstantTimeCompare([]byte(b.sign(parts[0]+"."+parts[1])), []byte(parts[2])) != 1 {
		return "", false
	}

	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", false
	}

	var claims struct {
		JTI string `json:"jti"`
	}

	if json.Unmarshal(raw, &claims) != nil || claims.JTI == "" {
		return "", false
	}

	return claims.JTI, true
}

func (b *InMemoryBackend) sweepLocked() {
	now := b.now()

	for _, c := range b.authCodes.All() {
		if now.After(c.ExpiresAt.Add(authCodeTTL)) {
			b.authCodes.Delete(c.Code)
		}
	}

	for _, s := range b.sessions.All() {
		if now.After(s.RefreshExpiresAt.Add(refreshTTL)) {
			b.sessions.Delete(s.RefreshToken)
		}
	}

	for _, t := range b.accessTokens.All() {
		if now.After(t.ExpiresAt) {
			b.accessTokens.Delete(t.JTI)
		}
	}
}

// IssueAuthorizationCode mints a single-use code bound to the caller, client, redirect URI and PKCE challenge.
func (b *InMemoryBackend) IssueAuthorizationCode(
	ctx context.Context, clientID, redirectURI, challenge, method string,
) (string, error) {
	if clientID == "" || redirectURI == "" || challenge == "" {
		return "", validation("client_id, redirect_uri and code_challenge are required")
	}

	if method != pkceMethodS256 {
		return "", validation("code_challenge_method must be S256")
	}

	if u, err := url.Parse(redirectURI); err != nil || u.Scheme == "" || u.Host == "" {
		return "", validation("redirect_uri must be an absolute URL")
	}

	c := b.callerOf(ctx)
	if c.anon {
		return "", accessDenied(oauthInsufficientPerms, 403, "anonymous callers cannot authorize")
	}

	b.mu.Lock("IssueAuthorizationCode")
	defer b.mu.Unlock()

	b.sweepLocked()

	code := randText(textLen * 2)
	b.authCodes.Put(&AuthCode{
		Code: code, ClientID: clientID, RedirectURI: redirectURI, Challenge: challenge,
		Account: c.account, PrincipalArn: c.arn, UserID: c.userID, ExpiresAt: b.now().Add(authCodeTTL),
	})

	return code, nil
}

// CreateOAuth2Token redeems an authorization code or refreshes a session.
func (b *InMemoryBackend) CreateOAuth2Token(req TokenRequest) (*TokenResult, error) {
	if req.ClientID == "" || req.GrantType == "" {
		return nil, validation("clientId and grantType are required")
	}

	b.mu.Lock("CreateOAuth2Token")
	defer b.mu.Unlock()

	b.sweepLocked()

	switch req.GrantType {
	case grantAuthorizationCode:
		return b.redeemCodeLocked(req)
	case grantRefreshToken:
		return b.refreshLocked(req)
	}

	return nil, validation("grantType must be authorization_code or refresh_token")
}

func (b *InMemoryBackend) redeemCodeLocked(req TokenRequest) (*TokenResult, error) {
	if req.Code == "" || req.RedirectURI == "" || req.CodeVerifier == "" {
		return nil, validation("code, redirectUri and codeVerifier are required")
	}

	if n := len(req.CodeVerifier); n < minVerifierLen || n > maxVerifierLen {
		return nil, validation("codeVerifier must be 43 to 128 characters")
	}

	ac, ok := b.authCodes.Get(req.Code)
	if !ok {
		return nil, validation("invalid authorization code")
	}

	if ac.Used {
		if s, found := b.sessions.Get(ac.SessionKey); found {
			s.Revoked = true
		}

		return nil, validation("authorization code already used")
	}

	now := b.now()
	if !now.Before(ac.ExpiresAt) {
		return nil, accessDenied(oauthAuthcodeExpired, 401, "authorization code expired")
	}

	sum := sha256.Sum256([]byte(req.CodeVerifier))
	got := base64.RawURLEncoding.EncodeToString(sum[:])

	if ac.ClientID != req.ClientID || ac.RedirectURI != req.RedirectURI ||
		subtle.ConstantTimeCompare([]byte(got), []byte(ac.Challenge)) != 1 {
		return nil, validation("authorization code does not match the request")
	}

	s := &Session{
		RefreshToken: refreshTokenPrefix + randText(textLen*3), SessionID: uuid.NewString(), Account: ac.Account,
		Region: b.region, PrincipalArn: ac.PrincipalArn, UserID: ac.UserID, ClientID: req.ClientID,
		CreatedAt: now.UTC(), RefreshExpiresAt: now.Add(refreshTTL).UTC(),
	}
	b.mintCredsLocked(s)
	b.sessions.Put(s)
	ac.Used = true
	ac.SessionKey = s.RefreshToken

	res := b.resultOf(s)
	res.IDToken = b.makeJWT(map[string]any{
		"iss": issuer, "sub": s.PrincipalArn, "aud": s.ClientID, "iat": now.Unix(), "exp": now.Add(credentialsTTL).Unix(),
		"account_id": s.Account, "jti": uuid.NewString(),
	})

	return res, nil
}

func (b *InMemoryBackend) refreshLocked(req TokenRequest) (*TokenResult, error) {
	if req.RefreshToken == "" {
		return nil, validation("refreshToken is required")
	}

	s, ok := b.sessions.Get(req.RefreshToken)
	if !ok || s.ClientID != req.ClientID {
		return nil, validation("invalid refresh token")
	}

	now := b.now()

	switch {
	case s.Revoked:
		return nil, accessDenied(oauthCredentialsChanged, 403, "refresh token has been revoked")
	case !now.Before(s.RefreshExpiresAt):
		return nil, accessDenied(oauthTokenExpired, 401, "refresh token expired")
	}

	if !now.Before(s.CredsExpireAt) {
		b.mintCredsLocked(s)
	}

	return b.resultOf(s), nil
}

func (b *InMemoryBackend) mintCredsLocked(s *Session) {
	s.AccessKeyID = "ASIA" + randText(keyIDLen)
	s.SecretKey = randText(secretKeyLen)
	s.SessionToken = randText(textLen * 4) //nolint:mnd // realistic session token length
	s.CredsExpireAt = b.now().Add(credentialsTTL).UTC()
}

func (b *InMemoryBackend) resultOf(s *Session) *TokenResult {
	return &TokenResult{
		AccessKeyID: s.AccessKeyID, SecretKey: s.SecretKey, SessionToken: s.SessionToken,
		RefreshToken: s.RefreshToken, ExpiresIn: int32(s.CredsExpireAt.Sub(b.now()) / time.Second),
	}
}

// CreateOAuth2TokenWithIAM exchanges the caller's identity for a resource-scoped JWT access token.
func (b *InMemoryBackend) CreateOAuth2TokenWithIAM(ctx context.Context, grantType, resource string) (*IAMTokenResult, error) {
	if grantType != grantClientCredentials {
		return nil, validation("grant_type must be client_credentials")
	}

	if resource == "" {
		return nil, validation("resource is required")
	}

	c := b.callerOf(ctx)
	if c.anon {
		return nil, accessDenied(oauthInsufficientPerms, 403, "anonymous callers cannot request tokens")
	}

	b.mu.Lock("CreateOAuth2TokenWithIAM")
	defer b.mu.Unlock()

	b.sweepLocked()

	now := b.now().UTC()
	t := &AccessToken{
		JTI: uuid.NewString(), Account: c.account, Subject: c.arn, UserID: c.userID, Audience: resource,
		SessionArn: arn.Build("signin", c.region, c.account, "session/"+uuid.NewString()),
		IssuedAt:   now, ExpiresAt: now.Add(iamTokenTTL),
	}
	b.accessTokens.Put(t)

	tok := b.makeJWT(map[string]any{
		"iss": issuer, "sub": t.Subject, "aud": t.Audience, "jti": t.JTI, "iat": t.IssuedAt.Unix(),
		"nbf": t.IssuedAt.Unix(), "exp": t.ExpiresAt.Unix(), "account_id": t.Account, "user_id": t.UserID,
		"signin_session": t.SessionArn,
	})

	return &IAMTokenResult{AccessToken: tok, ExpiresIn: int32(iamTokenTTL / time.Second)}, nil
}

// IntrospectOAuth2Token reports the state of an access or refresh token owned by the caller's account.
func (b *InMemoryBackend) IntrospectOAuth2Token(ctx context.Context, token, hint string) (*Introspection, error) {
	if token == "" {
		return nil, validation("token is required")
	}

	if hint != "" && hint != tokenKindAccess && hint != tokenKindRefresh {
		return nil, validation("token_type_hint must be access_token or refresh_token")
	}

	c := b.callerOf(ctx)
	if c.anon {
		return nil, accessDenied(oauthInsufficientPerms, 403, "anonymous callers cannot introspect tokens")
	}

	b.mu.RLock("IntrospectOAuth2Token")
	defer b.mu.RUnlock()

	now := b.now()
	lookups := []func() *Introspection{
		func() *Introspection { return b.introspectAccess(token, c.account, now) },
		func() *Introspection { return b.introspectRefresh(token, c.account, now) },
	}

	if hint == tokenKindRefresh {
		lookups[0], lookups[1] = lookups[1], lookups[0]
	}

	for _, look := range lookups {
		if r := look(); r != nil {
			return r, nil
		}
	}

	return &Introspection{}, nil
}

func (b *InMemoryBackend) introspectAccess(token, account string, now time.Time) *Introspection {
	jti, ok := b.parseJWT(token)
	if !ok {
		return nil
	}

	t, found := b.accessTokens.Get(jti)
	if !found {
		return nil
	}

	if t.Account != account || !now.Before(t.ExpiresAt) || now.Before(t.IssuedAt) {
		return &Introspection{}
	}

	return &Introspection{
		Active: true, Account: t.Account, Aud: t.Audience, Exp: t.ExpiresAt.Unix(),
		Iat: t.IssuedAt.Unix(), Iss: issuer, Jti: t.JTI, Nbf: t.IssuedAt.Unix(), Session: t.SessionArn,
		Sub: t.Subject, TokenType: tokenKindAccess, UserID: t.UserID,
	}
}

func (b *InMemoryBackend) introspectRefresh(token, account string, now time.Time) *Introspection {
	s, ok := b.sessions.Get(token)
	if !ok {
		return nil
	}

	if s.Account != account || s.Revoked || !now.Before(s.RefreshExpiresAt) {
		return &Introspection{}
	}

	return &Introspection{
		Active: true, Account: s.Account, ClientID: s.ClientID, Exp: s.RefreshExpiresAt.Unix(),
		Iat: s.CreatedAt.Unix(), Iss: issuer, Jti: s.SessionID, Nbf: s.CreatedAt.Unix(),
		Session: arn.Build("signin", s.Region, s.Account, "session/"+s.SessionID), Sub: s.PrincipalArn,
		TokenType: tokenKindRefresh, UserID: s.UserID,
	}
}

// RevokeOAuth2Token revokes a refresh token; unknown, expired or foreign tokens succeed silently.
func (b *InMemoryBackend) RevokeOAuth2Token(ctx context.Context, token string) error {
	if token == "" {
		return validation("token is required")
	}

	if !strings.HasPrefix(token, refreshTokenPrefix) {
		return validation("only refresh tokens can be revoked")
	}

	c := b.callerOf(ctx)
	if c.anon {
		return accessDenied(oauthInsufficientPerms, 403, "anonymous callers cannot revoke tokens")
	}

	b.mu.Lock("RevokeOAuth2Token")
	defer b.mu.Unlock()

	if s, ok := b.sessions.Get(token); ok && s.Account == c.account {
		s.Revoked = true
	}

	return nil
}
