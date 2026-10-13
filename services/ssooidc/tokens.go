package ssooidc

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"slices"
	"time"
)

// CreateTokenInput carries the CreateToken and CreateTokenWithIAM request members.
type CreateTokenInput struct {
	ClientID     string
	ClientSecret string
	GrantType    string
	Code         string
	CodeVerifier string
	DeviceCode   string
	RedirectURI  string
	RefreshToken string
}

// CreateToken exchanges a device code, authorization code or refresh token for tokens.
// IAM-authenticated callers (CreateTokenWithIAM) present no client secret.
func (b *InMemoryBackend) CreateToken(in *CreateTokenInput, iamCaller bool) (*TokenResult, *APIError) {
	if in.GrantType == "" {
		return nil, errInvalidRequest("grantType is required")
	}

	now := time.Now().UTC()

	b.mu.Lock("CreateToken")
	defer b.mu.Unlock()

	c, err := b.clientLocked(in.ClientID, in.ClientSecret, !iamCaller, now)
	if err != nil {
		return nil, err
	}

	if !supportedGrant(in.GrantType) {
		return nil, errUnsupportedGrant("Unsupported grant type: " + in.GrantType)
	}

	if iamCaller && in.GrantType == grantDeviceCode {
		return nil, errUnsupportedGrant("CreateTokenWithIAM does not support the device code grant")
	}

	if !c.allowsGrant(in.GrantType) {
		return nil, errUnauthorizedClient("The client is not registered for the " + in.GrantType + " grant")
	}

	defer b.pruneLocked(now)

	switch in.GrantType {
	case grantDeviceCode:
		return b.deviceGrantLocked(c, in, now)
	case grantAuthCode:
		return b.authCodeGrantLocked(c, in, now)
	default:
		return b.refreshGrantLocked(c, in, now)
	}
}

func (b *InMemoryBackend) deviceGrantLocked(c *Client, in *CreateTokenInput, now time.Time) (*TokenResult, *APIError) {
	if in.DeviceCode == "" {
		return nil, errInvalidRequest("deviceCode is required")
	}

	d, ok := b.devices.Get(in.DeviceCode)
	if !ok || d.ClientID != c.ClientID {
		return nil, errInvalidGrant("The device code is invalid")
	}

	if now.After(d.ExpiresAt) {
		b.devices.Delete(d.DeviceCode)

		return nil, errExpired("The device code has expired")
	}

	switch d.Status {
	case deviceDenied:
		b.devices.Delete(d.DeviceCode)

		return nil, errDenied("The user denied the authorization request")
	case devicePending:
		return nil, b.pendingLocked(d, now)
	}

	b.devices.Delete(d.DeviceCode)

	return b.issueLocked(c, d.Subject, d.StartURL, now), nil
}

// pendingLocked answers a poll of a still-pending device: slow_down when it came before Interval elapsed.
func (b *InMemoryBackend) pendingLocked(d *Device, now time.Time) *APIError {
	cp := *d
	cp.LastPolled = now

	defer b.devices.Put(&cp)

	if !d.LastPolled.IsZero() && now.Sub(d.LastPolled) < time.Duration(d.Interval)*time.Second {
		cp.Interval += slowDownIncrement

		return errSlowDown()
	}

	return errPending()
}

func (b *InMemoryBackend) authCodeGrantLocked(
	c *Client,
	in *CreateTokenInput,
	now time.Time,
) (*TokenResult, *APIError) {
	if in.Code == "" || in.RedirectURI == "" || in.CodeVerifier == "" {
		return nil, errInvalidRequest("code, redirectUri and codeVerifier are required")
	}

	ac, ok := b.codes.Get(in.Code)
	if !ok || ac.ClientID != c.ClientID {
		return nil, errInvalidGrant("The authorization code is invalid")
	}

	b.codes.Delete(ac.Code)

	if now.After(ac.ExpiresAt) {
		return nil, errExpired("The authorization code has expired")
	}

	if ac.RedirectURI != in.RedirectURI || !pkceMatches(ac.CodeChallenge, in.CodeVerifier) {
		return nil, errInvalidGrant("The redirect URI or code verifier does not match the authorization request")
	}

	return b.issueLocked(c, ac.Subject, "", now), nil
}

func pkceMatches(challenge, verifier string) bool {
	sum := sha256.Sum256([]byte(verifier))
	got := base64.RawURLEncoding.EncodeToString(sum[:])

	return subtle.ConstantTimeCompare([]byte(got), []byte(challenge)) == 1
}

func (b *InMemoryBackend) refreshGrantLocked(c *Client, in *CreateTokenInput, now time.Time) (*TokenResult, *APIError) {
	if in.RefreshToken == "" {
		return nil, errInvalidRequest("refreshToken is required")
	}

	rt, ok := b.tokens.Get(in.RefreshToken)
	if !ok || rt.Kind != kindRefresh || rt.ClientID != c.ClientID {
		return nil, errInvalidGrant("The refresh token is invalid")
	}

	if now.After(rt.ExpiresAt) {
		return nil, errExpired("The refresh token has expired")
	}

	at := b.newAccessLocked(c, rt.Subject, rt.StartURL, rt.SessionID, now)

	return &TokenResult{
		AccessToken:  at.Token,
		RefreshToken: rt.Token,
		TokenType:    tokenTypeBearer,
		Scopes:       slices.Clone(at.Scopes),
		ExpiresIn:    int32(accessTokenTTL / time.Second),
	}, nil
}

func (b *InMemoryBackend) newAccessLocked(c *Client, s Subject, startURL, sessionID string, now time.Time) *Token {
	at := &Token{
		Token:     "aoa" + randomString(tokenBytes),
		Kind:      kindAccess,
		ClientID:  c.ClientID,
		SessionID: sessionID,
		Subject:   s,
		StartURL:  startURL,
		Scopes:    slices.Clone(c.Scopes),
		ExpiresAt: now.Add(accessTokenTTL),
	}
	b.tokens.Put(at)

	return at
}

// issueLocked mints a sign-in session; a refresh token needs the refresh_token grant on the client.
func (b *InMemoryBackend) issueLocked(c *Client, s Subject, startURL string, now time.Time) *TokenResult {
	sessionID := randomString(sessionIDBytes)
	at := b.newAccessLocked(c, s, startURL, sessionID, now)

	res := &TokenResult{
		AccessToken: at.Token,
		TokenType:   tokenTypeBearer,
		Scopes:      slices.Clone(at.Scopes),
		ExpiresIn:   int32(accessTokenTTL / time.Second),
	}

	if slices.Contains(c.GrantTypes, grantRefreshToken) {
		rt := &Token{
			Token:     "aor" + randomString(tokenBytes),
			Kind:      kindRefresh,
			ClientID:  c.ClientID,
			SessionID: sessionID,
			Subject:   s,
			StartURL:  startURL,
			Scopes:    slices.Clone(c.Scopes),
			ExpiresAt: now.Add(refreshTokenTTL),
		}
		b.tokens.Put(rt)
		res.RefreshToken = rt.Token
	}

	return res
}

// IssueAuthCode validates an authorization request and returns a single-use code bound to its PKCE challenge.
func (b *InMemoryBackend) IssueAuthCode(
	clientID, redirectURI, challenge, method string, subject Subject,
) (string, *APIError) {
	if challenge == "" || (method != "" && method != "S256") {
		return "", errInvalidRequest("a PKCE S256 code_challenge is required")
	}

	now := time.Now().UTC()

	b.mu.Lock("IssueAuthCode")
	defer b.mu.Unlock()

	c, err := b.clientLocked(clientID, "", false, now)
	if err != nil {
		return "", err
	}

	if !c.allowsGrant(grantAuthCode) {
		return "", errUnauthorizedClient("The client is not registered for the authorization_code grant")
	}

	if !c.redirectAllowed(redirectURI) {
		return "", errInvalidRedirectURI("The redirect URI is not registered for this client")
	}

	b.pruneLocked(now)

	ac := &AuthCode{
		Code:          randomString(deviceCodeBytes),
		ClientID:      clientID,
		RedirectURI:   redirectURI,
		CodeChallenge: challenge,
		Subject:       subject,
		ExpiresAt:     now.Add(authCodeTTL),
	}
	b.codes.Put(ac)

	return ac.Code, nil
}

// SessionForAccessToken resolves a bearer access token to its sign-in session.
func (b *InMemoryBackend) SessionForAccessToken(token string) (Session, error) {
	b.mu.RLock("SessionForAccessToken")
	defer b.mu.RUnlock()

	t, ok := b.tokens.Get(token)
	if !ok || t.Kind != kindAccess {
		return Session{}, ErrTokenNotFound
	}

	if time.Now().After(t.ExpiresAt) {
		return Session{}, ErrTokenExpired
	}

	return Session{Subject: t.Subject, ClientID: t.ClientID, StartURL: t.StartURL, ExpiresAt: t.ExpiresAt}, nil
}

// RevokeSession deletes every token of the sign-in session that issued token.
func (b *InMemoryBackend) RevokeSession(token string) error {
	b.mu.Lock("RevokeSession")
	defer b.mu.Unlock()

	t, ok := b.tokens.Get(token)
	if !ok || t.Kind != kindAccess {
		return ErrTokenNotFound
	}

	for _, other := range b.tokens.All() {
		if other.SessionID == t.SessionID {
			b.tokens.Delete(other.Token)
		}
	}

	return nil
}
