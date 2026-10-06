package cognitoidp

import "time"

// refreshTokenEntry holds the pool/user context for a refresh token.
type refreshTokenEntry struct {
	ExpiresAt time.Time `json:"expiresAt"`
	PoolID    string    `json:"poolId,omitempty"`
	ClientID  string    `json:"clientId,omitempty"`
	Username  string    `json:"username,omitempty"`
	// Scopes are the OAuth scopes granted at /oauth2/token; empty means the client's AllowedOAuthScopes.
	Scopes []string `json:"scopes,omitempty"`
	// AuthTime is the original authentication time (Unix seconds) of the
	// session that minted this refresh-token chain. AWS Cognito preserves
	// auth_time across REFRESH_TOKEN_AUTH; it is not reset on each refresh.
	AuthTime int64 `json:"authTime,omitempty"`
}

// clientTokenSettings holds the per-app-client token issuance knobs derived
// from UserPoolClient.AllowedOAuthScopes/*TokenValidity/TokenValidityUnits.
type clientTokenSettings struct {
	scopes            []string
	accessTokenExpiry time.Duration
	idTokenExpiry     time.Duration
	refreshTokenTTL   time.Duration
}

type revokeTokenInput struct {
	Token        string `json:"Token,omitempty"`
	ClientID     string `json:"ClientId,omitempty"`
	ClientSecret string `json:"ClientSecret,omitempty"`
}

type revokeTokenOutput struct{}

type adminUserGlobalSignOutInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	Username   string `json:"Username,omitempty"`
}

type adminUserGlobalSignOutOutput struct{}

type globalSignOutInput struct {
	AccessToken string `json:"AccessToken,omitempty"`
}

type globalSignOutOutput struct{}

type getSigningCertificateInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
}

type getSigningCertificateOutput struct {
	Certificate string `json:"Certificate,omitempty"`
}

type getTokensFromRefreshTokenInput struct {
	RefreshToken string `json:"RefreshToken,omitempty"`
	ClientID     string `json:"ClientId,omitempty"`
	ClientSecret string `json:"ClientSecret,omitempty"`
}

type getTokensFromRefreshTokenOutput struct {
	AuthenticationResult *authResult `json:"AuthenticationResult,omitempty"`
}
