package signin

import "time"

// AuthCode is a single-use authorization code awaiting redemption.
type AuthCode struct {
	ExpiresAt    time.Time `json:"expiresAt"`
	Code         string    `json:"code"`
	ClientID     string    `json:"clientID"`
	RedirectURI  string    `json:"redirectURI"`
	Challenge    string    `json:"challenge"`
	Account      string    `json:"account"`
	PrincipalArn string    `json:"principalArn"`
	UserID       string    `json:"userID"`
	SessionKey   string    `json:"sessionKey,omitempty"`
	Used         bool      `json:"used"`
}

// Session is a human OAuth session: a refresh token plus the credentials minted from it.
type Session struct {
	CreatedAt        time.Time `json:"createdAt"`
	RefreshExpiresAt time.Time `json:"refreshExpiresAt"`
	CredsExpireAt    time.Time `json:"credsExpireAt"`
	RefreshToken     string    `json:"refreshToken"`
	SessionID        string    `json:"sessionID"`
	Account          string    `json:"account"`
	Region           string    `json:"region"`
	PrincipalArn     string    `json:"principalArn"`
	UserID           string    `json:"userID"`
	ClientID         string    `json:"clientID"`
	AccessKeyID      string    `json:"accessKeyID"`
	SecretKey        string    `json:"secretKey"`
	SessionToken     string    `json:"sessionToken"`
	Revoked          bool      `json:"revoked"`
}

// AccessToken is a JWT access token issued by CreateOAuth2TokenWithIAM.
type AccessToken struct {
	IssuedAt   time.Time `json:"issuedAt"`
	ExpiresAt  time.Time `json:"expiresAt"`
	JTI        string    `json:"jti"`
	Account    string    `json:"account"`
	Subject    string    `json:"subject"`
	UserID     string    `json:"userID"`
	Audience   string    `json:"audience"`
	SessionArn string    `json:"sessionArn"`
}

// ConsoleConfig is the console authorization configuration of an account or organization target.
type ConsoleConfig struct {
	TargetID string `json:"targetID"`
	Scope    string `json:"scope"`
	Enabled  bool   `json:"enabled"`
}

// PermissionStatement is one statement of an account's Sign-In resource-based policy.
type PermissionStatement struct {
	CreatedAt         time.Time                      `json:"createdAt"`
	Condition         map[string]map[string][]string `json:"condition"`
	Key               string                         `json:"key"`
	Account           string                         `json:"account"`
	SID               string                         `json:"sid"`
	ClientToken       string                         `json:"clientToken"`
	Fingerprint       string                         `json:"fingerprint"`
	ConsoleSourceVpce string                         `json:"consoleSourceVpce,omitempty"`
	ExcludedPrincipal string                         `json:"excludedPrincipal,omitempty"`
	RequestedRegion   string                         `json:"requestedRegion,omitempty"`
	SigninSourceVpce  string                         `json:"signinSourceVpce,omitempty"`
	SourceIP          string                         `json:"sourceIp,omitempty"`
	SourceVpc         string                         `json:"sourceVpc,omitempty"`
	VpcSourceIP       string                         `json:"vpcSourceIp,omitempty"`
}
