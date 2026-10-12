package ssooidc

import "time"

// Subject is the Identity Center user a device or authorization-code approval signs in.
type Subject struct {
	UserName        string `json:"userName,omitempty"`
	UserID          string `json:"userId,omitempty"`
	IdentityStoreID string `json:"identityStoreId,omitempty"`
}

// Client is a public client registered by RegisterClient.
type Client struct {
	IssuedAt     time.Time `json:"issuedAt"`
	ExpiresAt    time.Time `json:"expiresAt"`
	ClientID     string    `json:"clientId"`
	ClientSecret string    `json:"clientSecret"`
	ClientName   string    `json:"clientName"`
	IssuerURL    string    `json:"issuerUrl,omitempty"`
	EntitledApp  string    `json:"entitledApplicationArn,omitempty"`
	GrantTypes   []string  `json:"grantTypes,omitempty"`
	RedirectURIs []string  `json:"redirectUris,omitempty"`
	Scopes       []string  `json:"scopes,omitempty"`
}

// Device status values.
const (
	devicePending  = "PENDING"
	deviceApproved = "APPROVED"
	deviceDenied   = "DENIED"
)

// Device is one in-flight StartDeviceAuthorization request.
type Device struct {
	ExpiresAt  time.Time `json:"expiresAt"`
	LastPolled time.Time `json:"lastPolled"`
	Subject    Subject   `json:"subject"`
	DeviceCode string    `json:"deviceCode"`
	UserCode   string    `json:"userCode"`
	ClientID   string    `json:"clientId"`
	StartURL   string    `json:"startUrl"`
	Status     string    `json:"status"`
	Interval   int32     `json:"interval"`
}

// AuthCode is a single-use authorization code awaiting CreateToken.
type AuthCode struct {
	ExpiresAt     time.Time `json:"expiresAt"`
	Subject       Subject   `json:"subject"`
	Code          string    `json:"code"`
	ClientID      string    `json:"clientId"`
	RedirectURI   string    `json:"redirectUri"`
	CodeChallenge string    `json:"codeChallenge"`
}

// Token kinds.
const (
	kindAccess  = "access"
	kindRefresh = "refresh"
)

// Token is an issued access or refresh token.
type Token struct {
	ExpiresAt time.Time `json:"expiresAt"`
	Subject   Subject   `json:"subject"`
	Token     string    `json:"token"`
	Kind      string    `json:"kind"`
	ClientID  string    `json:"clientId"`
	SessionID string    `json:"sessionId"`
	StartURL  string    `json:"startUrl,omitempty"`
	Scopes    []string  `json:"scopes,omitempty"`
}

// Session is what the portal learns about a valid access token.
type Session struct {
	ExpiresAt time.Time
	Subject   Subject
	ClientID  string
	StartURL  string
}

// TokenResult is the outcome of CreateToken.
type TokenResult struct {
	AccessToken  string
	RefreshToken string
	TokenType    string
	Scopes       []string
	ExpiresIn    int32
}

// ClientRegistration is the outcome of RegisterClient.
type ClientRegistration struct {
	ClientID              string
	ClientSecret          string
	ClientIDIssuedAt      int64
	ClientSecretExpiresAt int64
}

// DeviceAuthorization is the outcome of StartDeviceAuthorization.
type DeviceAuthorization struct {
	DeviceCode string
	UserCode   string
	ExpiresIn  int32
	Interval   int32
}
