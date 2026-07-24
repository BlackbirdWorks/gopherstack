package cognitoidp

import "time"

// UserPoolClient represents an app client registered to a user pool.
type UserPoolClient struct {
	CreatedAt                       time.Time         `json:"createdAt"`
	UpdatedAt                       time.Time         `json:"updatedAt"`
	TokenValidityUnits              map[string]string `json:"tokenValidityUnits,omitempty"`
	ClientID                        string            `json:"clientId,omitempty"`
	ClientName                      string            `json:"clientName,omitempty"`
	UserPoolID                      string            `json:"userPoolId,omitempty"`
	ClientSecret                    string            `json:"clientSecret,omitempty"`
	PreventUserExistenceErrors      string            `json:"preventUserExistenceErrors,omitempty"`
	AllowedOAuthScopes              []string          `json:"allowedOAuthScopes,omitempty"`
	ExplicitAuthFlows               []string          `json:"explicitAuthFlows,omitempty"`
	CallbackURLs                    []string          `json:"callbackURLs,omitempty"`
	LogoutURLs                      []string          `json:"logoutURLs,omitempty"`
	SupportedIdentityProviders      []string          `json:"supportedIdentityProviders,omitempty"`
	AllowedOAuthFlows               []string          `json:"allowedOAuthFlows,omitempty"`
	AccessTokenValidity             int32             `json:"accessTokenValidity,omitempty"`
	IDTokenValidity                 int32             `json:"idTokenValidity,omitempty"`
	RefreshTokenValidity            int32             `json:"refreshTokenValidity,omitempty"`
	EnableTokenRevocation           bool              `json:"enableTokenRevocation,omitempty"`
	AllowedOAuthFlowsUserPoolClient bool              `json:"allowedOAuthFlowsUserPoolClient,omitempty"`
}

// UserPoolClientOptions holds optional parameters for CreateUserPoolClientWithOpts and UpdateUserPoolClientWithOpts.
type UserPoolClientOptions struct {
	TokenValidityUnits              map[string]string `json:"tokenValidityUnits,omitempty"`
	PreventUserExistenceErrors      string            `json:"preventUserExistenceErrors,omitempty"`
	SupportedIdentityProviders      []string          `json:"supportedIdentityProviders,omitempty"`
	ExplicitAuthFlows               []string          `json:"explicitAuthFlows,omitempty"`
	CallbackURLs                    []string          `json:"callbackURLs,omitempty"`
	LogoutURLs                      []string          `json:"logoutURLs,omitempty"`
	AllowedOAuthScopes              []string          `json:"allowedOAuthScopes,omitempty"`
	AllowedOAuthFlows               []string          `json:"allowedOAuthFlows,omitempty"`
	AccessTokenValidity             int32             `json:"accessTokenValidity,omitempty"`
	IDTokenValidity                 int32             `json:"idTokenValidity,omitempty"`
	RefreshTokenValidity            int32             `json:"refreshTokenValidity,omitempty"`
	GenerateSecret                  bool              `json:"generateSecret,omitempty"`
	EnableTokenRevocation           bool              `json:"enableTokenRevocation,omitempty"`
	AllowedOAuthFlowsUserPoolClient bool              `json:"allowedOAuthFlowsUserPoolClient,omitempty"`
}

type deleteUserPoolClientInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	ClientID   string `json:"ClientId,omitempty"`
}

type deleteUserPoolClientOutput struct{}

type addUserPoolClientSecretInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	ClientID   string `json:"ClientId,omitempty"`
}

type addUserPoolClientSecretOutput struct {
	ClientSecret string `json:"ClientSecret,omitempty"`
}

// clientDataAccurate is the wire format for UserPoolClient including OAuth fields.
type clientDataAccurate struct {
	TokenValidityUnits              map[string]string `json:"TokenValidityUnits,omitempty"`
	ClientID                        string            `json:"ClientId,omitempty"`
	ClientName                      string            `json:"ClientName,omitempty"`
	UserPoolID                      string            `json:"UserPoolId,omitempty"`
	ClientSecret                    string            `json:"ClientSecret,omitempty"`
	PreventUserExistenceErrors      string            `json:"PreventUserExistenceErrors,omitempty"`
	AllowedOAuthFlows               []string          `json:"AllowedOAuthFlows,omitempty"`
	AllowedOAuthScopes              []string          `json:"AllowedOAuthScopes,omitempty"`
	ExplicitAuthFlows               []string          `json:"ExplicitAuthFlows,omitempty"`
	CallbackURLs                    []string          `json:"CallbackURLs,omitempty"`
	LogoutURLs                      []string          `json:"LogoutURLs,omitempty"`
	SupportedIdentityProviders      []string          `json:"SupportedIdentityProviders,omitempty"`
	CreationDate                    float64           `json:"CreationDate,omitempty"`
	LastModifiedDate                float64           `json:"LastModifiedDate,omitempty"`
	AccessTokenValidity             int32             `json:"AccessTokenValidity,omitempty"`
	IDTokenValidity                 int32             `json:"IdTokenValidity,omitempty"`
	RefreshTokenValidity            int32             `json:"RefreshTokenValidity,omitempty"`
	EnableTokenRevocation           bool              `json:"EnableTokenRevocation,omitempty"`
	AllowedOAuthFlowsUserPoolClient bool              `json:"AllowedOAuthFlowsUserPoolClient,omitempty"`
}

type createUserPoolClientWithOptsInput struct {
	TokenValidityUnits              map[string]string `json:"TokenValidityUnits,omitempty"`
	UserPoolID                      string            `json:"UserPoolId,omitempty"`
	ClientName                      string            `json:"ClientName,omitempty"`
	PreventUserExistenceErrors      string            `json:"PreventUserExistenceErrors,omitempty"`
	AllowedOAuthFlows               []string          `json:"AllowedOAuthFlows,omitempty"`
	AllowedOAuthScopes              []string          `json:"AllowedOAuthScopes,omitempty"`
	ExplicitAuthFlows               []string          `json:"ExplicitAuthFlows,omitempty"`
	CallbackURLs                    []string          `json:"CallbackURLs,omitempty"`
	LogoutURLs                      []string          `json:"LogoutURLs,omitempty"`
	SupportedIdentityProviders      []string          `json:"SupportedIdentityProviders,omitempty"`
	AccessTokenValidity             int32             `json:"AccessTokenValidity,omitempty"`
	IDTokenValidity                 int32             `json:"IdTokenValidity,omitempty"`
	RefreshTokenValidity            int32             `json:"RefreshTokenValidity,omitempty"`
	GenerateSecret                  bool              `json:"GenerateSecret,omitempty"`
	EnableTokenRevocation           bool              `json:"EnableTokenRevocation,omitempty"`
	AllowedOAuthFlowsUserPoolClient bool              `json:"AllowedOAuthFlowsUserPoolClient,omitempty"`
}

type createUserPoolClientWithOptsOutput struct {
	UserPoolClient clientDataAccurate `json:"UserPoolClient"`
}

type updateUserPoolClientWithOptsInput struct {
	TokenValidityUnits              map[string]string `json:"TokenValidityUnits,omitempty"`
	UserPoolID                      string            `json:"UserPoolId,omitempty"`
	ClientID                        string            `json:"ClientId,omitempty"`
	ClientName                      string            `json:"ClientName,omitempty"`
	PreventUserExistenceErrors      string            `json:"PreventUserExistenceErrors,omitempty"`
	AllowedOAuthFlows               []string          `json:"AllowedOAuthFlows,omitempty"`
	AllowedOAuthScopes              []string          `json:"AllowedOAuthScopes,omitempty"`
	ExplicitAuthFlows               []string          `json:"ExplicitAuthFlows,omitempty"`
	CallbackURLs                    []string          `json:"CallbackURLs,omitempty"`
	LogoutURLs                      []string          `json:"LogoutURLs,omitempty"`
	SupportedIdentityProviders      []string          `json:"SupportedIdentityProviders,omitempty"`
	AccessTokenValidity             int32             `json:"AccessTokenValidity,omitempty"`
	IDTokenValidity                 int32             `json:"IdTokenValidity,omitempty"`
	RefreshTokenValidity            int32             `json:"RefreshTokenValidity,omitempty"`
	EnableTokenRevocation           bool              `json:"EnableTokenRevocation,omitempty"`
	AllowedOAuthFlowsUserPoolClient bool              `json:"AllowedOAuthFlowsUserPoolClient,omitempty"`
}

type updateUserPoolClientWithOptsOutput struct {
	UserPoolClient clientDataAccurate `json:"UserPoolClient"`
}

type describeUserPoolClientAccurateInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	ClientID   string `json:"ClientId,omitempty"`
}

type describeUserPoolClientAccurateOutput struct {
	UserPoolClient clientDataAccurate `json:"UserPoolClient"`
}

type listUserPoolClientsAccurateInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	MaxResults int    `json:"MaxResults,omitempty"`
}

type listUserPoolClientsAccurateOutput struct {
	UserPoolClients []clientDataAccurate `json:"UserPoolClients"`
}

type deleteUserPoolClientSecretInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	ClientID   string `json:"ClientId,omitempty"`
	SecretHash string `json:"SecretHash,omitempty"`
}

type deleteUserPoolClientSecretOutput struct{}

type listUserPoolClientSecretsInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	ClientID   string `json:"ClientId,omitempty"`
}

type listUserPoolClientSecretsOutput struct {
	Secrets []string `json:"Secrets"`
}
