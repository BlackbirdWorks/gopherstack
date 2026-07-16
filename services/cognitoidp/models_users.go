package cognitoidp

import "time"

// User represents a Cognito user within a pool.
type User struct {
	CreatedAt            time.Time         `json:"createdAt"`
	UpdatedAt            time.Time         `json:"updatedAt"`
	ConfirmCodeExpiresAt time.Time         `json:"confirmCodeExpiresAt"`
	LastAuthTime         time.Time         `json:"lastAuthTime"`
	Attributes           map[string]string `json:"attributes,omitempty"`
	UserPoolID           string            `json:"userPoolID,omitempty"`
	Sub                  string            `json:"sub,omitempty"`
	Username             string            `json:"username,omitempty"`
	PasswordHash         string            `json:"passwordHash,omitempty"`
	Status               string            `json:"status,omitempty"`
	ConfirmCode          string            `json:"confirmCode,omitempty"`
	PreferredMfaSetting  string            `json:"preferredMfaSetting,omitempty"`
	TOTPSecret           string            `json:"totpSecret,omitempty"`
	UserMFASettingList   []string          `json:"userMFASettingList,omitempty"`
	MFAOptions           []MFAOptionType   `json:"mfaOptions,omitempty"`
	LinkedProviders      []ProviderLink    `json:"linkedProviders,omitempty"`
	Enabled              bool              `json:"enabled,omitempty"`
	TOTPVerified         bool              `json:"totpVerified,omitempty"`
}

type adminCreateUserInput struct {
	UserPoolID        string          `json:"UserPoolId,omitempty"`
	Username          string          `json:"Username,omitempty"`
	TemporaryPassword string          `json:"TemporaryPassword,omitempty"`
	UserAttributes    []attributeType `json:"UserAttributes,omitempty"`
}

type adminUserType struct {
	Username       string          `json:"Username,omitempty"`
	UserStatus     string          `json:"UserStatus,omitempty"`
	Attributes     []attributeType `json:"Attributes,omitempty"`
	UserCreateDate float64         `json:"UserCreateDate,omitempty"`
	Enabled        bool            `json:"Enabled"`
}

type adminCreateUserOutput struct {
	User adminUserType `json:"User"`
}

type adminSetUserPasswordInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	Username   string `json:"Username,omitempty"`
	Password   string `json:"Password,omitempty"`
	Permanent  bool   `json:"Permanent,omitempty"`
}

type adminSetUserPasswordOutput struct{}

type adminGetUserInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	Username   string `json:"Username,omitempty"`
}

type adminGetUserOutput struct {
	Username             string          `json:"Username,omitempty"`
	UserStatus           string          `json:"UserStatus,omitempty"`
	UserAttributes       []attributeType `json:"UserAttributes,omitempty"`
	PreferredMfaSetting  string          `json:"PreferredMfaSetting,omitempty"`
	UserMFASettingList   []string        `json:"UserMFASettingList,omitempty"`
	UserCreateDate       float64         `json:"UserCreateDate,omitempty"`
	UserLastModifiedDate float64         `json:"UserLastModifiedDate,omitempty"`
	Enabled              bool            `json:"Enabled"`
}

type adminDeleteUserInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	Username   string `json:"Username,omitempty"`
}

type adminDeleteUserOutput struct{}

type listUsersInput struct {
	UserPoolID      string `json:"UserPoolId,omitempty"`
	Filter          string `json:"Filter,omitempty"`
	PaginationToken string `json:"PaginationToken,omitempty"`
	Limit           int    `json:"Limit,omitempty"`
}

type listUsersOutput struct {
	PaginationToken string         `json:"PaginationToken,omitempty"`
	Users           []*userSummary `json:"Users"`
}

type userSummary struct {
	Username         string          `json:"Username,omitempty"`
	UserStatus       string          `json:"UserStatus,omitempty"`
	Attributes       []attributeType `json:"Attributes,omitempty"`
	UserCreateDate   float64         `json:"UserCreateDate,omitempty"`
	UserLastModified float64         `json:"UserLastModifiedDate,omitempty"`
	Enabled          bool            `json:"Enabled"`
}

type getUserInput struct {
	AccessToken string `json:"AccessToken,omitempty"`
}

type getUserOutput struct {
	Username       string          `json:"Username,omitempty"`
	UserAttributes []attributeType `json:"UserAttributes,omitempty"`
}

type providerUserIdentifierType struct {
	ProviderAttributeName  string `json:"ProviderAttributeName,omitempty"`
	ProviderAttributeValue string `json:"ProviderAttributeValue,omitempty"`
	ProviderName           string `json:"ProviderName,omitempty"`
}

type adminDisableUserInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	Username   string `json:"Username,omitempty"`
}

type adminDisableUserOutput struct{}

type adminEnableUserInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	Username   string `json:"Username,omitempty"`
}

type adminEnableUserOutput struct{}

type deleteUserInput struct {
	AccessToken string `json:"AccessToken,omitempty"`
}

type deleteUserOutput struct{}

// getUserWithMFAOutput extends getUserOutput with MFA preference fields.
type getUserWithMFAOutput struct {
	Username            string          `json:"Username,omitempty"`
	PreferredMfaSetting string          `json:"PreferredMfaSetting,omitempty"`
	UserAttributes      []attributeType `json:"UserAttributes,omitempty"`
	UserMFASettingList  []string        `json:"UserMFASettingList,omitempty"`
}

type getUserAccurateInput struct {
	AccessToken string `json:"AccessToken,omitempty"`
}

type adminCreateUserFullInput struct {
	UserPoolID             string          `json:"UserPoolId,omitempty"`
	Username               string          `json:"Username,omitempty"`
	TemporaryPassword      string          `json:"TemporaryPassword,omitempty"`
	UserAttributes         []attributeType `json:"UserAttributes,omitempty"`
	MessageAction          string          `json:"MessageAction,omitempty"`
	DesiredDeliveryMediums []string        `json:"DesiredDeliveryMediums,omitempty"`
	ForceAliasCreation     bool            `json:"ForceAliasCreation,omitempty"`
}

type adminCreateUserFullOutput struct {
	User *adminUserJSON `json:"User,omitempty"`
}

type adminUserJSON struct {
	Username             string          `json:"Username,omitempty"`
	UserStatus           string          `json:"UserStatus,omitempty"`
	UserAttributes       []attributeType `json:"UserAttributes,omitempty"`
	UserCreateDate       float64         `json:"UserCreateDate,omitempty"`
	UserLastModifiedDate float64         `json:"UserLastModifiedDate,omitempty"`
	Enabled              bool            `json:"Enabled,omitempty"`
}

type adminSetUserPasswordFullInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	Username   string `json:"Username,omitempty"`
	Password   string `json:"Password,omitempty"`
	Permanent  bool   `json:"Permanent,omitempty"`
}

type adminSetUserPasswordFullOutput struct{}

type getUserAuthFactorsInput struct {
	AccessToken string `json:"AccessToken,omitempty"`
}

type getUserAuthFactorsOutput struct {
	Username                  string   `json:"Username,omitempty"`
	ConfiguredUserAuthFactors []string `json:"ConfiguredUserAuthFactors,omitempty"`
}
