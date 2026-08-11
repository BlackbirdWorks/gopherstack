package cognitoidp

import "time"

// PasswordPolicy holds the password-complexity requirements for a user pool.
type PasswordPolicy struct {
	MinimumLength                 int  `json:"MinimumLength,omitempty"`
	RequireUppercase              bool `json:"RequireUppercase,omitempty"`
	RequireLowercase              bool `json:"RequireLowercase,omitempty"`
	RequireNumbers                bool `json:"RequireNumbers,omitempty"`
	RequireSymbols                bool `json:"RequireSymbols,omitempty"`
	TemporaryPasswordValidityDays int  `json:"TemporaryPasswordValidityDays,omitempty"`
}

// UserPool represents a Cognito User Pool.
type UserPool struct {
	CreatedAt              time.Time `json:"createdAt"`
	UpdatedAt              time.Time `json:"updatedAt"`
	issuer                 *tokenIssuer
	LambdaConfig           map[string]any    `json:"lambdaConfig,omitempty"`
	EmailConfiguration     map[string]any    `json:"emailConfiguration,omitempty"`
	AccountRecoverySetting map[string]any    `json:"accountRecoverySetting,omitempty"`
	PasswordPolicy         *PasswordPolicy   `json:"passwordPolicy,omitempty"`
	ID                     string            `json:"id,omitempty"`
	Name                   string            `json:"name,omitempty"`
	ARN                    string            `json:"arn,omitempty"`
	MfaConfiguration       string            `json:"mfaConfiguration,omitempty"`
	DeletionProtection     string            `json:"deletionProtection,omitempty"`
	CustomAttributes       []SchemaAttribute `json:"customAttributes,omitempty"`
	AutoVerifiedAttributes []string          `json:"autoVerifiedAttributes,omitempty"`
}

// PoolMetrics holds aggregate statistics for a user pool.
type PoolMetrics struct {
	UserCount        int `json:"userCount,omitempty"`
	ClientCount      int `json:"clientCount,omitempty"`
	GroupCount       int `json:"groupCount,omitempty"`
	ActiveTokenCount int `json:"activeTokenCount,omitempty"`
}

// UserPoolOptions holds optional parameters for CreateUserPoolWithOpts.
type UserPoolOptions struct {
	LambdaConfig           map[string]any  `json:"lambdaConfig,omitempty"`
	EmailConfiguration     map[string]any  `json:"emailConfiguration,omitempty"`
	AccountRecoverySetting map[string]any  `json:"accountRecoverySetting,omitempty"`
	PasswordPolicy         *PasswordPolicy `json:"passwordPolicy,omitempty"`
	DeletionProtection     string          `json:"deletionProtection,omitempty"`
	AutoVerifiedAttributes []string        `json:"autoVerifiedAttributes,omitempty"`
}

// UserPoolMfaFullConfig holds the complete MFA configuration for a pool.
type UserPoolMfaFullConfig struct {
	SmsMfaConfiguration   *SmsMfaConfiguration           `json:"smsMfaConfiguration,omitempty"`
	SoftwareTokenMfa      *SoftwareTokenMfaConfiguration `json:"softwareTokenMfa,omitempty"`
	EmailMfaConfiguration *EmailMfaConfiguration         `json:"emailMfaConfiguration,omitempty"`
	MfaConfiguration      string                         `json:"mfaConfiguration,omitempty"`
}

// userPoolPoliciesData holds the Policies block returned by the Cognito IDP API.
// Returning a non-nil Policies object prevents nil-pointer panics in the Terraform
// AWS provider, which accesses Policies.PasswordPolicy and Policies.SignInPolicy
// unconditionally.
type userPoolPoliciesData struct {
	PasswordPolicy *userPoolPasswordPolicyData `json:"PasswordPolicy,omitempty"`
	SignInPolicy   *userPoolSignInPolicyData   `json:"SignInPolicy,omitempty"`
}

// userPoolPasswordPolicyData is a minimal representation of PasswordPolicyType.
type userPoolPasswordPolicyData struct{}

// userPoolSignInPolicyData is a minimal representation of SignInPolicyType.
type userPoolSignInPolicyData struct{}

type userPoolData struct {
	Policies           userPoolPoliciesData `json:"Policies"`
	ID                 string               `json:"Id,omitempty"`
	Name               string               `json:"Name,omitempty"`
	ARN                string               `json:"Arn,omitempty"`
	DeletionProtection string               `json:"DeletionProtection,omitempty"`
	MfaConfiguration   string               `json:"MfaConfiguration,omitempty"`
	SchemaAttributes   []SchemaAttribute    `json:"SchemaAttributes,omitempty"`
	CreationDate       float64              `json:"CreationDate,omitempty"`
	LastModifiedDate   float64              `json:"LastModifiedDate,omitempty"`
}

type listUserPoolsInput struct {
	NextToken  string `json:"NextToken,omitempty"`
	MaxResults int    `json:"MaxResults,omitempty"`
}

type listUserPoolsOutput struct {
	NextToken string         `json:"NextToken,omitempty"`
	UserPools []userPoolData `json:"UserPools"`
}

type deleteUserPoolInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
}

type deleteUserPoolOutput struct{}

type createUserPoolWithOptsInput struct {
	LambdaConfig           map[string]any         `json:"LambdaConfig,omitempty"`
	EmailConfiguration     map[string]any         `json:"EmailConfiguration,omitempty"`
	AccountRecoverySetting map[string]any         `json:"AccountRecoverySetting,omitempty"`
	Policies               *userPoolPoliciesInput `json:"Policies,omitempty"`
	UserPoolTags           map[string]string      `json:"UserPoolTags,omitempty"`
	PoolName               string                 `json:"PoolName,omitempty"`
	MfaConfiguration       string                 `json:"MfaConfiguration,omitempty"`
	DeletionProtection     string                 `json:"DeletionProtection,omitempty"`
	AutoVerifiedAttributes []string               `json:"AutoVerifiedAttributes,omitempty"`
}

type userPoolPoliciesInput struct {
	PasswordPolicy *passwordPolicyInput `json:"PasswordPolicy,omitempty"`
}

type passwordPolicyInput struct {
	MinimumLength                 int  `json:"MinimumLength,omitempty"`
	RequireUppercase              bool `json:"RequireUppercase,omitempty"`
	RequireLowercase              bool `json:"RequireLowercase,omitempty"`
	RequireNumbers                bool `json:"RequireNumbers,omitempty"`
	RequireSymbols                bool `json:"RequireSymbols,omitempty"`
	TemporaryPasswordValidityDays int  `json:"TemporaryPasswordValidityDays,omitempty"`
}

type createUserPoolWithOptsOutput struct {
	UserPool userPoolDataAccurate `json:"UserPool"`
}

// userPoolDataAccurate extends userPoolData with PasswordPolicy details.
type userPoolDataAccurate struct {
	// Policies is always present (non-pointer, no omitempty) because the
	// Terraform AWS provider unconditionally accesses Policies.PasswordPolicy
	// and Policies.SignInPolicy, and will nil-panic if the key is absent.
	LambdaConfig           map[string]any           `json:"LambdaConfig,omitempty"`
	EmailConfiguration     map[string]any           `json:"EmailConfiguration,omitempty"`
	AccountRecoverySetting map[string]any           `json:"AccountRecoverySetting,omitempty"`
	Policies               userPoolPoliciesAccurate `json:"Policies"`
	ID                     string                   `json:"Id,omitempty"`
	Name                   string                   `json:"Name,omitempty"`
	ARN                    string                   `json:"Arn,omitempty"`
	DeletionProtection     string                   `json:"DeletionProtection,omitempty"`
	MfaConfiguration       string                   `json:"MfaConfiguration,omitempty"`
	SchemaAttributes       []SchemaAttribute        `json:"SchemaAttributes,omitempty"`
	AutoVerifiedAttributes []string                 `json:"AutoVerifiedAttributes,omitempty"`
	CreationDate           float64                  `json:"CreationDate,omitempty"`
	LastModifiedDate       float64                  `json:"LastModifiedDate,omitempty"`
}

type userPoolPoliciesAccurate struct {
	PasswordPolicy *passwordPolicyData `json:"PasswordPolicy,omitempty"`
	SignInPolicy   *signInPolicyData   `json:"SignInPolicy,omitempty"`
}

// signInPolicyData mirrors SignInPolicyType; empty placeholder keeps provider happy.
type signInPolicyData struct{}

type passwordPolicyData struct {
	MinimumLength                 int  `json:"MinimumLength,omitempty"`
	RequireUppercase              bool `json:"RequireUppercase,omitempty"`
	RequireLowercase              bool `json:"RequireLowercase,omitempty"`
	RequireNumbers                bool `json:"RequireNumbers,omitempty"`
	RequireSymbols                bool `json:"RequireSymbols,omitempty"`
	TemporaryPasswordValidityDays int  `json:"TemporaryPasswordValidityDays,omitempty"`
}

type updateUserPoolWithOptsInput struct {
	LambdaConfig           map[string]any         `json:"LambdaConfig,omitempty"`
	EmailConfiguration     map[string]any         `json:"EmailConfiguration,omitempty"`
	AccountRecoverySetting map[string]any         `json:"AccountRecoverySetting,omitempty"`
	Policies               *userPoolPoliciesInput `json:"Policies,omitempty"`
	UserPoolID             string                 `json:"UserPoolId,omitempty"`
	MfaConfiguration       string                 `json:"MfaConfiguration,omitempty"`
	DeletionProtection     string                 `json:"DeletionProtection,omitempty"`
	AutoVerifiedAttributes []string               `json:"AutoVerifiedAttributes,omitempty"`
}

type updateUserPoolWithOptsOutput struct{}

type describeUserPoolAccurateInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
}

type describeUserPoolAccurateOutput struct {
	UserPool userPoolDataAccurate `json:"UserPool"`
}

type getUserPoolMfaConfigFullInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
}

type getUserPoolMfaConfigFullOutput struct {
	SmsMfaConfiguration           *smsMfaConfigJSON           `json:"SmsMfaConfiguration,omitempty"`
	SoftwareTokenMfaConfiguration *softwareTokenMfaConfigJSON `json:"SoftwareTokenMfaConfiguration,omitempty"`
	EmailMfaConfiguration         *emailMfaConfigJSON         `json:"EmailMfaConfiguration,omitempty"`
	MfaConfiguration              string                      `json:"MfaConfiguration,omitempty"`
}

type setUserPoolMfaConfigFullInput struct {
	SmsMfaConfiguration           *smsMfaConfigJSON           `json:"SmsMfaConfiguration,omitempty"`
	SoftwareTokenMfaConfiguration *softwareTokenMfaConfigJSON `json:"SoftwareTokenMfaConfiguration,omitempty"`
	EmailMfaConfiguration         *emailMfaConfigJSON         `json:"EmailMfaConfiguration,omitempty"`
	UserPoolID                    string                      `json:"UserPoolId,omitempty"`
	MfaConfiguration              string                      `json:"MfaConfiguration,omitempty"`
}

type setUserPoolMfaConfigFullOutput struct {
	SmsMfaConfiguration           *smsMfaConfigJSON           `json:"SmsMfaConfiguration,omitempty"`
	SoftwareTokenMfaConfiguration *softwareTokenMfaConfigJSON `json:"SoftwareTokenMfaConfiguration,omitempty"`
	EmailMfaConfiguration         *emailMfaConfigJSON         `json:"EmailMfaConfiguration,omitempty"`
	MfaConfiguration              string                      `json:"MfaConfiguration,omitempty"`
}
