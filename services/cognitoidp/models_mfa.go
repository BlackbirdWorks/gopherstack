package cognitoidp

import "time"

// mfaSessionEntry holds the pending challenge context (MFA or NEW_PASSWORD_REQUIRED).
type mfaSessionEntry struct {
	ExpiresAt     time.Time `json:"expiresAt"`
	PoolID        string    `json:"poolID,omitempty"`
	ClientID      string    `json:"clientID,omitempty"`
	Username      string    `json:"username,omitempty"`
	ChallengeType string    `json:"challengeType,omitempty"` // "SOFTWARE_TOKEN_MFA", "NEW_PASSWORD_REQUIRED" ...
	// SRPA/SRPb/SRPB/SRPSecretBlock hold the server's per-session SRP-6a state between
	// InitiateAuth (which picks b and computes B) and RespondToAuthChallenge (which
	// needs A, b, and B again to recompute S and verify the client's password-claim
	// signature). All four are padded-hex (A/b/B) or base64 (SecretBlock) strings.
	SRPA           string `json:"srpA,omitempty"`
	SRPb           string `json:"srpSmallB,omitempty"`
	SRPB           string `json:"srpLargeB,omitempty"`
	SRPSecretBlock string `json:"srpSecretBlock,omitempty"`
	// Code holds the one-time code generated for SMS_MFA/EMAIL_OTP challenges. Unlike
	// SOFTWARE_TOKEN_MFA (verified cryptographically against the user's TOTP secret), SMS
	// and email codes have no client-held secret to re-derive from, so — exactly like
	// ForgotPassword/ConfirmSignUp confirmation codes elsewhere in this backend — the code
	// is generated once here and the challenge response must match it exactly.
	Code string `json:"code,omitempty"`
	// CustomAuthChallengeName is the Lambda-chosen challenge name from the most recent
	// DefineAuthChallenge response (e.g. "CUSTOM_CHALLENGE"). This is the bookkeeping name
	// used in the session/history passed to DefineAuthChallenge on the next round -- it is
	// distinct from the fixed "CUSTOM_CHALLENGE" ChallengeName Cognito always returns to
	// the client over the wire for CUSTOM_AUTH.
	CustomAuthChallengeName string `json:"customAuthChallengeName,omitempty"`
	// CustomAuthChallengeMetadata is the challengeMetadata from the most recent
	// CreateAuthChallenge response, echoed into the session history's challengeMetadata on
	// the next DefineAuthChallenge round (see aws-lambda-go events
	// CognitoEventUserPoolsChallengeResult).
	CustomAuthChallengeMetadata string `json:"customAuthChallengeMetadata,omitempty"`
	// CustomAuthPrivateParams holds the CreateAuthChallenge response's
	// privateChallengeParameters, which VerifyAuthChallengeResponse needs to judge the
	// user's answer but which must never be exposed to the client.
	CustomAuthPrivateParams map[string]string `json:"customAuthPrivateParams,omitempty"`
	// CustomAuthSession accumulates the round-by-round challenge history
	// (CognitoEventUserPoolsChallengeResult) that DefineAuthChallenge receives on each
	// subsequent round, letting the Lambda decide (e.g.) "fail after 3 wrong answers".
	CustomAuthSession []customAuthChallengeResult `json:"customAuthSession,omitempty"`
}

// customAuthChallengeResult mirrors aws-lambda-go's
// events.CognitoEventUserPoolsChallengeResult -- one round's outcome in a CUSTOM_AUTH
// session history.
type customAuthChallengeResult struct {
	ChallengeName     string `json:"challengeName,omitempty"`
	ChallengeMetadata string `json:"challengeMetadata,omitempty"`
	ChallengeResult   bool   `json:"challengeResult"`
}

// SmsConfiguration holds the SNS topic ARN for SMS delivery.
type SmsConfiguration struct {
	SnsCallerArn string `json:"SnsCallerArn,omitempty"`
	SnsRegion    string `json:"SnsRegion,omitempty"`
	ExternalID   string `json:"ExternalId,omitempty"`
}

// SmsMfaConfiguration holds SMS MFA settings for a user pool.
type SmsMfaConfiguration struct {
	SmsConfiguration         *SmsConfiguration `json:"SmsConfiguration,omitempty"`
	SmsAuthenticationMessage string            `json:"SmsAuthenticationMessage,omitempty"`
}

// SoftwareTokenMfaConfiguration holds TOTP MFA settings for a user pool.
type SoftwareTokenMfaConfiguration struct {
	Enabled bool `json:"Enabled,omitempty"`
}

// EmailMfaConfiguration holds email OTP MFA settings for a user pool.
type EmailMfaConfiguration struct {
	Message string `json:"Message,omitempty"`
	Subject string `json:"Subject,omitempty"`
}

// MFAOptionType is the legacy per-user MFA delivery option
// (SetUserSettings/AdminSetUserSettings), e.g. {DeliveryMedium: "SMS", AttributeName: "phone_number"}.
type MFAOptionType struct {
	DeliveryMedium string `json:"deliveryMedium,omitempty"`
	AttributeName  string `json:"attributeName,omitempty"`
}

type associateSoftwareTokenAccurateInput struct {
	AccessToken string `json:"AccessToken,omitempty"`
	Session     string `json:"Session,omitempty"`
}

type associateSoftwareTokenAccurateOutput struct {
	SecretCode string `json:"SecretCode,omitempty"`
	Session    string `json:"Session,omitempty"`
}

type verifySoftwareTokenAccurateInput struct {
	AccessToken        string `json:"AccessToken,omitempty"`
	UserCode           string `json:"UserCode,omitempty"`
	FriendlyDeviceName string `json:"FriendlyDeviceName,omitempty"`
	Session            string `json:"Session,omitempty"`
}

type verifySoftwareTokenAccurateOutput struct {
	Status  string `json:"Status,omitempty"`
	Session string `json:"Session,omitempty"`
}

type smsMFASetting struct {
	Enabled      bool `json:"Enabled,omitempty"`
	PreferredMfa bool `json:"PreferredMfa,omitempty"`
}

type softwareTokenMFASetting struct {
	Enabled      bool `json:"Enabled,omitempty"`
	PreferredMfa bool `json:"PreferredMfa,omitempty"`
}

type setUserMFAPreferenceAccurateInput struct {
	SMSMfaSettings           *smsMFASetting           `json:"SMSMfaSettings,omitempty"`
	SoftwareTokenMfaSettings *softwareTokenMFASetting `json:"SoftwareTokenMfaSettings,omitempty"`
	AccessToken              string                   `json:"AccessToken,omitempty"`
}

type setUserMFAPreferenceAccurateOutput struct{}

type adminSetUserMFASettingInput struct {
	SMSMfaSettings           *smsMFASetting           `json:"SMSMfaSettings,omitempty"`
	SoftwareTokenMfaSettings *softwareTokenMFASetting `json:"SoftwareTokenMfaSettings,omitempty"`
	UserPoolID               string                   `json:"UserPoolId,omitempty"`
	Username                 string                   `json:"Username,omitempty"`
}

type adminSetUserMFASettingOutput struct{}

type adminSetUserMFAPreferenceAccurateInput struct {
	SMSMfaSettings           *smsMFASetting           `json:"SMSMfaSettings,omitempty"`
	SoftwareTokenMfaSettings *softwareTokenMFASetting `json:"SoftwareTokenMfaSettings,omitempty"`
	UserPoolID               string                   `json:"UserPoolId,omitempty"`
	Username                 string                   `json:"Username,omitempty"`
}

type adminSetUserMFAPreferenceAccurateOutput struct{}

type smsMfaConfigJSON struct {
	SmsConfiguration         *smsConfigJSON `json:"SmsConfiguration,omitempty"`
	SmsAuthenticationMessage string         `json:"SmsAuthenticationMessage,omitempty"`
}

type smsConfigJSON struct {
	SnsCallerArn string `json:"SnsCallerArn,omitempty"`
	SnsRegion    string `json:"SnsRegion,omitempty"`
	ExternalID   string `json:"ExternalId,omitempty"`
}

type softwareTokenMfaConfigJSON struct {
	Enabled bool `json:"Enabled,omitempty"`
}

type emailMfaConfigJSON struct {
	Message string `json:"Message,omitempty"`
	Subject string `json:"Subject,omitempty"`
}

type adminSetUserMFAPreferenceInput struct {
	SMSMfaSettings           *mfaSettings `json:"SMSMfaSettings,omitempty"`
	SoftwareTokenMfaSettings *mfaSettings `json:"SoftwareTokenMfaSettings,omitempty"`
	UserPoolID               string       `json:"UserPoolId,omitempty"`
	Username                 string       `json:"Username,omitempty"`
}

type mfaSettings struct {
	Enabled      bool `json:"Enabled,omitempty"`
	PreferredMfa bool `json:"PreferredMfa,omitempty"`
}

type adminSetUserMFAPreferenceOutput struct{}

type mfaOptionType struct {
	DeliveryMedium string `json:"DeliveryMedium,omitempty"`
	AttributeName  string `json:"AttributeName,omitempty"`
}

type adminSetUserSettingsInput struct {
	UserPoolID string          `json:"UserPoolId,omitempty"`
	Username   string          `json:"Username,omitempty"`
	MFAOptions []mfaOptionType `json:"MFAOptions,omitempty"`
}

type adminSetUserSettingsOutput struct{}

type associateSoftwareTokenInput struct {
	AccessToken string `json:"AccessToken,omitempty"`
	Session     string `json:"Session,omitempty"`
}

type associateSoftwareTokenOutput struct {
	SecretCode string `json:"SecretCode,omitempty"`
	Session    string `json:"Session,omitempty"`
}

type setUserMFAPreferenceInput struct {
	SMSMfaSettings           *mfaSettings `json:"SMSMfaSettings,omitempty"`
	SoftwareTokenMfaSettings *mfaSettings `json:"SoftwareTokenMfaSettings,omitempty"`
	AccessToken              string       `json:"AccessToken,omitempty"`
}

type setUserMFAPreferenceOutput struct{}

type setUserSettingsInput struct {
	AccessToken string          `json:"AccessToken,omitempty"`
	MFAOptions  []mfaOptionType `json:"MFAOptions,omitempty"`
}

type setUserSettingsOutput struct{}

type verifySoftwareTokenInput struct {
	AccessToken        string `json:"AccessToken,omitempty"`
	UserCode           string `json:"UserCode,omitempty"`
	FriendlyDeviceName string `json:"FriendlyDeviceName,omitempty"`
	Session            string `json:"Session,omitempty"`
}

type verifySoftwareTokenOutput struct {
	Status  string `json:"Status,omitempty"`
	Session string `json:"Session,omitempty"`
}
