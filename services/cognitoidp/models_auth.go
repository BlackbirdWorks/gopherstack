package cognitoidp

// AuthResult is the result of a successful authentication or a pending challenge.
type AuthResult struct {
	// Tokens is set when authentication is complete.
	Tokens *TokenResult `json:"tokens,omitempty"`
	// MFASession is set when a challenge is required; the caller must respond to it.
	MFASession string `json:"mfaSession,omitempty"`
	// ChallengeName identifies the type of challenge (SOFTWARE_TOKEN_MFA, NEW_PASSWORD_REQUIRED, etc.).
	ChallengeName string `json:"challengeName,omitempty"`
}

type signUpInput struct {
	Username       string          `json:"Username,omitempty"`
	Password       string          `json:"Password,omitempty"`
	ClientID       string          `json:"ClientId,omitempty"`
	UserAttributes []attributeType `json:"UserAttributes,omitempty"`
}

type signUpOutput struct {
	CodeDeliveryDetails map[string]string `json:"CodeDeliveryDetails,omitempty"`
	UserSub             string            `json:"UserSub,omitempty"`
	UserConfirmed       bool              `json:"UserConfirmed"`
}

type confirmSignUpInput struct {
	Username         string `json:"Username,omitempty"`
	ConfirmationCode string `json:"ConfirmationCode,omitempty"`
	ClientID         string `json:"ClientId,omitempty"`
}

type confirmSignUpOutput struct{}

type authInput struct {
	AuthParameters map[string]string `json:"AuthParameters,omitempty"`
	AuthFlow       string            `json:"AuthFlow,omitempty"`
	ClientID       string            `json:"ClientId,omitempty"`
	UserPoolID     string            `json:"UserPoolId,omitempty"`
}

type authResult struct {
	AccessToken  string `json:"AccessToken,omitempty"`
	IDToken      string `json:"IdToken,omitempty"`
	RefreshToken string `json:"RefreshToken,omitempty"`
	TokenType    string `json:"TokenType,omitempty"`
	ExpiresIn    int32  `json:"ExpiresIn,omitempty"`
}

type authOutput struct {
	AuthenticationResult *authResult       `json:"AuthenticationResult,omitempty"`
	ChallengeName        *string           `json:"ChallengeName,omitempty"`
	Session              *string           `json:"Session,omitempty"`
	ChallengeParameters  map[string]string `json:"ChallengeParameters,omitempty"`
}

type adminConfirmSignUpInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	Username   string `json:"Username,omitempty"`
}

type adminConfirmSignUpOutput struct{}

type forgotPasswordInput struct {
	ClientID string `json:"ClientId,omitempty"`
	Username string `json:"Username,omitempty"`
}

type forgotPasswordOutput struct {
	CodeDeliveryDetails map[string]string `json:"CodeDeliveryDetails,omitempty"`
}

type confirmForgotPasswordInput struct {
	ClientID         string `json:"ClientId,omitempty"`
	Username         string `json:"Username,omitempty"`
	ConfirmationCode string `json:"ConfirmationCode,omitempty"`
	Password         string `json:"Password,omitempty"`
}

type confirmForgotPasswordOutput struct{}

type changePasswordInput struct {
	AccessToken      string `json:"AccessToken,omitempty"`
	PreviousPassword string `json:"PreviousPassword,omitempty"`
	ProposedPassword string `json:"ProposedPassword,omitempty"`
}

type changePasswordOutput struct{}

type resendConfirmationCodeInput struct {
	ClientID string `json:"ClientId,omitempty"`
	Username string `json:"Username,omitempty"`
}

type resendConfirmationCodeOutput struct {
	CodeDeliveryDetails map[string]string `json:"CodeDeliveryDetails,omitempty"`
}

type adminResetUserPasswordInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	Username   string `json:"Username,omitempty"`
}

type adminResetUserPasswordOutput struct{}

type respondToAuthChallengeAccurateInput struct {
	ClientID           string            `json:"ClientId,omitempty"`
	ChallengeName      string            `json:"ChallengeName,omitempty"`
	ChallengeResponses map[string]string `json:"ChallengeResponses,omitempty"`
	Session            string            `json:"Session,omitempty"`
}

type respondToAuthChallengeAccurateOutput struct {
	AuthenticationResult *authResult `json:"AuthenticationResult,omitempty"`
	ChallengeName        string      `json:"ChallengeName,omitempty"`
	Session              string      `json:"Session,omitempty"`
}

type adminRespondToAuthChallengeInput struct {
	UserPoolID         string            `json:"UserPoolId,omitempty"`
	ClientID           string            `json:"ClientId,omitempty"`
	ChallengeName      string            `json:"ChallengeName,omitempty"`
	ChallengeResponses map[string]string `json:"ChallengeResponses,omitempty"`
	Session            string            `json:"Session,omitempty"`
}

type adminRespondToAuthChallengeOutput struct {
	AuthenticationResult *authResult `json:"AuthenticationResult,omitempty"`
	ChallengeName        string      `json:"ChallengeName,omitempty"`
	Session              string      `json:"Session,omitempty"`
}

type signUpAccurateInput struct {
	Username       string          `json:"Username,omitempty"`
	Password       string          `json:"Password,omitempty"`
	ClientID       string          `json:"ClientId,omitempty"`
	SecretHash     string          `json:"SecretHash,omitempty"`
	UserAttributes []attributeType `json:"UserAttributes,omitempty"`
}

type signUpAccurateOutput struct {
	CodeDeliveryDetails map[string]string `json:"CodeDeliveryDetails,omitempty"`
	UserSub             string            `json:"UserSub,omitempty"`
	UserConfirmed       bool              `json:"UserConfirmed"`
}

type initiateAuthAccurateInput struct {
	AuthParameters map[string]string `json:"AuthParameters,omitempty"`
	AuthFlow       string            `json:"AuthFlow,omitempty"`
	ClientID       string            `json:"ClientId,omitempty"`
}

type adminInitiateAuthAccurateInput struct {
	AuthParameters map[string]string `json:"AuthParameters,omitempty"`
	AuthFlow       string            `json:"AuthFlow,omitempty"`
	ClientID       string            `json:"ClientId,omitempty"`
	UserPoolID     string            `json:"UserPoolId,omitempty"`
}

type confirmSignUpAccurateInput struct {
	Username         string `json:"Username,omitempty"`
	ConfirmationCode string `json:"ConfirmationCode,omitempty"`
	ClientID         string `json:"ClientId,omitempty"`
	SecretHash       string `json:"SecretHash,omitempty"`
}

type confirmSignUpAccurateOutput struct{}

type forgotPasswordAccurateInput struct {
	ClientID   string `json:"ClientId,omitempty"`
	Username   string `json:"Username,omitempty"`
	SecretHash string `json:"SecretHash,omitempty"`
}

type forgotPasswordAccurateOutput struct {
	CodeDeliveryDetails map[string]string `json:"CodeDeliveryDetails,omitempty"`
}

type confirmForgotPasswordAccurateInput struct {
	ClientID         string `json:"ClientId,omitempty"`
	Username         string `json:"Username,omitempty"`
	ConfirmationCode string `json:"ConfirmationCode,omitempty"`
	Password         string `json:"Password,omitempty"`
	SecretHash       string `json:"SecretHash,omitempty"`
}

type confirmForgotPasswordAccurateOutput struct{}

type resendConfirmationCodeAccurateInput struct {
	ClientID   string `json:"ClientId,omitempty"`
	Username   string `json:"Username,omitempty"`
	SecretHash string `json:"SecretHash,omitempty"`
}

type resendConfirmationCodeAccurateOutput struct {
	CodeDeliveryDetails map[string]string `json:"CodeDeliveryDetails,omitempty"`
}

type respondToAuthChallengeInput struct {
	ClientID           string            `json:"ClientId,omitempty"`
	ChallengeName      string            `json:"ChallengeName,omitempty"`
	ChallengeResponses map[string]string `json:"ChallengeResponses,omitempty"`
	Session            string            `json:"Session,omitempty"`
}

type respondToAuthChallengeOutput struct {
	AuthenticationResult *authResult `json:"AuthenticationResult,omitempty"`
	ChallengeName        string      `json:"ChallengeName,omitempty"`
	Session              string      `json:"Session,omitempty"`
}
