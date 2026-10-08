package cognitoidp

// AuthResult is the result of a successful authentication or a pending challenge.
type AuthResult struct {
	Tokens              *TokenResult      `json:"tokens,omitempty"`
	ChallengeParameters map[string]string `json:"challengeParameters,omitempty"`
	MFASession          string            `json:"mfaSession,omitempty"`
	ChallengeName       string            `json:"challengeName,omitempty"`
	// AvailableChallenges is set only for a USER_AUTH SELECT_CHALLENGE result (user_auth.go).
	AvailableChallenges []string `json:"availableChallenges,omitempty"`
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
	AvailableChallenges  []string          `json:"AvailableChallenges,omitempty"`
}

type adminConfirmSignUpInput struct {
	ClientMetadata map[string]string `json:"ClientMetadata,omitempty"`
	UserPoolID     string            `json:"UserPoolId,omitempty"`
	Username       string            `json:"Username,omitempty"`
}

type adminConfirmSignUpOutput struct{}

type changePasswordInput struct {
	AccessToken      string `json:"AccessToken,omitempty"`
	PreviousPassword string `json:"PreviousPassword,omitempty"`
	ProposedPassword string `json:"ProposedPassword,omitempty"`
}

type changePasswordOutput struct{}

type adminResetUserPasswordInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	Username   string `json:"Username,omitempty"`
}

type adminResetUserPasswordOutput struct{}

type respondToAuthChallengeAccurateInput struct {
	ClientMetadata     map[string]string `json:"ClientMetadata,omitempty"`
	ClientID           string            `json:"ClientId,omitempty"`
	ChallengeName      string            `json:"ChallengeName,omitempty"`
	ChallengeResponses map[string]string `json:"ChallengeResponses,omitempty"`
	Session            string            `json:"Session,omitempty"`
}

type respondToAuthChallengeAccurateOutput struct {
	AuthenticationResult *authResult       `json:"AuthenticationResult,omitempty"`
	ChallengeParameters  map[string]string `json:"ChallengeParameters,omitempty"`
	ChallengeName        string            `json:"ChallengeName,omitempty"`
	Session              string            `json:"Session,omitempty"`
}

type adminRespondToAuthChallengeInput struct {
	ClientMetadata     map[string]string `json:"ClientMetadata,omitempty"`
	UserPoolID         string            `json:"UserPoolId,omitempty"`
	ClientID           string            `json:"ClientId,omitempty"`
	ChallengeName      string            `json:"ChallengeName,omitempty"`
	ChallengeResponses map[string]string `json:"ChallengeResponses,omitempty"`
	Session            string            `json:"Session,omitempty"`
}

type adminRespondToAuthChallengeOutput struct {
	AuthenticationResult *authResult       `json:"AuthenticationResult,omitempty"`
	ChallengeParameters  map[string]string `json:"ChallengeParameters,omitempty"`
	ChallengeName        string            `json:"ChallengeName,omitempty"`
	Session              string            `json:"Session,omitempty"`
}

type signUpAccurateInput struct {
	ClientMetadata map[string]string `json:"ClientMetadata,omitempty"`
	ValidationData []attributeType   `json:"ValidationData,omitempty"`
	Username       string            `json:"Username,omitempty"`
	Password       string            `json:"Password,omitempty"`
	ClientID       string            `json:"ClientId,omitempty"`
	SecretHash     string            `json:"SecretHash,omitempty"`
	UserAttributes []attributeType   `json:"UserAttributes,omitempty"`
}

type signUpAccurateOutput struct {
	CodeDeliveryDetails map[string]string `json:"CodeDeliveryDetails,omitempty"`
	UserSub             string            `json:"UserSub,omitempty"`
	UserConfirmed       bool              `json:"UserConfirmed"`
}

type initiateAuthAccurateInput struct {
	ClientMetadata map[string]string `json:"ClientMetadata,omitempty"`
	AuthParameters map[string]string `json:"AuthParameters,omitempty"`
	AuthFlow       string            `json:"AuthFlow,omitempty"`
	ClientID       string            `json:"ClientId,omitempty"`
}

type adminInitiateAuthAccurateInput struct {
	ClientMetadata map[string]string `json:"ClientMetadata,omitempty"`
	AuthParameters map[string]string `json:"AuthParameters,omitempty"`
	AuthFlow       string            `json:"AuthFlow,omitempty"`
	ClientID       string            `json:"ClientId,omitempty"`
	UserPoolID     string            `json:"UserPoolId,omitempty"`
}

type confirmSignUpAccurateInput struct {
	ClientMetadata     map[string]string `json:"ClientMetadata,omitempty"`
	Username           string            `json:"Username,omitempty"`
	ConfirmationCode   string            `json:"ConfirmationCode,omitempty"`
	ClientID           string            `json:"ClientId,omitempty"`
	SecretHash         string            `json:"SecretHash,omitempty"`
	ForceAliasCreation bool              `json:"ForceAliasCreation,omitempty"`
}

type confirmSignUpAccurateOutput struct{}

type forgotPasswordAccurateInput struct {
	ClientMetadata map[string]string `json:"ClientMetadata,omitempty"`
	ClientID       string            `json:"ClientId,omitempty"`
	Username       string            `json:"Username,omitempty"`
	SecretHash     string            `json:"SecretHash,omitempty"`
}

type forgotPasswordAccurateOutput struct {
	CodeDeliveryDetails map[string]string `json:"CodeDeliveryDetails,omitempty"`
}

type confirmForgotPasswordAccurateInput struct {
	ClientMetadata   map[string]string `json:"ClientMetadata,omitempty"`
	ClientID         string            `json:"ClientId,omitempty"`
	Username         string            `json:"Username,omitempty"`
	ConfirmationCode string            `json:"ConfirmationCode,omitempty"`
	Password         string            `json:"Password,omitempty"`
	SecretHash       string            `json:"SecretHash,omitempty"`
}

type confirmForgotPasswordAccurateOutput struct{}

type resendConfirmationCodeAccurateInput struct {
	ClientMetadata map[string]string `json:"ClientMetadata,omitempty"`
	ClientID       string            `json:"ClientId,omitempty"`
	Username       string            `json:"Username,omitempty"`
	SecretHash     string            `json:"SecretHash,omitempty"`
}

type resendConfirmationCodeAccurateOutput struct {
	CodeDeliveryDetails map[string]string `json:"CodeDeliveryDetails,omitempty"`
}
