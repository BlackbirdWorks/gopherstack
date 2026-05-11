package cognitoidp

// accuracy_handler.go wires the accuracy improvements from accuracy_backend.go into
// the Cognito IDP HTTP handler. It updates existing operations that were missing
// fields and adds the handler dispatch entries for new operations.

import (
	"context"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Operation name constants prevent goconst warnings and centralize dispatch keys.
const (
	opAssociateSoftwareToken      = "AssociateSoftwareToken"
	opVerifySoftwareToken         = "VerifySoftwareToken"
	opSetUserMFAPreference        = "SetUserMFAPreference"
	opCreateUserPool              = "CreateUserPool"
	opUpdateUserPool              = "UpdateUserPool"
	opCreateUserPoolClient        = "CreateUserPoolClient"
	opUpdateUserPoolClient        = "UpdateUserPoolClient"
	opSignUp                      = "SignUp"
	opGetUser                     = "GetUser"
	opDescribeUserPool            = "DescribeUserPool"
	opDescribeUserPoolClient      = "DescribeUserPoolClient"
	opListUserPoolClients         = "ListUserPoolClients"
	opCreateResourceServer        = "CreateResourceServer"
	opDescribeResourceServer      = "DescribeResourceServer"
	opListResourceServers         = "ListResourceServers"
	opUpdateResourceServer        = "UpdateResourceServer"
	opDeleteResourceServer        = "DeleteResourceServer"
	opRespondToAuthChallenge      = "RespondToAuthChallenge"
	opAdminRespondToAuthChallenge = "AdminRespondToAuthChallenge"
)

// accuracyDispatchTable returns extra dispatch entries for the operations implemented in this file.
func (h *Handler) accuracyDispatchTable() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		opAssociateSoftwareToken:      wrapAccuracy(h.handleAssociateSoftwareTokenAccurate),
		opVerifySoftwareToken:         wrapAccuracy(h.handleVerifySoftwareTokenAccurate),
		opSetUserMFAPreference:        wrapAccuracy(h.handleSetUserMFAPreferenceAccurate),
		"AdminSetUserMFASetting":      wrapAccuracy(h.handleAdminSetUserMFASetting),
		opCreateUserPool:              wrapAccuracy(h.handleCreateUserPoolWithOpts),
		opUpdateUserPool:              wrapAccuracy(h.handleUpdateUserPoolWithOpts),
		opCreateUserPoolClient:        wrapAccuracy(h.handleCreateUserPoolClientWithOpts),
		opUpdateUserPoolClient:        wrapAccuracy(h.handleUpdateUserPoolClientWithOpts),
		opSignUp:                      wrapAccuracy(h.handleSignUpAccurate),
		opGetUser:                     wrapAccuracy(h.handleGetUserAccurate),
		opDescribeUserPool:            wrapAccuracy(h.handleDescribeUserPoolAccurate),
		opDescribeUserPoolClient:      wrapAccuracy(h.handleDescribeUserPoolClientAccurate),
		opListUserPoolClients:         wrapAccuracy(h.handleListUserPoolClientsAccurate),
		opCreateResourceServer:        wrapAccuracy(h.handleCreateResourceServerAccurate),
		opDescribeResourceServer:      wrapAccuracy(h.handleDescribeResourceServerAccurate),
		opListResourceServers:         wrapAccuracy(h.handleListResourceServersAccurate),
		opUpdateResourceServer:        wrapAccuracy(h.handleUpdateResourceServerAccurate),
		opDeleteResourceServer:        wrapAccuracy(h.handleDeleteResourceServerAccurate),
		opRespondToAuthChallenge:      wrapAccuracy(h.handleRespondToAuthChallengeAccurate),
		opAdminRespondToAuthChallenge: wrapAccuracy(h.handleAdminRespondToAuthChallengeAccurate),
	}
}

// wrapAccuracy adapts a typed handler function to the generic dispatch signature.
func wrapAccuracy[I any, O any](fn func(context.Context, *I) (*O, error)) service.JSONOpFunc {
	return service.WrapOp(fn)
}

// ---- getUserOutput with MFA fields ----

// getUserWithMFAOutput extends getUserOutput with MFA preference fields.
type getUserWithMFAOutput struct {
	Username            string          `json:"Username"`
	PreferredMfaSetting string          `json:"PreferredMfaSetting,omitempty"`
	UserAttributes      []attributeType `json:"UserAttributes"`
	UserMFASettingList  []string        `json:"UserMFASettingList,omitempty"`
}

// ---- AssociateSoftwareToken (accurate) ----

type associateSoftwareTokenAccurateInput struct {
	AccessToken string `json:"AccessToken"`
	Session     string `json:"Session,omitempty"`
}

type associateSoftwareTokenAccurateOutput struct {
	SecretCode string `json:"SecretCode"`
	Session    string `json:"Session,omitempty"`
}

func (h *Handler) handleAssociateSoftwareTokenAccurate(
	_ context.Context,
	in *associateSoftwareTokenAccurateInput,
) (*associateSoftwareTokenAccurateOutput, error) {
	secret, err := h.Backend.AssociateSoftwareToken(in.AccessToken)
	if err != nil {
		return nil, err
	}

	return &associateSoftwareTokenAccurateOutput{SecretCode: secret}, nil
}

// ---- VerifySoftwareToken (accurate) ----

type verifySoftwareTokenAccurateInput struct {
	AccessToken        string `json:"AccessToken"`
	UserCode           string `json:"UserCode"`
	FriendlyDeviceName string `json:"FriendlyDeviceName,omitempty"`
	Session            string `json:"Session,omitempty"`
}

type verifySoftwareTokenAccurateOutput struct {
	Status  string `json:"Status"`
	Session string `json:"Session,omitempty"`
}

func (h *Handler) handleVerifySoftwareTokenAccurate(
	_ context.Context,
	in *verifySoftwareTokenAccurateInput,
) (*verifySoftwareTokenAccurateOutput, error) {
	if err := h.Backend.VerifySoftwareToken(in.AccessToken, in.UserCode); err != nil {
		return nil, err
	}

	return &verifySoftwareTokenAccurateOutput{Status: "SUCCESS"}, nil
}

// ---- SetUserMFAPreference (accurate) ----

type smsMFASetting struct {
	Enabled      bool `json:"Enabled"`
	PreferredMfa bool `json:"PreferredMfa"`
}

type softwareTokenMFASetting struct {
	Enabled      bool `json:"Enabled"`
	PreferredMfa bool `json:"PreferredMfa"`
}

type setUserMFAPreferenceAccurateInput struct {
	SMSMfaSettings           *smsMFASetting           `json:"SMSMfaSettings,omitempty"`
	SoftwareTokenMfaSettings *softwareTokenMFASetting `json:"SoftwareTokenMfaSettings,omitempty"`
	AccessToken              string                   `json:"AccessToken"`
}

type setUserMFAPreferenceAccurateOutput struct{}

func (h *Handler) handleSetUserMFAPreferenceAccurate(
	_ context.Context,
	in *setUserMFAPreferenceAccurateInput,
) (*setUserMFAPreferenceAccurateOutput, error) {
	smsEnabled := in.SMSMfaSettings != nil && in.SMSMfaSettings.Enabled
	softwareEnabled := in.SoftwareTokenMfaSettings != nil && in.SoftwareTokenMfaSettings.Enabled

	preferredMFA := ""

	switch {
	case in.SMSMfaSettings != nil && in.SMSMfaSettings.PreferredMfa:
		preferredMFA = challengeSMSMFA
	case in.SoftwareTokenMfaSettings != nil && in.SoftwareTokenMfaSettings.PreferredMfa:
		preferredMFA = challengeSoftwareTokenMFA
	}

	if err := h.Backend.SetUserMFAPreference(in.AccessToken, smsEnabled, softwareEnabled, preferredMFA); err != nil {
		return nil, err
	}

	return &setUserMFAPreferenceAccurateOutput{}, nil
}

// ---- AdminSetUserMFASetting ----

type adminSetUserMFASettingInput struct {
	SMSMfaSettings           *smsMFASetting           `json:"SMSMfaSettings,omitempty"`
	SoftwareTokenMfaSettings *softwareTokenMFASetting `json:"SoftwareTokenMfaSettings,omitempty"`
	UserPoolID               string                   `json:"UserPoolId"`
	Username                 string                   `json:"Username"`
}

type adminSetUserMFASettingOutput struct{}

func (h *Handler) handleAdminSetUserMFASetting(
	_ context.Context,
	in *adminSetUserMFASettingInput,
) (*adminSetUserMFASettingOutput, error) {
	smsEnabled := in.SMSMfaSettings != nil && in.SMSMfaSettings.Enabled
	softwareEnabled := in.SoftwareTokenMfaSettings != nil && in.SoftwareTokenMfaSettings.Enabled

	preferredMFA := ""

	switch {
	case in.SMSMfaSettings != nil && in.SMSMfaSettings.PreferredMfa:
		preferredMFA = challengeSMSMFA
	case in.SoftwareTokenMfaSettings != nil && in.SoftwareTokenMfaSettings.PreferredMfa:
		preferredMFA = challengeSoftwareTokenMFA
	}

	if err := h.Backend.AdminSetUserMFASetting(
		in.UserPoolID, in.Username, smsEnabled, softwareEnabled, preferredMFA,
	); err != nil {
		return nil, err
	}

	return &adminSetUserMFASettingOutput{}, nil
}

// ---- CreateUserPool with PasswordPolicy (accurate) ----

type createUserPoolWithOptsInput struct {
	Policies               *userPoolPoliciesInput `json:"Policies,omitempty"`
	PoolName               string                 `json:"PoolName"`
	MfaConfiguration       string                 `json:"MfaConfiguration,omitempty"`
	AutoVerifiedAttributes []string               `json:"AutoVerifiedAttributes,omitempty"`
}

type userPoolPoliciesInput struct {
	PasswordPolicy *passwordPolicyInput `json:"PasswordPolicy,omitempty"`
}

type passwordPolicyInput struct {
	MinimumLength                 int  `json:"MinimumLength"`
	RequireUppercase              bool `json:"RequireUppercase"`
	RequireLowercase              bool `json:"RequireLowercase"`
	RequireNumbers                bool `json:"RequireNumbers"`
	RequireSymbols                bool `json:"RequireSymbols"`
	TemporaryPasswordValidityDays int  `json:"TemporaryPasswordValidityDays"`
}

type createUserPoolWithOptsOutput struct {
	UserPool userPoolDataAccurate `json:"UserPool"`
}

// userPoolDataAccurate extends userPoolData with PasswordPolicy details.
type userPoolDataAccurate struct {
	// Policies is always present (non-pointer, no omitempty) because the
	// Terraform AWS provider unconditionally accesses Policies.PasswordPolicy
	// and Policies.SignInPolicy, and will nil-panic if the key is absent.
	Policies               userPoolPoliciesAccurate `json:"Policies"`
	ID                     string                   `json:"Id"`
	Name                   string                   `json:"Name"`
	ARN                    string                   `json:"Arn"`
	DeletionProtection     string                   `json:"DeletionProtection"`
	MfaConfiguration       string                   `json:"MfaConfiguration"`
	SchemaAttributes       []SchemaAttribute        `json:"SchemaAttributes,omitempty"`
	AutoVerifiedAttributes []string                 `json:"AutoVerifiedAttributes,omitempty"`
	CreationDate           float64                  `json:"CreationDate"`
	LastModifiedDate       float64                  `json:"LastModifiedDate"`
}

type userPoolPoliciesAccurate struct {
	PasswordPolicy *passwordPolicyData `json:"PasswordPolicy"`
	SignInPolicy   *signInPolicyData   `json:"SignInPolicy"`
}

// signInPolicyData mirrors SignInPolicyType; empty placeholder keeps provider happy.
type signInPolicyData struct{}

type passwordPolicyData struct {
	MinimumLength                 int  `json:"MinimumLength"`
	RequireUppercase              bool `json:"RequireUppercase"`
	RequireLowercase              bool `json:"RequireLowercase"`
	RequireNumbers                bool `json:"RequireNumbers"`
	RequireSymbols                bool `json:"RequireSymbols"`
	TemporaryPasswordValidityDays int  `json:"TemporaryPasswordValidityDays"`
}

func poolToAccurateData(pool *UserPool) userPoolDataAccurate {
	data := userPoolDataAccurate{
		ID:                     pool.ID,
		Name:                   pool.Name,
		ARN:                    pool.ARN,
		CreationDate:           float64(pool.CreatedAt.Unix()),
		LastModifiedDate:       float64(pool.CreatedAt.Unix()),
		DeletionProtection:     "INACTIVE",
		MfaConfiguration:       mfaConfigOrDefault(pool.MfaConfiguration),
		SchemaAttributes:       sortedCustomAttributes(pool.CustomAttributes),
		AutoVerifiedAttributes: pool.AutoVerifiedAttributes,
	}

	if pool.PasswordPolicy != nil {
		data.Policies.PasswordPolicy = &passwordPolicyData{
			MinimumLength:                 pool.PasswordPolicy.MinimumLength,
			RequireUppercase:              pool.PasswordPolicy.RequireUppercase,
			RequireLowercase:              pool.PasswordPolicy.RequireLowercase,
			RequireNumbers:                pool.PasswordPolicy.RequireNumbers,
			RequireSymbols:                pool.PasswordPolicy.RequireSymbols,
			TemporaryPasswordValidityDays: pool.PasswordPolicy.TemporaryPasswordValidityDays,
		}
	}

	return data
}

func (h *Handler) handleCreateUserPoolWithOpts(
	_ context.Context,
	in *createUserPoolWithOptsInput,
) (*createUserPoolWithOptsOutput, error) {
	opts := UserPoolOptions{
		AutoVerifiedAttributes: in.AutoVerifiedAttributes,
	}

	if in.Policies != nil && in.Policies.PasswordPolicy != nil {
		pp := in.Policies.PasswordPolicy
		opts.PasswordPolicy = &PasswordPolicy{
			MinimumLength:                 pp.MinimumLength,
			RequireUppercase:              pp.RequireUppercase,
			RequireLowercase:              pp.RequireLowercase,
			RequireNumbers:                pp.RequireNumbers,
			RequireSymbols:                pp.RequireSymbols,
			TemporaryPasswordValidityDays: pp.TemporaryPasswordValidityDays,
		}
	}

	pool, err := h.Backend.CreateUserPoolWithOpts(in.PoolName, opts)
	if err != nil {
		return nil, err
	}

	return &createUserPoolWithOptsOutput{UserPool: poolToAccurateData(pool)}, nil
}

// ---- Resource Servers (accurate - persistent) ----

type resourceServerScopeType struct {
	ScopeName        string `json:"ScopeName"`
	ScopeDescription string `json:"ScopeDescription"`
}

type resourceServerAccurateType struct {
	UserPoolID string                    `json:"UserPoolId"`
	Identifier string                    `json:"Identifier"`
	Name       string                    `json:"Name,omitempty"`
	Scopes     []resourceServerScopeType `json:"Scopes,omitempty"`
}

func toResourceServerType(rs *ResourceServer) resourceServerAccurateType {
	scopes := make([]resourceServerScopeType, len(rs.Scopes))
	for i, s := range rs.Scopes {
		scopes[i] = resourceServerScopeType(s)
	}

	return resourceServerAccurateType{
		UserPoolID: rs.UserPoolID,
		Identifier: rs.Identifier,
		Name:       rs.Name,
		Scopes:     scopes,
	}
}

func toBackendScopes(scopes []resourceServerScopeType) []ResourceServerScope {
	out := make([]ResourceServerScope, len(scopes))
	for i, s := range scopes {
		out[i] = ResourceServerScope(s)
	}

	return out
}

type createResourceServerAccurateInput struct {
	UserPoolID string                    `json:"UserPoolId"`
	Identifier string                    `json:"Identifier"`
	Name       string                    `json:"Name"`
	Scopes     []resourceServerScopeType `json:"Scopes,omitempty"`
}

type createResourceServerAccurateOutput struct {
	ResourceServer resourceServerAccurateType `json:"ResourceServer"`
}

func (h *Handler) handleCreateResourceServerAccurate(
	_ context.Context,
	in *createResourceServerAccurateInput,
) (*createResourceServerAccurateOutput, error) {
	rs, err := h.Backend.CreateResourceServer(in.UserPoolID, in.Identifier, in.Name, toBackendScopes(in.Scopes))
	if err != nil {
		return nil, err
	}

	return &createResourceServerAccurateOutput{ResourceServer: toResourceServerType(rs)}, nil
}

type describeResourceServerAccurateInput struct {
	UserPoolID string `json:"UserPoolId"`
	Identifier string `json:"Identifier"`
}

type describeResourceServerAccurateOutput struct {
	ResourceServer resourceServerAccurateType `json:"ResourceServer"`
}

func (h *Handler) handleDescribeResourceServerAccurate(
	_ context.Context,
	in *describeResourceServerAccurateInput,
) (*describeResourceServerAccurateOutput, error) {
	rs, err := h.Backend.DescribeResourceServer(in.UserPoolID, in.Identifier)
	if err != nil {
		return nil, err
	}

	return &describeResourceServerAccurateOutput{ResourceServer: toResourceServerType(rs)}, nil
}

type listResourceServersAccurateInput struct {
	UserPoolID string `json:"UserPoolId"`
	MaxResults int    `json:"MaxResults,omitempty"`
}

type listResourceServersAccurateOutput struct {
	ResourceServers []resourceServerAccurateType `json:"ResourceServers"`
}

func (h *Handler) handleListResourceServersAccurate(
	_ context.Context,
	in *listResourceServersAccurateInput,
) (*listResourceServersAccurateOutput, error) {
	servers, err := h.Backend.ListResourceServers(in.UserPoolID)
	if err != nil {
		return nil, err
	}

	out := make([]resourceServerAccurateType, len(servers))
	for i, rs := range servers {
		out[i] = toResourceServerType(rs)
	}

	return &listResourceServersAccurateOutput{ResourceServers: out}, nil
}

type updateResourceServerAccurateInput struct {
	UserPoolID string                    `json:"UserPoolId"`
	Identifier string                    `json:"Identifier"`
	Name       string                    `json:"Name,omitempty"`
	Scopes     []resourceServerScopeType `json:"Scopes,omitempty"`
}

type updateResourceServerAccurateOutput struct {
	ResourceServer resourceServerAccurateType `json:"ResourceServer"`
}

func (h *Handler) handleUpdateResourceServerAccurate(
	_ context.Context,
	in *updateResourceServerAccurateInput,
) (*updateResourceServerAccurateOutput, error) {
	rs, err := h.Backend.UpdateResourceServer(in.UserPoolID, in.Identifier, in.Name, toBackendScopes(in.Scopes))
	if err != nil {
		return nil, err
	}

	return &updateResourceServerAccurateOutput{ResourceServer: toResourceServerType(rs)}, nil
}

type deleteResourceServerAccurateInput struct {
	UserPoolID string `json:"UserPoolId"`
	Identifier string `json:"Identifier"`
}

type deleteResourceServerAccurateOutput struct{}

func (h *Handler) handleDeleteResourceServerAccurate(
	_ context.Context,
	in *deleteResourceServerAccurateInput,
) (*deleteResourceServerAccurateOutput, error) {
	if err := h.Backend.DeleteResourceServer(in.UserPoolID, in.Identifier); err != nil {
		return nil, err
	}

	return &deleteResourceServerAccurateOutput{}, nil
}

// ---- RespondToAuthChallenge (accurate) ----

type respondToAuthChallengeAccurateInput struct {
	ClientID           string            `json:"ClientId"`
	ChallengeName      string            `json:"ChallengeName"`
	ChallengeResponses map[string]string `json:"ChallengeResponses,omitempty"`
	Session            string            `json:"Session,omitempty"`
}

type respondToAuthChallengeAccurateOutput struct {
	AuthenticationResult *authResult `json:"AuthenticationResult,omitempty"`
	ChallengeName        string      `json:"ChallengeName,omitempty"`
	Session              string      `json:"Session,omitempty"`
}

func (h *Handler) handleRespondToAuthChallengeAccurate(
	_ context.Context,
	in *respondToAuthChallengeAccurateInput,
) (*respondToAuthChallengeAccurateOutput, error) {
	switch in.ChallengeName {
	case challengeSoftwareTokenMFA:
		totpCode := in.ChallengeResponses["SOFTWARE_TOKEN_MFA_CODE"]

		tokens, err := h.Backend.RespondToMFAChallenge(in.ClientID, in.Session, totpCode)
		if err != nil {
			return nil, err
		}

		return &respondToAuthChallengeAccurateOutput{
			AuthenticationResult: authResultFromTokenResult(tokens),
		}, nil

	case challengeSMSMFA:
		// SMS MFA: accept any numeric code (simulation — no real SMS gateway).
		totpCode := in.ChallengeResponses["SMS_MFA_CODE"]

		tokens, err := h.Backend.RespondToMFAChallenge(in.ClientID, in.Session, totpCode)
		if err != nil {
			return nil, err
		}

		return &respondToAuthChallengeAccurateOutput{
			AuthenticationResult: authResultFromTokenResult(tokens),
		}, nil

	case challengeEmailOTP:
		// EMAIL_OTP: accept any numeric code (simulation).
		otpCode := in.ChallengeResponses["EMAIL_OTP_CODE"]

		tokens, err := h.Backend.RespondToMFAChallenge(in.ClientID, in.Session, otpCode)
		if err != nil {
			return nil, err
		}

		return &respondToAuthChallengeAccurateOutput{
			AuthenticationResult: authResultFromTokenResult(tokens),
		}, nil

	case challengeNewPasswordRequired:
		newPassword := in.ChallengeResponses["NEW_PASSWORD"]

		tokens, err := h.Backend.RespondToNewPasswordRequired(in.ClientID, in.Session, newPassword)
		if err != nil {
			return nil, err
		}

		return &respondToAuthChallengeAccurateOutput{
			AuthenticationResult: authResultFromTokenResult(tokens),
		}, nil

	case challengePasswordVerifier:
		// USER_SRP_AUTH second step: credentials were verified in InitiateAuth; just issue tokens.
		tokens, err := h.Backend.RespondToSRPChallenge(in.ClientID, in.Session)
		if err != nil {
			return nil, err
		}

		return &respondToAuthChallengeAccurateOutput{
			AuthenticationResult: authResultFromTokenResult(tokens),
		}, nil

	default:
		return &respondToAuthChallengeAccurateOutput{}, nil
	}
}

// ---- AdminRespondToAuthChallenge ----

type adminRespondToAuthChallengeInput struct {
	UserPoolID         string            `json:"UserPoolId"`
	ClientID           string            `json:"ClientId"`
	ChallengeName      string            `json:"ChallengeName"`
	ChallengeResponses map[string]string `json:"ChallengeResponses,omitempty"`
	Session            string            `json:"Session,omitempty"`
}

type adminRespondToAuthChallengeOutput struct {
	AuthenticationResult *authResult `json:"AuthenticationResult,omitempty"`
	ChallengeName        string      `json:"ChallengeName,omitempty"`
	Session              string      `json:"Session,omitempty"`
}

func (h *Handler) handleAdminRespondToAuthChallengeAccurate(
	_ context.Context,
	in *adminRespondToAuthChallengeInput,
) (*adminRespondToAuthChallengeOutput, error) {
	switch in.ChallengeName {
	case challengeSoftwareTokenMFA, challengeSMSMFA, challengeEmailOTP:
		var code string

		switch in.ChallengeName {
		case challengeSoftwareTokenMFA:
			code = in.ChallengeResponses["SOFTWARE_TOKEN_MFA_CODE"]
		case challengeSMSMFA:
			code = in.ChallengeResponses["SMS_MFA_CODE"]
		case challengeEmailOTP:
			code = in.ChallengeResponses["EMAIL_OTP_CODE"]
		}

		tokens, err := h.Backend.RespondToMFAChallenge(in.ClientID, in.Session, code)
		if err != nil {
			return nil, err
		}

		return &adminRespondToAuthChallengeOutput{
			AuthenticationResult: authResultFromTokenResult(tokens),
		}, nil

	case challengeNewPasswordRequired:
		newPassword := in.ChallengeResponses["NEW_PASSWORD"]

		tokens, err := h.Backend.RespondToNewPasswordRequired(in.ClientID, in.Session, newPassword)
		if err != nil {
			return nil, err
		}

		return &adminRespondToAuthChallengeOutput{
			AuthenticationResult: authResultFromTokenResult(tokens),
		}, nil

	case challengePasswordVerifier:
		tokens, err := h.Backend.RespondToSRPChallenge(in.ClientID, in.Session)
		if err != nil {
			return nil, err
		}

		return &adminRespondToAuthChallengeOutput{
			AuthenticationResult: authResultFromTokenResult(tokens),
		}, nil

	default:
		return &adminRespondToAuthChallengeOutput{}, nil
	}
}

// ---- clientToAccurateData: extend UserPoolClient wire format with OAuth fields ----

// clientDataAccurate is the wire format for UserPoolClient including OAuth fields.
type clientDataAccurate struct {
	ClientID              string   `json:"ClientId"`
	ClientName            string   `json:"ClientName"`
	UserPoolID            string   `json:"UserPoolId"`
	ClientSecret          string   `json:"ClientSecret,omitempty"`
	AllowedOAuthFlows     []string `json:"AllowedOAuthFlows,omitempty"`
	AllowedOAuthScopes    []string `json:"AllowedOAuthScopes,omitempty"`
	ExplicitAuthFlows     []string `json:"ExplicitAuthFlows,omitempty"`
	CreationDate          float64  `json:"CreationDate"`
	EnableTokenRevocation bool     `json:"EnableTokenRevocation,omitempty"`
}

func clientToAccurateData(c *UserPoolClient) clientDataAccurate {
	flows := make([]string, len(c.AllowedOAuthFlows))
	copy(flows, c.AllowedOAuthFlows)
	scopes := make([]string, len(c.AllowedOAuthScopes))
	copy(scopes, c.AllowedOAuthScopes)
	ef := make([]string, len(c.ExplicitAuthFlows))
	copy(ef, c.ExplicitAuthFlows)
	sort.Strings(flows)
	sort.Strings(scopes)

	return clientDataAccurate{
		ClientID:              c.ClientID,
		ClientName:            c.ClientName,
		UserPoolID:            c.UserPoolID,
		ClientSecret:          c.ClientSecret,
		AllowedOAuthFlows:     flows,
		AllowedOAuthScopes:    scopes,
		ExplicitAuthFlows:     ef,
		CreationDate:          float64(c.CreatedAt.Unix()),
		EnableTokenRevocation: c.EnableTokenRevocation,
	}
}

// ---- UpdateUserPool with opts ----

type updateUserPoolWithOptsInput struct {
	UserPoolID             string                 `json:"UserPoolId"`
	MfaConfiguration       string                 `json:"MfaConfiguration,omitempty"`
	Policies               *userPoolPoliciesInput `json:"Policies,omitempty"`
	AutoVerifiedAttributes []string               `json:"AutoVerifiedAttributes,omitempty"`
}

type updateUserPoolWithOptsOutput struct{}

func (h *Handler) handleUpdateUserPoolWithOpts(
	_ context.Context,
	in *updateUserPoolWithOptsInput,
) (*updateUserPoolWithOptsOutput, error) {
	opts := UserPoolOptions{
		AutoVerifiedAttributes: in.AutoVerifiedAttributes,
	}

	if in.Policies != nil && in.Policies.PasswordPolicy != nil {
		pp := in.Policies.PasswordPolicy
		opts.PasswordPolicy = &PasswordPolicy{
			MinimumLength:                 pp.MinimumLength,
			RequireUppercase:              pp.RequireUppercase,
			RequireLowercase:              pp.RequireLowercase,
			RequireNumbers:                pp.RequireNumbers,
			RequireSymbols:                pp.RequireSymbols,
			TemporaryPasswordValidityDays: pp.TemporaryPasswordValidityDays,
		}
	}

	if err := h.Backend.UpdateUserPoolWithOpts(in.UserPoolID, in.MfaConfiguration, opts); err != nil {
		return nil, err
	}

	return &updateUserPoolWithOptsOutput{}, nil
}

// ---- CreateUserPoolClient with OAuth fields ----

type createUserPoolClientWithOptsInput struct {
	UserPoolID            string   `json:"UserPoolId"`
	ClientName            string   `json:"ClientName"`
	AllowedOAuthFlows     []string `json:"AllowedOAuthFlows,omitempty"`
	AllowedOAuthScopes    []string `json:"AllowedOAuthScopes,omitempty"`
	ExplicitAuthFlows     []string `json:"ExplicitAuthFlows,omitempty"`
	GenerateSecret        bool     `json:"GenerateSecret,omitempty"`
	EnableTokenRevocation bool     `json:"EnableTokenRevocation,omitempty"`
}

type createUserPoolClientWithOptsOutput struct {
	UserPoolClient clientDataAccurate `json:"UserPoolClient"`
}

func (h *Handler) handleCreateUserPoolClientWithOpts(
	_ context.Context,
	in *createUserPoolClientWithOptsInput,
) (*createUserPoolClientWithOptsOutput, error) {
	opts := UserPoolClientOptions{
		AllowedOAuthFlows:     in.AllowedOAuthFlows,
		AllowedOAuthScopes:    in.AllowedOAuthScopes,
		ExplicitAuthFlows:     in.ExplicitAuthFlows,
		GenerateSecret:        in.GenerateSecret,
		EnableTokenRevocation: in.EnableTokenRevocation,
	}

	client, err := h.Backend.CreateUserPoolClientWithOpts(in.UserPoolID, in.ClientName, opts)
	if err != nil {
		return nil, err
	}

	return &createUserPoolClientWithOptsOutput{UserPoolClient: clientToAccurateData(client)}, nil
}

// ---- UpdateUserPoolClient with OAuth fields ----

type updateUserPoolClientWithOptsInput struct {
	UserPoolID            string   `json:"UserPoolId"`
	ClientID              string   `json:"ClientId"`
	ClientName            string   `json:"ClientName,omitempty"`
	AllowedOAuthFlows     []string `json:"AllowedOAuthFlows,omitempty"`
	AllowedOAuthScopes    []string `json:"AllowedOAuthScopes,omitempty"`
	ExplicitAuthFlows     []string `json:"ExplicitAuthFlows,omitempty"`
	EnableTokenRevocation bool     `json:"EnableTokenRevocation,omitempty"`
}

type updateUserPoolClientWithOptsOutput struct {
	UserPoolClient clientDataAccurate `json:"UserPoolClient"`
}

func (h *Handler) handleUpdateUserPoolClientWithOpts(
	_ context.Context,
	in *updateUserPoolClientWithOptsInput,
) (*updateUserPoolClientWithOptsOutput, error) {
	opts := UserPoolClientOptions{
		AllowedOAuthFlows:     in.AllowedOAuthFlows,
		AllowedOAuthScopes:    in.AllowedOAuthScopes,
		ExplicitAuthFlows:     in.ExplicitAuthFlows,
		EnableTokenRevocation: in.EnableTokenRevocation,
	}

	client, err := h.Backend.UpdateUserPoolClientWithOpts(in.UserPoolID, in.ClientID, in.ClientName, opts)
	if err != nil {
		return nil, err
	}

	return &updateUserPoolClientWithOptsOutput{UserPoolClient: clientToAccurateData(client)}, nil
}

// ---- SignUp with PasswordPolicy enforcement and AutoVerifiedAttributes ----

type signUpAccurateInput struct {
	Username       string          `json:"Username"`
	Password       string          `json:"Password"`
	ClientID       string          `json:"ClientId"`
	UserAttributes []attributeType `json:"UserAttributes"`
}

type signUpAccurateOutput struct {
	CodeDeliveryDetails map[string]string `json:"CodeDeliveryDetails,omitempty"`
	UserSub             string            `json:"UserSub"`
	UserConfirmed       bool              `json:"UserConfirmed"`
}

func (h *Handler) handleSignUpAccurate(
	_ context.Context,
	in *signUpAccurateInput,
) (*signUpAccurateOutput, error) {
	attrs := attributeListToMap(in.UserAttributes)

	user, err := h.Backend.SignUpWithValidation(in.ClientID, in.Username, in.Password, attrs)
	if err != nil {
		return nil, err
	}

	out := &signUpAccurateOutput{
		UserSub:       user.Sub,
		UserConfirmed: user.Status == UserStatusConfirmed,
	}

	if user.ConfirmCode != "" {
		out.CodeDeliveryDetails = map[string]string{
			keyDeliveryMedium:   medEmail,
			keyDestination:      mockDestination,
			keyAttributeName:    attrEmail,
			keyConfirmationCode: user.ConfirmCode,
		}
	}

	return out, nil
}

// ---- GetUser with MFA fields ----

type getUserAccurateInput struct {
	AccessToken string `json:"AccessToken"`
}

func (h *Handler) handleGetUserAccurate(
	_ context.Context,
	in *getUserAccurateInput,
) (*getUserWithMFAOutput, error) {
	user, err := h.Backend.GetUser(in.AccessToken)
	if err != nil {
		return nil, err
	}

	return &getUserWithMFAOutput{
		Username:            user.Username,
		UserAttributes:      sortedAttributeList(userAttrsWithSub(user)),
		UserMFASettingList:  user.UserMFASettingList,
		PreferredMfaSetting: user.PreferredMfaSetting,
	}, nil
}

// ---- DescribeUserPool with full fields ----

type describeUserPoolAccurateInput struct {
	UserPoolID string `json:"UserPoolId"`
}

type describeUserPoolAccurateOutput struct {
	UserPool userPoolDataAccurate `json:"UserPool"`
}

func (h *Handler) handleDescribeUserPoolAccurate(
	_ context.Context,
	in *describeUserPoolAccurateInput,
) (*describeUserPoolAccurateOutput, error) {
	pool, err := h.Backend.DescribeUserPool(in.UserPoolID)
	if err != nil {
		return nil, err
	}

	return &describeUserPoolAccurateOutput{UserPool: poolToAccurateData(pool)}, nil
}

// ---- DescribeUserPoolClient with OAuth fields ----

type describeUserPoolClientAccurateInput struct {
	UserPoolID string `json:"UserPoolId"`
	ClientID   string `json:"ClientId"`
}

type describeUserPoolClientAccurateOutput struct {
	UserPoolClient clientDataAccurate `json:"UserPoolClient"`
}

func (h *Handler) handleDescribeUserPoolClientAccurate(
	_ context.Context,
	in *describeUserPoolClientAccurateInput,
) (*describeUserPoolClientAccurateOutput, error) {
	client, err := h.Backend.DescribeUserPoolClient(in.UserPoolID, in.ClientID)
	if err != nil {
		return nil, err
	}

	return &describeUserPoolClientAccurateOutput{UserPoolClient: clientToAccurateData(client)}, nil
}

// ---- ListUserPoolClients with OAuth fields ----

type listUserPoolClientsAccurateInput struct {
	UserPoolID string `json:"UserPoolId"`
	MaxResults int    `json:"MaxResults"`
}

type listUserPoolClientsAccurateOutput struct {
	UserPoolClients []clientDataAccurate `json:"UserPoolClients"`
}

func (h *Handler) handleListUserPoolClientsAccurate(
	_ context.Context,
	in *listUserPoolClientsAccurateInput,
) (*listUserPoolClientsAccurateOutput, error) {
	clients, err := h.Backend.ListUserPoolClients(in.UserPoolID)
	if err != nil {
		return nil, err
	}

	items := make([]clientDataAccurate, 0, len(clients))
	for _, c := range clients {
		items = append(items, clientToAccurateData(c))
	}

	return &listUserPoolClientsAccurateOutput{UserPoolClients: items}, nil
}
