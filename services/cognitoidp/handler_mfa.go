package cognitoidp

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

func mfaConfigOrDefault(s string) string {
	if s == "" {
		return mfaConfigOFF
	}

	return s
}

// Operation name constants prevent goconst warnings and centralize dispatch keys.
const (
	opAssociateSoftwareToken      = "AssociateSoftwareToken"
	opVerifySoftwareToken         = "VerifySoftwareToken"
	opSetUserMFAPreference        = "SetUserMFAPreference"
	opAdminSetUserMFAPreference   = "AdminSetUserMFAPreference"
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
	opInitiateAuth                = "InitiateAuth"
	opAdminInitiateAuth           = "AdminInitiateAuth"
	opConfirmSignUp               = "ConfirmSignUp"
	opForgotPassword              = "ForgotPassword"
	opConfirmForgotPassword       = "ConfirmForgotPassword"
	opResendConfirmationCode      = "ResendConfirmationCode"
)

func (h *Handler) handleAssociateSoftwareTokenAccurate(
	_ context.Context,
	in *associateSoftwareTokenAccurateInput,
) (*associateSoftwareTokenAccurateOutput, error) {
	secret, session, err := h.Backend.AssociateSoftwareToken(in.AccessToken, in.Session)
	if err != nil {
		return nil, err
	}

	return &associateSoftwareTokenAccurateOutput{SecretCode: secret, Session: session}, nil
}

func (h *Handler) handleVerifySoftwareTokenAccurate(
	_ context.Context,
	in *verifySoftwareTokenAccurateInput,
) (*verifySoftwareTokenAccurateOutput, error) {
	session, err := h.Backend.VerifySoftwareToken(in.AccessToken, in.Session, in.UserCode)
	if err != nil {
		return nil, err
	}

	return &verifySoftwareTokenAccurateOutput{Status: "SUCCESS", Session: session}, nil
}

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

func (h *Handler) handleAdminSetUserMFAPreferenceAccurate(
	_ context.Context,
	in *adminSetUserMFAPreferenceAccurateInput,
) (*adminSetUserMFAPreferenceAccurateOutput, error) {
	smsEnabled := in.SMSMfaSettings != nil && in.SMSMfaSettings.Enabled
	softwareEnabled := in.SoftwareTokenMfaSettings != nil && in.SoftwareTokenMfaSettings.Enabled

	preferredMFA := ""

	switch {
	case in.SMSMfaSettings != nil && in.SMSMfaSettings.PreferredMfa:
		preferredMFA = challengeSMSMFA
	case in.SoftwareTokenMfaSettings != nil && in.SoftwareTokenMfaSettings.PreferredMfa:
		preferredMFA = challengeSoftwareTokenMFA
	}

	if err := h.Backend.AdminSetUserMFAPreference(
		in.UserPoolID, in.Username, smsEnabled, softwareEnabled, preferredMFA,
	); err != nil {
		return nil, err
	}

	return &adminSetUserMFAPreferenceAccurateOutput{}, nil
}

// toMFAOptionRecords converts wire-shaped MFAOptions into backend records.
func toMFAOptionRecords(opts []mfaOptionType) []MFAOptionType {
	out := make([]MFAOptionType, 0, len(opts))
	for _, o := range opts {
		out = append(out, MFAOptionType(o))
	}

	return out
}

func (h *Handler) handleAdminSetUserSettings(
	_ context.Context,
	in *adminSetUserSettingsInput,
) (*adminSetUserSettingsOutput, error) {
	mfaOptions := toMFAOptionRecords(in.MFAOptions)
	if err := h.Backend.AdminSetUserSettings(in.UserPoolID, in.Username, mfaOptions); err != nil {
		return nil, err
	}

	return &adminSetUserSettingsOutput{}, nil
}

func (h *Handler) handleSetUserSettings(_ context.Context, in *setUserSettingsInput) (*setUserSettingsOutput, error) {
	if err := h.Backend.SetUserSettings(in.AccessToken, toMFAOptionRecords(in.MFAOptions)); err != nil {
		return nil, err
	}

	return &setUserSettingsOutput{}, nil
}

func (h *Handler) mfaOpsA() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"AdminSetUserSettings": service.WrapOp(h.handleAdminSetUserSettings),
		"SetUserSettings":      service.WrapOp(h.handleSetUserSettings),
	}
}

func (h *Handler) mfaOpsB() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		opAssociateSoftwareToken:    wrapAccuracy(h.handleAssociateSoftwareTokenAccurate),
		opVerifySoftwareToken:       wrapAccuracy(h.handleVerifySoftwareTokenAccurate),
		opSetUserMFAPreference:      wrapAccuracy(h.handleSetUserMFAPreferenceAccurate),
		opAdminSetUserMFAPreference: wrapAccuracy(h.handleAdminSetUserMFAPreferenceAccurate),
		"AdminSetUserMFASetting":    wrapAccuracy(h.handleAdminSetUserMFASetting),
	}
}
