package cognitoidp

// batch2_handler.go wires batch-2 accuracy improvements into the Cognito IDP HTTP handler.
// Overrides selected ops with stateful, AWS-accurate implementations.

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// batch2DispatchTable returns dispatch entries that override the base table.
func (h *Handler) batch2DispatchTable() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		opGetUserPoolMfaConfig:       wrapAccuracy(h.handleGetUserPoolMfaConfigFull),
		opSetUserPoolMfaConfig:       wrapAccuracy(h.handleSetUserPoolMfaConfigFull),
		opGetUICustomization:         wrapAccuracy(h.handleGetUICustomizationFull),
		opSetUICustomization:         wrapAccuracy(h.handleSetUICustomizationFull),
		opGetUserAttributeVerifCode:  wrapAccuracy(h.handleGetUserAttributeVerificationCodeFull),
		opVerifyUserAttribute:        wrapAccuracy(h.handleVerifyUserAttributeFull),
		opCreateIdentityProvider:     wrapAccuracy(h.handleCreateIdentityProviderFull),
		opUpdateIdentityProvider:     wrapAccuracy(h.handleUpdateIdentityProviderFull),
		opDescribeIdentityProvider:   wrapAccuracy(h.handleDescribeIdentityProviderFull),
		opGetIdentityProviderByIdent: wrapAccuracy(h.handleGetIdentityProviderByIdentifierFull),
		opListIdentityProviders:      wrapAccuracy(h.handleListIdentityProvidersFull),
		opCreateUserPoolDomain:       wrapAccuracy(h.handleCreateUserPoolDomainFull),
		opUpdateUserPoolDomain:       wrapAccuracy(h.handleUpdateUserPoolDomainFull),
		opDescribeRiskConfiguration:  wrapAccuracy(h.handleDescribeRiskConfigurationFull),
		opSetRiskConfiguration:       wrapAccuracy(h.handleSetRiskConfigurationFull),
		opAdminCreateUser:            wrapAccuracy(h.handleAdminCreateUserFull),
		opAdminSetUserPassword:       wrapAccuracy(h.handleAdminSetUserPasswordFull),
		opCreateGroup:                wrapAccuracy(h.handleCreateGroupFull),
		opUpdateGroup:                wrapAccuracy(h.handleUpdateGroupFull),
		opGetGroup:                   wrapAccuracy(h.handleGetGroupFull),
		opListGroups:                 wrapAccuracy(h.handleListGroupsFull),
		opListUsersInGroup:           wrapAccuracy(h.handleListUsersInGroupFull),
	}
}

// Additional op name constants for batch-2.
const (
	opGetUserPoolMfaConfig       = "GetUserPoolMfaConfig"
	opSetUserPoolMfaConfig       = "SetUserPoolMfaConfig"
	opGetUICustomization         = "GetUICustomization"
	opSetUICustomization         = "SetUICustomization"
	opGetUserAttributeVerifCode  = "GetUserAttributeVerificationCode"
	opVerifyUserAttribute        = "VerifyUserAttribute"
	opCreateIdentityProvider     = "CreateIdentityProvider"
	opUpdateIdentityProvider     = "UpdateIdentityProvider"
	opDescribeIdentityProvider   = "DescribeIdentityProvider"
	opGetIdentityProviderByIdent = "GetIdentityProviderByIdentifier"
	opListIdentityProviders      = "ListIdentityProviders"
	opCreateUserPoolDomain       = "CreateUserPoolDomain"
	opUpdateUserPoolDomain       = "UpdateUserPoolDomain"
	opDescribeRiskConfiguration  = "DescribeRiskConfiguration"
	opSetRiskConfiguration       = "SetRiskConfiguration"
	opAdminCreateUser            = "AdminCreateUser"
	opAdminSetUserPassword       = "AdminSetUserPassword"
	opCreateGroup                = "CreateGroup"
	opUpdateGroup                = "UpdateGroup"
	opGetGroup                   = "GetGroup"
	opListGroups                 = "ListGroups"
	opListUsersInGroup           = "ListUsersInGroup"
)

// ---------------------------------------------------------------------------
// MFA Configuration — full SMS/TOTP/Email sub-config
// ---------------------------------------------------------------------------

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

type getUserPoolMfaConfigFullInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
}

type getUserPoolMfaConfigFullOutput struct {
	SmsMfaConfiguration           *smsMfaConfigJSON           `json:"SmsMfaConfiguration,omitempty"`
	SoftwareTokenMfaConfiguration *softwareTokenMfaConfigJSON `json:"SoftwareTokenMfaConfiguration,omitempty"`
	EmailMfaConfiguration         *emailMfaConfigJSON         `json:"EmailMfaConfiguration,omitempty"`
	MfaConfiguration              string                      `json:"MfaConfiguration,omitempty"`
}

func (h *Handler) handleGetUserPoolMfaConfigFull(
	_ context.Context,
	in *getUserPoolMfaConfigFullInput,
) (*getUserPoolMfaConfigFullOutput, error) {
	cfg, err := h.Backend.GetUserPoolMfaConfigFull(in.UserPoolID)
	if err != nil {
		return nil, err
	}

	out := &getUserPoolMfaConfigFullOutput{MfaConfiguration: cfg.MfaConfiguration}
	if out.MfaConfiguration == "" {
		out.MfaConfiguration = mfaConfigOFF
	}

	if cfg.SmsMfaConfiguration != nil {
		out.SmsMfaConfiguration = &smsMfaConfigJSON{
			SmsAuthenticationMessage: cfg.SmsMfaConfiguration.SmsAuthenticationMessage,
		}

		if cfg.SmsMfaConfiguration.SmsConfiguration != nil {
			out.SmsMfaConfiguration.SmsConfiguration = &smsConfigJSON{
				SnsCallerArn: cfg.SmsMfaConfiguration.SmsConfiguration.SnsCallerArn,
				SnsRegion:    cfg.SmsMfaConfiguration.SmsConfiguration.SnsRegion,
				ExternalID:   cfg.SmsMfaConfiguration.SmsConfiguration.ExternalID,
			}
		}
	}

	if cfg.SoftwareTokenMfa != nil {
		out.SoftwareTokenMfaConfiguration = &softwareTokenMfaConfigJSON{Enabled: cfg.SoftwareTokenMfa.Enabled}
	}

	if cfg.EmailMfaConfiguration != nil {
		out.EmailMfaConfiguration = &emailMfaConfigJSON{
			Message: cfg.EmailMfaConfiguration.Message,
			Subject: cfg.EmailMfaConfiguration.Subject,
		}
	}

	return out, nil
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

func (h *Handler) handleSetUserPoolMfaConfigFull(
	_ context.Context,
	in *setUserPoolMfaConfigFullInput,
) (*setUserPoolMfaConfigFullOutput, error) {
	cfg := UserPoolMfaFullConfig{
		MfaConfiguration: in.MfaConfiguration,
	}

	if in.SmsMfaConfiguration != nil {
		smsCfg := &SmsMfaConfiguration{
			SmsAuthenticationMessage: in.SmsMfaConfiguration.SmsAuthenticationMessage,
		}

		if in.SmsMfaConfiguration.SmsConfiguration != nil {
			smsCfg.SmsConfiguration = &SmsConfiguration{
				SnsCallerArn: in.SmsMfaConfiguration.SmsConfiguration.SnsCallerArn,
				SnsRegion:    in.SmsMfaConfiguration.SmsConfiguration.SnsRegion,
				ExternalID:   in.SmsMfaConfiguration.SmsConfiguration.ExternalID,
			}
		}

		cfg.SmsMfaConfiguration = smsCfg
	}

	if in.SoftwareTokenMfaConfiguration != nil {
		cfg.SoftwareTokenMfa = &SoftwareTokenMfaConfiguration{Enabled: in.SoftwareTokenMfaConfiguration.Enabled}
	}

	if in.EmailMfaConfiguration != nil {
		cfg.EmailMfaConfiguration = &EmailMfaConfiguration{
			Message: in.EmailMfaConfiguration.Message,
			Subject: in.EmailMfaConfiguration.Subject,
		}
	}

	if err := h.Backend.SetUserPoolMfaConfigFull(in.UserPoolID, cfg); err != nil {
		return nil, err
	}

	out := &setUserPoolMfaConfigFullOutput{MfaConfiguration: in.MfaConfiguration}
	out.SmsMfaConfiguration = in.SmsMfaConfiguration
	out.SoftwareTokenMfaConfiguration = in.SoftwareTokenMfaConfiguration
	out.EmailMfaConfiguration = in.EmailMfaConfiguration

	return out, nil
}

// ---------------------------------------------------------------------------
// UICustomization — extended with ImageUrl and timestamps
// ---------------------------------------------------------------------------

type setUICustomizationFullInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	ClientID   string `json:"ClientId,omitempty"`
	CSS        string `json:"CSS,omitempty"`
	ImageData  string `json:"ImageData,omitempty"`
}

type uiCustomizationJSON struct {
	UserPoolID       string  `json:"UserPoolId,omitempty"`
	ClientID         string  `json:"ClientId,omitempty"`
	CSS              string  `json:"CSS,omitempty"`
	ImageURL         string  `json:"ImageUrl,omitempty"`
	CreationDate     float64 `json:"CreationDate,omitempty"`
	LastModifiedDate float64 `json:"LastModifiedDate,omitempty"`
}

type setUICustomizationFullOutput struct {
	UICustomization *uiCustomizationJSON `json:"UICustomization,omitempty"`
}

func (h *Handler) handleSetUICustomizationFull(
	_ context.Context,
	in *setUICustomizationFullInput,
) (*setUICustomizationFullOutput, error) {
	ui, err := h.Backend.SetUICustomizationFull(in.UserPoolID, in.ClientID, in.CSS, in.ImageData)
	if err != nil {
		return nil, err
	}

	return &setUICustomizationFullOutput{UICustomization: toUICustomizationJSON(ui)}, nil
}

type getUICustomizationFullInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	ClientID   string `json:"ClientId,omitempty"`
}

type getUICustomizationFullOutput struct {
	UICustomization *uiCustomizationJSON `json:"UICustomization,omitempty"`
}

func (h *Handler) handleGetUICustomizationFull(
	_ context.Context,
	in *getUICustomizationFullInput,
) (*getUICustomizationFullOutput, error) {
	ui, err := h.Backend.GetUICustomizationFull(in.UserPoolID, in.ClientID)
	if err != nil {
		return nil, err
	}

	return &getUICustomizationFullOutput{UICustomization: toUICustomizationJSON(ui)}, nil
}

func toUICustomizationJSON(ui *UICustomization) *uiCustomizationJSON {
	out := &uiCustomizationJSON{
		UserPoolID: ui.UserPoolID,
		ClientID:   ui.ClientID,
		CSS:        ui.CSS,
		ImageURL:   ui.ImageURL,
	}

	if !ui.CreatedAt.IsZero() {
		out.CreationDate = float64(ui.CreatedAt.Unix())
	}

	if !ui.LastModifiedAt.IsZero() {
		out.LastModifiedDate = float64(ui.LastModifiedAt.Unix())
	}

	return out
}

// ---------------------------------------------------------------------------
// GetUserAttributeVerificationCode — stateful
// ---------------------------------------------------------------------------

type getUserAttributeVerifCodeFullInput struct {
	AccessToken   string `json:"AccessToken,omitempty"`
	AttributeName string `json:"AttributeName,omitempty"`
}

type getUserAttributeVerifCodeFullOutput struct {
	CodeDeliveryDetails map[string]string `json:"CodeDeliveryDetails,omitempty"`
}

func (h *Handler) handleGetUserAttributeVerificationCodeFull(
	_ context.Context,
	in *getUserAttributeVerifCodeFullInput,
) (*getUserAttributeVerifCodeFullOutput, error) {
	_, dest, medium, err := h.Backend.GetUserAttributeVerificationCode(in.AccessToken, in.AttributeName)
	if err != nil {
		return nil, err
	}

	return &getUserAttributeVerifCodeFullOutput{
		CodeDeliveryDetails: map[string]string{
			keyDeliveryMedium: medium,
			keyDestination:    dest,
			keyAttributeName:  in.AttributeName,
		},
	}, nil
}

// ---------------------------------------------------------------------------
// VerifyUserAttribute — stateful code validation
// ---------------------------------------------------------------------------

type verifyUserAttributeFullInput struct {
	AccessToken   string `json:"AccessToken,omitempty"`
	AttributeName string `json:"AttributeName,omitempty"`
	Code          string `json:"Code,omitempty"`
}

type verifyUserAttributeFullOutput struct{}

func (h *Handler) handleVerifyUserAttributeFull(
	_ context.Context,
	in *verifyUserAttributeFullInput,
) (*verifyUserAttributeFullOutput, error) {
	if err := h.Backend.VerifyUserAttributeWithCode(in.AccessToken, in.AttributeName, in.Code); err != nil {
		return nil, err
	}

	return &verifyUserAttributeFullOutput{}, nil
}

// ---------------------------------------------------------------------------
// IdentityProvider — full with AttributeMapping and IdpIdentifiers
// ---------------------------------------------------------------------------

type (
	idpProviderDetails  map[string]string
	idpAttributeMapping map[string]string
)

type createIdentityProviderFullInput struct {
	UserPoolID       string              `json:"UserPoolId,omitempty"`
	ProviderName     string              `json:"ProviderName,omitempty"`
	ProviderType     string              `json:"ProviderType,omitempty"`
	ProviderDetails  idpProviderDetails  `json:"ProviderDetails"`
	AttributeMapping idpAttributeMapping `json:"AttributeMapping"`
	IdpIdentifiers   []string            `json:"IdpIdentifiers,omitempty"`
}

type identityProviderJSON struct {
	UserPoolID       string              `json:"UserPoolId,omitempty"`
	ProviderName     string              `json:"ProviderName,omitempty"`
	ProviderType     string              `json:"ProviderType,omitempty"`
	ProviderDetails  idpProviderDetails  `json:"ProviderDetails"`
	AttributeMapping idpAttributeMapping `json:"AttributeMapping"`
	IdpIdentifiers   []string            `json:"IdpIdentifiers,omitempty"`
	CreationDate     float64             `json:"CreationDate,omitempty"`
	LastModifiedDate float64             `json:"LastModifiedDate,omitempty"`
}

type createIdentityProviderFullOutput struct {
	IdentityProvider *identityProviderJSON `json:"IdentityProvider,omitempty"`
}

func (h *Handler) handleCreateIdentityProviderFull(
	_ context.Context,
	in *createIdentityProviderFullInput,
) (*createIdentityProviderFullOutput, error) {
	idp, err := h.Backend.CreateIdentityProviderFull(
		in.UserPoolID, in.ProviderName, in.ProviderType,
		map[string]string(in.ProviderDetails),
		map[string]string(in.AttributeMapping),
		in.IdpIdentifiers,
	)
	if err != nil {
		return nil, err
	}

	return &createIdentityProviderFullOutput{IdentityProvider: toIdentityProviderJSON(idp)}, nil
}

type updateIdentityProviderFullInput struct {
	UserPoolID       string              `json:"UserPoolId,omitempty"`
	ProviderName     string              `json:"ProviderName,omitempty"`
	ProviderDetails  idpProviderDetails  `json:"ProviderDetails"`
	AttributeMapping idpAttributeMapping `json:"AttributeMapping"`
	IdpIdentifiers   []string            `json:"IdpIdentifiers,omitempty"`
}

type updateIdentityProviderFullOutput struct {
	IdentityProvider *identityProviderJSON `json:"IdentityProvider,omitempty"`
}

func (h *Handler) handleUpdateIdentityProviderFull(
	_ context.Context,
	in *updateIdentityProviderFullInput,
) (*updateIdentityProviderFullOutput, error) {
	idp, err := h.Backend.UpdateIdentityProviderFull(
		in.UserPoolID, in.ProviderName,
		map[string]string(in.ProviderDetails),
		map[string]string(in.AttributeMapping),
		in.IdpIdentifiers,
	)
	if err != nil {
		return nil, err
	}

	return &updateIdentityProviderFullOutput{IdentityProvider: toIdentityProviderJSON(idp)}, nil
}

type describeIdentityProviderFullInput struct {
	UserPoolID   string `json:"UserPoolId,omitempty"`
	ProviderName string `json:"ProviderName,omitempty"`
}

type describeIdentityProviderFullOutput struct {
	IdentityProvider *identityProviderJSON `json:"IdentityProvider,omitempty"`
}

func (h *Handler) handleDescribeIdentityProviderFull(
	_ context.Context,
	in *describeIdentityProviderFullInput,
) (*describeIdentityProviderFullOutput, error) {
	idp, err := h.Backend.DescribeIdentityProvider(in.UserPoolID, in.ProviderName)
	if err != nil {
		return nil, err
	}

	return &describeIdentityProviderFullOutput{IdentityProvider: toIdentityProviderJSON(idp)}, nil
}

type getIdentityProviderByIdentFullInput struct {
	UserPoolID    string `json:"UserPoolId,omitempty"`
	IdpIdentifier string `json:"IdpIdentifier,omitempty"`
}

type getIdentityProviderByIdentFullOutput struct {
	IdentityProvider *identityProviderJSON `json:"IdentityProvider,omitempty"`
}

func (h *Handler) handleGetIdentityProviderByIdentifierFull(
	_ context.Context,
	in *getIdentityProviderByIdentFullInput,
) (*getIdentityProviderByIdentFullOutput, error) {
	idp, err := h.Backend.GetIdentityProviderByIdentifier(in.UserPoolID, in.IdpIdentifier)
	if err != nil {
		return nil, err
	}

	return &getIdentityProviderByIdentFullOutput{IdentityProvider: toIdentityProviderJSON(idp)}, nil
}

type listIdentityProvidersFullInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	NextToken  string `json:"NextToken,omitempty"`
	MaxResults int    `json:"MaxResults,omitempty"`
}

type listIdentityProvidersFullOutput struct {
	NextToken string                        `json:"NextToken,omitempty"`
	Providers []identityProviderSummaryJSON `json:"Providers,omitempty"`
}

type identityProviderSummaryJSON struct {
	ProviderName     string  `json:"ProviderName,omitempty"`
	ProviderType     string  `json:"ProviderType,omitempty"`
	CreationDate     float64 `json:"CreationDate,omitempty"`
	LastModifiedDate float64 `json:"LastModifiedDate,omitempty"`
}

func (h *Handler) handleListIdentityProvidersFull(
	_ context.Context,
	in *listIdentityProvidersFullInput,
) (*listIdentityProvidersFullOutput, error) {
	idps, err := h.Backend.ListIdentityProviders(in.UserPoolID)
	if err != nil {
		return nil, err
	}

	out := make([]identityProviderSummaryJSON, 0, len(idps))

	for _, idp := range idps {
		s := identityProviderSummaryJSON{
			ProviderName: idp.ProviderName,
			ProviderType: idp.ProviderType,
		}

		if !idp.CreatedAt.IsZero() {
			s.CreationDate = float64(idp.CreatedAt.Unix())
		}

		if !idp.LastModifiedAt.IsZero() {
			s.LastModifiedDate = float64(idp.LastModifiedAt.Unix())
		}

		out = append(out, s)
	}

	return &listIdentityProvidersFullOutput{Providers: out}, nil
}

func toIdentityProviderJSON(idp *IdentityProvider) *identityProviderJSON {
	out := &identityProviderJSON{
		UserPoolID:       idp.UserPoolID,
		ProviderName:     idp.ProviderName,
		ProviderType:     idp.ProviderType,
		ProviderDetails:  idpProviderDetails(idp.ProviderDetails),
		AttributeMapping: idpAttributeMapping(idp.AttributeMapping),
		IdpIdentifiers:   idp.IdpIdentifiers,
	}

	if !idp.CreatedAt.IsZero() {
		out.CreationDate = float64(idp.CreatedAt.Unix())
	}

	if !idp.LastModifiedAt.IsZero() {
		out.LastModifiedDate = float64(idp.LastModifiedAt.Unix())
	}

	return out
}

// ---------------------------------------------------------------------------
// UserPoolDomain — with CustomDomainConfig (CertificateArn)
// ---------------------------------------------------------------------------

type customDomainConfigJSON struct {
	CertificateArn string `json:"CertificateArn,omitempty"`
}

type createUserPoolDomainFullInput struct {
	CustomDomainConfig *customDomainConfigJSON `json:"CustomDomainConfig,omitempty"`
	UserPoolID         string                  `json:"UserPoolId,omitempty"`
	Domain             string                  `json:"Domain,omitempty"`
}

type createUserPoolDomainFullOutput struct {
	CloudFrontDomain string `json:"CloudFrontDomain,omitempty"`
}

func (h *Handler) handleCreateUserPoolDomainFull(
	_ context.Context,
	in *createUserPoolDomainFullInput,
) (*createUserPoolDomainFullOutput, error) {
	certArn := ""
	if in.CustomDomainConfig != nil {
		certArn = in.CustomDomainConfig.CertificateArn
	}

	d, err := h.Backend.CreateUserPoolDomainFull(in.UserPoolID, in.Domain, certArn)
	if err != nil {
		return nil, err
	}

	// AWS only returns CloudFrontDomain for custom domains (with a certificate).
	// Managed Cognito domains return an empty response.
	cfDomain := ""
	if certArn != "" {
		cfDomain = d.CloudFrontDistribution
	}

	return &createUserPoolDomainFullOutput{CloudFrontDomain: cfDomain}, nil
}

type updateUserPoolDomainFullInput struct {
	CustomDomainConfig *customDomainConfigJSON `json:"CustomDomainConfig,omitempty"`
	UserPoolID         string                  `json:"UserPoolId,omitempty"`
	Domain             string                  `json:"Domain,omitempty"`
}

type updateUserPoolDomainFullOutput struct {
	CloudFrontDomain string `json:"CloudFrontDomain,omitempty"`
}

func (h *Handler) handleUpdateUserPoolDomainFull(
	_ context.Context,
	in *updateUserPoolDomainFullInput,
) (*updateUserPoolDomainFullOutput, error) {
	certArn := ""
	if in.CustomDomainConfig != nil {
		certArn = in.CustomDomainConfig.CertificateArn
	}

	cfDomain, err := h.Backend.UpdateUserPoolDomainFull(in.UserPoolID, in.Domain, certArn)
	if err != nil {
		return nil, err
	}

	return &updateUserPoolDomainFullOutput{CloudFrontDomain: cfDomain}, nil
}

// ---------------------------------------------------------------------------
// RiskConfiguration — fully typed
// ---------------------------------------------------------------------------

type compromisedCredActionsJSON struct {
	EventAction string `json:"EventAction,omitempty"`
}

type compromisedCredRiskConfigJSON struct {
	Actions     *compromisedCredActionsJSON `json:"Actions,omitempty"`
	EventFilter []string                    `json:"EventFilter,omitempty"`
}

type accountTakeoverActionTypeJSON struct {
	EventAction string `json:"EventAction,omitempty"`
	Notify      bool   `json:"Notify,omitempty"`
}

type accountTakeoverActionsJSON struct {
	LowAction    *accountTakeoverActionTypeJSON `json:"LowAction,omitempty"`
	MediumAction *accountTakeoverActionTypeJSON `json:"MediumAction,omitempty"`
	HighAction   *accountTakeoverActionTypeJSON `json:"HighAction,omitempty"`
}

type notifyEmailTypeJSON struct {
	HTMLBody string `json:"HtmlBody,omitempty"`
	Subject  string `json:"Subject,omitempty"`
	TextBody string `json:"TextBody,omitempty"`
}

type notifyConfigJSON struct {
	BlockEmail    *notifyEmailTypeJSON `json:"BlockEmail,omitempty"`
	MfaEmail      *notifyEmailTypeJSON `json:"MfaEmail,omitempty"`
	NoActionEmail *notifyEmailTypeJSON `json:"NoActionEmail,omitempty"`
	From          string               `json:"From,omitempty"`
	ReplyTo       string               `json:"ReplyTo,omitempty"`
	SourceArn     string               `json:"SourceArn,omitempty"`
}

type accountTakeoverRiskConfigJSON struct {
	Actions             *accountTakeoverActionsJSON `json:"Actions,omitempty"`
	NotifyConfiguration *notifyConfigJSON           `json:"NotifyConfiguration,omitempty"`
}

type riskExceptionConfigJSON struct {
	BlockedIPRangeList []string `json:"BlockedIPRangeList,omitempty"`
	SkippedIPRangeList []string `json:"SkippedIPRangeList,omitempty"`
}

type describeRiskConfigFullInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	ClientID   string `json:"ClientId,omitempty"`
}

type riskConfigurationJSON struct {
	CompromisedCredentialsRiskConfiguration *compromisedCredRiskConfigJSON `json:"CompromisedCredentialsRiskConfiguration,omitempty"` //nolint:lll // AWS field name
	AccountTakeoverRiskConfiguration        *accountTakeoverRiskConfigJSON `json:"AccountTakeoverRiskConfiguration,omitempty"`        //nolint:lll // AWS field name
	RiskExceptionConfiguration              *riskExceptionConfigJSON       `json:"RiskExceptionConfiguration,omitempty"`
	UserPoolID                              string                         `json:"UserPoolId,omitempty"`
	ClientID                                string                         `json:"ClientId,omitempty"`
}

type describeRiskConfigFullOutput struct {
	RiskConfiguration *riskConfigurationJSON `json:"RiskConfiguration,omitempty"`
}

func (h *Handler) handleDescribeRiskConfigurationFull(
	_ context.Context,
	in *describeRiskConfigFullInput,
) (*describeRiskConfigFullOutput, error) {
	cfg, err := h.Backend.GetTypedRiskConfiguration(in.UserPoolID, in.ClientID)
	if err != nil {
		return nil, err
	}

	return &describeRiskConfigFullOutput{RiskConfiguration: toRiskConfigJSON(cfg)}, nil
}

type setRiskConfigFullInput struct {
	CompromisedCredentialsRiskConfiguration *compromisedCredRiskConfigJSON `json:"CompromisedCredentialsRiskConfiguration,omitempty"` //nolint:lll // AWS field name
	AccountTakeoverRiskConfiguration        *accountTakeoverRiskConfigJSON `json:"AccountTakeoverRiskConfiguration,omitempty"`        //nolint:lll // AWS field name
	RiskExceptionConfiguration              *riskExceptionConfigJSON       `json:"RiskExceptionConfiguration,omitempty"`
	UserPoolID                              string                         `json:"UserPoolId,omitempty"`
	ClientID                                string                         `json:"ClientId,omitempty"`
}

type setRiskConfigFullOutput struct {
	RiskConfiguration *riskConfigurationJSON `json:"RiskConfiguration,omitempty"`
}

func (h *Handler) handleSetRiskConfigurationFull(
	_ context.Context,
	in *setRiskConfigFullInput,
) (*setRiskConfigFullOutput, error) {
	cfg := &TypedRiskConfiguration{
		UserPoolID: in.UserPoolID,
		ClientID:   in.ClientID,
	}

	if in.CompromisedCredentialsRiskConfiguration != nil {
		c := &CompromisedCredentialsRiskConfig{
			EventFilter: in.CompromisedCredentialsRiskConfiguration.EventFilter,
		}

		if in.CompromisedCredentialsRiskConfiguration.Actions != nil {
			c.Actions = &CompromisedCredentialsActions{
				EventAction: in.CompromisedCredentialsRiskConfiguration.Actions.EventAction,
			}
		}

		cfg.CompromisedCredentialsRiskConfig = c
	}

	if in.AccountTakeoverRiskConfiguration != nil {
		at := &AccountTakeoverRiskConfig{}

		if in.AccountTakeoverRiskConfiguration.Actions != nil {
			at.Actions = fromAccountTakeoverActionsJSON(in.AccountTakeoverRiskConfiguration.Actions)
		}

		if in.AccountTakeoverRiskConfiguration.NotifyConfiguration != nil {
			at.NotifyConfiguration = fromNotifyConfigJSON(in.AccountTakeoverRiskConfiguration.NotifyConfiguration)
		}

		cfg.AccountTakeoverRiskConfig = at
	}

	if in.RiskExceptionConfiguration != nil {
		exc := make([]string, len(in.RiskExceptionConfiguration.BlockedIPRangeList))
		copy(exc, in.RiskExceptionConfiguration.BlockedIPRangeList)
		skp := make([]string, len(in.RiskExceptionConfiguration.SkippedIPRangeList))
		copy(skp, in.RiskExceptionConfiguration.SkippedIPRangeList)
		cfg.RiskExceptionConfiguration = &RiskExceptionConfig{
			BlockedIPRangeList: exc,
			SkippedIPRangeList: skp,
		}
	}

	if err := h.Backend.SetTypedRiskConfiguration(cfg); err != nil {
		return nil, err
	}

	stored, err := h.Backend.GetTypedRiskConfiguration(in.UserPoolID, in.ClientID)
	if err != nil {
		return nil, err
	}

	return &setRiskConfigFullOutput{RiskConfiguration: toRiskConfigJSON(stored)}, nil
}

func toRiskConfigJSON(cfg *TypedRiskConfiguration) *riskConfigurationJSON {
	out := &riskConfigurationJSON{
		UserPoolID: cfg.UserPoolID,
		ClientID:   cfg.ClientID,
	}

	if cfg.CompromisedCredentialsRiskConfig != nil {
		c := &compromisedCredRiskConfigJSON{
			EventFilter: cfg.CompromisedCredentialsRiskConfig.EventFilter,
		}

		if cfg.CompromisedCredentialsRiskConfig.Actions != nil {
			c.Actions = &compromisedCredActionsJSON{
				EventAction: cfg.CompromisedCredentialsRiskConfig.Actions.EventAction,
			}
		}

		out.CompromisedCredentialsRiskConfiguration = c
	}

	if cfg.AccountTakeoverRiskConfig != nil {
		at := &accountTakeoverRiskConfigJSON{}

		if cfg.AccountTakeoverRiskConfig.Actions != nil {
			at.Actions = toAccountTakeoverActionsJSON(cfg.AccountTakeoverRiskConfig.Actions)
		}

		if cfg.AccountTakeoverRiskConfig.NotifyConfiguration != nil {
			at.NotifyConfiguration = toNotifyConfigJSON(cfg.AccountTakeoverRiskConfig.NotifyConfiguration)
		}

		out.AccountTakeoverRiskConfiguration = at
	}

	if cfg.RiskExceptionConfiguration != nil {
		out.RiskExceptionConfiguration = &riskExceptionConfigJSON{
			BlockedIPRangeList: cfg.RiskExceptionConfiguration.BlockedIPRangeList,
			SkippedIPRangeList: cfg.RiskExceptionConfiguration.SkippedIPRangeList,
		}
	}

	return out
}

func toAccountTakeoverActionsJSON(a *AccountTakeoverActions) *accountTakeoverActionsJSON {
	out := &accountTakeoverActionsJSON{}

	if a.LowAction != nil {
		out.LowAction = &accountTakeoverActionTypeJSON{Notify: a.LowAction.Notify, EventAction: a.LowAction.EventAction}
	}

	if a.MediumAction != nil {
		out.MediumAction = &accountTakeoverActionTypeJSON{
			Notify:      a.MediumAction.Notify,
			EventAction: a.MediumAction.EventAction,
		}
	}

	if a.HighAction != nil {
		out.HighAction = &accountTakeoverActionTypeJSON{
			Notify:      a.HighAction.Notify,
			EventAction: a.HighAction.EventAction,
		}
	}

	return out
}

func fromAccountTakeoverActionsJSON(in *accountTakeoverActionsJSON) *AccountTakeoverActions {
	out := &AccountTakeoverActions{}

	if in.LowAction != nil {
		out.LowAction = &AccountTakeoverActionType{Notify: in.LowAction.Notify, EventAction: in.LowAction.EventAction}
	}

	if in.MediumAction != nil {
		out.MediumAction = &AccountTakeoverActionType{
			Notify:      in.MediumAction.Notify,
			EventAction: in.MediumAction.EventAction,
		}
	}

	if in.HighAction != nil {
		out.HighAction = &AccountTakeoverActionType{
			Notify:      in.HighAction.Notify,
			EventAction: in.HighAction.EventAction,
		}
	}

	return out
}

func toNotifyConfigJSON(n *NotifyConfigurationType) *notifyConfigJSON {
	out := &notifyConfigJSON{
		From:      n.From,
		ReplyTo:   n.ReplyTo,
		SourceArn: n.SourceArn,
	}

	if n.BlockEmail != nil {
		out.BlockEmail = &notifyEmailTypeJSON{
			HTMLBody: n.BlockEmail.HTMLBody,
			Subject:  n.BlockEmail.Subject,
			TextBody: n.BlockEmail.TextBody,
		}
	}

	if n.MfaEmail != nil {
		out.MfaEmail = &notifyEmailTypeJSON{
			HTMLBody: n.MfaEmail.HTMLBody,
			Subject:  n.MfaEmail.Subject,
			TextBody: n.MfaEmail.TextBody,
		}
	}

	if n.NoActionEmail != nil {
		out.NoActionEmail = &notifyEmailTypeJSON{
			HTMLBody: n.NoActionEmail.HTMLBody,
			Subject:  n.NoActionEmail.Subject,
			TextBody: n.NoActionEmail.TextBody,
		}
	}

	return out
}

func fromNotifyConfigJSON(in *notifyConfigJSON) *NotifyConfigurationType {
	out := &NotifyConfigurationType{
		From:      in.From,
		ReplyTo:   in.ReplyTo,
		SourceArn: in.SourceArn,
	}

	if in.BlockEmail != nil {
		out.BlockEmail = &NotifyEmailType{
			HTMLBody: in.BlockEmail.HTMLBody,
			Subject:  in.BlockEmail.Subject,
			TextBody: in.BlockEmail.TextBody,
		}
	}

	if in.MfaEmail != nil {
		out.MfaEmail = &NotifyEmailType{
			HTMLBody: in.MfaEmail.HTMLBody,
			Subject:  in.MfaEmail.Subject,
			TextBody: in.MfaEmail.TextBody,
		}
	}

	if in.NoActionEmail != nil {
		out.NoActionEmail = &NotifyEmailType{
			HTMLBody: in.NoActionEmail.HTMLBody,
			Subject:  in.NoActionEmail.Subject,
			TextBody: in.NoActionEmail.TextBody,
		}
	}

	return out
}

// ---------------------------------------------------------------------------
// AdminCreateUser — full with MessageAction and DesiredDeliveryMediums
// ---------------------------------------------------------------------------

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

func (h *Handler) handleAdminCreateUserFull(
	_ context.Context,
	in *adminCreateUserFullInput,
) (*adminCreateUserFullOutput, error) {
	attrs := attributeListToMap(in.UserAttributes)

	user, err := h.Backend.AdminCreateUserFull(
		in.UserPoolID,
		in.Username,
		in.TemporaryPassword,
		attrs,
		in.MessageAction,
		in.DesiredDeliveryMediums,
		in.ForceAliasCreation,
	)
	if err != nil {
		return nil, err
	}

	return &adminCreateUserFullOutput{User: toAdminUserJSON(user)}, nil
}

func toAdminUserJSON(u *User) *adminUserJSON {
	return &adminUserJSON{
		Username:             u.Username,
		UserStatus:           u.Status,
		UserAttributes:       sortedAttributeList(u.Attributes),
		UserCreateDate:       float64(u.CreatedAt.Unix()),
		UserLastModifiedDate: float64(u.UpdatedAt.Unix()),
		Enabled:              u.Enabled,
	}
}

// ---------------------------------------------------------------------------
// AdminSetUserPassword — with policy enforcement
// ---------------------------------------------------------------------------

type adminSetUserPasswordFullInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	Username   string `json:"Username,omitempty"`
	Password   string `json:"Password,omitempty"`
	Permanent  bool   `json:"Permanent,omitempty"`
}

type adminSetUserPasswordFullOutput struct{}

func (h *Handler) handleAdminSetUserPasswordFull(
	_ context.Context,
	in *adminSetUserPasswordFullInput,
) (*adminSetUserPasswordFullOutput, error) {
	if err := h.Backend.AdminSetUserPasswordFull(in.UserPoolID, in.Username, in.Password, in.Permanent); err != nil {
		return nil, err
	}

	return &adminSetUserPasswordFullOutput{}, nil
}

// ---------------------------------------------------------------------------
// Group — full with RoleArn and pagination
// ---------------------------------------------------------------------------

type createGroupFullInput struct {
	UserPoolID  string `json:"UserPoolId,omitempty"`
	GroupName   string `json:"GroupName,omitempty"`
	Description string `json:"Description,omitempty"`
	RoleArn     string `json:"RoleArn,omitempty"`
	Precedence  int32  `json:"Precedence,omitempty"`
}

type groupFullSummary struct {
	GroupName        string  `json:"GroupName,omitempty"`
	UserPoolID       string  `json:"UserPoolId,omitempty"`
	Description      string  `json:"Description,omitempty"`
	RoleArn          string  `json:"RoleArn,omitempty"`
	Precedence       int32   `json:"Precedence,omitempty"`
	CreationDate     float64 `json:"CreationDate,omitempty"`
	LastModifiedDate float64 `json:"LastModifiedDate,omitempty"`
}

type createGroupFullOutput struct {
	Group *groupFullSummary `json:"Group,omitempty"`
}

func (h *Handler) handleCreateGroupFull(
	_ context.Context,
	in *createGroupFullInput,
) (*createGroupFullOutput, error) {
	g, err := h.Backend.CreateGroupFull(in.UserPoolID, in.GroupName, in.Description, in.RoleArn, in.Precedence)
	if err != nil {
		return nil, err
	}

	return &createGroupFullOutput{Group: toGroupFullSummary(g)}, nil
}

type updateGroupFullInput struct {
	UserPoolID  string `json:"UserPoolId,omitempty"`
	GroupName   string `json:"GroupName,omitempty"`
	Description string `json:"Description,omitempty"`
	RoleArn     string `json:"RoleArn,omitempty"`
	Precedence  int32  `json:"Precedence,omitempty"`
}

type updateGroupFullOutput struct {
	Group *groupFullSummary `json:"Group,omitempty"`
}

func (h *Handler) handleUpdateGroupFull(
	_ context.Context,
	in *updateGroupFullInput,
) (*updateGroupFullOutput, error) {
	g, err := h.Backend.UpdateGroupFull(in.UserPoolID, in.GroupName, in.Description, in.RoleArn, in.Precedence)
	if err != nil {
		return nil, err
	}

	return &updateGroupFullOutput{Group: toGroupFullSummary(g)}, nil
}

type getGroupFullInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	GroupName  string `json:"GroupName,omitempty"`
}

type getGroupFullOutput struct {
	Group *groupFullSummary `json:"Group,omitempty"`
}

func (h *Handler) handleGetGroupFull(
	_ context.Context,
	in *getGroupFullInput,
) (*getGroupFullOutput, error) {
	g, err := h.Backend.GetGroup(in.UserPoolID, in.GroupName)
	if err != nil {
		return nil, err
	}

	return &getGroupFullOutput{Group: toGroupFullSummary(g)}, nil
}

type listGroupsFullInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	NextToken  string `json:"NextToken,omitempty"`
	Limit      int    `json:"Limit,omitempty"`
}

type listGroupsFullOutput struct {
	NextToken string              `json:"NextToken,omitempty"`
	Groups    []*groupFullSummary `json:"Groups,omitempty"`
}

func (h *Handler) handleListGroupsFull(
	_ context.Context,
	in *listGroupsFullInput,
) (*listGroupsFullOutput, error) {
	groups, nextToken, err := h.Backend.ListGroupsPage(in.UserPoolID, in.Limit, in.NextToken)
	if err != nil {
		return nil, err
	}

	out := make([]*groupFullSummary, 0, len(groups))

	for _, g := range groups {
		out = append(out, toGroupFullSummary(g))
	}

	return &listGroupsFullOutput{Groups: out, NextToken: nextToken}, nil
}

type listUsersInGroupFullInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	GroupName  string `json:"GroupName,omitempty"`
	NextToken  string `json:"NextToken,omitempty"`
	Limit      int    `json:"Limit,omitempty"`
}

type listUsersInGroupFullOutput struct {
	NextToken string          `json:"NextToken,omitempty"`
	Users     []adminUserJSON `json:"Users,omitempty"`
}

func (h *Handler) handleListUsersInGroupFull(
	_ context.Context,
	in *listUsersInGroupFullInput,
) (*listUsersInGroupFullOutput, error) {
	users, nextToken, err := h.Backend.ListUsersInGroupPage(in.UserPoolID, in.GroupName, in.Limit, in.NextToken)
	if err != nil {
		return nil, err
	}

	out := make([]adminUserJSON, 0, len(users))

	for _, u := range users {
		out = append(out, *toAdminUserJSON(u))
	}

	return &listUsersInGroupFullOutput{Users: out, NextToken: nextToken}, nil
}

func toGroupFullSummary(g *Group) *groupFullSummary {
	out := &groupFullSummary{
		GroupName:   g.GroupName,
		UserPoolID:  g.UserPoolID,
		Description: g.Description,
		RoleArn:     g.RoleArn,
		Precedence:  g.Precedence,
	}

	if !g.CreatedAt.IsZero() {
		out.CreationDate = float64(g.CreatedAt.Unix())
	}

	if !g.LastModifiedAt.IsZero() {
		out.LastModifiedDate = float64(g.LastModifiedAt.Unix())
	}

	return out
}
