package cognitoidp

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// completenessDispatchTable returns stub dispatch entries for all previously
// notImplemented Cognito IDP operations.
func (h *Handler) completenessDispatchTable() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"AdminGetDevice":                       service.WrapOp(h.handleAdminGetDevice),
		"AdminLinkProviderForUser":             service.WrapOp(h.handleAdminLinkProviderForUser),
		"AdminListDevices":                     service.WrapOp(h.handleAdminListDevices),
		"AdminListUserAuthEvents":              service.WrapOp(h.handleAdminListUserAuthEvents),
		"AdminSetUserMFAPreference":            service.WrapOp(h.handleAdminSetUserMFAPreference),
		"AdminSetUserSettings":                 service.WrapOp(h.handleAdminSetUserSettings),
		"AdminUpdateAuthEventFeedback":         service.WrapOp(h.handleAdminUpdateAuthEventFeedback),
		"AdminUpdateDeviceStatus":              service.WrapOp(h.handleAdminUpdateDeviceStatus),
		"AssociateSoftwareToken":               service.WrapOp(h.handleAssociateSoftwareToken),
		"CompleteWebAuthnRegistration":         service.WrapOp(h.handleCompleteWebAuthnRegistration),
		"ConfirmDevice":                        service.WrapOp(h.handleConfirmDevice),
		"CreateIdentityProvider":               service.WrapOp(h.handleCreateIdentityProvider),
		"CreateManagedLoginBranding":           service.WrapOp(h.handleCreateManagedLoginBranding),
		"CreateResourceServer":                 service.WrapOp(h.handleCreateResourceServer),
		"CreateTerms":                          service.WrapOp(h.handleCreateTerms),
		"CreateUserImportJob":                  service.WrapOp(h.handleCreateUserImportJob),
		"CreateUserPoolDomain":                 service.WrapOp(h.handleCreateUserPoolDomain),
		"DeleteIdentityProvider":               service.WrapOp(h.handleDeleteIdentityProvider),
		"DeleteManagedLoginBranding":           service.WrapOp(h.handleDeleteManagedLoginBranding),
		"DeleteResourceServer":                 service.WrapOp(h.handleDeleteResourceServer),
		"DeleteTerms":                          service.WrapOp(h.handleDeleteTerms),
		"DeleteUserPoolClientSecret":           service.WrapOp(h.handleDeleteUserPoolClientSecret),
		"DeleteUserPoolDomain":                 service.WrapOp(h.handleDeleteUserPoolDomain),
		"DeleteWebAuthnCredential":             service.WrapOp(h.handleDeleteWebAuthnCredential),
		"DescribeIdentityProvider":             service.WrapOp(h.handleDescribeIdentityProvider),
		"DescribeManagedLoginBranding":         service.WrapOp(h.handleDescribeManagedLoginBranding),
		"DescribeManagedLoginBrandingByClient": service.WrapOp(h.handleDescribeManagedLoginBrandingByClient),
		"DescribeResourceServer":               service.WrapOp(h.handleDescribeResourceServer),
		"DescribeRiskConfiguration":            service.WrapOp(h.handleDescribeRiskConfiguration),
		"DescribeTerms":                        service.WrapOp(h.handleDescribeTerms),
		"DescribeUserImportJob":                service.WrapOp(h.handleDescribeUserImportJob),
		"DescribeUserPoolDomain":               service.WrapOp(h.handleDescribeUserPoolDomain),
		"ForgetDevice":                         service.WrapOp(h.handleForgetDevice),
		"GetCSVHeader":                         service.WrapOp(h.handleGetCSVHeader),
		"GetDevice":                            service.WrapOp(h.handleGetDevice),
		"GetIdentityProviderByIdentifier":      service.WrapOp(h.handleGetIdentityProviderByIdentifier),
		"GetLogDeliveryConfiguration":          service.WrapOp(h.handleGetLogDeliveryConfiguration),
		"GetTokensFromRefreshToken":            service.WrapOp(h.handleGetTokensFromRefreshToken),
		"GetUICustomization":                   service.WrapOp(h.handleGetUICustomization),
		"GetUserAttributeVerificationCode":     service.WrapOp(h.handleGetUserAttributeVerificationCode),
		"GetUserAuthFactors":                   service.WrapOp(h.handleGetUserAuthFactors),
		"ListDevices":                          service.WrapOp(h.handleListDevices),
		"ListIdentityProviders":                service.WrapOp(h.handleListIdentityProviders),
		"ListResourceServers":                  service.WrapOp(h.handleListResourceServers),
		"ListTagsForResource":                  service.WrapOp(h.handleListTagsForResource),
		"ListTerms":                            service.WrapOp(h.handleListTerms),
		"ListUserImportJobs":                   service.WrapOp(h.handleListUserImportJobs),
		"ListUserPoolClientSecrets":            service.WrapOp(h.handleListUserPoolClientSecrets),
		"ListWebAuthnCredentials":              service.WrapOp(h.handleListWebAuthnCredentials),
		"RespondToAuthChallenge":               service.WrapOp(h.handleRespondToAuthChallenge),
		"SetLogDeliveryConfiguration":          service.WrapOp(h.handleSetLogDeliveryConfiguration),
		"SetRiskConfiguration":                 service.WrapOp(h.handleSetRiskConfiguration),
		"SetUICustomization":                   service.WrapOp(h.handleSetUICustomization),
		"SetUserMFAPreference":                 service.WrapOp(h.handleSetUserMFAPreference),
		"SetUserSettings":                      service.WrapOp(h.handleSetUserSettings),
		"StartUserImportJob":                   service.WrapOp(h.handleStartUserImportJob),
		"StartWebAuthnRegistration":            service.WrapOp(h.handleStartWebAuthnRegistration),
		"StopUserImportJob":                    service.WrapOp(h.handleStopUserImportJob),
		"TagResource":                          service.WrapOp(h.handleTagResource),
		"UntagResource":                        service.WrapOp(h.handleUntagResource),
		"UpdateAuthEventFeedback":              service.WrapOp(h.handleUpdateAuthEventFeedback),
		"UpdateDeviceStatus":                   service.WrapOp(h.handleUpdateDeviceStatus),
		"UpdateIdentityProvider":               service.WrapOp(h.handleUpdateIdentityProvider),
		"UpdateManagedLoginBranding":           service.WrapOp(h.handleUpdateManagedLoginBranding),
		"UpdateResourceServer":                 service.WrapOp(h.handleUpdateResourceServer),
		"UpdateTerms":                          service.WrapOp(h.handleUpdateTerms),
		"UpdateUserPoolDomain":                 service.WrapOp(h.handleUpdateUserPoolDomain),
		"VerifySoftwareToken":                  service.WrapOp(h.handleVerifySoftwareToken),
	}
}

// ----- Devices -----

type adminGetDeviceInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	Username   string `json:"Username,omitempty"`
	DeviceKey  string `json:"DeviceKey,omitempty"`
}

type deviceType struct {
	DeviceCreateDate            *float64        `json:"DeviceCreateDate,omitempty"`
	DeviceLastModifiedDate      *float64        `json:"DeviceLastModifiedDate,omitempty"`
	DeviceLastAuthenticatedDate *float64        `json:"DeviceLastAuthenticatedDate,omitempty"`
	DeviceKey                   string          `json:"DeviceKey,omitempty"`
	DeviceStatus                string          `json:"DeviceStatus,omitempty"`
	DeviceAttributes            []attributeType `json:"DeviceAttributes,omitempty"`
}

// toDeviceType converts a stored Device into its AWS wire representation.
func toDeviceType(d *Device) *deviceType {
	if d == nil {
		return nil
	}

	created := float64(d.CreatedAt.Unix())
	modified := float64(d.LastModifiedAt.Unix())
	lastAuth := float64(d.LastAuthenticatedAt.Unix())

	return &deviceType{
		DeviceKey:                   d.DeviceKey,
		DeviceStatus:                d.Status,
		DeviceCreateDate:            &created,
		DeviceLastModifiedDate:      &modified,
		DeviceLastAuthenticatedDate: &lastAuth,
		DeviceAttributes:            sortedAttributeList(d.Attributes),
	}
}

type adminGetDeviceOutput struct {
	Device *deviceType `json:"Device,omitempty"`
}

func (h *Handler) handleAdminGetDevice(_ context.Context, in *adminGetDeviceInput) (*adminGetDeviceOutput, error) {
	dev, err := h.Backend.AdminGetDevice(in.UserPoolID, in.Username, in.DeviceKey)
	if err != nil {
		return nil, err
	}

	return &adminGetDeviceOutput{Device: toDeviceType(dev)}, nil
}

type adminLinkProviderForUserInput struct {
	DestinationUser *providerUserIdentifierType `json:"DestinationUser,omitempty"`
	SourceUser      *providerUserIdentifierType `json:"SourceUser,omitempty"`
	UserPoolID      string                      `json:"UserPoolId,omitempty"`
}

type adminLinkProviderForUserOutput struct{}

func (h *Handler) handleAdminLinkProviderForUser(
	_ context.Context,
	in *adminLinkProviderForUserInput,
) (*adminLinkProviderForUserOutput, error) {
	if in.DestinationUser == nil || in.SourceUser == nil {
		return nil, fmt.Errorf("%w: DestinationUser and SourceUser are required", ErrInvalidParameter)
	}

	if err := h.Backend.AdminLinkProviderForUser(
		in.UserPoolID,
		in.DestinationUser.ProviderAttributeValue,
		in.SourceUser.ProviderName,
		in.SourceUser.ProviderAttributeName,
		in.SourceUser.ProviderAttributeValue,
	); err != nil {
		return nil, err
	}

	return &adminLinkProviderForUserOutput{}, nil
}

type adminListDevicesInput struct {
	UserPoolID      string `json:"UserPoolId,omitempty"`
	Username        string `json:"Username,omitempty"`
	PaginationToken string `json:"PaginationToken,omitempty"`
	Limit           int    `json:"Limit,omitempty"`
}

type adminListDevicesOutput struct {
	PaginationToken string       `json:"PaginationToken,omitempty"`
	Devices         []deviceType `json:"Devices,omitempty"`
}

func (h *Handler) handleAdminListDevices(
	_ context.Context,
	in *adminListDevicesInput,
) (*adminListDevicesOutput, error) {
	limit, err := validateCognitoMaxResults(in.Limit)
	if err != nil {
		return nil, err
	}

	devices, token, err := h.Backend.AdminListDevices(in.UserPoolID, in.Username, limit, in.PaginationToken)
	if err != nil {
		return nil, err
	}

	out := make([]deviceType, 0, len(devices))
	for _, d := range devices {
		out = append(out, *toDeviceType(d))
	}

	return &adminListDevicesOutput{Devices: out, PaginationToken: token}, nil
}

type authEventFeedbackType struct {
	FeedbackValue string  `json:"FeedbackValue,omitempty"`
	FeedbackDate  float64 `json:"FeedbackDate,omitempty"`
}

type authEventOutputType struct {
	EventFeedback *authEventFeedbackType `json:"EventFeedback,omitempty"`
	EventID       string                 `json:"EventId,omitempty"`
	EventType     string                 `json:"EventType,omitempty"`
	EventResponse string                 `json:"EventResponse,omitempty"`
	CreationDate  float64                `json:"CreationDate,omitempty"`
}

type adminListUserAuthEventsInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	Username   string `json:"Username,omitempty"`
	NextToken  string `json:"NextToken,omitempty"`
	MaxResults int    `json:"MaxResults,omitempty"`
}

type adminListUserAuthEventsOutput struct {
	NextToken  string                `json:"NextToken,omitempty"`
	AuthEvents []authEventOutputType `json:"AuthEvents,omitempty"`
}

func (h *Handler) handleAdminListUserAuthEvents(
	_ context.Context,
	in *adminListUserAuthEventsInput,
) (*adminListUserAuthEventsOutput, error) {
	limit, err := validateCognitoMaxResults(in.MaxResults)
	if err != nil {
		return nil, err
	}

	events, token, err := h.Backend.AdminListUserAuthEvents(in.UserPoolID, in.Username, limit, in.NextToken)
	if err != nil {
		return nil, err
	}

	out := make([]authEventOutputType, 0, len(events))
	for _, e := range events {
		item := authEventOutputType{
			EventID:       e.EventID,
			EventType:     e.EventType,
			CreationDate:  float64(e.CreatedAt.Unix()),
			EventResponse: e.EventResponse,
		}

		if e.FeedbackValue != "" {
			item.EventFeedback = &authEventFeedbackType{
				FeedbackValue: e.FeedbackValue,
				FeedbackDate:  float64(e.FeedbackDate.Unix()),
			}
		}

		out = append(out, item)
	}

	return &adminListUserAuthEventsOutput{AuthEvents: out, NextToken: token}, nil
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

func (h *Handler) handleAdminSetUserMFAPreference(
	_ context.Context,
	_ *adminSetUserMFAPreferenceInput,
) (*adminSetUserMFAPreferenceOutput, error) {
	return &adminSetUserMFAPreferenceOutput{}, nil
}

type mfaOptionType struct {
	DeliveryMedium string `json:"DeliveryMedium,omitempty"`
	AttributeName  string `json:"AttributeName,omitempty"`
}

// toMFAOptionRecords converts wire-shaped MFAOptions into backend records.
func toMFAOptionRecords(opts []mfaOptionType) []MFAOptionType {
	out := make([]MFAOptionType, 0, len(opts))
	for _, o := range opts {
		out = append(out, MFAOptionType(o))
	}

	return out
}

type adminSetUserSettingsInput struct {
	UserPoolID string          `json:"UserPoolId,omitempty"`
	Username   string          `json:"Username,omitempty"`
	MFAOptions []mfaOptionType `json:"MFAOptions,omitempty"`
}

type adminSetUserSettingsOutput struct{}

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

type adminUpdateAuthEventFeedbackInput struct {
	UserPoolID    string `json:"UserPoolId,omitempty"`
	Username      string `json:"Username,omitempty"`
	EventID       string `json:"EventId,omitempty"`
	FeedbackValue string `json:"FeedbackValue,omitempty"`
}

type adminUpdateAuthEventFeedbackOutput struct{}

func (h *Handler) handleAdminUpdateAuthEventFeedback(
	_ context.Context,
	in *adminUpdateAuthEventFeedbackInput,
) (*adminUpdateAuthEventFeedbackOutput, error) {
	if err := h.Backend.AdminUpdateAuthEventFeedback(
		in.UserPoolID, in.Username, in.EventID, in.FeedbackValue,
	); err != nil {
		return nil, err
	}

	return &adminUpdateAuthEventFeedbackOutput{}, nil
}

type adminUpdateDeviceStatusInput struct {
	UserPoolID             string `json:"UserPoolId,omitempty"`
	Username               string `json:"Username,omitempty"`
	DeviceKey              string `json:"DeviceKey,omitempty"`
	DeviceRememberedStatus string `json:"DeviceRememberedStatus,omitempty"`
}

type adminUpdateDeviceStatusOutput struct{}

func (h *Handler) handleAdminUpdateDeviceStatus(
	_ context.Context,
	in *adminUpdateDeviceStatusInput,
) (*adminUpdateDeviceStatusOutput, error) {
	if err := h.Backend.AdminUpdateDeviceStatus(
		in.UserPoolID, in.Username, in.DeviceKey, in.DeviceRememberedStatus,
	); err != nil {
		return nil, err
	}

	return &adminUpdateDeviceStatusOutput{}, nil
}

type associateSoftwareTokenInput struct {
	AccessToken string `json:"AccessToken,omitempty"`
	Session     string `json:"Session,omitempty"`
}

type associateSoftwareTokenOutput struct {
	SecretCode string `json:"SecretCode,omitempty"`
	Session    string `json:"Session,omitempty"`
}

// handleAssociateSoftwareToken returns a stub TOTP secret code.
// The secret is a test value only; it is not a real credential.
func (h *Handler) handleAssociateSoftwareToken(
	_ context.Context,
	_ *associateSoftwareTokenInput,
) (*associateSoftwareTokenOutput, error) {
	//nolint:gosec // canonical TOTP example seed from RFC 6238 — not a real credential
	return &associateSoftwareTokenOutput{SecretCode: "JBSWY3DPEHPK3PXP"}, nil
}

type completeWebAuthnRegistrationInput struct {
	Credential  map[string]any `json:"Credential,omitempty"`
	AccessToken string         `json:"AccessToken,omitempty"`
}

type completeWebAuthnRegistrationOutput struct{}

func (h *Handler) handleCompleteWebAuthnRegistration(
	_ context.Context,
	in *completeWebAuthnRegistrationInput,
) (*completeWebAuthnRegistrationOutput, error) {
	credentialID, _ := in.Credential["id"].(string)
	attachment, _ := in.Credential["authenticatorAttachment"].(string)

	if _, err := h.Backend.CompleteWebAuthnRegistration(in.AccessToken, credentialID, attachment); err != nil {
		return nil, err
	}

	return &completeWebAuthnRegistrationOutput{}, nil
}

type confirmDeviceInput struct {
	AccessToken                string            `json:"AccessToken,omitempty"`
	DeviceKey                  string            `json:"DeviceKey,omitempty"`
	DeviceSecretVerifierConfig map[string]string `json:"DeviceSecretVerifierConfig,omitempty"`
	DeviceName                 string            `json:"DeviceName,omitempty"`
}

type confirmDeviceOutput struct {
	UserConfirmationNecessary bool `json:"UserConfirmationNecessary,omitempty"`
}

func (h *Handler) handleConfirmDevice(_ context.Context, in *confirmDeviceInput) (*confirmDeviceOutput, error) {
	_, necessary, err := h.Backend.ConfirmDevice(in.AccessToken, in.DeviceKey, in.DeviceName)
	if err != nil {
		return nil, err
	}

	return &confirmDeviceOutput{UserConfirmationNecessary: necessary}, nil
}

// ----- Identity Provider -----

type identityProviderType struct {
	ProviderDetails  map[string]string `json:"ProviderDetails,omitempty"`
	CreationDate     *float64          `json:"CreationDate,omitempty"`
	LastModifiedDate *float64          `json:"LastModifiedDate,omitempty"`
	UserPoolID       string            `json:"UserPoolId,omitempty"`
	ProviderName     string            `json:"ProviderName,omitempty"`
	ProviderType     string            `json:"ProviderType,omitempty"`
}

type createIdentityProviderInput struct {
	ProviderDetails map[string]string `json:"ProviderDetails,omitempty"`
	UserPoolID      string            `json:"UserPoolId,omitempty"`
	ProviderName    string            `json:"ProviderName,omitempty"`
	ProviderType    string            `json:"ProviderType,omitempty"`
}

type createIdentityProviderOutput struct {
	IdentityProvider *identityProviderType `json:"IdentityProvider,omitempty"`
}

func (h *Handler) handleCreateIdentityProvider(
	_ context.Context,
	in *createIdentityProviderInput,
) (*createIdentityProviderOutput, error) {
	idp, err := h.Backend.CreateIdentityProvider(in.UserPoolID, in.ProviderName, in.ProviderType, in.ProviderDetails)
	if err != nil {
		return nil, err
	}

	ts := float64(idp.CreatedAt.Unix())

	return &createIdentityProviderOutput{
		IdentityProvider: &identityProviderType{
			UserPoolID:       idp.UserPoolID,
			ProviderName:     idp.ProviderName,
			ProviderType:     idp.ProviderType,
			ProviderDetails:  idp.ProviderDetails,
			CreationDate:     &ts,
			LastModifiedDate: &ts,
		},
	}, nil
}

type deleteIdentityProviderInput struct {
	UserPoolID   string `json:"UserPoolId,omitempty"`
	ProviderName string `json:"ProviderName,omitempty"`
}

type deleteIdentityProviderOutput struct{}

func (h *Handler) handleDeleteIdentityProvider(
	_ context.Context,
	in *deleteIdentityProviderInput,
) (*deleteIdentityProviderOutput, error) {
	if err := h.Backend.DeleteIdentityProvider(in.UserPoolID, in.ProviderName); err != nil {
		return nil, err
	}

	return &deleteIdentityProviderOutput{}, nil
}

type describeIdentityProviderInput struct {
	UserPoolID   string `json:"UserPoolId,omitempty"`
	ProviderName string `json:"ProviderName,omitempty"`
}

type describeIdentityProviderOutput struct {
	IdentityProvider *identityProviderType `json:"IdentityProvider,omitempty"`
}

func (h *Handler) handleDescribeIdentityProvider(
	_ context.Context,
	in *describeIdentityProviderInput,
) (*describeIdentityProviderOutput, error) {
	idp, err := h.Backend.DescribeIdentityProvider(in.UserPoolID, in.ProviderName)
	if err != nil {
		return nil, err
	}

	ts := float64(idp.CreatedAt.Unix())
	mod := float64(idp.LastModifiedAt.Unix())

	return &describeIdentityProviderOutput{
		IdentityProvider: &identityProviderType{
			UserPoolID:       idp.UserPoolID,
			ProviderName:     idp.ProviderName,
			ProviderType:     idp.ProviderType,
			ProviderDetails:  idp.ProviderDetails,
			CreationDate:     &ts,
			LastModifiedDate: &mod,
		},
	}, nil
}

type getIdentityProviderByIdentifierInput struct {
	UserPoolID    string `json:"UserPoolId,omitempty"`
	IdpIdentifier string `json:"IdpIdentifier,omitempty"`
}

type getIdentityProviderByIdentifierOutput struct {
	IdentityProvider *identityProviderType `json:"IdentityProvider,omitempty"`
}

func (h *Handler) handleGetIdentityProviderByIdentifier(
	_ context.Context,
	in *getIdentityProviderByIdentifierInput,
) (*getIdentityProviderByIdentifierOutput, error) {
	idp, err := h.Backend.GetIdentityProviderByIdentifier(in.UserPoolID, in.IdpIdentifier)
	if err != nil {
		return nil, err
	}

	ts := float64(idp.CreatedAt.Unix())

	return &getIdentityProviderByIdentifierOutput{
		IdentityProvider: &identityProviderType{
			UserPoolID:      idp.UserPoolID,
			ProviderName:    idp.ProviderName,
			ProviderType:    idp.ProviderType,
			ProviderDetails: idp.ProviderDetails,
			CreationDate:    &ts,
		},
	}, nil
}

type listIdentityProvidersInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
}

type identityProviderSummary struct {
	ProviderName string `json:"ProviderName,omitempty"`
	ProviderType string `json:"ProviderType,omitempty"`
}

type listIdentityProvidersOutput struct {
	Providers []identityProviderSummary `json:"Providers"`
}

func (h *Handler) handleListIdentityProviders(
	_ context.Context,
	in *listIdentityProvidersInput,
) (*listIdentityProvidersOutput, error) {
	idps, err := h.Backend.ListIdentityProviders(in.UserPoolID)
	if err != nil {
		return nil, err
	}

	summaries := make([]identityProviderSummary, 0, len(idps))
	for _, idp := range idps {
		summaries = append(summaries, identityProviderSummary{
			ProviderName: idp.ProviderName,
			ProviderType: idp.ProviderType,
		})
	}

	return &listIdentityProvidersOutput{Providers: summaries}, nil
}

type updateIdentityProviderInput struct {
	ProviderDetails map[string]string `json:"ProviderDetails,omitempty"`
	UserPoolID      string            `json:"UserPoolId,omitempty"`
	ProviderName    string            `json:"ProviderName,omitempty"`
}

type updateIdentityProviderOutput struct {
	IdentityProvider *identityProviderType `json:"IdentityProvider,omitempty"`
}

func (h *Handler) handleUpdateIdentityProvider(
	_ context.Context,
	in *updateIdentityProviderInput,
) (*updateIdentityProviderOutput, error) {
	idp, err := h.Backend.UpdateIdentityProvider(in.UserPoolID, in.ProviderName, in.ProviderDetails)
	if err != nil {
		return nil, err
	}

	mod := float64(idp.LastModifiedAt.Unix())

	return &updateIdentityProviderOutput{
		IdentityProvider: &identityProviderType{
			UserPoolID:       idp.UserPoolID,
			ProviderName:     idp.ProviderName,
			ProviderType:     idp.ProviderType,
			ProviderDetails:  idp.ProviderDetails,
			LastModifiedDate: &mod,
		},
	}, nil
}

// ----- Managed Login Branding -----

type createManagedLoginBrandingInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	ClientID   string `json:"ClientId,omitempty"`
}

type managedLoginBrandingType struct {
	ManagedLoginBrandingID string `json:"ManagedLoginBrandingId,omitempty"`
	UserPoolID             string `json:"UserPoolId,omitempty"`
}

type createManagedLoginBrandingOutput struct {
	ManagedLoginBranding *managedLoginBrandingType `json:"ManagedLoginBranding,omitempty"`
}

func (h *Handler) handleCreateManagedLoginBranding(
	_ context.Context,
	in *createManagedLoginBrandingInput,
) (*createManagedLoginBrandingOutput, error) {
	mlb, err := h.Backend.CreateManagedLoginBranding(in.UserPoolID, in.ClientID)
	if err != nil {
		return nil, err
	}

	return &createManagedLoginBrandingOutput{
		ManagedLoginBranding: &managedLoginBrandingType{
			UserPoolID:             mlb.UserPoolID,
			ManagedLoginBrandingID: mlb.ManagedLoginBrandingID,
		},
	}, nil
}

type deleteManagedLoginBrandingInput struct {
	ManagedLoginBrandingID string `json:"ManagedLoginBrandingId,omitempty"`
	UserPoolID             string `json:"UserPoolId,omitempty"`
}

type deleteManagedLoginBrandingOutput struct{}

func (h *Handler) handleDeleteManagedLoginBranding(
	_ context.Context,
	in *deleteManagedLoginBrandingInput,
) (*deleteManagedLoginBrandingOutput, error) {
	if err := h.Backend.DeleteManagedLoginBranding(in.UserPoolID, in.ManagedLoginBrandingID); err != nil {
		return nil, err
	}

	return &deleteManagedLoginBrandingOutput{}, nil
}

type describeManagedLoginBrandingInput struct {
	ManagedLoginBrandingID string `json:"ManagedLoginBrandingId,omitempty"`
	UserPoolID             string `json:"UserPoolId,omitempty"`
}

type describeManagedLoginBrandingOutput struct {
	ManagedLoginBranding *managedLoginBrandingType `json:"ManagedLoginBranding,omitempty"`
}

func (h *Handler) handleDescribeManagedLoginBranding(
	_ context.Context,
	in *describeManagedLoginBrandingInput,
) (*describeManagedLoginBrandingOutput, error) {
	mlb, err := h.Backend.DescribeManagedLoginBranding(in.UserPoolID, in.ManagedLoginBrandingID)
	if err != nil {
		return nil, err
	}

	return &describeManagedLoginBrandingOutput{
		ManagedLoginBranding: &managedLoginBrandingType{
			ManagedLoginBrandingID: mlb.ManagedLoginBrandingID,
			UserPoolID:             mlb.UserPoolID,
		},
	}, nil
}

type describeManagedLoginBrandingByClientInput struct {
	ClientID   string `json:"ClientId,omitempty"`
	UserPoolID string `json:"UserPoolId,omitempty"`
}

type describeManagedLoginBrandingByClientOutput struct {
	ManagedLoginBranding *managedLoginBrandingType `json:"ManagedLoginBranding,omitempty"`
}

func (h *Handler) handleDescribeManagedLoginBrandingByClient(
	_ context.Context,
	in *describeManagedLoginBrandingByClientInput,
) (*describeManagedLoginBrandingByClientOutput, error) {
	mlb, err := h.Backend.DescribeManagedLoginBrandingByClient(in.UserPoolID, in.ClientID)
	if err != nil {
		return nil, err
	}

	return &describeManagedLoginBrandingByClientOutput{
		ManagedLoginBranding: &managedLoginBrandingType{
			ManagedLoginBrandingID: mlb.ManagedLoginBrandingID,
			UserPoolID:             mlb.UserPoolID,
		},
	}, nil
}

type updateManagedLoginBrandingInput struct {
	ManagedLoginBrandingID string `json:"ManagedLoginBrandingId,omitempty"`
	UserPoolID             string `json:"UserPoolId,omitempty"`
}

type updateManagedLoginBrandingOutput struct {
	ManagedLoginBranding *managedLoginBrandingType `json:"ManagedLoginBranding,omitempty"`
}

func (h *Handler) handleUpdateManagedLoginBranding(
	_ context.Context,
	in *updateManagedLoginBrandingInput,
) (*updateManagedLoginBrandingOutput, error) {
	mlb, err := h.Backend.UpdateManagedLoginBranding(in.UserPoolID, in.ManagedLoginBrandingID)
	if err != nil {
		return nil, err
	}

	return &updateManagedLoginBrandingOutput{
		ManagedLoginBranding: &managedLoginBrandingType{
			ManagedLoginBrandingID: mlb.ManagedLoginBrandingID,
			UserPoolID:             mlb.UserPoolID,
		},
	}, nil
}

// ----- Resource Server -----

type resourceServerType struct {
	UserPoolID string              `json:"UserPoolId,omitempty"`
	Identifier string              `json:"Identifier,omitempty"`
	Name       string              `json:"Name,omitempty"`
	Scopes     []map[string]string `json:"Scopes,omitempty"`
}

type createResourceServerInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	Identifier string `json:"Identifier,omitempty"`
	Name       string `json:"Name,omitempty"`
}

type createResourceServerOutput struct {
	ResourceServer *resourceServerType `json:"ResourceServer,omitempty"`
}

func (h *Handler) handleCreateResourceServer(
	_ context.Context,
	in *createResourceServerInput,
) (*createResourceServerOutput, error) {
	return &createResourceServerOutput{
		ResourceServer: &resourceServerType{UserPoolID: in.UserPoolID, Identifier: in.Identifier, Name: in.Name},
	}, nil
}

type deleteResourceServerInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	Identifier string `json:"Identifier,omitempty"`
}

type deleteResourceServerOutput struct{}

func (h *Handler) handleDeleteResourceServer(
	_ context.Context,
	_ *deleteResourceServerInput,
) (*deleteResourceServerOutput, error) {
	return &deleteResourceServerOutput{}, nil
}

type describeResourceServerInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	Identifier string `json:"Identifier,omitempty"`
}

type describeResourceServerOutput struct {
	ResourceServer *resourceServerType `json:"ResourceServer,omitempty"`
}

func (h *Handler) handleDescribeResourceServer(
	_ context.Context,
	in *describeResourceServerInput,
) (*describeResourceServerOutput, error) {
	return &describeResourceServerOutput{
		ResourceServer: &resourceServerType{UserPoolID: in.UserPoolID, Identifier: in.Identifier},
	}, nil
}

type listResourceServersInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
}

type listResourceServersOutput struct {
	ResourceServers []resourceServerType `json:"ResourceServers,omitempty"`
}

func (h *Handler) handleListResourceServers(
	_ context.Context,
	_ *listResourceServersInput,
) (*listResourceServersOutput, error) {
	return &listResourceServersOutput{ResourceServers: []resourceServerType{}}, nil
}

type updateResourceServerInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	Identifier string `json:"Identifier,omitempty"`
	Name       string `json:"Name,omitempty"`
}

type updateResourceServerOutput struct {
	ResourceServer *resourceServerType `json:"ResourceServer,omitempty"`
}

func (h *Handler) handleUpdateResourceServer(
	_ context.Context,
	in *updateResourceServerInput,
) (*updateResourceServerOutput, error) {
	return &updateResourceServerOutput{
		ResourceServer: &resourceServerType{UserPoolID: in.UserPoolID, Identifier: in.Identifier, Name: in.Name},
	}, nil
}

// ----- Terms -----

type termsType struct {
	DefaultTermsAndConditions string `json:"DefaultTermsAndConditions,omitempty"`
}

type createTermsInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
}

type createTermsOutput struct {
	Terms *termsType `json:"Terms,omitempty"`
}

func (h *Handler) handleCreateTerms(_ context.Context, in *createTermsInput) (*createTermsOutput, error) {
	t, err := h.Backend.CreateTerms(in.UserPoolID, "")
	if err != nil {
		return nil, err
	}

	return &createTermsOutput{Terms: &termsType{DefaultTermsAndConditions: t.Text}}, nil
}

type deleteTermsInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
}

type deleteTermsOutput struct{}

func (h *Handler) handleDeleteTerms(_ context.Context, in *deleteTermsInput) (*deleteTermsOutput, error) {
	if err := h.Backend.DeleteTerms(in.UserPoolID); err != nil {
		return nil, err
	}

	return &deleteTermsOutput{}, nil
}

type describeTermsInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
}

type describeTermsOutput struct {
	Terms *termsType `json:"Terms,omitempty"`
}

func (h *Handler) handleDescribeTerms(_ context.Context, in *describeTermsInput) (*describeTermsOutput, error) {
	t, err := h.Backend.DescribeTerms(in.UserPoolID)
	if err != nil {
		return nil, err
	}

	return &describeTermsOutput{Terms: &termsType{DefaultTermsAndConditions: t.Text}}, nil
}

type listTermsInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
}

type listTermsOutput struct {
	Terms []termsType `json:"Terms"`
}

func (h *Handler) handleListTerms(_ context.Context, in *listTermsInput) (*listTermsOutput, error) {
	ts, err := h.Backend.ListTerms(in.UserPoolID)
	if err != nil {
		return nil, err
	}

	out := make([]termsType, 0, len(ts))
	for _, t := range ts {
		out = append(out, termsType{DefaultTermsAndConditions: t.Text})
	}

	return &listTermsOutput{Terms: out}, nil
}

type updateTermsInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
}

type updateTermsOutput struct {
	Terms *termsType `json:"Terms,omitempty"`
}

func (h *Handler) handleUpdateTerms(_ context.Context, in *updateTermsInput) (*updateTermsOutput, error) {
	t, err := h.Backend.UpdateTerms(in.UserPoolID, "")
	if err != nil {
		return nil, err
	}

	return &updateTermsOutput{Terms: &termsType{DefaultTermsAndConditions: t.Text}}, nil
}

// ----- User Import Job -----

type userImportJobType struct {
	JobID      string `json:"JobId,omitempty"`
	JobName    string `json:"JobName,omitempty"`
	Status     string `json:"Status,omitempty"`
	UserPoolID string `json:"UserPoolId,omitempty"`
}

type createUserImportJobInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	JobName    string `json:"JobName,omitempty"`
}

type createUserImportJobOutput struct {
	UserImportJob *userImportJobType `json:"UserImportJob,omitempty"`
}

func (h *Handler) handleCreateUserImportJob(
	_ context.Context,
	in *createUserImportJobInput,
) (*createUserImportJobOutput, error) {
	job, err := h.Backend.CreateUserImportJob(in.UserPoolID, in.JobName)
	if err != nil {
		return nil, err
	}

	return &createUserImportJobOutput{
		UserImportJob: &userImportJobType{
			JobID:      job.JobID,
			JobName:    job.JobName,
			UserPoolID: job.UserPoolID,
			Status:     job.Status,
		},
	}, nil
}

type describeUserImportJobInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	JobID      string `json:"JobId,omitempty"`
}

type describeUserImportJobOutput struct {
	UserImportJob *userImportJobType `json:"UserImportJob,omitempty"`
}

func (h *Handler) handleDescribeUserImportJob(
	_ context.Context,
	in *describeUserImportJobInput,
) (*describeUserImportJobOutput, error) {
	job, err := h.Backend.DescribeUserImportJob(in.UserPoolID, in.JobID)
	if err != nil {
		return nil, err
	}

	return &describeUserImportJobOutput{
		UserImportJob: &userImportJobType{
			JobID:      job.JobID,
			JobName:    job.JobName,
			UserPoolID: job.UserPoolID,
			Status:     job.Status,
		},
	}, nil
}

type listUserImportJobsInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
}

type listUserImportJobsOutput struct {
	UserImportJobs []userImportJobType `json:"UserImportJobs,omitempty"`
}

func (h *Handler) handleListUserImportJobs(
	_ context.Context,
	in *listUserImportJobsInput,
) (*listUserImportJobsOutput, error) {
	jobs, err := h.Backend.ListUserImportJobs(in.UserPoolID)
	if err != nil {
		return nil, err
	}

	out := make([]userImportJobType, 0, len(jobs))
	for _, job := range jobs {
		out = append(out, userImportJobType{
			JobID:      job.JobID,
			JobName:    job.JobName,
			UserPoolID: job.UserPoolID,
			Status:     job.Status,
		})
	}

	return &listUserImportJobsOutput{UserImportJobs: out}, nil
}

type startUserImportJobInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	JobID      string `json:"JobId,omitempty"`
}

type startUserImportJobOutput struct {
	UserImportJob *userImportJobType `json:"UserImportJob,omitempty"`
}

func (h *Handler) handleStartUserImportJob(
	_ context.Context,
	in *startUserImportJobInput,
) (*startUserImportJobOutput, error) {
	job, err := h.Backend.StartUserImportJob(in.UserPoolID, in.JobID)
	if err != nil {
		return nil, err
	}

	return &startUserImportJobOutput{
		UserImportJob: &userImportJobType{
			JobID:      job.JobID,
			JobName:    job.JobName,
			UserPoolID: job.UserPoolID,
			Status:     job.Status,
		},
	}, nil
}

type stopUserImportJobInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	JobID      string `json:"JobId,omitempty"`
}

type stopUserImportJobOutput struct {
	UserImportJob *userImportJobType `json:"UserImportJob,omitempty"`
}

func (h *Handler) handleStopUserImportJob(
	_ context.Context,
	in *stopUserImportJobInput,
) (*stopUserImportJobOutput, error) {
	job, err := h.Backend.StopUserImportJob(in.UserPoolID, in.JobID)
	if err != nil {
		return nil, err
	}

	return &stopUserImportJobOutput{
		UserImportJob: &userImportJobType{
			JobID:      job.JobID,
			JobName:    job.JobName,
			UserPoolID: job.UserPoolID,
			Status:     job.Status,
		},
	}, nil
}

// ----- User Pool Domain -----

type createUserPoolDomainInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	Domain     string `json:"Domain,omitempty"`
}

type createUserPoolDomainOutput struct {
	CloudFrontDomain string `json:"CloudFrontDomain,omitempty"`
}

func (h *Handler) handleCreateUserPoolDomain(
	_ context.Context,
	in *createUserPoolDomainInput,
) (*createUserPoolDomainOutput, error) {
	d, err := h.Backend.CreateUserPoolDomain(in.UserPoolID, in.Domain)
	if err != nil {
		return nil, err
	}

	return &createUserPoolDomainOutput{CloudFrontDomain: d.CloudFrontDistribution}, nil
}

type deleteUserPoolDomainInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	Domain     string `json:"Domain,omitempty"`
}

type deleteUserPoolDomainOutput struct{}

func (h *Handler) handleDeleteUserPoolDomain(
	_ context.Context,
	in *deleteUserPoolDomainInput,
) (*deleteUserPoolDomainOutput, error) {
	if err := h.Backend.DeleteUserPoolDomain(in.UserPoolID, in.Domain); err != nil {
		return nil, err
	}

	return &deleteUserPoolDomainOutput{}, nil
}

type describeUserPoolDomainInput struct {
	Domain string `json:"Domain,omitempty"`
}

type userPoolDomainDescription struct {
	Domain                 string `json:"Domain,omitempty"`
	UserPoolID             string `json:"UserPoolId,omitempty"`
	Status                 string `json:"Status,omitempty"`
	CloudFrontDistribution string `json:"CloudFrontDistribution,omitempty"`
}

type describeUserPoolDomainOutput struct {
	DomainDescription *userPoolDomainDescription `json:"DomainDescription,omitempty"`
}

func (h *Handler) handleDescribeUserPoolDomain(
	_ context.Context,
	in *describeUserPoolDomainInput,
) (*describeUserPoolDomainOutput, error) {
	// FindUserPoolDomain returns nil for unknown domains (AWS returns empty description, not an error).
	d := h.Backend.FindUserPoolDomain(in.Domain)
	if d == nil {
		return &describeUserPoolDomainOutput{DomainDescription: &userPoolDomainDescription{}}, nil
	}

	return &describeUserPoolDomainOutput{
		DomainDescription: &userPoolDomainDescription{
			Domain:                 d.Domain,
			UserPoolID:             d.UserPoolID,
			Status:                 d.Status,
			CloudFrontDistribution: d.CloudFrontDistribution,
		},
	}, nil
}

type updateUserPoolDomainInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	Domain     string `json:"Domain,omitempty"`
}

type updateUserPoolDomainOutput struct {
	CloudFrontDomain string `json:"CloudFrontDomain,omitempty"`
}

func (h *Handler) handleUpdateUserPoolDomain(
	_ context.Context,
	in *updateUserPoolDomainInput,
) (*updateUserPoolDomainOutput, error) {
	cfDomain, err := h.Backend.UpdateUserPoolDomain(in.UserPoolID, in.Domain)
	if err != nil {
		return nil, err
	}

	return &updateUserPoolDomainOutput{CloudFrontDomain: cfDomain}, nil
}

// ----- WebAuthn -----

type deleteWebAuthnCredentialInput struct {
	AccessToken  string `json:"AccessToken,omitempty"`
	CredentialID string `json:"CredentialId,omitempty"`
}

type deleteWebAuthnCredentialOutput struct{}

func (h *Handler) handleDeleteWebAuthnCredential(
	_ context.Context,
	in *deleteWebAuthnCredentialInput,
) (*deleteWebAuthnCredentialOutput, error) {
	if err := h.Backend.DeleteWebAuthnCredential(in.AccessToken, in.CredentialID); err != nil {
		return nil, err
	}

	return &deleteWebAuthnCredentialOutput{}, nil
}

type listWebAuthnCredentialsInput struct {
	AccessToken string `json:"AccessToken,omitempty"`
	NextToken   string `json:"NextToken,omitempty"`
	MaxResults  int    `json:"MaxResults,omitempty"`
}

type webAuthnCredentialDescriptionType struct {
	CredentialID            string  `json:"CredentialId,omitempty"`
	FriendlyName            string  `json:"FriendlyName,omitempty"`
	RelyingPartyID          string  `json:"RelyingPartyId,omitempty"`
	AuthenticatorAttachment string  `json:"AuthenticatorAttachment,omitempty"`
	CreatedAt               float64 `json:"CreatedAt,omitempty"`
}

type listWebAuthnCredentialsOutput struct {
	NextToken   string                              `json:"NextToken,omitempty"`
	Credentials []webAuthnCredentialDescriptionType `json:"Credentials,omitempty"`
}

func (h *Handler) handleListWebAuthnCredentials(
	_ context.Context,
	in *listWebAuthnCredentialsInput,
) (*listWebAuthnCredentialsOutput, error) {
	limit, err := validateCognitoMaxResults(in.MaxResults)
	if err != nil {
		return nil, err
	}

	creds, token, err := h.Backend.ListWebAuthnCredentials(in.AccessToken, limit, in.NextToken)
	if err != nil {
		return nil, err
	}

	out := make([]webAuthnCredentialDescriptionType, 0, len(creds))
	for _, c := range creds {
		out = append(out, webAuthnCredentialDescriptionType{
			CredentialID:            c.CredentialID,
			FriendlyName:            c.FriendlyName,
			RelyingPartyID:          c.RelyingPartyID,
			AuthenticatorAttachment: c.AuthenticatorAttachment,
			CreatedAt:               float64(c.CreatedAt.Unix()),
		})
	}

	return &listWebAuthnCredentialsOutput{Credentials: out, NextToken: token}, nil
}

type startWebAuthnRegistrationInput struct {
	AccessToken string `json:"AccessToken,omitempty"`
}

type startWebAuthnRegistrationOutput struct {
	CredentialCreationOptions map[string]any `json:"CredentialCreationOptions,omitempty"`
}

func (h *Handler) handleStartWebAuthnRegistration(
	_ context.Context,
	in *startWebAuthnRegistrationInput,
) (*startWebAuthnRegistrationOutput, error) {
	opts, err := h.Backend.StartWebAuthnRegistration(in.AccessToken)
	if err != nil {
		return nil, err
	}

	return &startWebAuthnRegistrationOutput{CredentialCreationOptions: opts}, nil
}

// ----- Risk Configuration -----

type describeRiskConfigurationInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	ClientID   string `json:"ClientId,omitempty"`
}

type riskConfigurationType struct{}

type describeRiskConfigurationOutput struct {
	RiskConfiguration *riskConfigurationType `json:"RiskConfiguration,omitempty"`
}

func (h *Handler) handleDescribeRiskConfiguration(
	_ context.Context,
	in *describeRiskConfigurationInput,
) (*describeRiskConfigurationOutput, error) {
	if _, err := h.Backend.DescribeRiskConfiguration(in.UserPoolID, in.ClientID); err != nil {
		return nil, err
	}

	return &describeRiskConfigurationOutput{RiskConfiguration: &riskConfigurationType{}}, nil
}

type setRiskConfigurationInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	ClientID   string `json:"ClientId,omitempty"`
}

type setRiskConfigurationOutput struct {
	RiskConfiguration *riskConfigurationType `json:"RiskConfiguration,omitempty"`
}

func (h *Handler) handleSetRiskConfiguration(
	_ context.Context,
	in *setRiskConfigurationInput,
) (*setRiskConfigurationOutput, error) {
	if err := h.Backend.SetRiskConfiguration(in.UserPoolID, in.ClientID, nil); err != nil {
		return nil, err
	}

	return &setRiskConfigurationOutput{RiskConfiguration: &riskConfigurationType{}}, nil
}

// ----- Log Delivery -----

type getLogDeliveryConfigurationInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
}

type getLogDeliveryConfigurationOutput struct {
	LogDeliveryConfiguration map[string]any `json:"LogDeliveryConfiguration,omitempty"`
}

func (h *Handler) handleGetLogDeliveryConfiguration(
	_ context.Context,
	in *getLogDeliveryConfigurationInput,
) (*getLogDeliveryConfigurationOutput, error) {
	cfg, err := h.Backend.GetLogDeliveryConfiguration(in.UserPoolID)
	if err != nil {
		return nil, err
	}

	raw := cfg.Raw
	if raw == nil {
		raw = map[string]any{}
	}

	return &getLogDeliveryConfigurationOutput{LogDeliveryConfiguration: raw}, nil
}

type setLogDeliveryConfigurationInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
}

type setLogDeliveryConfigurationOutput struct {
	LogDeliveryConfiguration map[string]any `json:"LogDeliveryConfiguration,omitempty"`
}

func (h *Handler) handleSetLogDeliveryConfiguration(
	_ context.Context,
	in *setLogDeliveryConfigurationInput,
) (*setLogDeliveryConfigurationOutput, error) {
	if err := h.Backend.SetLogDeliveryConfiguration(in.UserPoolID, nil); err != nil {
		return nil, err
	}

	return &setLogDeliveryConfigurationOutput{LogDeliveryConfiguration: map[string]any{}}, nil
}

// ----- Tokens -----

type getTokensFromRefreshTokenInput struct {
	RefreshToken string `json:"RefreshToken,omitempty"`
	ClientID     string `json:"ClientId,omitempty"`
}

type getTokensFromRefreshTokenOutput struct {
	AuthenticationResult *authResult `json:"AuthenticationResult,omitempty"`
}

func (h *Handler) handleGetTokensFromRefreshToken(
	_ context.Context,
	in *getTokensFromRefreshTokenInput,
) (*getTokensFromRefreshTokenOutput, error) {
	tokens, err := h.Backend.InitiateAuthRefreshToken(in.ClientID, in.RefreshToken)
	if err != nil {
		return nil, err
	}

	return &getTokensFromRefreshTokenOutput{
		AuthenticationResult: &authResult{
			AccessToken:  tokens.AccessToken,
			IDToken:      tokens.IDToken,
			RefreshToken: tokens.RefreshToken,
			TokenType:    authTypeBearer,
			ExpiresIn:    tokens.ExpiresIn,
		},
	}, nil
}

// ----- UI Customization -----

type uiCustomizationType struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	ClientID   string `json:"ClientId,omitempty"`
	CSS        string `json:"CSS,omitempty"`
}

type getUICustomizationInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	ClientID   string `json:"ClientId,omitempty"`
}

type getUICustomizationOutput struct {
	UICustomization *uiCustomizationType `json:"UICustomization,omitempty"`
}

func (h *Handler) handleGetUICustomization(
	_ context.Context,
	in *getUICustomizationInput,
) (*getUICustomizationOutput, error) {
	ui, err := h.Backend.GetUICustomization(in.UserPoolID, in.ClientID)
	if err != nil {
		return nil, err
	}

	return &getUICustomizationOutput{UICustomization: &uiCustomizationType{
		UserPoolID: ui.UserPoolID,
		ClientID:   ui.ClientID,
		CSS:        ui.CSS,
	}}, nil
}

type setUICustomizationInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	ClientID   string `json:"ClientId,omitempty"`
	CSS        string `json:"CSS,omitempty"`
}

type setUICustomizationOutput struct {
	UICustomization *uiCustomizationType `json:"UICustomization,omitempty"`
}

func (h *Handler) handleSetUICustomization(
	_ context.Context,
	in *setUICustomizationInput,
) (*setUICustomizationOutput, error) {
	ui, err := h.Backend.SetUICustomization(in.UserPoolID, in.ClientID, in.CSS)
	if err != nil {
		return nil, err
	}

	return &setUICustomizationOutput{UICustomization: &uiCustomizationType{
		UserPoolID: ui.UserPoolID,
		ClientID:   ui.ClientID,
		CSS:        ui.CSS,
	}}, nil
}

// ----- User Attribute Verification -----

type getUserAttributeVerificationCodeInput struct {
	AccessToken   string `json:"AccessToken,omitempty"`
	AttributeName string `json:"AttributeName,omitempty"`
}

type getUserAttributeVerificationCodeOutput struct {
	CodeDeliveryDetails map[string]string `json:"CodeDeliveryDetails,omitempty"`
}

func (h *Handler) handleGetUserAttributeVerificationCode(
	_ context.Context,
	in *getUserAttributeVerificationCodeInput,
) (*getUserAttributeVerificationCodeOutput, error) {
	return &getUserAttributeVerificationCodeOutput{
		CodeDeliveryDetails: map[string]string{
			keyDeliveryMedium: medEmail,
			keyDestination:    "user@example.com",
			keyAttributeName:  in.AttributeName,
		},
	}, nil
}

// ----- User Auth Factors -----

type getUserAuthFactorsInput struct {
	AccessToken string `json:"AccessToken,omitempty"`
}

type getUserAuthFactorsOutput struct {
	Username                  string   `json:"Username,omitempty"`
	ConfiguredUserAuthFactors []string `json:"ConfiguredUserAuthFactors,omitempty"`
}

func (h *Handler) handleGetUserAuthFactors(
	_ context.Context,
	in *getUserAuthFactorsInput,
) (*getUserAuthFactorsOutput, error) {
	user, factors, err := h.Backend.GetUserAuthFactors(in.AccessToken)
	if err != nil {
		return nil, err
	}

	return &getUserAuthFactorsOutput{Username: user.Username, ConfiguredUserAuthFactors: factors}, nil
}

// ----- Devices (non-admin) -----

type listDevicesInput struct {
	AccessToken     string `json:"AccessToken,omitempty"`
	PaginationToken string `json:"PaginationToken,omitempty"`
	Limit           int    `json:"Limit,omitempty"`
}

type listDevicesOutput struct {
	PaginationToken string       `json:"PaginationToken,omitempty"`
	Devices         []deviceType `json:"Devices,omitempty"`
}

func (h *Handler) handleListDevices(_ context.Context, in *listDevicesInput) (*listDevicesOutput, error) {
	limit, err := validateCognitoMaxResults(in.Limit)
	if err != nil {
		return nil, err
	}

	devices, token, err := h.Backend.ListDevices(in.AccessToken, limit, in.PaginationToken)
	if err != nil {
		return nil, err
	}

	out := make([]deviceType, 0, len(devices))
	for _, d := range devices {
		out = append(out, *toDeviceType(d))
	}

	return &listDevicesOutput{Devices: out, PaginationToken: token}, nil
}

type forgetDeviceInput struct {
	AccessToken string `json:"AccessToken,omitempty"`
	DeviceKey   string `json:"DeviceKey,omitempty"`
}

type forgetDeviceOutput struct{}

func (h *Handler) handleForgetDevice(_ context.Context, in *forgetDeviceInput) (*forgetDeviceOutput, error) {
	if err := h.Backend.ForgetDevice(in.AccessToken, in.DeviceKey); err != nil {
		return nil, err
	}

	return &forgetDeviceOutput{}, nil
}

type getDeviceInput struct {
	AccessToken string `json:"AccessToken,omitempty"`
	DeviceKey   string `json:"DeviceKey,omitempty"`
}

type getDeviceOutput struct {
	Device *deviceType `json:"Device,omitempty"`
}

func (h *Handler) handleGetDevice(_ context.Context, in *getDeviceInput) (*getDeviceOutput, error) {
	dev, err := h.Backend.GetDevice(in.AccessToken, in.DeviceKey)
	if err != nil {
		return nil, err
	}

	return &getDeviceOutput{Device: toDeviceType(dev)}, nil
}

type updateDeviceStatusInput struct {
	AccessToken            string `json:"AccessToken,omitempty"`
	DeviceKey              string `json:"DeviceKey,omitempty"`
	DeviceRememberedStatus string `json:"DeviceRememberedStatus,omitempty"`
}

type updateDeviceStatusOutput struct{}

func (h *Handler) handleUpdateDeviceStatus(
	_ context.Context,
	in *updateDeviceStatusInput,
) (*updateDeviceStatusOutput, error) {
	if err := h.Backend.UpdateDeviceStatus(in.AccessToken, in.DeviceKey, in.DeviceRememberedStatus); err != nil {
		return nil, err
	}

	return &updateDeviceStatusOutput{}, nil
}

// ----- CSV Header -----

type getCSVHeaderInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
}

type getCSVHeaderOutput struct {
	UserPoolID string   `json:"UserPoolId,omitempty"`
	CSVHeader  []string `json:"CSVHeader,omitempty"`
}

func (h *Handler) handleGetCSVHeader(_ context.Context, in *getCSVHeaderInput) (*getCSVHeaderOutput, error) {
	return &getCSVHeaderOutput{
		UserPoolID: in.UserPoolID,
		CSVHeader:  []string{"cognito:username", "name", "given_name", "family_name", "email", "email_verified"},
	}, nil
}

// ----- Tags -----

type listTagsForResourceInput struct {
	ResourceArn string `json:"ResourceArn,omitempty"`
}

type listTagsForResourceOutput struct {
	Tags map[string]string `json:"Tags,omitempty"`
}

func (h *Handler) handleListTagsForResource(
	_ context.Context,
	in *listTagsForResourceInput,
) (*listTagsForResourceOutput, error) {
	tags := h.Backend.ListTagsForResource(in.ResourceArn)
	if tags == nil {
		tags = map[string]string{}
	}

	return &listTagsForResourceOutput{Tags: tags}, nil
}

type tagResourceInput struct {
	Tags        map[string]string `json:"Tags,omitempty"`
	ResourceArn string            `json:"ResourceArn,omitempty"`
}

type tagResourceOutput struct{}

func (h *Handler) handleTagResource(_ context.Context, in *tagResourceInput) (*tagResourceOutput, error) {
	h.Backend.TagResource(in.ResourceArn, in.Tags)

	return &tagResourceOutput{}, nil
}

type untagResourceInput struct {
	ResourceArn string   `json:"ResourceArn,omitempty"`
	TagKeys     []string `json:"TagKeys,omitempty"`
}

type untagResourceOutput struct{}

func (h *Handler) handleUntagResource(_ context.Context, in *untagResourceInput) (*untagResourceOutput, error) {
	h.Backend.UntagResource(in.ResourceArn, in.TagKeys)

	return &untagResourceOutput{}, nil
}

// ----- User Pool Client Secret -----

type deleteUserPoolClientSecretInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	ClientID   string `json:"ClientId,omitempty"`
	SecretHash string `json:"SecretHash,omitempty"`
}

type deleteUserPoolClientSecretOutput struct{}

func (h *Handler) handleDeleteUserPoolClientSecret(
	_ context.Context,
	in *deleteUserPoolClientSecretInput,
) (*deleteUserPoolClientSecretOutput, error) {
	if err := h.Backend.DeleteUserPoolClientSecret(in.UserPoolID, in.ClientID); err != nil {
		return nil, err
	}

	return &deleteUserPoolClientSecretOutput{}, nil
}

type listUserPoolClientSecretsInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	ClientID   string `json:"ClientId,omitempty"`
}

type listUserPoolClientSecretsOutput struct {
	Secrets []string `json:"Secrets"`
}

func (h *Handler) handleListUserPoolClientSecrets(
	_ context.Context,
	in *listUserPoolClientSecretsInput,
) (*listUserPoolClientSecretsOutput, error) {
	secrets, err := h.Backend.ListUserPoolClientSecrets(in.UserPoolID, in.ClientID)
	if err != nil {
		return nil, err
	}

	return &listUserPoolClientSecretsOutput{Secrets: secrets}, nil
}

// ----- MFA Preferences / Settings -----

type setUserMFAPreferenceInput struct {
	SMSMfaSettings           *mfaSettings `json:"SMSMfaSettings,omitempty"`
	SoftwareTokenMfaSettings *mfaSettings `json:"SoftwareTokenMfaSettings,omitempty"`
	AccessToken              string       `json:"AccessToken,omitempty"`
}

type setUserMFAPreferenceOutput struct{}

func (h *Handler) handleSetUserMFAPreference(
	_ context.Context,
	_ *setUserMFAPreferenceInput,
) (*setUserMFAPreferenceOutput, error) {
	return &setUserMFAPreferenceOutput{}, nil
}

type setUserSettingsInput struct {
	AccessToken string          `json:"AccessToken,omitempty"`
	MFAOptions  []mfaOptionType `json:"MFAOptions,omitempty"`
}

type setUserSettingsOutput struct{}

func (h *Handler) handleSetUserSettings(_ context.Context, in *setUserSettingsInput) (*setUserSettingsOutput, error) {
	if err := h.Backend.SetUserSettings(in.AccessToken, toMFAOptionRecords(in.MFAOptions)); err != nil {
		return nil, err
	}

	return &setUserSettingsOutput{}, nil
}

// ----- Auth Event Feedback -----

type updateAuthEventFeedbackInput struct {
	UserPoolID    string `json:"UserPoolId,omitempty"`
	Username      string `json:"Username,omitempty"`
	EventID       string `json:"EventId,omitempty"`
	FeedbackToken string `json:"FeedbackToken,omitempty"`
	FeedbackValue string `json:"FeedbackValue,omitempty"`
}

type updateAuthEventFeedbackOutput struct{}

// handleUpdateAuthEventFeedback matches AWS: this op is unauthenticated (no
// AccessToken) and instead takes UserPoolId/Username directly plus a
// FeedbackToken issued out-of-band in a risk-notification email. This
// emulator does not mint or verify those tokens, so it requires one be
// present (matching the request's required-field contract) without
// cryptographically validating it.
func (h *Handler) handleUpdateAuthEventFeedback(
	_ context.Context,
	in *updateAuthEventFeedbackInput,
) (*updateAuthEventFeedbackOutput, error) {
	if in.FeedbackToken == "" {
		return nil, fmt.Errorf("%w: FeedbackToken is required", ErrInvalidParameter)
	}

	if err := h.Backend.UpdateAuthEventFeedback(in.UserPoolID, in.Username, in.EventID, in.FeedbackValue); err != nil {
		return nil, err
	}

	return &updateAuthEventFeedbackOutput{}, nil
}

// ----- Respond To Auth Challenge -----

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

func (h *Handler) handleRespondToAuthChallenge(
	_ context.Context,
	in *respondToAuthChallengeInput,
) (*respondToAuthChallengeOutput, error) {
	if in.ChallengeName != "SOFTWARE_TOKEN_MFA" {
		return &respondToAuthChallengeOutput{}, nil
	}

	totpCode := in.ChallengeResponses["SOFTWARE_TOKEN_MFA_CODE"]

	tokens, err := h.Backend.RespondToMFAChallenge(in.ClientID, in.Session, totpCode)
	if err != nil {
		return nil, err
	}

	return &respondToAuthChallengeOutput{
		AuthenticationResult: &authResult{
			AccessToken:  tokens.AccessToken,
			IDToken:      tokens.IDToken,
			RefreshToken: tokens.RefreshToken,
			TokenType:    authTypeBearer,
			ExpiresIn:    tokens.ExpiresIn,
		},
	}, nil
}

// ----- Verify Software Token -----

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

func (h *Handler) handleVerifySoftwareToken(
	_ context.Context,
	_ *verifySoftwareTokenInput,
) (*verifySoftwareTokenOutput, error) {
	return &verifySoftwareTokenOutput{Status: "SUCCESS"}, nil
}
