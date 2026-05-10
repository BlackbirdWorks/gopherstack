package cognitoidp

import (
	"context"

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

// ----- Device stubs -----

type adminGetDeviceInput struct {
	UserPoolID string `json:"UserPoolId"`
	Username   string `json:"Username"`
	DeviceKey  string `json:"DeviceKey"`
}

type deviceType struct {
	DeviceCreateDate            *float64        `json:"DeviceCreateDate,omitempty"`
	DeviceLastModifiedDate      *float64        `json:"DeviceLastModifiedDate,omitempty"`
	DeviceLastAuthenticatedDate *float64        `json:"DeviceLastAuthenticatedDate,omitempty"`
	DeviceKey                   string          `json:"DeviceKey,omitempty"`
	DeviceAttributes            []attributeType `json:"DeviceAttributes,omitempty"`
}

type adminGetDeviceOutput struct {
	Device *deviceType `json:"Device,omitempty"`
}

func (h *Handler) handleAdminGetDevice(_ context.Context, _ *adminGetDeviceInput) (*adminGetDeviceOutput, error) {
	return &adminGetDeviceOutput{Device: &deviceType{}}, nil
}

type adminLinkProviderForUserInput struct {
	DestinationUser map[string]any `json:"DestinationUser"`
	SourceUser      map[string]any `json:"SourceUser"`
	UserPoolID      string         `json:"UserPoolId"`
}

type adminLinkProviderForUserOutput struct{}

func (h *Handler) handleAdminLinkProviderForUser(
	_ context.Context,
	_ *adminLinkProviderForUserInput,
) (*adminLinkProviderForUserOutput, error) {
	return &adminLinkProviderForUserOutput{}, nil
}

type adminListDevicesInput struct {
	UserPoolID string `json:"UserPoolId"`
	Username   string `json:"Username"`
}

type adminListDevicesOutput struct {
	Devices []deviceType `json:"Devices"`
}

func (h *Handler) handleAdminListDevices(_ context.Context, _ *adminListDevicesInput) (*adminListDevicesOutput, error) {
	return &adminListDevicesOutput{Devices: []deviceType{}}, nil
}

type adminListUserAuthEventsInput struct {
	UserPoolID string `json:"UserPoolId"`
	Username   string `json:"Username"`
}

type adminListUserAuthEventsOutput struct {
	AuthEvents []map[string]any `json:"AuthEvents"`
}

func (h *Handler) handleAdminListUserAuthEvents(
	_ context.Context,
	_ *adminListUserAuthEventsInput,
) (*adminListUserAuthEventsOutput, error) {
	return &adminListUserAuthEventsOutput{AuthEvents: []map[string]any{}}, nil
}

type adminSetUserMFAPreferenceInput struct {
	SMSMfaSettings           *mfaSettings `json:"SMSMfaSettings,omitempty"`
	SoftwareTokenMfaSettings *mfaSettings `json:"SoftwareTokenMfaSettings,omitempty"`
	UserPoolID               string       `json:"UserPoolId"`
	Username                 string       `json:"Username"`
}

type mfaSettings struct {
	Enabled      bool `json:"Enabled"`
	PreferredMfa bool `json:"PreferredMfa"`
}

type adminSetUserMFAPreferenceOutput struct{}

func (h *Handler) handleAdminSetUserMFAPreference(
	_ context.Context,
	_ *adminSetUserMFAPreferenceInput,
) (*adminSetUserMFAPreferenceOutput, error) {
	return &adminSetUserMFAPreferenceOutput{}, nil
}

type adminSetUserSettingsInput struct {
	UserPoolID string `json:"UserPoolId"`
	Username   string `json:"Username"`
}

type adminSetUserSettingsOutput struct{}

func (h *Handler) handleAdminSetUserSettings(
	_ context.Context,
	_ *adminSetUserSettingsInput,
) (*adminSetUserSettingsOutput, error) {
	return &adminSetUserSettingsOutput{}, nil
}

type adminUpdateAuthEventFeedbackInput struct {
	UserPoolID    string `json:"UserPoolId"`
	Username      string `json:"Username"`
	EventID       string `json:"EventId"`
	FeedbackValue string `json:"FeedbackValue"`
}

type adminUpdateAuthEventFeedbackOutput struct{}

func (h *Handler) handleAdminUpdateAuthEventFeedback(
	_ context.Context,
	_ *adminUpdateAuthEventFeedbackInput,
) (*adminUpdateAuthEventFeedbackOutput, error) {
	return &adminUpdateAuthEventFeedbackOutput{}, nil
}

type adminUpdateDeviceStatusInput struct {
	UserPoolID             string `json:"UserPoolId"`
	Username               string `json:"Username"`
	DeviceKey              string `json:"DeviceKey"`
	DeviceRememberedStatus string `json:"DeviceRememberedStatus"`
}

type adminUpdateDeviceStatusOutput struct{}

func (h *Handler) handleAdminUpdateDeviceStatus(
	_ context.Context,
	_ *adminUpdateDeviceStatusInput,
) (*adminUpdateDeviceStatusOutput, error) {
	return &adminUpdateDeviceStatusOutput{}, nil
}

type associateSoftwareTokenInput struct {
	AccessToken string `json:"AccessToken"`
	Session     string `json:"Session"`
}

type associateSoftwareTokenOutput struct {
	SecretCode string `json:"SecretCode,omitempty"`
	Session    string `json:"Session,omitempty"`
}

// handleAssociateSoftwareToken returns a stub TOTP secret code.
// The secret is a test value only; it is not a real credential.
//
//nolint:gosec // JBSWY3DPEHPK3PXP is the canonical example TOTP seed from RFC 6238 — not a real credential.
func (h *Handler) handleAssociateSoftwareToken(
	_ context.Context,
	_ *associateSoftwareTokenInput,
) (*associateSoftwareTokenOutput, error) {
	return &associateSoftwareTokenOutput{SecretCode: "JBSWY3DPEHPK3PXP"}, nil
}

type completeWebAuthnRegistrationInput struct {
	Credential  map[string]any `json:"Credential"`
	AccessToken string         `json:"AccessToken"`
}

type completeWebAuthnRegistrationOutput struct{}

func (h *Handler) handleCompleteWebAuthnRegistration(
	_ context.Context,
	_ *completeWebAuthnRegistrationInput,
) (*completeWebAuthnRegistrationOutput, error) {
	return &completeWebAuthnRegistrationOutput{}, nil
}

type confirmDeviceInput struct {
	AccessToken                string            `json:"AccessToken"`
	DeviceKey                  string            `json:"DeviceKey"`
	DeviceSecretVerifierConfig map[string]string `json:"DeviceSecretVerifierConfig,omitempty"`
	DeviceName                 string            `json:"DeviceName,omitempty"`
}

type confirmDeviceOutput struct {
	UserConfirmationNecessary bool `json:"UserConfirmationNecessary"`
}

func (h *Handler) handleConfirmDevice(_ context.Context, _ *confirmDeviceInput) (*confirmDeviceOutput, error) {
	return &confirmDeviceOutput{}, nil
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
	UserPoolID      string            `json:"UserPoolId"`
	ProviderName    string            `json:"ProviderName"`
	ProviderType    string            `json:"ProviderType"`
}

type createIdentityProviderOutput struct {
	IdentityProvider *identityProviderType `json:"IdentityProvider,omitempty"`
}

func (h *Handler) handleCreateIdentityProvider(
	_ context.Context,
	in *createIdentityProviderInput,
) (*createIdentityProviderOutput, error) {
	return &createIdentityProviderOutput{
		IdentityProvider: &identityProviderType{
			UserPoolID:   in.UserPoolID,
			ProviderName: in.ProviderName,
			ProviderType: in.ProviderType,
		},
	}, nil
}

type deleteIdentityProviderInput struct {
	UserPoolID   string `json:"UserPoolId"`
	ProviderName string `json:"ProviderName"`
}

type deleteIdentityProviderOutput struct{}

func (h *Handler) handleDeleteIdentityProvider(
	_ context.Context,
	_ *deleteIdentityProviderInput,
) (*deleteIdentityProviderOutput, error) {
	return &deleteIdentityProviderOutput{}, nil
}

type describeIdentityProviderInput struct {
	UserPoolID   string `json:"UserPoolId"`
	ProviderName string `json:"ProviderName"`
}

type describeIdentityProviderOutput struct {
	IdentityProvider *identityProviderType `json:"IdentityProvider,omitempty"`
}

func (h *Handler) handleDescribeIdentityProvider(
	_ context.Context,
	in *describeIdentityProviderInput,
) (*describeIdentityProviderOutput, error) {
	return &describeIdentityProviderOutput{
		IdentityProvider: &identityProviderType{
			UserPoolID:   in.UserPoolID,
			ProviderName: in.ProviderName,
		},
	}, nil
}

type getIdentityProviderByIdentifierInput struct {
	UserPoolID    string `json:"UserPoolId"`
	IdpIdentifier string `json:"IdpIdentifier"`
}

type getIdentityProviderByIdentifierOutput struct {
	IdentityProvider *identityProviderType `json:"IdentityProvider,omitempty"`
}

func (h *Handler) handleGetIdentityProviderByIdentifier(
	_ context.Context,
	_ *getIdentityProviderByIdentifierInput,
) (*getIdentityProviderByIdentifierOutput, error) {
	return &getIdentityProviderByIdentifierOutput{IdentityProvider: &identityProviderType{}}, nil
}

type listIdentityProvidersInput struct {
	UserPoolID string `json:"UserPoolId"`
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
	_ *listIdentityProvidersInput,
) (*listIdentityProvidersOutput, error) {
	return &listIdentityProvidersOutput{Providers: []identityProviderSummary{}}, nil
}

type updateIdentityProviderInput struct {
	ProviderDetails map[string]string `json:"ProviderDetails,omitempty"`
	UserPoolID      string            `json:"UserPoolId"`
	ProviderName    string            `json:"ProviderName"`
}

type updateIdentityProviderOutput struct {
	IdentityProvider *identityProviderType `json:"IdentityProvider,omitempty"`
}

func (h *Handler) handleUpdateIdentityProvider(
	_ context.Context,
	in *updateIdentityProviderInput,
) (*updateIdentityProviderOutput, error) {
	return &updateIdentityProviderOutput{
		IdentityProvider: &identityProviderType{
			UserPoolID:   in.UserPoolID,
			ProviderName: in.ProviderName,
		},
	}, nil
}

// ----- Managed Login Branding -----

type createManagedLoginBrandingInput struct {
	UserPoolID string `json:"UserPoolId"`
	ClientID   string `json:"ClientId"`
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
	return &createManagedLoginBrandingOutput{
		ManagedLoginBranding: &managedLoginBrandingType{UserPoolID: in.UserPoolID, ManagedLoginBrandingID: "mlb-stub"},
	}, nil
}

type deleteManagedLoginBrandingInput struct {
	ManagedLoginBrandingID string `json:"ManagedLoginBrandingId"`
	UserPoolID             string `json:"UserPoolId"`
}

type deleteManagedLoginBrandingOutput struct{}

func (h *Handler) handleDeleteManagedLoginBranding(
	_ context.Context,
	_ *deleteManagedLoginBrandingInput,
) (*deleteManagedLoginBrandingOutput, error) {
	return &deleteManagedLoginBrandingOutput{}, nil
}

type describeManagedLoginBrandingInput struct {
	ManagedLoginBrandingID string `json:"ManagedLoginBrandingId"`
	UserPoolID             string `json:"UserPoolId"`
}

type describeManagedLoginBrandingOutput struct {
	ManagedLoginBranding *managedLoginBrandingType `json:"ManagedLoginBranding,omitempty"`
}

func (h *Handler) handleDescribeManagedLoginBranding(
	_ context.Context,
	in *describeManagedLoginBrandingInput,
) (*describeManagedLoginBrandingOutput, error) {
	return &describeManagedLoginBrandingOutput{
		ManagedLoginBranding: &managedLoginBrandingType{
			ManagedLoginBrandingID: in.ManagedLoginBrandingID,
			UserPoolID:             in.UserPoolID,
		},
	}, nil
}

type describeManagedLoginBrandingByClientInput struct {
	ClientID   string `json:"ClientId"`
	UserPoolID string `json:"UserPoolId"`
}

type describeManagedLoginBrandingByClientOutput struct {
	ManagedLoginBranding *managedLoginBrandingType `json:"ManagedLoginBranding,omitempty"`
}

func (h *Handler) handleDescribeManagedLoginBrandingByClient(
	_ context.Context,
	_ *describeManagedLoginBrandingByClientInput,
) (*describeManagedLoginBrandingByClientOutput, error) {
	return &describeManagedLoginBrandingByClientOutput{ManagedLoginBranding: &managedLoginBrandingType{}}, nil
}

type updateManagedLoginBrandingInput struct {
	ManagedLoginBrandingID string `json:"ManagedLoginBrandingId"`
	UserPoolID             string `json:"UserPoolId"`
}

type updateManagedLoginBrandingOutput struct {
	ManagedLoginBranding *managedLoginBrandingType `json:"ManagedLoginBranding,omitempty"`
}

func (h *Handler) handleUpdateManagedLoginBranding(
	_ context.Context,
	in *updateManagedLoginBrandingInput,
) (*updateManagedLoginBrandingOutput, error) {
	return &updateManagedLoginBrandingOutput{
		ManagedLoginBranding: &managedLoginBrandingType{
			ManagedLoginBrandingID: in.ManagedLoginBrandingID,
			UserPoolID:             in.UserPoolID,
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
	UserPoolID string `json:"UserPoolId"`
	Identifier string `json:"Identifier"`
	Name       string `json:"Name"`
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
	UserPoolID string `json:"UserPoolId"`
	Identifier string `json:"Identifier"`
}

type deleteResourceServerOutput struct{}

func (h *Handler) handleDeleteResourceServer(
	_ context.Context,
	_ *deleteResourceServerInput,
) (*deleteResourceServerOutput, error) {
	return &deleteResourceServerOutput{}, nil
}

type describeResourceServerInput struct {
	UserPoolID string `json:"UserPoolId"`
	Identifier string `json:"Identifier"`
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
	UserPoolID string `json:"UserPoolId"`
}

type listResourceServersOutput struct {
	ResourceServers []resourceServerType `json:"ResourceServers"`
}

func (h *Handler) handleListResourceServers(
	_ context.Context,
	_ *listResourceServersInput,
) (*listResourceServersOutput, error) {
	return &listResourceServersOutput{ResourceServers: []resourceServerType{}}, nil
}

type updateResourceServerInput struct {
	UserPoolID string `json:"UserPoolId"`
	Identifier string `json:"Identifier"`
	Name       string `json:"Name"`
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
	UserPoolID string `json:"UserPoolId"`
}

type createTermsOutput struct {
	Terms *termsType `json:"Terms,omitempty"`
}

func (h *Handler) handleCreateTerms(_ context.Context, _ *createTermsInput) (*createTermsOutput, error) {
	return &createTermsOutput{Terms: &termsType{}}, nil
}

type deleteTermsInput struct {
	UserPoolID string `json:"UserPoolId"`
}

type deleteTermsOutput struct{}

func (h *Handler) handleDeleteTerms(_ context.Context, _ *deleteTermsInput) (*deleteTermsOutput, error) {
	return &deleteTermsOutput{}, nil
}

type describeTermsInput struct {
	UserPoolID string `json:"UserPoolId"`
}

type describeTermsOutput struct {
	Terms *termsType `json:"Terms,omitempty"`
}

func (h *Handler) handleDescribeTerms(_ context.Context, _ *describeTermsInput) (*describeTermsOutput, error) {
	return &describeTermsOutput{Terms: &termsType{}}, nil
}

type listTermsInput struct {
	UserPoolID string `json:"UserPoolId"`
}

type listTermsOutput struct {
	Terms []termsType `json:"Terms"`
}

func (h *Handler) handleListTerms(_ context.Context, _ *listTermsInput) (*listTermsOutput, error) {
	return &listTermsOutput{Terms: []termsType{}}, nil
}

type updateTermsInput struct {
	UserPoolID string `json:"UserPoolId"`
}

type updateTermsOutput struct {
	Terms *termsType `json:"Terms,omitempty"`
}

func (h *Handler) handleUpdateTerms(_ context.Context, _ *updateTermsInput) (*updateTermsOutput, error) {
	return &updateTermsOutput{Terms: &termsType{}}, nil
}

// ----- User Import Job -----

type userImportJobType struct {
	JobID      string `json:"JobId,omitempty"`
	JobName    string `json:"JobName,omitempty"`
	Status     string `json:"Status,omitempty"`
	UserPoolID string `json:"UserPoolId,omitempty"`
}

type createUserImportJobInput struct {
	UserPoolID string `json:"UserPoolId"`
	JobName    string `json:"JobName"`
}

type createUserImportJobOutput struct {
	UserImportJob *userImportJobType `json:"UserImportJob,omitempty"`
}

func (h *Handler) handleCreateUserImportJob(
	_ context.Context,
	in *createUserImportJobInput,
) (*createUserImportJobOutput, error) {
	return &createUserImportJobOutput{
		UserImportJob: &userImportJobType{
			JobID:      "job-stub",
			JobName:    in.JobName,
			UserPoolID: in.UserPoolID,
			Status:     "Created",
		},
	}, nil
}

type describeUserImportJobInput struct {
	UserPoolID string `json:"UserPoolId"`
	JobID      string `json:"JobId"`
}

type describeUserImportJobOutput struct {
	UserImportJob *userImportJobType `json:"UserImportJob,omitempty"`
}

func (h *Handler) handleDescribeUserImportJob(
	_ context.Context,
	in *describeUserImportJobInput,
) (*describeUserImportJobOutput, error) {
	return &describeUserImportJobOutput{
		UserImportJob: &userImportJobType{JobID: in.JobID, UserPoolID: in.UserPoolID, Status: "Created"},
	}, nil
}

type listUserImportJobsInput struct {
	UserPoolID string `json:"UserPoolId"`
}

type listUserImportJobsOutput struct {
	UserImportJobs []userImportJobType `json:"UserImportJobs"`
}

func (h *Handler) handleListUserImportJobs(
	_ context.Context,
	_ *listUserImportJobsInput,
) (*listUserImportJobsOutput, error) {
	return &listUserImportJobsOutput{UserImportJobs: []userImportJobType{}}, nil
}

type startUserImportJobInput struct {
	UserPoolID string `json:"UserPoolId"`
	JobID      string `json:"JobId"`
}

type startUserImportJobOutput struct {
	UserImportJob *userImportJobType `json:"UserImportJob,omitempty"`
}

func (h *Handler) handleStartUserImportJob(
	_ context.Context,
	in *startUserImportJobInput,
) (*startUserImportJobOutput, error) {
	return &startUserImportJobOutput{
		UserImportJob: &userImportJobType{JobID: in.JobID, UserPoolID: in.UserPoolID, Status: "InProgress"},
	}, nil
}

type stopUserImportJobInput struct {
	UserPoolID string `json:"UserPoolId"`
	JobID      string `json:"JobId"`
}

type stopUserImportJobOutput struct {
	UserImportJob *userImportJobType `json:"UserImportJob,omitempty"`
}

func (h *Handler) handleStopUserImportJob(
	_ context.Context,
	in *stopUserImportJobInput,
) (*stopUserImportJobOutput, error) {
	return &stopUserImportJobOutput{
		UserImportJob: &userImportJobType{JobID: in.JobID, UserPoolID: in.UserPoolID, Status: "Stopped"},
	}, nil
}

// ----- User Pool Domain -----

type createUserPoolDomainInput struct {
	UserPoolID string `json:"UserPoolId"`
	Domain     string `json:"Domain"`
}

type createUserPoolDomainOutput struct {
	CloudFrontDomain string `json:"CloudFrontDomain,omitempty"`
}

func (h *Handler) handleCreateUserPoolDomain(
	_ context.Context,
	in *createUserPoolDomainInput,
) (*createUserPoolDomainOutput, error) {
	return &createUserPoolDomainOutput{CloudFrontDomain: in.Domain + ".auth.us-east-1.amazoncognito.com"}, nil
}

type deleteUserPoolDomainInput struct {
	UserPoolID string `json:"UserPoolId"`
	Domain     string `json:"Domain"`
}

type deleteUserPoolDomainOutput struct{}

func (h *Handler) handleDeleteUserPoolDomain(
	_ context.Context,
	_ *deleteUserPoolDomainInput,
) (*deleteUserPoolDomainOutput, error) {
	return &deleteUserPoolDomainOutput{}, nil
}

type describeUserPoolDomainInput struct {
	Domain string `json:"Domain"`
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
	return &describeUserPoolDomainOutput{
		DomainDescription: &userPoolDomainDescription{Domain: in.Domain, Status: "ACTIVE"},
	}, nil
}

type updateUserPoolDomainInput struct {
	UserPoolID string `json:"UserPoolId"`
	Domain     string `json:"Domain"`
}

type updateUserPoolDomainOutput struct {
	CloudFrontDomain string `json:"CloudFrontDomain,omitempty"`
}

func (h *Handler) handleUpdateUserPoolDomain(
	_ context.Context,
	in *updateUserPoolDomainInput,
) (*updateUserPoolDomainOutput, error) {
	return &updateUserPoolDomainOutput{CloudFrontDomain: in.Domain + ".auth.us-east-1.amazoncognito.com"}, nil
}

// ----- WebAuthn -----

type deleteWebAuthnCredentialInput struct {
	AccessToken  string `json:"AccessToken"`
	CredentialID string `json:"CredentialId"`
}

type deleteWebAuthnCredentialOutput struct{}

func (h *Handler) handleDeleteWebAuthnCredential(
	_ context.Context,
	_ *deleteWebAuthnCredentialInput,
) (*deleteWebAuthnCredentialOutput, error) {
	return &deleteWebAuthnCredentialOutput{}, nil
}

type listWebAuthnCredentialsInput struct {
	AccessToken string `json:"AccessToken"`
}

type listWebAuthnCredentialsOutput struct {
	Credentials []map[string]any `json:"Credentials"`
}

func (h *Handler) handleListWebAuthnCredentials(
	_ context.Context,
	_ *listWebAuthnCredentialsInput,
) (*listWebAuthnCredentialsOutput, error) {
	return &listWebAuthnCredentialsOutput{Credentials: []map[string]any{}}, nil
}

type startWebAuthnRegistrationInput struct {
	AccessToken string `json:"AccessToken"`
}

type startWebAuthnRegistrationOutput struct {
	CredentialCreationOptions map[string]any `json:"CredentialCreationOptions,omitempty"`
}

func (h *Handler) handleStartWebAuthnRegistration(
	_ context.Context,
	_ *startWebAuthnRegistrationInput,
) (*startWebAuthnRegistrationOutput, error) {
	return &startWebAuthnRegistrationOutput{CredentialCreationOptions: map[string]any{}}, nil
}

// ----- Risk Configuration -----

type describeRiskConfigurationInput struct {
	UserPoolID string `json:"UserPoolId"`
	ClientID   string `json:"ClientId,omitempty"`
}

type riskConfigurationType struct{}

type describeRiskConfigurationOutput struct {
	RiskConfiguration *riskConfigurationType `json:"RiskConfiguration,omitempty"`
}

func (h *Handler) handleDescribeRiskConfiguration(
	_ context.Context,
	_ *describeRiskConfigurationInput,
) (*describeRiskConfigurationOutput, error) {
	return &describeRiskConfigurationOutput{RiskConfiguration: &riskConfigurationType{}}, nil
}

type setRiskConfigurationInput struct {
	UserPoolID string `json:"UserPoolId"`
	ClientID   string `json:"ClientId,omitempty"`
}

type setRiskConfigurationOutput struct {
	RiskConfiguration *riskConfigurationType `json:"RiskConfiguration,omitempty"`
}

func (h *Handler) handleSetRiskConfiguration(
	_ context.Context,
	_ *setRiskConfigurationInput,
) (*setRiskConfigurationOutput, error) {
	return &setRiskConfigurationOutput{RiskConfiguration: &riskConfigurationType{}}, nil
}

// ----- Log Delivery -----

type getLogDeliveryConfigurationInput struct {
	UserPoolID string `json:"UserPoolId"`
}

type getLogDeliveryConfigurationOutput struct {
	LogDeliveryConfiguration map[string]any `json:"LogDeliveryConfiguration,omitempty"`
}

func (h *Handler) handleGetLogDeliveryConfiguration(
	_ context.Context,
	_ *getLogDeliveryConfigurationInput,
) (*getLogDeliveryConfigurationOutput, error) {
	return &getLogDeliveryConfigurationOutput{LogDeliveryConfiguration: map[string]any{}}, nil
}

type setLogDeliveryConfigurationInput struct {
	UserPoolID string `json:"UserPoolId"`
}

type setLogDeliveryConfigurationOutput struct {
	LogDeliveryConfiguration map[string]any `json:"LogDeliveryConfiguration,omitempty"`
}

func (h *Handler) handleSetLogDeliveryConfiguration(
	_ context.Context,
	_ *setLogDeliveryConfigurationInput,
) (*setLogDeliveryConfigurationOutput, error) {
	return &setLogDeliveryConfigurationOutput{LogDeliveryConfiguration: map[string]any{}}, nil
}

// ----- Tokens -----

type getTokensFromRefreshTokenInput struct {
	RefreshToken string `json:"RefreshToken"`
	ClientID     string `json:"ClientId"`
}

type getTokensFromRefreshTokenOutput struct {
	AuthenticationResult *authResult `json:"AuthenticationResult,omitempty"`
}

func (h *Handler) handleGetTokensFromRefreshToken(
	_ context.Context,
	_ *getTokensFromRefreshTokenInput,
) (*getTokensFromRefreshTokenOutput, error) {
	return &getTokensFromRefreshTokenOutput{AuthenticationResult: &authResult{}}, nil
}

// ----- UI Customization -----

type uiCustomizationType struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	ClientID   string `json:"ClientId,omitempty"`
	CSS        string `json:"CSS,omitempty"`
}

type getUICustomizationInput struct {
	UserPoolID string `json:"UserPoolId"`
	ClientID   string `json:"ClientId,omitempty"`
}

type getUICustomizationOutput struct {
	UICustomization *uiCustomizationType `json:"UICustomization,omitempty"`
}

func (h *Handler) handleGetUICustomization(
	_ context.Context,
	in *getUICustomizationInput,
) (*getUICustomizationOutput, error) {
	return &getUICustomizationOutput{UICustomization: &uiCustomizationType{UserPoolID: in.UserPoolID}}, nil
}

type setUICustomizationInput struct {
	UserPoolID string `json:"UserPoolId"`
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
	return &setUICustomizationOutput{UICustomization: &uiCustomizationType{UserPoolID: in.UserPoolID}}, nil
}

// ----- User Attribute Verification -----

type getUserAttributeVerificationCodeInput struct {
	AccessToken   string `json:"AccessToken"`
	AttributeName string `json:"AttributeName"`
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
	AccessToken string `json:"AccessToken"`
}

type getUserAuthFactorsOutput struct {
	Username                  string   `json:"Username,omitempty"`
	ConfiguredUserAuthFactors []string `json:"ConfiguredUserAuthFactors"`
}

func (h *Handler) handleGetUserAuthFactors(
	_ context.Context,
	_ *getUserAuthFactorsInput,
) (*getUserAuthFactorsOutput, error) {
	return &getUserAuthFactorsOutput{ConfiguredUserAuthFactors: []string{}}, nil
}

// ----- Devices -----

type listDevicesInput struct {
	AccessToken string `json:"AccessToken"`
}

type listDevicesOutput struct {
	Devices []deviceType `json:"Devices"`
}

func (h *Handler) handleListDevices(_ context.Context, _ *listDevicesInput) (*listDevicesOutput, error) {
	return &listDevicesOutput{Devices: []deviceType{}}, nil
}

type forgetDeviceInput struct {
	AccessToken string `json:"AccessToken"`
	DeviceKey   string `json:"DeviceKey"`
}

type forgetDeviceOutput struct{}

func (h *Handler) handleForgetDevice(_ context.Context, _ *forgetDeviceInput) (*forgetDeviceOutput, error) {
	return &forgetDeviceOutput{}, nil
}

type getDeviceInput struct {
	AccessToken string `json:"AccessToken"`
	DeviceKey   string `json:"DeviceKey"`
}

type getDeviceOutput struct {
	Device *deviceType `json:"Device,omitempty"`
}

func (h *Handler) handleGetDevice(_ context.Context, _ *getDeviceInput) (*getDeviceOutput, error) {
	return &getDeviceOutput{Device: &deviceType{}}, nil
}

type updateDeviceStatusInput struct {
	AccessToken            string `json:"AccessToken"`
	DeviceKey              string `json:"DeviceKey"`
	DeviceRememberedStatus string `json:"DeviceRememberedStatus"`
}

type updateDeviceStatusOutput struct{}

func (h *Handler) handleUpdateDeviceStatus(
	_ context.Context,
	_ *updateDeviceStatusInput,
) (*updateDeviceStatusOutput, error) {
	return &updateDeviceStatusOutput{}, nil
}

// ----- CSV Header -----

type getCSVHeaderInput struct {
	UserPoolID string `json:"UserPoolId"`
}

type getCSVHeaderOutput struct {
	UserPoolID string   `json:"UserPoolId,omitempty"`
	CSVHeader  []string `json:"CSVHeader"`
}

func (h *Handler) handleGetCSVHeader(_ context.Context, in *getCSVHeaderInput) (*getCSVHeaderOutput, error) {
	return &getCSVHeaderOutput{
		UserPoolID: in.UserPoolID,
		CSVHeader:  []string{"cognito:username", "name", "given_name", "family_name", "email", "email_verified"},
	}, nil
}

// ----- Tags -----

type listTagsForResourceInput struct {
	ResourceArn string `json:"ResourceArn"`
}

type listTagsForResourceOutput struct {
	Tags map[string]string `json:"Tags"`
}

func (h *Handler) handleListTagsForResource(
	_ context.Context,
	_ *listTagsForResourceInput,
) (*listTagsForResourceOutput, error) {
	return &listTagsForResourceOutput{Tags: map[string]string{}}, nil
}

type tagResourceInput struct {
	Tags        map[string]string `json:"Tags"`
	ResourceArn string            `json:"ResourceArn"`
}

type tagResourceOutput struct{}

func (h *Handler) handleTagResource(_ context.Context, _ *tagResourceInput) (*tagResourceOutput, error) {
	return &tagResourceOutput{}, nil
}

type untagResourceInput struct {
	ResourceArn string   `json:"ResourceArn"`
	TagKeys     []string `json:"TagKeys"`
}

type untagResourceOutput struct{}

func (h *Handler) handleUntagResource(_ context.Context, _ *untagResourceInput) (*untagResourceOutput, error) {
	return &untagResourceOutput{}, nil
}

// ----- User Pool Client Secret -----

type deleteUserPoolClientSecretInput struct {
	UserPoolID string `json:"UserPoolId"`
	ClientID   string `json:"ClientId"`
	SecretHash string `json:"SecretHash"`
}

type deleteUserPoolClientSecretOutput struct{}

func (h *Handler) handleDeleteUserPoolClientSecret(
	_ context.Context,
	_ *deleteUserPoolClientSecretInput,
) (*deleteUserPoolClientSecretOutput, error) {
	return &deleteUserPoolClientSecretOutput{}, nil
}

type listUserPoolClientSecretsInput struct {
	UserPoolID string `json:"UserPoolId"`
	ClientID   string `json:"ClientId"`
}

type listUserPoolClientSecretsOutput struct {
	Secrets []string `json:"Secrets"`
}

func (h *Handler) handleListUserPoolClientSecrets(
	_ context.Context,
	_ *listUserPoolClientSecretsInput,
) (*listUserPoolClientSecretsOutput, error) {
	return &listUserPoolClientSecretsOutput{Secrets: []string{}}, nil
}

// ----- MFA Preferences / Settings -----

type setUserMFAPreferenceInput struct {
	SMSMfaSettings           *mfaSettings `json:"SMSMfaSettings,omitempty"`
	SoftwareTokenMfaSettings *mfaSettings `json:"SoftwareTokenMfaSettings,omitempty"`
	AccessToken              string       `json:"AccessToken"`
}

type setUserMFAPreferenceOutput struct{}

func (h *Handler) handleSetUserMFAPreference(
	_ context.Context,
	_ *setUserMFAPreferenceInput,
) (*setUserMFAPreferenceOutput, error) {
	return &setUserMFAPreferenceOutput{}, nil
}

type setUserSettingsInput struct {
	AccessToken string              `json:"AccessToken"`
	MFAOptions  []map[string]string `json:"MFAOptions"`
}

type setUserSettingsOutput struct{}

func (h *Handler) handleSetUserSettings(_ context.Context, _ *setUserSettingsInput) (*setUserSettingsOutput, error) {
	return &setUserSettingsOutput{}, nil
}

// ----- Auth Event Feedback -----

type updateAuthEventFeedbackInput struct {
	UserPoolID    string `json:"UserPoolId"`
	Username      string `json:"Username"`
	EventID       string `json:"EventId"`
	FeedbackToken string `json:"FeedbackToken"`
	FeedbackValue string `json:"FeedbackValue"`
}

type updateAuthEventFeedbackOutput struct{}

func (h *Handler) handleUpdateAuthEventFeedback(
	_ context.Context,
	_ *updateAuthEventFeedbackInput,
) (*updateAuthEventFeedbackOutput, error) {
	return &updateAuthEventFeedbackOutput{}, nil
}

// ----- Respond To Auth Challenge -----

type respondToAuthChallengeInput struct {
	ClientID           string            `json:"ClientId"`
	ChallengeName      string            `json:"ChallengeName"`
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
	AccessToken        string `json:"AccessToken"`
	UserCode           string `json:"UserCode"`
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
