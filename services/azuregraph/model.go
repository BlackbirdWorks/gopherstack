package azuregraph

import (
	"encoding/json"
	"time"
)

// Object kinds, also the OData type names (#microsoft.graph.<kind>).
const (
	kindApplication      = "application"
	kindServicePrincipal = "servicePrincipal"
	kindUser             = "user"
	kindGroup            = "group"
)

// PermissionScope is an api.oauth2PermissionScopes entry.
type PermissionScope struct {
	ID                      string `json:"id"`
	AdminConsentDescription string `json:"adminConsentDescription,omitempty"`
	AdminConsentDisplayName string `json:"adminConsentDisplayName,omitempty"`
	Type                    string `json:"type,omitempty"`
	UserConsentDescription  string `json:"userConsentDescription,omitempty"`
	UserConsentDisplayName  string `json:"userConsentDisplayName,omitempty"`
	Value                   string `json:"value,omitempty"`
	IsEnabled               bool   `json:"isEnabled"`
}

// AppRole is an appRoles entry.
type AppRole struct {
	ID                 string   `json:"id"`
	Description        string   `json:"description,omitempty"`
	DisplayName        string   `json:"displayName,omitempty"`
	Origin             string   `json:"origin,omitempty"`
	Value              string   `json:"value,omitempty"`
	AllowedMemberTypes []string `json:"allowedMemberTypes"`
	IsEnabled          bool     `json:"isEnabled"`
}

// ResourceAccess is a requiredResourceAccess.resourceAccess entry.
type ResourceAccess struct {
	ID   string `json:"id"`
	Type string `json:"type"`
}

// RequiredResourceAccess declares the permissions an application needs from one resource.
type RequiredResourceAccess struct {
	ResourceAppID  string           `json:"resourceAppId"`
	ResourceAccess []ResourceAccess `json:"resourceAccess"`
}

// PreAuthorizedApplication is an api.preAuthorizedApplications entry.
type PreAuthorizedApplication struct {
	AppID                  string   `json:"appId"`
	DelegatedPermissionIDs []string `json:"delegatedPermissionIds"`
}

// APIApplication is the application's api facet.
type APIApplication struct {
	AcceptMappedClaims          *bool                      `json:"acceptMappedClaims"`
	RequestedAccessTokenVersion *int                       `json:"requestedAccessTokenVersion"`
	KnownClientApplications     []string                   `json:"knownClientApplications"`
	OAuth2PermissionScopes      []PermissionScope          `json:"oauth2PermissionScopes"`
	PreAuthorizedApplications   []PreAuthorizedApplication `json:"preAuthorizedApplications"`
}

// ImplicitGrantSettings is web.implicitGrantSettings.
type ImplicitGrantSettings struct {
	EnableAccessTokenIssuance bool `json:"enableAccessTokenIssuance"`
	EnableIDTokenIssuance     bool `json:"enableIdTokenIssuance"`
}

// WebApplication is the application's web facet.
type WebApplication struct {
	HomePageURL           string                `json:"homePageUrl"`
	LogoutURL             string                `json:"logoutUrl"`
	RedirectURIs          []string              `json:"redirectUris"`
	ImplicitGrantSettings ImplicitGrantSettings `json:"implicitGrantSettings"`
}

// RedirectFacet carries redirectUris for the spa and publicClient facets.
type RedirectFacet struct {
	RedirectURIs []string `json:"redirectUris"`
}

// PasswordCredential is a client secret; SecretText is only set in the addPassword response.
type PasswordCredential struct {
	StartDateTime       time.Time `json:"startDateTime"`
	EndDateTime         time.Time `json:"endDateTime"`
	SecretText          *string   `json:"secretText"`
	CustomKeyIdentifier *string   `json:"customKeyIdentifier"`
	DisplayName         *string   `json:"displayName"`
	Hint                *string   `json:"hint"`
	KeyID               string    `json:"keyId"`
}

// KeyCredential is a certificate or key credential; the key material is never returned.
type KeyCredential struct {
	StartDateTime       time.Time `json:"startDateTime"`
	EndDateTime         time.Time `json:"endDateTime"`
	Key                 *string   `json:"key"`
	CustomKeyIdentifier *string   `json:"customKeyIdentifier"`
	DisplayName         *string   `json:"displayName"`
	KeyID               string    `json:"keyId"`
	Type                string    `json:"type"`
	Usage               string    `json:"usage"`
}

// Application is an application registration.
type Application struct {
	CreatedDateTime            time.Time                `json:"createdDateTime"`
	IsDeviceOnlyAuthSupported  *bool                    `json:"isDeviceOnlyAuthSupported"`
	ServiceManagementReference string                   `json:"serviceManagementReference,omitempty"`
	Notes                      string                   `json:"notes,omitempty"`
	ApplicationTemplateID      string                   `json:"applicationTemplateId,omitempty"`
	PublisherDomain            string                   `json:"publisherDomain"`
	SAMLMetadataURL            string                   `json:"samlMetadataUrl,omitempty"`
	DefaultRedirectURI         string                   `json:"defaultRedirectUri,omitempty"`
	AppID                      string                   `json:"appId"`
	TokenEncryptionKeyID       string                   `json:"tokenEncryptionKeyId,omitempty"`
	ID                         string                   `json:"id"`
	GroupMembershipClaims      string                   `json:"groupMembershipClaims,omitempty"`
	SignInAudience             string                   `json:"signInAudience"`
	UniqueName                 string                   `json:"uniqueName,omitempty"`
	DisplayName                string                   `json:"displayName"`
	Description                string                   `json:"description,omitempty"`
	API                        APIApplication           `json:"api"`
	Certification              json.RawMessage          `json:"certification,omitempty"`
	VerifiedPublisher          json.RawMessage          `json:"verifiedPublisher,omitempty"`
	SPA                        RedirectFacet            `json:"spa"`
	DisabledByMicrosoftStatus  json.RawMessage          `json:"disabledByMicrosoftStatus,omitempty"`
	AddIns                     json.RawMessage          `json:"addIns,omitempty"`
	ParentalControlSettings    json.RawMessage          `json:"parentalControlSettings,omitempty"`
	Info                       json.RawMessage          `json:"info,omitempty"`
	OptionalClaims             json.RawMessage          `json:"optionalClaims,omitempty"`
	AuthenticationBehaviors    json.RawMessage          `json:"authenticationBehaviors,omitempty"`
	PublicClient               RedirectFacet            `json:"publicClient"`
	IdentifierURIs             []string                 `json:"identifierUris"`
	Tags                       []string                 `json:"tags"`
	AppRoles                   []AppRole                `json:"appRoles"`
	RequiredResourceAccess     []RequiredResourceAccess `json:"requiredResourceAccess"`
	PasswordCredentials        []PasswordCredential     `json:"passwordCredentials"`
	KeyCredentials             []KeyCredential          `json:"keyCredentials"`
	Web                        WebApplication           `json:"web"`
	IsFallbackPublicClient     bool                     `json:"isFallbackPublicClient"`
	OAuth2RequirePostResponse  bool                     `json:"oauth2RequirePostResponse"`
}

// ServicePrincipal is the local instance of an application in the tenant.
type ServicePrincipal struct {
	AddIns                        json.RawMessage      `json:"addIns,omitempty"`
	DisabledByMicrosoftStatus     json.RawMessage      `json:"disabledByMicrosoftStatus,omitempty"`
	ResourceSpecificPermissions   json.RawMessage      `json:"resourceSpecificApplicationPermissions,omitempty"`
	PasswordSSOSettings           json.RawMessage      `json:"passwordSingleSignOnSettings,omitempty"`
	PreferredKeyEndDateTime       json.RawMessage      `json:"preferredTokenSigningKeyEndDateTime,omitempty"`
	CustomSecurityAttributes      json.RawMessage      `json:"customSecurityAttributes,omitempty"`
	SAMLMetadataURL               string               `json:"samlMetadataUrl,omitempty"`
	ApplicationTemplateID         string               `json:"applicationTemplateId,omitempty"`
	CreatedDateTime               time.Time            `json:"createdDateTime"`
	Info                          json.RawMessage      `json:"info,omitempty"`
	VerifiedPublisher             json.RawMessage      `json:"verifiedPublisher,omitempty"`
	SAMLSingleSignOnSettings      json.RawMessage      `json:"samlSingleSignOnSettings,omitempty"`
	ID                            string               `json:"id"`
	AppID                         string               `json:"appId"`
	DisplayName                   string               `json:"displayName"`
	AppDisplayName                string               `json:"appDisplayName"`
	AppDescription                string               `json:"appDescription,omitempty"`
	AppOwnerOrganizationID        string               `json:"appOwnerOrganizationId,omitempty"`
	Description                   string               `json:"description,omitempty"`
	LoginURL                      string               `json:"loginUrl,omitempty"`
	LogoutURL                     string               `json:"logoutUrl,omitempty"`
	Homepage                      string               `json:"homepage,omitempty"`
	Notes                         string               `json:"notes,omitempty"`
	PreferredSingleSignOnMode     string               `json:"preferredSingleSignOnMode,omitempty"`
	PreferredTokenSigningKeyThumb string               `json:"preferredTokenSigningKeyThumbprint,omitempty"`
	ServicePrincipalType          string               `json:"servicePrincipalType"`
	SignInAudience                string               `json:"signInAudience"`
	TokenEncryptionKeyID          string               `json:"tokenEncryptionKeyId,omitempty"`
	AlternativeNames              []string             `json:"alternativeNames"`
	NotificationEmailAddresses    []string             `json:"notificationEmailAddresses"`
	ReplyURLs                     []string             `json:"replyUrls"`
	ServicePrincipalNames         []string             `json:"servicePrincipalNames"`
	Tags                          []string             `json:"tags"`
	AppRoles                      []AppRole            `json:"appRoles"`
	OAuth2PermissionScopes        []PermissionScope    `json:"oauth2PermissionScopes"`
	PasswordCredentials           []PasswordCredential `json:"passwordCredentials"`
	KeyCredentials                []KeyCredential      `json:"keyCredentials"`
	AccountEnabled                bool                 `json:"accountEnabled"`
	AppRoleAssignmentRequired     bool                 `json:"appRoleAssignmentRequired"`
}

// PasswordProfile is accepted on write and never returned.
type PasswordProfile struct {
	Password                      string `json:"password"`
	ForceChangePasswordNextSignIn bool   `json:"forceChangePasswordNextSignIn"`
	ForceChangePasswordNextMFA    bool   `json:"forceChangePasswordNextSignInWithMfa"`
}

// User is a directory user.
type User struct {
	CreatedDateTime   time.Time        `json:"createdDateTime"`
	PasswordProfile   *PasswordProfile `json:"passwordProfile,omitempty"`
	ID                string           `json:"id"`
	UserPrincipalName string           `json:"userPrincipalName"`
	DisplayName       string           `json:"displayName"`
	MailNickname      string           `json:"mailNickname"`
	GivenName         string           `json:"givenName,omitempty"`
	Surname           string           `json:"surname,omitempty"`
	Mail              string           `json:"mail,omitempty"`
	JobTitle          string           `json:"jobTitle,omitempty"`
	Department        string           `json:"department,omitempty"`
	CompanyName       string           `json:"companyName,omitempty"`
	OfficeLocation    string           `json:"officeLocation,omitempty"`
	City              string           `json:"city,omitempty"`
	State             string           `json:"state,omitempty"`
	Country           string           `json:"country,omitempty"`
	StreetAddress     string           `json:"streetAddress,omitempty"`
	PostalCode        string           `json:"postalCode,omitempty"`
	MobilePhone       string           `json:"mobilePhone,omitempty"`
	PreferredLanguage string           `json:"preferredLanguage,omitempty"`
	UsageLocation     string           `json:"usageLocation,omitempty"`
	EmployeeID        string           `json:"employeeId,omitempty"`
	PasswordPolicies  string           `json:"passwordPolicies,omitempty"`
	OnPremImmutableID string           `json:"onPremisesImmutableId,omitempty"`
	UserType          string           `json:"userType"`
	BusinessPhones    []string         `json:"businessPhones"`
	OtherMails        []string         `json:"otherMails"`
	AccountEnabled    bool             `json:"accountEnabled"`
}

// Group is a security, Microsoft 365 or distribution group.
type Group struct {
	CreatedDateTime               time.Time `json:"createdDateTime"`
	IsAssignableToRole            *bool     `json:"isAssignableToRole"`
	MembershipRule                string    `json:"membershipRule,omitempty"`
	MembershipRuleProcessingState string    `json:"membershipRuleProcessingState,omitempty"`
	MailNickname                  string    `json:"mailNickname"`
	Mail                          string    `json:"mail,omitempty"`
	Visibility                    string    `json:"visibility,omitempty"`
	Classification                string    `json:"classification,omitempty"`
	DisplayName                   string    `json:"displayName"`
	Description                   string    `json:"description,omitempty"`
	PreferredDataLocation         string    `json:"preferredDataLocation,omitempty"`
	ID                            string    `json:"id"`
	ResourceBehaviorOptions       []string  `json:"resourceBehaviorOptions"`
	ResourceProvisioningOptions   []string  `json:"resourceProvisioningOptions"`
	GroupTypes                    []string  `json:"groupTypes"`
	MailEnabled                   bool      `json:"mailEnabled"`
	SecurityEnabled               bool      `json:"securityEnabled"`
}

// AppRoleAssignment grants an app role of a resource service principal to a principal.
type AppRoleAssignment struct {
	CreatedDateTime      time.Time `json:"createdDateTime"`
	ID                   string    `json:"id"`
	AppRoleID            string    `json:"appRoleId"`
	PrincipalID          string    `json:"principalId"`
	PrincipalType        string    `json:"principalType"`
	PrincipalDisplayName string    `json:"principalDisplayName"`
	ResourceID           string    `json:"resourceId"`
	ResourceDisplayName  string    `json:"resourceDisplayName"`
}

// OAuth2PermissionGrant is a delegated permission grant.
type OAuth2PermissionGrant struct {
	ID          string `json:"id"`
	ClientID    string `json:"clientId"`
	ConsentType string `json:"consentType"`
	PrincipalID string `json:"principalId,omitempty"`
	ResourceID  string `json:"resourceId"`
	Scope       string `json:"scope"`
}
