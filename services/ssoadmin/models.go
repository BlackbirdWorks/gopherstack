package ssoadmin

import (
	"encoding/json"
	"regexp"
	"time"
)

const (
	statusSucceeded   = "SUCCEEDED"
	statusInProgress  = "IN_PROGRESS"
	statusFailed      = "FAILED"
	appProviderCustom = "arn:aws:sso::aws:applicationProvider/custom"
)

const (
	identityStoreIDPrefixLen = 8
	identityStoreIDMaxLen    = 12
	uuidShortLen             = 8

	// Instance/application status constants.
	instanceStatusActive           = "ACTIVE"
	instanceStatusCreateInProgress = "CREATE_IN_PROGRESS"
	instanceStatusDeleteInProgress = "DELETE_IN_PROGRESS"
	appStatusEnabled               = "ENABLED"
	appStatusDisabled              = "DISABLED"

	// ABAC configuration status.
	abacStatusEnabled            = "ENABLED"
	abacStatusCreationInProgress = "CREATION_IN_PROGRESS"
	abacStatusCreationFailed     = "CREATION_FAILED"

	// Default session duration for new permission sets.
	defaultSessionDuration = "PT1H"

	// Session duration limits in minutes.
	minutesPerHour    = 60
	minSessionMinutes = 60  // 1 hour
	maxSessionMinutes = 720 // 12 hours

	// AWS tag limits.
	maxTagsPerResource = 50
	maxTagKeyLen       = 128
	maxTagValueLen     = 256

	// AWS permission set name length limit.
	maxPermissionSetNameLen = 32

	// maxStatusEntries caps each status map to prevent unbounded growth.
	maxStatusEntries = 1000

	// CustomerManagedPolicyReference limits.
	maxCMPRNameLen = 128
	maxCMPRPathLen = 512

	// AccessControlAttribute limits per AWS spec.
	maxACAAttributes    = 50
	maxACAKeyLen        = 128
	maxACASourceItems   = 10
	maxACASourceItemLen = 256

	// TrustedTokenIssuer type — only OIDC_JWT is supported by AWS.
	ttiTypeOIDCJWT = "OIDC_JWT"

	// ttiDefaultType is kept as an alias for backward compat in seeding code.
	ttiDefaultType = ttiTypeOIDCJWT

	// OIDC JWT configuration.
	jwksRetrievalOpenIDDiscovery = "OPEN_ID_DISCOVERY"
	maxIssuerURLLen              = 512

	// Region status values for multi-region SSO instances.
	regionStatusActive   = "ACTIVE"
	regionStatusAdding   = "ADDING"
	regionStatusRemoving = "REMOVING"

	// Valid principal types for account assignments.
	principalTypeUser  = "USER"
	principalTypeGroup = "GROUP"

	// Valid target types for ProvisionPermissionSet.
	targetTypeAWSAccount             = "AWS_ACCOUNT"
	targetTypeAllProvisionedAccounts = "ALL_PROVISIONED_ACCOUNTS"

	// Valid values for the ProvisioningStatus filter on
	// ListPermissionSetsProvisionedToAccount/ListAccountsForProvisionedPermissionSet.
	provisioningStatusLatestProvisioned    = "LATEST_PERMISSION_SET_PROVISIONED"
	provisioningStatusLatestNotProvisioned = "LATEST_PERMISSION_SET_NOT_PROVISIONED"

	// Valid authentication method types.
	authMethodTypeIAM = "IAM"

	// Application portal visibility.
	portalVisibilityEnabled  = "ENABLED"
	portalVisibilityDisabled = "DISABLED"

	// UserBackgroundSessionApplicationStatus values for
	// PutApplicationSessionConfiguration/GetApplicationSessionConfiguration.
	userBackgroundSessionStatusEnabled  = "ENABLED"
	userBackgroundSessionStatusDisabled = "DISABLED"

	// Application sign-in origin.
	signInOriginApplication    = "APPLICATION"
	signInOriginIdentityCenter = "IDENTITY_CENTER"

	// Federation protocol constants.
	federationProtocolSAML = "SAML"
)

// Compiled validation regexes.
var (
	// permissionSetNameRe matches valid permission set names per AWS spec.
	permissionSetNameRe = regexp.MustCompile(`^[\w+=,.@-]+$`)

	// cmprNameRe matches valid CustomerManagedPolicyReference names per AWS spec.
	cmprNameRe = regexp.MustCompile(`^[\w+=,.@-]+$`)

	// acaKeyRe matches valid AccessControlAttribute keys per AWS spec.
	acaKeyRe = regexp.MustCompile(`^[\p{L}\p{Z}\p{N}_.:/=+\-@]+$`)
)

// Instance represents an AWS SSO instance.
type Instance struct {
	CreatedDate     time.Time         `json:"CreatedDate"`
	Tags            map[string]string `json:"Tags"`
	IdentityStoreID string            `json:"IdentityStoreId"`
	InstanceArn     string            `json:"InstanceArn"`
	Name            string            `json:"Name"`
	OwnerAccountID  string            `json:"OwnerAccountId"`
	Status          string            `json:"Status"`
}

// PermissionSet represents an AWS SSO permission set.
type PermissionSet struct {
	CreatedDate time.Time `json:"CreatedDate"`
	// ModifiedDate is internal bookkeeping only -- not a real AWS wire field
	// (aws-sdk-go-v2's types.PermissionSet has no LastModified concept) --
	// used to derive ListPermissionSetsProvisionedToAccount/
	// ListAccountsForProvisionedPermissionSet's ProvisioningStatus filter:
	// an account is LATEST_PERMISSION_SET_PROVISIONED iff the permission set
	// has not been edited since it was last provisioned to that account.
	ModifiedDate     time.Time         `json:"ModifiedDate"`
	Tags             map[string]string `json:"Tags"`
	PermissionSetArn string            `json:"PermissionSetArn"`
	InstanceArn      string            `json:"InstanceArn"`
	Name             string            `json:"Name"`
	Description      string            `json:"Description"`
	SessionDuration  string            `json:"SessionDuration"`
	RelayState       string            `json:"RelayState"`
	InlinePolicy     string            `json:"InlinePolicy"`
	ManagedPolicies  []ManagedPolicy   `json:"ManagedPolicies"`
}

// ManagedPolicy represents an IAM managed policy attached to a permission set.
type ManagedPolicy struct {
	Arn  string `json:"Arn"`
	Name string `json:"Name"`
}

// AccountAssignment represents an assignment of a permission set to a principal in an account.
type AccountAssignment struct {
	AccountID        string `json:"AccountId"`
	PermissionSetArn string `json:"PermissionSetArn"`
	PrincipalID      string `json:"PrincipalId"`
	PrincipalType    string `json:"PrincipalType"`
}

// ProvisioningStatus represents the status of an async provisioning/assignment request.
type ProvisioningStatus struct {
	CreatedDate      time.Time `json:"CreatedDate"`
	RequestID        string    `json:"RequestId"`
	Status           string    `json:"Status"`
	FailureReason    string    `json:"FailureReason"`
	AccountID        string    `json:"AccountId,omitempty"`
	PermissionSetArn string    `json:"PermissionSetArn,omitempty"`
	TargetType       string    `json:"TargetType,omitempty"`
	PrincipalID      string    `json:"PrincipalId,omitempty"`
	PrincipalType    string    `json:"PrincipalType,omitempty"`
}

// CustomerManagedPolicyReference references a customer-managed policy.
type CustomerManagedPolicyReference struct {
	Name string `json:"Name"`
	Path string `json:"Path"`
}

// SignInOptions holds sign-in configuration for an application's portal.
type SignInOptions struct {
	Origin         string `json:"Origin"`
	ApplicationURL string `json:"ApplicationUrl"`
}

// PortalOptions holds portal configuration for an application.
type PortalOptions struct {
	Visibility    string        `json:"Visibility"`
	SignInOptions SignInOptions `json:"SignInOptions"`
}

// PermissionsBoundary is a union: either ManagedPolicyArn or CustomerManagedPolicyReference.
type PermissionsBoundary struct {
	// PermissionSetArn is the permission set this boundary is keyed by in the
	// permissionBoundaries Table (see store_setup.go). It is tagged json:"-"
	// because permissionBoundaries is a "dirty" table -- persistence.go
	// instead round-trips it through a dedicated permissionsBoundarySnapshot
	// DTO that carries the identity as a real JSON field, so it survives the
	// round trip despite being excluded here. It must never change after the
	// value is stored (store.Table's keyFn purity requirement).
	PermissionSetArn               string                          `json:"-"`
	CustomerManagedPolicyReference *CustomerManagedPolicyReference `json:"CustomerManagedPolicyReference,omitempty"`
	ManagedPolicyArn               string                          `json:"ManagedPolicyArn,omitempty"`
}

// ABACConfig holds ABAC configuration for an SSO instance with lifecycle status.
type ABACConfig struct {
	// InstanceArn is the instance this configuration is keyed by in the
	// instanceACAs Table (see store_setup.go). Tagged json:"-" for the same
	// reason as PermissionsBoundary.PermissionSetArn -- see its doc comment.
	InstanceArn             string                   `json:"-"`
	StatusReason            string                   `json:"StatusReason"`
	Status                  string                   `json:"Status"`
	AccessControlAttributes []AccessControlAttribute `json:"AccessControlAttributes"`
}

// Application represents an AWS SSO application.
type Application struct {
	CreatedDate            time.Time         `json:"CreatedDate"`
	Tags                   map[string]string `json:"Tags"`
	PortalOptions          *PortalOptions    `json:"PortalOptions,omitempty"`
	ApplicationArn         string            `json:"ApplicationArn"`
	ApplicationProviderArn string            `json:"ApplicationProviderArn"`
	Description            string            `json:"Description"`
	InstanceArn            string            `json:"InstanceArn"`
	Name                   string            `json:"Name"`
	Status                 string            `json:"Status"`
	// ApplicationAccount, CreatedFrom, and IdentityStoreArn mirror the real
	// ssoadmin.types.Application wire shape (also present flat on
	// CreateApplicationOutput/DescribeApplicationOutput -- see
	// handler_applications.go). Not part of the CreateApplication request;
	// derived from the owning instance at creation time.
	ApplicationAccount string `json:"ApplicationAccount"`
	CreatedFrom        string `json:"CreatedFrom"`
	IdentityStoreArn   string `json:"IdentityStoreArn"`
}

// ApplicationAssignment represents a principal assigned to an application.
type ApplicationAssignment struct {
	ApplicationArn string `json:"ApplicationArn"`
	PrincipalID    string `json:"PrincipalId"`
	PrincipalType  string `json:"PrincipalType"`
}

// AccessControlAttributeValue holds the source list for an attribute.
type AccessControlAttributeValue struct {
	Source []string `json:"Source"`
}

// AccessControlAttribute represents a single ABAC attribute.
type AccessControlAttribute struct {
	Key   string                      `json:"Key"`
	Value AccessControlAttributeValue `json:"Value"`
}

// ApplicationProviderDisplayData contains display metadata for an application provider.
type ApplicationProviderDisplayData struct {
	Description string `json:"Description,omitempty"`
	DisplayName string `json:"DisplayName,omitempty"`
	IconURL     string `json:"IconUrl,omitempty"`
}

// ApplicationProvider represents an SSO application provider.
type ApplicationProvider struct {
	ApplicationProviderArn string                         `json:"ApplicationProviderArn"`
	DisplayData            ApplicationProviderDisplayData `json:"DisplayData"`
	FederationProtocol     string                         `json:"FederationProtocol,omitempty"`
}

// InstanceAccessControlAttributeConfiguration holds ABAC configuration for an instance.
type InstanceAccessControlAttributeConfiguration struct {
	AccessControlAttributes []AccessControlAttribute `json:"AccessControlAttributes"`
}

// RegionMetadata represents a region associated with an SSO instance. Field
// names and JSON tags mirror the real ssoadmin.RegionMetadata wire shape
// (AddedDate, IsPrimaryRegion, RegionName, Status) -- see
// aws-sdk-go-v2/service/ssoadmin/types.RegionMetadata.
type RegionMetadata struct {
	AddedDate       time.Time `json:"AddedDate"`
	RegionName      string    `json:"RegionName"`
	Status          string    `json:"Status"`
	IsPrimaryRegion bool      `json:"IsPrimaryRegion"`
}

// OidcJwtConfiguration holds OIDC JWT trusted token issuer configuration.
type OidcJwtConfiguration struct {
	IssuerURL                  string `json:"IssuerUrl"`
	ClaimAttributePath         string `json:"ClaimAttributePath"`
	IdentityStoreAttributePath string `json:"IdentityStoreAttributePath"`
	JwksRetrievalOption        string `json:"JwksRetrievalOption"`
}

// TrustedTokenIssuerConfiguration holds the issuer-type-specific configuration.
type TrustedTokenIssuerConfiguration struct {
	OidcJwtConfiguration *OidcJwtConfiguration `json:"OidcJwtConfiguration,omitempty"`
}

// TrustedTokenIssuer represents a trusted token issuer.
type TrustedTokenIssuer struct {
	Tags                            map[string]string                `json:"Tags"`
	TrustedTokenIssuerConfiguration *TrustedTokenIssuerConfiguration `json:"TrustedTokenIssuerConfiguration,omitempty"`
	TrustedTokenIssuerArn           string                           `json:"TrustedTokenIssuerArn"`
	InstanceArn                     string                           `json:"InstanceArn"`
	Name                            string                           `json:"Name"`
	TrustedTokenIssuerType          string                           `json:"TrustedTokenIssuerType"`
}

// ScopeDetails describes an access scope and the ARNs it authorizes, matching
// the real ssoadmin.types.ScopeDetails wire shape returned by
// ListApplicationAccessScopes.
type ScopeDetails struct {
	Scope             string   `json:"Scope"`
	AuthorizedTargets []string `json:"AuthorizedTargets"`
}

// AuthMethod holds a typed authentication method with its full structured body.
type AuthMethod struct {
	AuthMethodType string          `json:"AuthenticationMethodType"`
	Body           json.RawMessage `json:"AuthenticationMethod"`
}

// ApplicationGrant holds a typed grant with its structured body.
type ApplicationGrant struct {
	GrantType string          `json:"GrantType"`
	Grant     json.RawMessage `json:"Grant"`
}
