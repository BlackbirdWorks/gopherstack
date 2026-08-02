// Package grafana emulates the Amazon Managed Grafana control-plane API
// (aws-sdk-go-v2/service/grafana, protocol restjson1).
package grafana

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// Workspace lifecycle statuses, mirroring types.WorkspaceStatus in
// aws-sdk-go-v2/service/grafana/types/enums.go. Only the subset this
// emulator actually drives (Active/Creating/Updating/Deleting/Upgrading/
// VersionUpdating) is exercised; the *_FAILED and Degraded values are wire-
// accurate constants but this in-memory backend has no fault-injection path
// that produces them today (see PARITY.md "deliberately simplified").
const (
	StatusActive               = "ACTIVE"
	StatusCreating             = "CREATING"
	StatusDeleting             = "DELETING"
	StatusFailed               = "FAILED"
	StatusUpdating             = "UPDATING"
	StatusUpgrading            = "UPGRADING"
	StatusDeletionFailed       = "DELETION_FAILED"
	StatusCreationFailed       = "CREATION_FAILED"
	StatusUpdateFailed         = "UPDATE_FAILED"
	StatusUpgradeFailed        = "UPGRADE_FAILED"
	StatusLicenseRemovalFailed = "LICENSE_REMOVAL_FAILED"
	StatusVersionUpdating      = "VERSION_UPDATING"
	StatusVersionUpdateFailed  = "VERSION_UPDATE_FAILED"
	StatusDegraded             = "DEGRADED"
)

// AuthenticationProviderTypes wire values (types.AuthenticationProviderTypes).
const (
	AuthProviderAWSSSO = "AWS_SSO"
	AuthProviderSAML   = "SAML"
)

// SamlConfigurationStatus wire values (types.SamlConfigurationStatus).
const (
	SamlStatusConfigured    = "CONFIGURED"
	SamlStatusNotConfigured = "NOT_CONFIGURED"
)

// LicenseType wire values (types.LicenseType).
const (
	LicenseEnterprise          = "ENTERPRISE"
	LicenseEnterpriseFreeTrial = "ENTERPRISE_FREE_TRIAL"
)

// PermissionType / AccountAccessType wire values.
const (
	PermissionTypeCustomerManaged = "CUSTOMER_MANAGED"
	PermissionTypeServiceManaged  = "SERVICE_MANAGED"

	AccountAccessTypeCurrentAccount = "CURRENT_ACCOUNT"
	AccountAccessTypeOrganization   = "ORGANIZATION"
)

// Role wire values (types.Role), shared by permissions, API keys, and
// service accounts.
const (
	RoleAdmin  = "ADMIN"
	RoleEditor = "EDITOR"
	RoleViewer = "VIEWER"
)

// UserType wire values (types.UserType).
const (
	UserTypeSSOUser  = "SSO_USER"
	UserTypeSSOGroup = "SSO_GROUP"
)

// UpdateAction wire values (types.UpdateAction).
const (
	UpdateActionAdd    = "ADD"
	UpdateActionRevoke = "REVOKE"
)

// NetworkAccessConfiguration mirrors types.NetworkAccessConfiguration.
// Both fields are real (possibly empty, never nil once set) lists: an empty
// slice on both means the workspace is fully locked (see doc comment on the
// real type), not "no configuration".
type NetworkAccessConfiguration struct {
	PrefixListIDs []string
	VpceIDs       []string
}

// VpcConfiguration mirrors types.VpcConfiguration.
type VpcConfiguration struct {
	SecurityGroupIDs []string
	SubnetIDs        []string
}

// AssertionAttributes mirrors types.AssertionAttributes.
type AssertionAttributes struct {
	Email  string
	Groups string
	Login  string
	Name   string
	Org    string
	Role   string
}

// RoleValues mirrors types.RoleValues.
type RoleValues struct {
	Admin  []string
	Editor []string
}

// SamlConfiguration mirrors types.SamlConfiguration. IdpMetadataURL and
// IdpMetadataXML represent the two members of the types.IdpMetadata union;
// exactly one may be set at a time (validated by UpdateWorkspaceAuthentication
// -- see authentication.go), mirroring "Specifying both will cause an error"
// on the real union's doc comment.
type SamlConfiguration struct {
	AssertionAttributes   *AssertionAttributes
	RoleValues            *RoleValues
	IdpMetadataURL        string
	IdpMetadataXML        string
	AllowedOrganizations  []string
	LoginValidityDuration int32
}

// Workspace is the internal state of one Amazon Managed Grafana workspace,
// a superset of types.WorkspaceDescription plus bookkeeping fields not on
// the wire (ARN, and the SAML/AWS-SSO sub-state that DescribeWorkspace does
// not itself expose but DescribeWorkspaceAuthentication does).
type Workspace struct {
	Created                  time.Time
	Modified                 time.Time
	LicenseExpiration        time.Time
	FreeTrialExpiration      time.Time
	NetworkAccessControl     *NetworkAccessConfiguration
	VpcConfiguration         *VpcConfiguration
	SamlConfig               *SamlConfiguration
	Tags                     *tags.Tags
	ID                       string
	ARN                      string
	Name                     string
	Description              string
	AccountAccessType        string
	PermissionType           string
	Status                   string
	DegradedWorkspaceReason  string
	Endpoint                 string
	GrafanaVersion           string
	Configuration            string
	OrganizationRoleName     string
	StackSetName             string
	KmsKeyID                 string
	IPAddressType            string
	WorkspaceRoleARN         string
	LicenseType              string
	GrafanaToken             string
	SsoClientID              string
	SamlConfigurationStatus  string
	AuthenticationProviders  []string
	DataSources              []string
	NotificationDestinations []string
	OrganizationalUnits      []string
	FreeTrialConsumed        bool
}

// APIKey mirrors the legacy CreateWorkspaceApiKey mechanism. Keyed by
// WorkspaceID + KeyName. The plaintext Key is generated once at creation and
// stored so it can be validated/echoed; the real API never lets a caller
// retrieve it again either (there is no GetWorkspaceApiKey operation).
type APIKey struct {
	CreatedAt   time.Time
	ExpiresAt   time.Time
	WorkspaceID string
	KeyName     string
	KeyRole     string
	Key         string
}

// ServiceAccount mirrors types.ServiceAccountSummary plus the WorkspaceID
// needed to key/index it.
type ServiceAccount struct {
	WorkspaceID string
	ID          string
	Name        string
	GrafanaRole string
	IsDisabled  bool
}

// ServiceAccountToken mirrors types.ServiceAccountTokenSummary plus the
// plaintext Key (only ever surfaced once, by CreateWorkspaceServiceAccountToken's
// ServiceAccountTokenSummaryWithKey response shape -- see service_account_tokens.go).
type ServiceAccountToken struct {
	CreatedAt        time.Time
	ExpiresAt        time.Time
	LastUsedAt       time.Time
	WorkspaceID      string
	ServiceAccountID string
	ID               string
	Name             string
	Key              string
}

// Permission is one (workspace, user) -> role grant, mirroring
// types.PermissionEntry (Role + User{Id,Type}) plus the WorkspaceID needed
// to key/index it.
type Permission struct {
	WorkspaceID string
	UserID      string
	UserType    string
	Role        string
}
