package workmail

import (
	"context"
	"time"
)

// StorageBackend is the interface for WorkMail storage operations.
type StorageBackend interface {
	// Organizations
	CreateOrganization(ctx context.Context, alias string, domains []string) (*Organization, error)
	DescribeOrganization(orgID string) (*Organization, error)
	DeleteOrganization(orgID string, deleteDirectory bool) error
	ListOrganizations(ctx context.Context, maxResults int32, nextToken string) ([]*OrgSummary, string, error)

	// Users
	CreateUser(orgID, name, displayName, password string, role string) (*User, error)
	DescribeUser(orgID, entityID string) (*User, error)
	UpdateUser(orgID, entityID, displayName, firstName, lastName string) error
	DeleteUser(orgID, entityID string) error
	ListUsers(orgID string, maxResults int32, nextToken string) ([]*UserSummary, string, error)
	RegisterToWorkMail(orgID, entityID, email string) error
	DeregisterFromWorkMail(orgID, entityID string) error
	ResetPassword(orgID, userID, password string) error
	GetMailboxDetails(orgID, userID string) (*MailboxDetails, error)
	UpdateMailboxQuota(orgID, userID string, quota int32) error
	UpdatePrimaryEmailAddress(orgID, entityID, email string) error

	// Groups
	CreateGroup(orgID, name string, hidden bool) (*Group, error)
	DescribeGroup(orgID, entityID string) (*Group, error)
	UpdateGroup(orgID, entityID string, hidden bool) error
	DeleteGroup(orgID, entityID string) error
	ListGroups(orgID string, maxResults int32, nextToken string) ([]*GroupSummary, string, error)
	AssociateMemberToGroup(orgID, groupID, memberID string) error
	DisassociateMemberFromGroup(orgID, groupID, memberID string) error
	ListGroupMembers(orgID, groupID string, maxResults int32, nextToken string) ([]*Member, string, error)
	ListGroupsForEntity(orgID, entityID string, maxResults int32, nextToken string) ([]*GroupSummary, string, error)

	// Resources
	CreateResource(orgID, name, resourceType, description string) (*Resource, error)
	DescribeResource(orgID, entityID string) (*Resource, error)
	UpdateResource(orgID, entityID, name, description string) error
	DeleteResource(orgID, entityID string) error
	ListResources(orgID string, maxResults int32, nextToken string) ([]*ResourceSummary, string, error)
	AssociateDelegateToResource(orgID, resourceID, entityID string) error
	DisassociateDelegateFromResource(orgID, resourceID, entityID string) error
	ListResourceDelegates(orgID, resourceID string, maxResults int32, nextToken string) ([]*Delegate, string, error)

	// Aliases
	CreateAlias(orgID, entityID, alias string) error
	DeleteAlias(orgID, entityID, alias string) error
	ListAliases(orgID, entityID string, maxResults int32, nextToken string) ([]string, string, error)

	// Mailbox permissions
	PutMailboxPermissions(orgID, entityID, granteeID string, perms []string) error
	DeleteMailboxPermissions(orgID, entityID, granteeID string) error
	ListMailboxPermissions(orgID, entityID string, maxResults int32, nextToken string) ([]*Permission, string, error)

	// Mail domains
	RegisterMailDomain(orgID, domainName string) error
	DeregisterMailDomain(orgID, domainName string) error
	GetMailDomain(orgID, domainName string) (*MailDomain, error)
	ListMailDomains(orgID string, maxResults int32, nextToken string) ([]*MailDomainSummary, string, error)
	UpdateDefaultMailDomain(orgID, domainName string) error

	// Access control rules
	PutAccessControlRule(
		orgID, name, effect, description string,
		ipRanges, notIPRanges []string,
		actions, notActions []string,
		userIDs, notUserIDs []string,
	) (*AccessControlRule, error)
	DeleteAccessControlRule(orgID, name string) error
	GetAccessControlEffect(orgID, ipAddr, action, userID string) (string, []string, error)
	ListAccessControlRules(orgID string) ([]*AccessControlRule, error)

	// Impersonation roles
	CreateImpersonationRole(
		orgID, name, roleType, description string,
		rules []ImpersonationRule,
	) (*ImpersonationRole, error)
	GetImpersonationRole(orgID, roleID string) (*ImpersonationRole, error)
	UpdateImpersonationRole(orgID, roleID, name, roleType, description string, rules []ImpersonationRule) error
	DeleteImpersonationRole(orgID, roleID string) error
	ListImpersonationRoles(orgID string, maxResults int32, nextToken string) ([]*ImpersonationRole, string, error)

	// Tags
	TagResource(resourceARN string, tags []Tag) error
	UntagResource(resourceARN string, tagKeys []string) error
	ListTagsForResource(resourceARN string) ([]Tag, error)

	// Describe entity (user, group, or resource by email or ID)
	DescribeEntity(orgID, email string) (*EntityDescription, error)

	// Availability configurations
	CreateAvailabilityConfiguration(
		orgID, domainName string,
		ewsProvider *AvailabilityEwsProvider,
		lambdaARN string,
	) (*AvailabilityConfiguration, error)
	DeleteAvailabilityConfiguration(orgID, domainName string) error
	UpdateAvailabilityConfiguration(
		orgID, domainName string,
		ewsProvider *AvailabilityEwsProvider,
		lambdaARN string,
	) error
	ListAvailabilityConfigurations(
		orgID string,
		maxResults int32,
		nextToken string,
	) ([]*AvailabilityConfiguration, string, error)
	TestAvailabilityConfiguration(orgID, domainName string) (bool, string, error)

	// Mobile device access rules
	CreateMobileDeviceAccessRule(orgID, name, effect, description string,
		deviceModels, notDeviceModels, deviceTypes, notDeviceTypes,
		deviceOperatingSystems, notDeviceOperatingSystems, deviceUserAgents, notDeviceUserAgents []string,
	) (*MobileDeviceAccessRule, error)
	DeleteMobileDeviceAccessRule(orgID, ruleID string) error
	UpdateMobileDeviceAccessRule(orgID, ruleID, name, effect, description string,
		deviceModels, notDeviceModels, deviceTypes, notDeviceTypes,
		deviceOperatingSystems, notDeviceOperatingSystems, deviceUserAgents, notDeviceUserAgents []string,
	) error
	ListMobileDeviceAccessRules(orgID string) ([]*MobileDeviceAccessRule, error)
	GetMobileDeviceAccessEffect(
		orgID, deviceType, deviceModel, deviceOS, deviceUserAgent string,
	) (string, []*MobileDeviceMatchedRule, error)

	// Mobile device access overrides
	PutMobileDeviceAccessOverride(orgID, userID, deviceID, effect, description string) error
	DeleteMobileDeviceAccessOverride(orgID, userID, deviceID string) error
	GetMobileDeviceAccessOverride(orgID, userID, deviceID string) (*MobileDeviceAccessOverride, error)
	ListMobileDeviceAccessOverrides(
		orgID, userID, deviceID string,
		maxResults int32,
		nextToken string,
	) ([]*MobileDeviceAccessOverride, string, error)

	// Email monitoring configuration
	PutEmailMonitoringConfiguration(orgID, roleARN, logGroupARN string) error
	DeleteEmailMonitoringConfiguration(orgID string) error
	DescribeEmailMonitoringConfiguration(orgID string) (*EmailMonitoringConfiguration, error)

	// Inbound DMARC settings
	PutInboundDmarcSettings(orgID string, enforced bool) error
	DescribeInboundDmarcSettings(orgID string) (bool, error)

	// Retention policies
	PutRetentionPolicy(orgID, id, name, description string, folderConfigurations []*FolderConfiguration) error
	DeleteRetentionPolicy(orgID, id string) error
	GetDefaultRetentionPolicy(orgID string) (*RetentionPolicy, error)

	// Mailbox export jobs
	StartMailboxExportJob(
		orgID, entityID, description, roleARN, kmsKeyARN, s3BucketName, s3Prefix string,
	) (*MailboxExportJob, error)
	CancelMailboxExportJob(orgID, jobID string) error
	DescribeMailboxExportJob(orgID, jobID string) (*MailboxExportJob, error)
	ListMailboxExportJobs(orgID string, maxResults int32, nextToken string) ([]*MailboxExportJob, string, error)

	// Identity center applications
	CreateIdentityCenterApplication(instanceARN, name string) (string, error)
	DeleteIdentityCenterApplication(applicationARN string) error

	// Identity provider configuration
	PutIdentityProviderConfiguration(
		orgID, authMode string,
		identityCenterAppARN, identityCenterInstanceARN, patStatus string,
		patLifetimeDays int32,
	) error
	DeleteIdentityProviderConfiguration(orgID string) error
	DescribeIdentityProviderConfiguration(orgID string) (*IdentityProviderConfiguration, error)

	// Personal access tokens
	DeletePersonalAccessToken(orgID, tokenID string) error
	GetPersonalAccessTokenMetadata(orgID, tokenID string) (*PersonalAccessToken, error)
	ListPersonalAccessTokens(
		orgID, userID string,
		maxResults int32,
		nextToken string,
	) ([]*PersonalAccessToken, string, error)

	// Impersonation role effect
	GetImpersonationRoleEffect(orgID, roleID, targetUser string) (string, string, []*ImpersonationMatchedRule, error)

	// Assume impersonation role
	AssumeImpersonationRole(orgID, roleID string) (string, int64, error)

	AccountID() string
	Region() string
	Reset()

	// Snapshot and Restore implement persistence.Persistable. Handler
	// delegates to them (see persistence.go) so cli.go's generic
	// setupPersistence picks WorkMail up.
	Snapshot(ctx context.Context) []byte
	Restore(ctx context.Context, data []byte) error
}

// Organization represents a WorkMail organization.
type Organization struct {
	CreatedAt         time.Time
	CompletedDate     time.Time
	OrgID             string
	Alias             string
	ARN               string
	State             string
	DirectoryID       string
	DirectoryType     string
	DefaultMailDomain string
	ErrorMessage      string
	Region            string
}

// OrgSummary is a summary of a WorkMail organization.
type OrgSummary struct {
	OrgID             string
	Alias             string
	DefaultMailDomain string
	State             string
	ErrorMessage      string
}

// User represents a WorkMail user.
type User struct {
	CreatedAt    time.Time
	EnabledDate  time.Time
	DisabledDate time.Time
	UserID       string
	Name         string
	Email        string
	DisplayName  string
	FirstName    string
	LastName     string
	Role         string
	State        string
	ARN          string
	// orgID is the store.Table composite-key qualifier (see orgKey in
	// backend.go); never part of the wire API, carried through persistence
	// via orgDTO (see persistence.go).
	orgID string
}

// UserSummary is a summary of a WorkMail user.
type UserSummary struct {
	UserID      string
	Name        string
	Email       string
	DisplayName string
	State       string
	Role        string
}

// Group represents a WorkMail group.
type Group struct {
	CreatedAt    time.Time
	EnabledDate  time.Time
	DisabledDate time.Time
	GroupID      string
	Name         string
	Email        string
	State        string
	ARN          string
	// orgID is the store.Table composite-key qualifier (see orgKey in
	// backend.go); carried through persistence via orgDTO.
	orgID  string
	Hidden bool
}

// GroupSummary is a summary of a WorkMail group.
type GroupSummary struct {
	GroupID string
	Name    string
	Email   string
	State   string
}

// Member represents a group member.
type Member struct {
	EnabledDate  time.Time
	DisabledDate time.Time
	MemberID     string
	Name         string
	State        string
	MemberType   string
}

// Resource represents a WorkMail resource.
type Resource struct {
	CreatedAt    time.Time
	EnabledDate  time.Time
	DisabledDate time.Time
	ResourceID   string
	Name         string
	Email        string
	ResourceType string
	Description  string
	State        string
	ARN          string
	// orgID is the store.Table composite-key qualifier (see orgKey in
	// backend.go); carried through persistence via orgDTO.
	orgID string
}

// ResourceSummary is a summary of a WorkMail resource.
type ResourceSummary struct {
	ResourceID   string
	Name         string
	Email        string
	ResourceType string
	State        string
	Description  string
}

// Delegate represents a resource delegate.
type Delegate struct {
	DelegateID   string
	DelegateType string
}

// Permission represents a mailbox permission.
type Permission struct {
	GranteeID   string
	GranteeType string
	orgID       string
	entityID    string
	Permissions []string
}

// MailboxDetails holds mailbox quota/storage info.
type MailboxDetails struct {
	MailboxQuota int32
	MailboxSize  float64
}

// MailDomain represents a registered mail domain.
type MailDomain struct {
	DomainName                  string
	OwnershipVerificationStatus string
	MxRecord                    string
	orgID                       string
	Records                     []DNSRecord
	IsDefault                   bool
	IsTestDomain                bool
}

// MailDomainSummary is a summary of a registered mail domain.
type MailDomainSummary struct {
	DomainName   string
	IsDefault    bool
	IsTestDomain bool
}

// DNSRecord is a DNS record for domain verification.
type DNSRecord struct {
	Hostname string
	Type     string
	Value    string
}

// AccessControlRule represents a WorkMail access control rule.
type AccessControlRule struct {
	DateCreated  time.Time
	DateModified time.Time
	Name         string
	Effect       string
	Description  string
	orgID        string
	IPRanges     []string
	NotIPRanges  []string
	Actions      []string
	NotActions   []string
	UserIDs      []string
	NotUserIDs   []string
}

// ImpersonationRole represents a WorkMail impersonation role.
type ImpersonationRole struct {
	DateCreated  time.Time
	DateModified time.Time
	RoleID       string
	Name         string
	RoleType     string
	Description  string
	orgID        string
	Rules        []ImpersonationRule
}

// ImpersonationRule is a rule within an impersonation role.
type ImpersonationRule struct {
	RuleID         string
	Name           string
	Description    string
	Effect         string
	TargetUsers    []string
	NotTargetUsers []string
}

// Tag is a key-value tag.
type Tag struct {
	Key   string
	Value string
}

// EntityDescription describes an entity (user, group, resource).
type EntityDescription struct {
	EntityID string
	Name     string
	Type     string
	State    string
}

// AvailabilityEwsProvider holds EWS availability provider credentials.
type AvailabilityEwsProvider struct {
	EwsEndpoint string
	EwsUsername string
	EwsPassword string
}

// AvailabilityConfiguration holds an availability provider config.
type AvailabilityConfiguration struct {
	DateCreated  time.Time
	DateModified time.Time
	DomainName   string
	ProviderType string
	EwsEndpoint  string
	EwsUsername  string
	LambdaARN    string
	// orgID is the store.Table composite-key qualifier (see orgKey in
	// backend.go); carried through persistence via orgDTO.
	orgID string
}

// MobileDeviceAccessRule holds a mobile device access rule.
type MobileDeviceAccessRule struct {
	DateModified              time.Time
	DateCreated               time.Time
	orgID                     string
	RuleID                    string
	Name                      string
	Effect                    string
	Description               string
	DeviceModels              []string
	DeviceTypes               []string
	NotDeviceTypes            []string
	DeviceOperatingSystems    []string
	NotDeviceOperatingSystems []string
	DeviceUserAgents          []string
	NotDeviceUserAgents       []string
	NotDeviceModels           []string
}

// MobileDeviceMatchedRule is a matched rule summary.
type MobileDeviceMatchedRule struct {
	RuleID string
	Name   string
}

// MobileDeviceAccessOverride holds a per-user per-device override.
type MobileDeviceAccessOverride struct {
	DateCreated  time.Time
	DateModified time.Time
	UserID       string
	DeviceID     string
	Effect       string
	Description  string
	// orgID is the store.Table composite-key qualifier (see orgKey in
	// backend.go); carried through persistence via orgDTO.
	orgID string
}

// EmailMonitoringConfiguration holds email monitoring config.
type EmailMonitoringConfiguration struct {
	RoleARN     string
	LogGroupARN string
	// orgID is the store.Table primary key for this one-per-org record (see
	// orgOnlyDTO in persistence.go); never part of the wire API.
	orgID string
}

// FolderConfiguration holds a retention policy folder config.
type FolderConfiguration struct {
	Period *int32
	Name   string
	Action string
}

// RetentionPolicy holds a retention policy.
type RetentionPolicy struct {
	ID                   string
	Name                 string
	Description          string
	orgID                string
	FolderConfigurations []*FolderConfiguration
}

// MailboxExportJob holds a mailbox export job.
type MailboxExportJob struct {
	StartTime    time.Time
	EndTime      time.Time
	S3Prefix     string
	RoleARN      string
	KmsKeyARN    string
	S3BucketName string
	JobID        string
	S3Path       string
	State        string
	ErrorInfo    string
	Description  string
	EntityID     string
	// orgID is the store.Table composite-key qualifier (see orgKey in
	// backend.go); carried through persistence via orgDTO.
	orgID             string
	EstimatedProgress int32
}

// IdentityProviderConfiguration holds IdP configuration.
type IdentityProviderConfiguration struct {
	PATLifetimeDays           *int32
	AuthMode                  string
	IdentityCenterAppARN      string
	IdentityCenterInstanceARN string
	PATStatus                 string
	// orgID is the store.Table primary key for this one-per-org record (see
	// orgOnlyDTO in persistence.go); never part of the wire API.
	orgID string
}

// PersonalAccessToken holds PAT metadata.
type PersonalAccessToken struct {
	DateCreated  time.Time
	DateLastUsed time.Time
	ExpiresTime  time.Time
	TokenID      string
	UserID       string
	Name         string
	orgID        string
	Scopes       []string
}

// ImpersonationMatchedRule is a matched impersonation rule.
type ImpersonationMatchedRule struct {
	RuleID string
	Name   string
}

var _ StorageBackend = (*InMemoryBackend)(nil)
