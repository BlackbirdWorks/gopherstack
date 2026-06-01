package workmail

import "time"

// StorageBackend is the interface for WorkMail storage operations.
type StorageBackend interface {
	// Organizations
	CreateOrganization(alias string, domains []string) (*Organization, error)
	DescribeOrganization(orgID string) (*Organization, error)
	DeleteOrganization(orgID string, deleteDirectory bool) error
	ListOrganizations(maxResults int32, nextToken string) ([]*OrgSummary, string, error)

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

	AccountID() string
	Region() string
	Reset()
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
}

// UserSummary is a summary of a WorkMail user.
type UserSummary struct {
	UserID      string
	Name        string
	Email       string
	DisplayName string
	State       string
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
	Hidden       bool
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

var _ StorageBackend = (*InMemoryBackend)(nil)
