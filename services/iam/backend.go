package iam

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

var (
	// ErrUserNotFound is returned when a requested user does not exist.
	ErrUserNotFound = errors.New("NoSuchEntity")
	// ErrUserAlreadyExists is returned when creating a user that already exists.
	ErrUserAlreadyExists = errors.New("EntityAlreadyExists")
	// ErrRoleNotFound is returned when a requested role does not exist.
	ErrRoleNotFound = errors.New("NoSuchEntity")
	// ErrRoleAlreadyExists is returned when creating a role that already exists.
	ErrRoleAlreadyExists = errors.New("EntityAlreadyExists")
	// ErrPolicyNotFound is returned when a requested policy does not exist.
	ErrPolicyNotFound = errors.New("NoSuchEntity")
	// ErrPolicyAlreadyExists is returned when creating a policy that already exists.
	ErrPolicyAlreadyExists = errors.New("EntityAlreadyExists")
	// ErrGroupNotFound is returned when a requested group does not exist.
	ErrGroupNotFound = errors.New("NoSuchEntity")
	// ErrGroupAlreadyExists is returned when creating a group that already exists.
	ErrGroupAlreadyExists = errors.New("EntityAlreadyExists")
	// ErrAccessKeyNotFound is returned when a requested access key does not exist.
	ErrAccessKeyNotFound = errors.New("NoSuchEntity")
	// ErrInstanceProfileNotFound is returned when a requested instance profile does not exist.
	ErrInstanceProfileNotFound = errors.New("NoSuchEntity")
	// ErrInstanceProfileAlreadyExists is returned when creating a profile that already exists.
	ErrInstanceProfileAlreadyExists = errors.New("EntityAlreadyExists")
	// ErrInvalidAction is returned when an unknown IAM action is requested.
	ErrInvalidAction = errors.New("InvalidAction")
	// ErrMalformedPolicyDocument is returned when a policy document is not valid JSON.
	ErrMalformedPolicyDocument = errors.New("MalformedPolicyDocument")
	// ErrDeleteConflict is returned when an entity has attached resources that prevent deletion.
	ErrDeleteConflict = errors.New("DeleteConflict")
	// ErrInlinePolicyNotFound is returned when a requested inline policy does not exist.
	ErrInlinePolicyNotFound = errors.New("NoSuchEntity")
	// ErrSAMLProviderNotFound is returned when a requested SAML provider does not exist.
	ErrSAMLProviderNotFound = errors.New("NoSuchEntity")
	// ErrSAMLProviderAlreadyExists is returned when creating a SAML provider that already exists.
	ErrSAMLProviderAlreadyExists = errors.New("EntityAlreadyExists")
	// ErrOIDCProviderNotFound is returned when a requested OIDC provider does not exist.
	ErrOIDCProviderNotFound = errors.New("NoSuchEntity")
	// ErrOIDCProviderAlreadyExists is returned when creating an OIDC provider that already exists.
	ErrOIDCProviderAlreadyExists = errors.New("EntityAlreadyExists")
	// ErrLoginProfileNotFound is returned when a requested login profile does not exist.
	ErrLoginProfileNotFound = errors.New("NoSuchEntity")
	// ErrLoginProfileAlreadyExists is returned when creating a login profile that already exists.
	ErrLoginProfileAlreadyExists = errors.New("EntityAlreadyExists")
	// ErrInvalidOIDCProviderURL is returned when an OIDC provider URL cannot be parsed.
	ErrInvalidOIDCProviderURL = errors.New("InvalidInput")
	// ErrInvalidPassword is returned when a password fails validation (e.g., empty).
	ErrInvalidPassword = errors.New("InvalidInput")
	// ErrLimitExceeded is returned when an inline policy or other entity exceeds an AWS quota.
	ErrLimitExceeded = errors.New("LimitExceeded")
)

// AWS IAM inline policy size limits (UTF-8 bytes, including whitespace) per
// https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_iam-quotas.html.
const (
	maxUserPolicySize  = 2048
	maxRolePolicySize  = 10240
	maxGroupPolicySize = 5120
)

// StorageBackend defines the interface for the IAM in-memory store.
type StorageBackend interface {
	// Users
	CreateUser(userName, path, permissionsBoundary string) (*User, error)
	DeleteUser(userName string) error
	ListUsers(marker string, maxItems int) (page.Page[User], error)
	GetUser(userName string) (*User, error)

	// Roles
	CreateRole(roleName, path, assumeRolePolicyDocument, permissionsBoundary string) (*Role, error)
	DeleteRole(roleName string) error
	ListRoles(marker string, maxItems int) (page.Page[Role], error)
	GetRole(roleName string) (*Role, error)
	GetRoleByArn(roleArn string) (*Role, error)
	UpdateRoleMaxSessionDuration(roleName string, maxSessionDuration int32) error

	// Policies
	CreatePolicy(policyName, path, policyDocument string) (*Policy, error)
	DeletePolicy(policyArn string) error
	ListPolicies(marker string, maxItems int) (page.Page[Policy], error)
	AttachUserPolicy(userName, policyArn string) error
	DetachUserPolicy(userName, policyArn string) error
	AttachRolePolicy(roleName, policyArn string) error
	DetachRolePolicy(roleName, policyArn string) error
	ListAttachedUserPolicies(userName string) ([]AttachedPolicy, error)
	ListAttachedRolePolicies(roleName string) ([]AttachedPolicy, error)
	GetPolicy(policyArn string) (*Policy, error)
	ListPolicyVersions(policyArn string) ([]StoredPolicyVersion, error)
	GetPolicyVersion(policyArn, versionID string) (*Policy, error)

	// Inline Policies - Users
	PutUserPolicy(userName, policyName, policyDocument string) error
	GetUserPolicy(userName, policyName string) (string, error)
	DeleteUserPolicy(userName, policyName string) error
	ListUserPolicies(userName string) ([]string, error)

	// Inline Policies - Roles
	PutRolePolicy(roleName, policyName, policyDocument string) error
	GetRolePolicy(roleName, policyName string) (string, error)
	DeleteRolePolicy(roleName, policyName string) error
	ListRolePolicies(roleName string) ([]string, error)

	// Inline Policies - Groups
	PutGroupPolicy(groupName, policyName, policyDocument string) error
	GetGroupPolicy(groupName, policyName string) (string, error)
	DeleteGroupPolicy(groupName, policyName string) error
	ListGroupPolicies(groupName string) ([]string, error)

	// Permission Boundaries
	PutUserPermissionsBoundary(userName, policyArn string) error
	DeleteUserPermissionsBoundary(userName string) error
	PutRolePermissionsBoundary(roleName, policyArn string) error
	DeleteRolePermissionsBoundary(roleName string) error

	// Groups
	CreateGroup(groupName, path string) (*Group, error)
	DeleteGroup(groupName string) error
	GetGroup(groupName string) (*Group, error)
	GetGroupUsers(groupName string) ([]User, error)
	ListGroups(marker string, maxItems int) (page.Page[Group], error)
	AddUserToGroup(groupName, userName string) error
	RemoveUserFromGroup(groupName, userName string) error
	AttachGroupPolicy(groupName, policyArn string) error
	DetachGroupPolicy(groupName, policyArn string) error
	ListAttachedGroupPolicies(groupName string) ([]AttachedPolicy, error)

	// Assume Role Policy
	UpdateAssumeRolePolicy(roleName, policyDocument string) error

	// Reporting and simulation
	GetAccountAuthorizationDetails() AccountAuthorizationDetails
	SimulatePrincipalPolicy(principalArn string, actionNames, resourceArns []string) ([]SimulationResult, error)
	GetCredentialReport() string
	GetAccountSummary() AccountSummary

	// Access Keys
	CreateAccessKey(userName string) (*AccessKey, error)
	DeleteAccessKey(userName, accessKeyID string) error
	ListAccessKeys(userName, marker string, maxItems int) (page.Page[AccessKey], error)

	// Instance Profiles
	CreateInstanceProfile(name, path string) (*InstanceProfile, error)
	DeleteInstanceProfile(name string) error
	ListInstanceProfiles(marker string, maxItems int) (page.Page[InstanceProfile], error)
	AddRoleToInstanceProfile(instanceProfileName, roleName string) error
	RemoveRoleFromInstanceProfile(instanceProfileName, roleName string) error

	// SAML Providers
	CreateSAMLProvider(name, samlMetadataDocument string) (*SAMLProvider, error)
	UpdateSAMLProvider(providerArn, samlMetadataDocument string) (*SAMLProvider, error)
	DeleteSAMLProvider(providerArn string) error
	GetSAMLProvider(providerArn string) (*SAMLProvider, error)
	ListSAMLProviders() ([]SAMLProvider, error)

	// OIDC Providers
	CreateOpenIDConnectProvider(rawURL string, clientIDs, thumbprints []string) (*OIDCProvider, error)
	UpdateOpenIDConnectProviderThumbprint(providerArn string, thumbprints []string) error
	DeleteOpenIDConnectProvider(providerArn string) error
	GetOpenIDConnectProvider(providerArn string) (*OIDCProvider, error)
	ListOpenIDConnectProviders() ([]OIDCProvider, error)

	// Login Profiles
	CreateLoginProfile(userName, password string, passwordResetRequired bool) (*LoginProfile, error)
	UpdateLoginProfile(userName, password string, passwordResetRequired bool) error
	DeleteLoginProfile(userName string) error
	GetLoginProfile(userName string) (*LoginProfile, error)

	// Account Aliases
	CreateAccountAlias(alias string) error
	ListAccountAliases() []string
	DeleteAccountAlias(alias string) error

	// Policy Versions
	CreatePolicyVersion(policyArn, policyDocument string, setAsDefault bool) (*StoredPolicyVersion, error)
	SetDefaultPolicyVersion(policyArn, versionID string) error
	DeletePolicyVersion(policyArn, versionID string) error

	// Service-Linked Roles
	CreateServiceLinkedRole(awsServiceName, description, customSuffix string) (*Role, error)
	GetServiceLinkedRoleDeletionStatus(deletionTaskID string) (string, error)

	// Service-Specific Credentials
	CreateServiceSpecificCredential(userName, serviceName string) (*ServiceSpecificCredential, error)
	ListServiceSpecificCredentials(userName, serviceName string) ([]ServiceSpecificCredential, error)
	DeleteServiceSpecificCredential(userName, credentialID string) error
	UpdateServiceSpecificCredential(userName, credentialID, status string) error

	// Virtual MFA Devices
	CreateVirtualMFADevice(virtualMFADeviceName, path string) (*VirtualMFADevice, error)
	CreateVirtualMFADeviceFull(virtualMFADeviceName, path string) (*VirtualMFADevice, error)
	ListVirtualMFADevices(marker string, maxItems int) (page.Page[VirtualMFADevice], error)
	DeleteVirtualMFADevice(serialNumber string) error
	EnableMFADevice(userName, serialNumber, authCode1, authCode2 string) error
	DeactivateMFADevice(userName, serialNumber string) error
	GetMFADeviceOwner(serialNumber string) string
	ListMFADevicesForUser(userName string) ([]VirtualMFADevice, error)

	// SSH Public Keys
	UploadSSHPublicKey(userName, body string) (*SSHPublicKey, error)
	GetSSHPublicKey(userName, keyID string) (*SSHPublicKey, error)
	ListSSHPublicKeys(userName string, marker string, maxItems int) (page.Page[SSHPublicKey], error)
	UpdateSSHPublicKey(userName, keyID, status string) error
	DeleteSSHPublicKey(userName, keyID string) error

	// Access Advisor
	GenerateServiceLastAccessedDetailsForEntity(entityARN string) string
	GetServiceLastAccessedDetails(jobID string) (status string, details []ServiceLastAccessedDetail, err error)
	RecordServiceAccess(entityARN, serviceNamespace, serviceName string)

	// Organizations Access Report
	GenerateOrganizationsAccessReport(entityPath string) string
	GetOrganizationsAccessReport(jobID string) (status string, createdAt time.Time, found bool)

	// Reset service-specific credential password
	ResetServiceSpecificCredentialFull(userName, credentialID string) (*ServiceSpecificCredential, error)

	// OIDC provider existence check (implements sts.OIDCLookup)
	OIDCProviderExists(issuerURL string) bool

	// Delegation Requests
	CreateDelegationRequest(targetAccountID string) (*DelegationRequest, error)
	AcceptDelegationRequest(delegationID string) error
	AssociateDelegationRequest(delegationID, policyArn string) error

	// Change Password
	ChangePassword(newPassword string) error

	// OIDC Client IDs
	AddClientIDToOpenIDConnectProvider(providerArn, clientID string) error
	RemoveClientIDFromOpenIDConnectProvider(providerArn, clientID string) error

	// Access Key management
	UpdateAccessKey(userName, accessKeyID, status string) error
	GetAccessKeyLastUsed(accessKeyID string) (*AccessKeyLastUsed, error)
	RecordAccessKeyUsage(accessKeyID, region, serviceName string)

	// Tags on resources (embedded in model, returned with resource)
	TagUser(userName string, tags map[string]string) error
	UntagUser(userName string, keys []string) error
	TagRole(roleName string, tags map[string]string) error
	UntagRole(roleName string, keys []string) error
	TagPolicy(policyArn string, tags map[string]string) error
	UntagPolicy(policyArn string, keys []string) error
	TagGroup(groupName string, tags map[string]string) error
	UntagGroup(groupName string, keys []string) error

	// Signing Certificates
	UploadSigningCertificate(userName, body string) (*SigningCertificate, error)
	ListSigningCertificates(userName string) ([]SigningCertificate, error)
	UpdateSigningCertificate(certificateID, status string) error
	DeleteSigningCertificate(certificateID string) error

	// Server Certificates
	UploadServerCertificate(name, path, certBody, certChain string) (*ServerCertificate, error)
	GetServerCertificate(name string) (*ServerCertificate, error)
	ListServerCertificates(pathPrefix string) ([]ServerCertificate, error)
	UpdateServerCertificate(name, newName, newPath string) error
	DeleteServerCertificate(name string) error

	// Group membership queries
	ListGroupsForUser(userName string) ([]Group, error)

	// Account Password Policy
	GetAccountPasswordPolicy() *PasswordPolicy
	UpdateAccountPasswordPolicy(pp PasswordPolicy) error
	DeleteAccountPasswordPolicy() error

	// Policy entity queries
	ListEntitiesForPolicy(policyArn, entityFilter string) (*PolicyEntities, error)

	// Entity mutations
	UpdateUser(userName, newPath, newUserName string) error
	UpdateRole(roleName, description string) error
	UpdateGroup(groupName, newPath, newGroupName string) error

	// Instance profiles extended
	GetInstanceProfile(name string) (*InstanceProfile, error)
	ListInstanceProfilesForRole(roleName string) ([]InstanceProfile, error)

	// Simulation
	SimulateCustomPolicy(policyInputList, actionNames, resourceArns []string) ([]SimulationResult, error)

	// Dashboard helpers
	ListAllUsers() []User
	ListAllRoles() []Role
	ListAllPolicies() []Policy
	ListAllGroups() []Group
	ListAllAccessKeys() []AccessKey
	ListAllInstanceProfiles() []InstanceProfile

	// Enforcement helpers
	GetUserByAccessKeyID(accessKeyID string) (*User, error)
	GetPoliciesForUser(userName string) ([]string, error)

	Purge(ctx context.Context, cutoff time.Time)
}

// iamDefaultMaxItems is the default page size for IAM list operations.
const iamDefaultMaxItems = 100

// accessKeyStatusActive is the "Active" access-key status string.
const accessKeyStatusActive = "Active"

// InMemoryBackend implements StorageBackend using in-memory maps.
type InMemoryBackend struct {
	roles                 map[string]Role
	delegationRequests    map[string]DelegationRequest
	policies              map[string]Policy
	policyByARN           map[string]string
	roleByARN             map[string]string
	accessKeys            map[string]AccessKey
	instanceProfiles      map[string]InstanceProfile
	samlProviders         map[string]SAMLProvider
	groupMembers          map[string][]string
	groupPolicies         map[string][]string
	userPolicies          map[string][]string
	policyAttachments     map[string]policyAttachmentRefs
	mu                    *lockmetrics.RWMutex
	loginProfiles         map[string]LoginProfile
	groups                map[string]Group
	oidcProviders         map[string]OIDCProvider
	userInlinePolicies    map[string]map[string]string
	roleInlinePolicies    map[string]map[string]string
	groupInlinePolicies   map[string]map[string]string
	rolePolicies          map[string][]string
	policyVersions        map[string][]StoredPolicyVersion
	policyVersionCounters map[string]int  // monotonic counter per policy ARN, never resets on delete
	deletedV1Policies     map[string]bool // tracks policies where v1 has been explicitly deleted
	serviceSpecificCreds  map[string]ServiceSpecificCredential
	virtualMFADevices     map[string]VirtualMFADevice
	signingCertificates   map[string]SigningCertificate // certID → SigningCertificate
	serverCertificates    map[string]ServerCertificate  // name → ServerCertificate
	passwordPolicy        *PasswordPolicy
	users                 map[string]User
	comprehensive         *comprehensiveBackend
	accountID             string
	accountAliases        []string
}

type policyAttachmentRefs struct {
	users  map[string]struct{}
	roles  map[string]struct{}
	groups map[string]struct{}
}

// NewInMemoryBackend creates a new empty IAM InMemoryBackend with default account ID.
func NewInMemoryBackend() *InMemoryBackend {
	return NewInMemoryBackendWithConfig(IAMAccountID)
}

// NewInMemoryBackendWithConfig creates a new IAM InMemoryBackend with the given account ID.
func NewInMemoryBackendWithConfig(accountID string) *InMemoryBackend {
	return &InMemoryBackend{
		users:                 make(map[string]User),
		roles:                 make(map[string]Role),
		roleByARN:             make(map[string]string),
		policies:              make(map[string]Policy),
		policyByARN:           make(map[string]string),
		groups:                make(map[string]Group),
		accessKeys:            make(map[string]AccessKey),
		instanceProfiles:      make(map[string]InstanceProfile),
		samlProviders:         make(map[string]SAMLProvider),
		oidcProviders:         make(map[string]OIDCProvider),
		loginProfiles:         make(map[string]LoginProfile),
		userPolicies:          make(map[string][]string),
		rolePolicies:          make(map[string][]string),
		groupPolicies:         make(map[string][]string),
		groupMembers:          make(map[string][]string),
		userInlinePolicies:    make(map[string]map[string]string),
		roleInlinePolicies:    make(map[string]map[string]string),
		groupInlinePolicies:   make(map[string]map[string]string),
		policyAttachments:     make(map[string]policyAttachmentRefs),
		accountAliases:        nil,
		policyVersions:        make(map[string][]StoredPolicyVersion),
		policyVersionCounters: make(map[string]int),
		deletedV1Policies:     make(map[string]bool),
		serviceSpecificCreds:  make(map[string]ServiceSpecificCredential),
		virtualMFADevices:     make(map[string]VirtualMFADevice),
		signingCertificates:   make(map[string]SigningCertificate),
		serverCertificates:    make(map[string]ServerCertificate),
		delegationRequests:    make(map[string]DelegationRequest),
		accountID:             accountID,
		mu:                    lockmetrics.New("iam"),
		comprehensive:         newComprehensiveBackend(),
	}
}

// normPath returns a normalized IAM path, defaulting to "/" if empty.
// Non-root paths are ensured to end with "/" so that ARN construction
// produces the correct "resource/path/name" form.
func normPath(path string) string {
	if path == "" {
		return "/"
	}

	if !strings.HasSuffix(path, "/") {
		return path + "/"
	}

	return path
}

func (b *InMemoryBackend) addPolicyAttachmentLocked(policyArn, entityName, entityType string) {
	refs := b.policyAttachments[policyArn]
	if refs.users == nil {
		refs.users = make(map[string]struct{})
	}

	if refs.roles == nil {
		refs.roles = make(map[string]struct{})
	}

	if refs.groups == nil {
		refs.groups = make(map[string]struct{})
	}

	switch entityType {
	case "user":
		refs.users[entityName] = struct{}{}
	case "role":
		refs.roles[entityName] = struct{}{}
	case "group":
		refs.groups[entityName] = struct{}{}
	}

	b.policyAttachments[policyArn] = refs
}

func (b *InMemoryBackend) removePolicyAttachmentLocked(policyArn, entityName, entityType string) {
	refs, exists := b.policyAttachments[policyArn]
	if !exists {
		return
	}

	switch entityType {
	case "user":
		delete(refs.users, entityName)
	case "role":
		delete(refs.roles, entityName)
	case "group":
		delete(refs.groups, entityName)
	}

	if len(refs.users) == 0 && len(refs.roles) == 0 && len(refs.groups) == 0 {
		delete(b.policyAttachments, policyArn)

		return
	}

	b.policyAttachments[policyArn] = refs
}

// firstKey returns an arbitrary key from a set-like map, or an empty string if the map is empty.
func firstKey(values map[string]struct{}) string {
	for value := range values {
		return value
	}

	return ""
}

func (b *InMemoryBackend) getPolicyByARNLocked(policyArn string) (Policy, bool) {
	policyName, exists := b.policyByARN[policyArn]
	if !exists {
		return Policy{}, false
	}

	pol, exists := b.policies[policyName]
	if !exists {
		return Policy{}, false
	}

	return pol, true
}

func (b *InMemoryBackend) rebuildIndexesLocked() {
	b.roleByARN = make(map[string]string, len(b.roles))
	for roleName, role := range b.roles {
		b.roleByARN[role.Arn] = roleName
	}

	b.policyByARN = make(map[string]string, len(b.policies))
	for policyName, policy := range b.policies {
		b.policyByARN[policy.Arn] = policyName
	}

	b.policyAttachments = make(map[string]policyAttachmentRefs)
	for userName, policyARNs := range b.userPolicies {
		for _, policyARN := range policyARNs {
			b.addPolicyAttachmentLocked(policyARN, userName, "user")
		}
	}

	for roleName, policyARNs := range b.rolePolicies {
		for _, policyARN := range policyARNs {
			b.addPolicyAttachmentLocked(policyARN, roleName, "role")
		}
	}

	for groupName, policyARNs := range b.groupPolicies {
		for _, policyARN := range policyARNs {
			b.addPolicyAttachmentLocked(policyARN, groupName, "group")
		}
	}
}

// ---- Users ----

// CreateUser creates a new IAM user.
func (b *InMemoryBackend) CreateUser(userName, path, permissionsBoundary string) (*User, error) {
	b.mu.Lock("CreateUser")
	defer b.mu.Unlock()

	if _, exists := b.users[userName]; exists {
		return nil, fmt.Errorf("%w: user %q already exists", ErrUserAlreadyExists, userName)
	}

	p := normPath(path)
	u := User{
		UserName:            userName,
		UserID:              newID("AIDA"),
		Arn:                 arn.Build("iam", "", b.accountID, "user"+p+userName),
		Path:                p,
		CreateDate:          time.Now().UTC(),
		PermissionsBoundary: permissionsBoundary,
	}
	b.users[userName] = u

	return &u, nil
}

// DeleteUser deletes an IAM user by name, removing all associated access keys and login profile.
func (b *InMemoryBackend) DeleteUser(userName string) error {
	b.mu.Lock("DeleteUser")
	defer b.mu.Unlock()

	if _, exists := b.users[userName]; !exists {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	if len(b.userPolicies[userName]) > 0 {
		return fmt.Errorf("%w: user %q has attached policies", ErrDeleteConflict, userName)
	}

	if len(b.userInlinePolicies[userName]) > 0 {
		return fmt.Errorf("%w: user %q has inline policies", ErrDeleteConflict, userName)
	}

	// Clean up access keys belonging to the user.
	for id, ak := range b.accessKeys {
		if ak.UserName == userName {
			delete(b.accessKeys, id)
		}
	}

	// Clean up login profile.
	delete(b.loginProfiles, userName)

	// Remove user from all group memberships.
	for groupName, members := range b.groupMembers {
		for i, m := range members {
			if m == userName {
				b.groupMembers[groupName] = append(members[:i], members[i+1:]...)

				break
			}
		}
	}

	delete(b.users, userName)

	return nil
}

// ListUsers returns a paginated list of IAM users sorted by name.
func (b *InMemoryBackend) ListUsers(marker string, maxItems int) (page.Page[User], error) {
	b.mu.RLock("ListUsers")
	defer b.mu.RUnlock()

	return page.New(sortedUsers(b.users), marker, maxItems, iamDefaultMaxItems), nil
}

// GetUser retrieves a single IAM user by name.
func (b *InMemoryBackend) GetUser(userName string) (*User, error) {
	b.mu.RLock("GetUser")
	defer b.mu.RUnlock()

	u, exists := b.users[userName]
	if !exists {
		return nil, fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	return &u, nil
}

// ---- Roles ----

// CreateRole creates a new IAM role.
func (b *InMemoryBackend) CreateRole(
	roleName, path, assumeRolePolicyDocument, permissionsBoundary string,
) (*Role, error) {
	b.mu.Lock("CreateRole")
	defer b.mu.Unlock()

	if _, exists := b.roles[roleName]; exists {
		return nil, fmt.Errorf("%w: role %q already exists", ErrRoleAlreadyExists, roleName)
	}

	if assumeRolePolicyDocument != "" && !json.Valid([]byte(assumeRolePolicyDocument)) {
		return nil, fmt.Errorf("%w: invalid JSON in AssumeRolePolicyDocument", ErrMalformedPolicyDocument)
	}

	if err := validateTrustPolicyPrincipal(assumeRolePolicyDocument); err != nil {
		return nil, err
	}

	p := normPath(path)
	r := Role{
		RoleName:                 roleName,
		RoleID:                   newID("AROA"),
		Arn:                      arn.Build("iam", "", b.accountID, "role"+p+roleName),
		Path:                     p,
		AssumeRolePolicyDocument: assumeRolePolicyDocument,
		CreateDate:               time.Now().UTC(),
		PermissionsBoundary:      permissionsBoundary,
	}
	b.roles[roleName] = r
	b.roleByARN[r.Arn] = roleName

	return &r, nil
}

// DeleteRole deletes an IAM role by name.
func (b *InMemoryBackend) DeleteRole(roleName string) error {
	b.mu.Lock("DeleteRole")
	defer b.mu.Unlock()

	if _, exists := b.roles[roleName]; !exists {
		return fmt.Errorf("%w: role %q not found", ErrRoleNotFound, roleName)
	}

	if len(b.rolePolicies[roleName]) > 0 {
		return fmt.Errorf("%w: role %q has attached policies", ErrDeleteConflict, roleName)
	}

	if len(b.roleInlinePolicies[roleName]) > 0 {
		return fmt.Errorf("%w: role %q has inline policies", ErrDeleteConflict, roleName)
	}

	role := b.roles[roleName]
	delete(b.roles, roleName)
	delete(b.roleByARN, role.Arn)

	return nil
}

// ListRoles returns a paginated list of IAM roles sorted by name.
func (b *InMemoryBackend) ListRoles(marker string, maxItems int) (page.Page[Role], error) {
	b.mu.RLock("ListRoles")
	defer b.mu.RUnlock()

	return page.New(sortedRoles(b.roles), marker, maxItems, iamDefaultMaxItems), nil
}

// GetRole retrieves a single IAM role by name.
func (b *InMemoryBackend) GetRole(roleName string) (*Role, error) {
	b.mu.RLock("GetRole")
	defer b.mu.RUnlock()

	r, exists := b.roles[roleName]
	if !exists {
		return nil, fmt.Errorf("%w: role %q not found", ErrRoleNotFound, roleName)
	}

	return &r, nil
}

// GetRoleByArn retrieves a single IAM role by its full ARN.
func (b *InMemoryBackend) GetRoleByArn(roleArn string) (*Role, error) {
	b.mu.RLock("GetRoleByArn")
	defer b.mu.RUnlock()

	roleName, exists := b.roleByARN[roleArn]
	if !exists {
		return nil, fmt.Errorf("%w: role with ARN %q not found", ErrRoleNotFound, roleArn)
	}

	role, exists := b.roles[roleName]
	if !exists {
		return nil, fmt.Errorf("%w: role with ARN %q not found", ErrRoleNotFound, roleArn)
	}

	return &role, nil
}

// UpdateRoleMaxSessionDuration sets the maximum session duration for a role.
func (b *InMemoryBackend) UpdateRoleMaxSessionDuration(roleName string, maxSessionDuration int32) error {
	b.mu.Lock("UpdateRoleMaxSessionDuration")
	defer b.mu.Unlock()

	r, exists := b.roles[roleName]
	if !exists {
		return fmt.Errorf("%w: role %q not found", ErrRoleNotFound, roleName)
	}

	r.MaxSessionDuration = maxSessionDuration
	b.roles[roleName] = r

	return nil
}

// ---- Policies ----

// AWS IAM managed policy document quota (UTF-8 bytes).
const maxManagedPolicySize = 6144

// CreatePolicy creates a new IAM managed policy.
func (b *InMemoryBackend) CreatePolicy(policyName, path, policyDocument string) (*Policy, error) {
	b.mu.Lock("CreatePolicy")
	defer b.mu.Unlock()

	if _, exists := b.policies[policyName]; exists {
		return nil, fmt.Errorf("%w: policy %q already exists", ErrPolicyAlreadyExists, policyName)
	}

	if policyDocument != "" && !json.Valid([]byte(policyDocument)) {
		return nil, fmt.Errorf("%w: invalid JSON in PolicyDocument", ErrMalformedPolicyDocument)
	}

	if len(policyDocument) > maxManagedPolicySize {
		return nil, fmt.Errorf("%w: managed policy %q exceeds %d bytes",
			ErrLimitExceeded, policyName, maxManagedPolicySize)
	}

	p := normPath(path)
	pol := Policy{
		PolicyName:     policyName,
		PolicyID:       newID("ANPA"),
		Arn:            arn.Build("iam", "", b.accountID, "policy"+p+policyName),
		Path:           p,
		PolicyDocument: policyDocument,
		CreateDate:     time.Now().UTC(),
	}
	b.policies[policyName] = pol
	b.policyByARN[pol.Arn] = policyName

	return &pol, nil
}

// DeletePolicy deletes an IAM policy by ARN.
func (b *InMemoryBackend) DeletePolicy(policyArn string) error {
	b.mu.Lock("DeletePolicy")
	defer b.mu.Unlock()

	if refs, exists := b.policyAttachments[policyArn]; exists {
		if userName := firstKey(refs.users); userName != "" {
			return fmt.Errorf("%w: policy %q is attached to user %q", ErrDeleteConflict, policyArn, userName)
		}

		if roleName := firstKey(refs.roles); roleName != "" {
			return fmt.Errorf("%w: policy %q is attached to role %q", ErrDeleteConflict, policyArn, roleName)
		}

		if groupName := firstKey(refs.groups); groupName != "" {
			return fmt.Errorf("%w: policy %q is attached to group %q", ErrDeleteConflict, policyArn, groupName)
		}
	}

	policyName, exists := b.policyByARN[policyArn]
	if !exists {
		return fmt.Errorf("%w: policy %q not found", ErrPolicyNotFound, policyArn)
	}

	delete(b.policies, policyName)
	delete(b.policyByARN, policyArn)
	delete(b.policyAttachments, policyArn)
	delete(b.policyVersions, policyArn)

	return nil
}

// ListPolicies returns a paginated list of IAM policies sorted by name.
func (b *InMemoryBackend) ListPolicies(marker string, maxItems int) (page.Page[Policy], error) {
	b.mu.RLock("ListPolicies")
	defer b.mu.RUnlock()

	return page.New(sortedPolicies(b.policies), marker, maxItems, iamDefaultMaxItems), nil
}

// AttachUserPolicy attaches a policy to a user.
func (b *InMemoryBackend) AttachUserPolicy(userName, policyArn string) error {
	b.mu.Lock("AttachUserPolicy")
	defer b.mu.Unlock()

	if _, exists := b.users[userName]; !exists {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	if slices.Contains(b.userPolicies[userName], policyArn) {
		return nil // already attached
	}

	b.userPolicies[userName] = append(b.userPolicies[userName], policyArn)
	b.addPolicyAttachmentLocked(policyArn, userName, "user")

	return nil
}

// DetachUserPolicy detaches a policy from a user.
func (b *InMemoryBackend) DetachUserPolicy(userName, policyArn string) error {
	b.mu.Lock("DetachUserPolicy")
	defer b.mu.Unlock()

	if _, exists := b.users[userName]; !exists {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	policies := b.userPolicies[userName]
	for i, p := range policies {
		if p == policyArn {
			b.userPolicies[userName] = append(policies[:i], policies[i+1:]...)
			b.removePolicyAttachmentLocked(policyArn, userName, "user")

			return nil
		}
	}

	return nil
}

// AttachRolePolicy attaches a policy to a role.
func (b *InMemoryBackend) AttachRolePolicy(roleName, policyArn string) error {
	b.mu.Lock("AttachRolePolicy")
	defer b.mu.Unlock()

	if _, exists := b.roles[roleName]; !exists {
		return fmt.Errorf("%w: role %q not found", ErrRoleNotFound, roleName)
	}

	if slices.Contains(b.rolePolicies[roleName], policyArn) {
		return nil // already attached
	}

	b.rolePolicies[roleName] = append(b.rolePolicies[roleName], policyArn)
	b.addPolicyAttachmentLocked(policyArn, roleName, "role")

	return nil
}

// DetachRolePolicy detaches a policy from a role.
func (b *InMemoryBackend) DetachRolePolicy(roleName, policyArn string) error {
	b.mu.Lock("DetachRolePolicy")
	defer b.mu.Unlock()

	if _, exists := b.roles[roleName]; !exists {
		return fmt.Errorf("%w: role %q not found", ErrRoleNotFound, roleName)
	}

	policies := b.rolePolicies[roleName]
	for i, p := range policies {
		if p == policyArn {
			b.rolePolicies[roleName] = append(policies[:i], policies[i+1:]...)
			b.removePolicyAttachmentLocked(policyArn, roleName, "role")

			return nil
		}
	}

	return nil
}

// ---- Groups ----

// CreateGroup creates a new IAM group.
func (b *InMemoryBackend) CreateGroup(groupName, path string) (*Group, error) {
	b.mu.Lock("CreateGroup")
	defer b.mu.Unlock()

	if _, exists := b.groups[groupName]; exists {
		return nil, fmt.Errorf("%w: group %q already exists", ErrGroupAlreadyExists, groupName)
	}

	p := normPath(path)
	g := Group{
		GroupName:  groupName,
		GroupID:    newID("AGPA"),
		Arn:        arn.Build("iam", "", b.accountID, "group"+p+groupName),
		Path:       p,
		CreateDate: time.Now().UTC(),
	}
	b.groups[groupName] = g

	return &g, nil
}

// DeleteGroup deletes an IAM group by name.
func (b *InMemoryBackend) DeleteGroup(groupName string) error {
	b.mu.Lock("DeleteGroup")
	defer b.mu.Unlock()

	if _, exists := b.groups[groupName]; !exists {
		return fmt.Errorf("%w: group %q not found", ErrGroupNotFound, groupName)
	}

	if len(b.groupPolicies[groupName]) > 0 {
		return fmt.Errorf("%w: group %q has attached policies", ErrDeleteConflict, groupName)
	}

	if len(b.groupInlinePolicies[groupName]) > 0 {
		return fmt.Errorf("%w: group %q has inline policies", ErrDeleteConflict, groupName)
	}

	delete(b.groups, groupName)

	// Clean up group membership tracking.
	delete(b.groupMembers, groupName)

	return nil
}

// AddUserToGroup adds a user to an IAM group, tracking the membership.
func (b *InMemoryBackend) AddUserToGroup(groupName, userName string) error {
	b.mu.Lock("AddUserToGroup")
	defer b.mu.Unlock()

	if _, exists := b.groups[groupName]; !exists {
		return fmt.Errorf("%w: group %q not found", ErrGroupNotFound, groupName)
	}

	if _, exists := b.users[userName]; !exists {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	if slices.Contains(b.groupMembers[groupName], userName) {
		return nil // already a member
	}

	b.groupMembers[groupName] = append(b.groupMembers[groupName], userName)

	return nil
}

// RemoveUserFromGroup removes a user from an IAM group.
func (b *InMemoryBackend) RemoveUserFromGroup(groupName, userName string) error {
	b.mu.Lock("RemoveUserFromGroup")
	defer b.mu.Unlock()

	if _, exists := b.groups[groupName]; !exists {
		return fmt.Errorf("%w: group %q not found", ErrGroupNotFound, groupName)
	}

	if _, exists := b.users[userName]; !exists {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	members := b.groupMembers[groupName]
	for i, m := range members {
		if m == userName {
			b.groupMembers[groupName] = append(members[:i], members[i+1:]...)

			return nil
		}
	}

	return nil
}

// GetGroup retrieves a single IAM group by name.
func (b *InMemoryBackend) GetGroup(groupName string) (*Group, error) {
	b.mu.RLock("GetGroup")
	defer b.mu.RUnlock()

	g, exists := b.groups[groupName]
	if !exists {
		return nil, fmt.Errorf("%w: group %q not found", ErrGroupNotFound, groupName)
	}

	return &g, nil
}

// GetGroupUsers returns the users that are members of the given group.
func (b *InMemoryBackend) GetGroupUsers(groupName string) ([]User, error) {
	b.mu.RLock("GetGroupUsers")
	defer b.mu.RUnlock()

	if _, exists := b.groups[groupName]; !exists {
		return nil, fmt.Errorf("%w: group %q not found", ErrGroupNotFound, groupName)
	}

	members := b.groupMembers[groupName]
	out := make([]User, 0, len(members))

	for _, userName := range members {
		if u, ok := b.users[userName]; ok {
			out = append(out, u)
		}
	}

	return out, nil
}

// AttachGroupPolicy attaches a policy to a group.
func (b *InMemoryBackend) AttachGroupPolicy(groupName, policyArn string) error {
	b.mu.Lock("AttachGroupPolicy")
	defer b.mu.Unlock()

	if _, exists := b.groups[groupName]; !exists {
		return fmt.Errorf("%w: group %q not found", ErrGroupNotFound, groupName)
	}

	if slices.Contains(b.groupPolicies[groupName], policyArn) {
		return nil // already attached
	}

	b.groupPolicies[groupName] = append(b.groupPolicies[groupName], policyArn)
	b.addPolicyAttachmentLocked(policyArn, groupName, "group")

	return nil
}

// DetachGroupPolicy detaches a policy from a group.
func (b *InMemoryBackend) DetachGroupPolicy(groupName, policyArn string) error {
	b.mu.Lock("DetachGroupPolicy")
	defer b.mu.Unlock()

	if _, exists := b.groups[groupName]; !exists {
		return fmt.Errorf("%w: group %q not found", ErrGroupNotFound, groupName)
	}

	policies := b.groupPolicies[groupName]
	for i, p := range policies {
		if p == policyArn {
			b.groupPolicies[groupName] = append(policies[:i], policies[i+1:]...)
			b.removePolicyAttachmentLocked(policyArn, groupName, "group")

			return nil
		}
	}

	return nil
}

// ListAttachedGroupPolicies returns all policy ARNs attached to the named group.
func (b *InMemoryBackend) ListAttachedGroupPolicies(groupName string) ([]AttachedPolicy, error) {
	b.mu.RLock("ListAttachedGroupPolicies")
	defer b.mu.RUnlock()

	if _, exists := b.groups[groupName]; !exists {
		return nil, fmt.Errorf("%w: group %q not found", ErrGroupNotFound, groupName)
	}

	arns := b.groupPolicies[groupName]
	result := make([]AttachedPolicy, 0, len(arns))

	for _, a := range arns {
		name := policyNameFromARN(a)
		result = append(result, AttachedPolicy{PolicyName: name, PolicyArn: a})
	}

	return result, nil
}

// ListGroups returns a paginated list of IAM groups sorted by name.
func (b *InMemoryBackend) ListGroups(marker string, maxItems int) (page.Page[Group], error) {
	b.mu.RLock("ListGroups")
	defer b.mu.RUnlock()

	return page.New(sortedGroups(b.groups), marker, maxItems, iamDefaultMaxItems), nil
}

// ---- Access Keys ----

// CreateAccessKey creates a new access key for an IAM user.
func (b *InMemoryBackend) CreateAccessKey(userName string) (*AccessKey, error) {
	b.mu.Lock("CreateAccessKey")
	defer b.mu.Unlock()

	if _, exists := b.users[userName]; !exists {
		return nil, fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	// AWS allows at most 2 access keys per user.
	const maxAccessKeysPerUser = 2
	var existingCount int
	for _, ak := range b.accessKeys {
		if ak.UserName == userName {
			existingCount++
		}
	}

	if existingCount >= maxAccessKeysPerUser {
		return nil, fmt.Errorf(
			"%w: user %q already has %d access keys (maximum %d)",
			ErrLimitExceeded, userName, existingCount, maxAccessKeysPerUser,
		)
	}

	secret, err := newSecretAccessKey()
	if err != nil {
		return nil, fmt.Errorf("creating access key: %w", err)
	}

	ak := AccessKey{
		AccessKeyID:     newAccessKeyID(),
		SecretAccessKey: secret,
		UserName:        userName,
		Status:          "Active",
		CreateDate:      time.Now().UTC(),
	}
	b.accessKeys[ak.AccessKeyID] = ak

	return &ak, nil
}

// DeleteAccessKey deletes an access key by ID.
func (b *InMemoryBackend) DeleteAccessKey(userName, accessKeyID string) error {
	b.mu.Lock("DeleteAccessKey")
	defer b.mu.Unlock()

	ak, exists := b.accessKeys[accessKeyID]
	if !exists || ak.UserName != userName {
		return fmt.Errorf("%w: access key %q not found for user %q", ErrAccessKeyNotFound, accessKeyID, userName)
	}

	delete(b.accessKeys, accessKeyID)

	return nil
}

// ListAccessKeys returns a paginated list of access keys for an IAM user.
func (b *InMemoryBackend) ListAccessKeys(userName, marker string, maxItems int) (page.Page[AccessKey], error) {
	b.mu.RLock("ListAccessKeys")
	defer b.mu.RUnlock()

	if _, exists := b.users[userName]; !exists {
		return page.Page[AccessKey]{}, fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	keys := make([]AccessKey, 0, len(b.accessKeys))
	for _, ak := range b.accessKeys {
		if ak.UserName == userName {
			keys = append(keys, ak)
		}
	}

	sort.Slice(keys, func(i, j int) bool { return keys[i].AccessKeyID < keys[j].AccessKeyID })

	return page.New(keys, marker, maxItems, iamDefaultMaxItems), nil
}

// ---- Instance Profiles ----

// CreateInstanceProfile creates a new IAM instance profile.
func (b *InMemoryBackend) CreateInstanceProfile(name, path string) (*InstanceProfile, error) {
	b.mu.Lock("CreateInstanceProfile")
	defer b.mu.Unlock()

	if _, exists := b.instanceProfiles[name]; exists {
		return nil, fmt.Errorf("%w: instance profile %q already exists", ErrInstanceProfileAlreadyExists, name)
	}

	p := normPath(path)
	ip := InstanceProfile{
		InstanceProfileName: name,
		InstanceProfileID:   newID("AIPA"),
		Arn:                 arn.Build("iam", "", b.accountID, "instance-profile"+p+name),
		Path:                p,
		Roles:               []string{},
		CreateDate:          time.Now().UTC(),
	}
	b.instanceProfiles[name] = ip

	return &ip, nil
}

// DeleteInstanceProfile deletes an IAM instance profile by name.
func (b *InMemoryBackend) DeleteInstanceProfile(name string) error {
	b.mu.Lock("DeleteInstanceProfile")
	defer b.mu.Unlock()

	if _, exists := b.instanceProfiles[name]; !exists {
		return fmt.Errorf("%w: instance profile %q not found", ErrInstanceProfileNotFound, name)
	}

	delete(b.instanceProfiles, name)

	return nil
}

// ListInstanceProfiles returns a paginated list of IAM instance profiles sorted by name.
func (b *InMemoryBackend) ListInstanceProfiles(marker string, maxItems int) (page.Page[InstanceProfile], error) {
	b.mu.RLock("ListInstanceProfiles")
	defer b.mu.RUnlock()

	return page.New(sortedInstanceProfiles(b.instanceProfiles), marker, maxItems, iamDefaultMaxItems), nil
}

// AddRoleToInstanceProfile adds a role to an IAM instance profile.
func (b *InMemoryBackend) AddRoleToInstanceProfile(instanceProfileName, roleName string) error {
	b.mu.Lock("AddRoleToInstanceProfile")
	defer b.mu.Unlock()

	ip, exists := b.instanceProfiles[instanceProfileName]
	if !exists {
		return fmt.Errorf("%w: instance profile %q not found", ErrInstanceProfileNotFound, instanceProfileName)
	}

	if _, roleExists := b.roles[roleName]; !roleExists {
		return fmt.Errorf("%w: role %q not found", ErrRoleNotFound, roleName)
	}

	if slices.Contains(ip.Roles, roleName) {
		return nil // already attached (idempotent)
	}

	// AWS instance profiles can contain exactly one role.
	if len(ip.Roles) >= 1 {
		return fmt.Errorf(
			"%w: instance profile %q already has a role; an instance profile can contain only one role",
			ErrLimitExceeded, instanceProfileName,
		)
	}

	ip.Roles = append(ip.Roles, roleName)
	b.instanceProfiles[instanceProfileName] = ip

	return nil
}

// RemoveRoleFromInstanceProfile removes a role from an IAM instance profile.
func (b *InMemoryBackend) RemoveRoleFromInstanceProfile(instanceProfileName, roleName string) error {
	b.mu.Lock("RemoveRoleFromInstanceProfile")
	defer b.mu.Unlock()

	ip, exists := b.instanceProfiles[instanceProfileName]
	if !exists {
		return fmt.Errorf("%w: instance profile %q not found", ErrInstanceProfileNotFound, instanceProfileName)
	}

	for i, r := range ip.Roles {
		if r == roleName {
			ip.Roles = append(ip.Roles[:i], ip.Roles[i+1:]...)
			b.instanceProfiles[instanceProfileName] = ip

			return nil
		}
	}

	return nil
}

// ---- Dashboard helpers ----

// ListAllUsers returns all users (for dashboard).
func (b *InMemoryBackend) ListAllUsers() []User {
	b.mu.RLock("ListAllUsers")
	defer b.mu.RUnlock()

	return sortedUsers(b.users)
}

// ListAllRoles returns all roles (for dashboard).
func (b *InMemoryBackend) ListAllRoles() []Role {
	b.mu.RLock("ListAllRoles")
	defer b.mu.RUnlock()

	roles := make([]Role, 0, len(b.roles))
	for _, r := range b.roles {
		roles = append(roles, r)
	}

	sort.Slice(roles, func(i, j int) bool { return roles[i].RoleName < roles[j].RoleName })

	return roles
}

// ListAllPolicies returns all policies (for dashboard).
func (b *InMemoryBackend) ListAllPolicies() []Policy {
	b.mu.RLock("ListAllPolicies")
	defer b.mu.RUnlock()

	policies := make([]Policy, 0, len(b.policies))
	for _, p := range b.policies {
		policies = append(policies, p)
	}

	sort.Slice(policies, func(i, j int) bool { return policies[i].PolicyName < policies[j].PolicyName })

	return policies
}

// ListAllGroups returns all groups (for dashboard).
func (b *InMemoryBackend) ListAllGroups() []Group {
	b.mu.RLock("ListAllGroups")
	defer b.mu.RUnlock()

	groups := make([]Group, 0, len(b.groups))
	for _, g := range b.groups {
		groups = append(groups, g)
	}

	sort.Slice(groups, func(i, j int) bool { return groups[i].GroupName < groups[j].GroupName })

	return groups
}

// ListAllAccessKeys returns all access keys (for dashboard).
func (b *InMemoryBackend) ListAllAccessKeys() []AccessKey {
	b.mu.RLock("ListAllAccessKeys")
	defer b.mu.RUnlock()

	keys := make([]AccessKey, 0, len(b.accessKeys))
	for _, ak := range b.accessKeys {
		keys = append(keys, ak)
	}

	sort.Slice(keys, func(i, j int) bool { return keys[i].AccessKeyID < keys[j].AccessKeyID })

	return keys
}

// ListAllInstanceProfiles returns all instance profiles (for dashboard).
func (b *InMemoryBackend) ListAllInstanceProfiles() []InstanceProfile {
	b.mu.RLock("ListAllInstanceProfiles")
	defer b.mu.RUnlock()

	profiles := make([]InstanceProfile, 0, len(b.instanceProfiles))
	for _, ip := range b.instanceProfiles {
		profiles = append(profiles, ip)
	}

	sort.Slice(profiles, func(i, j int) bool {
		return profiles[i].InstanceProfileName < profiles[j].InstanceProfileName
	})

	return profiles
}

// ---- Helpers ----

func sortedUsers(m map[string]User) []User {
	users := make([]User, 0, len(m))
	for _, u := range m {
		users = append(users, u)
	}

	sort.Slice(users, func(i, j int) bool { return users[i].UserName < users[j].UserName })

	return users
}

// idAlphabet is the set of characters used in AWS IAM entity IDs:
// uppercase letters A–Z and digits 0–9 (no lowercase, no dashes).
const idAlphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// randomAlphanumString returns a cryptographically random uppercase
// alphanumeric string of length n using idAlphabet.
func randomAlphanumString(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		// Fall back to UUID-derived string on entropy failure (should never happen).
		id := uuid.New().String()
		id = strings.ToUpper(strings.ReplaceAll(id, "-", ""))
		if len(id) >= n {
			return id[:n]
		}

		return id
	}

	result := make([]byte, n)
	for i, raw := range b {
		result[i] = idAlphabet[int(raw)%len(idAlphabet)]
	}

	return string(result)
}

// iamEntityIDSuffixLen is the length of the random suffix in IAM entity IDs (e.g. AIDA, AROA).
const iamEntityIDSuffixLen = 16

// newID generates a short unique identifier with the given prefix.
// The suffix is 16 uppercase alphanumeric characters matching AWS IAM ID format.
func newID(prefix string) string {
	return prefix + randomAlphanumString(iamEntityIDSuffixLen)
}

// newAccessKeyID generates a 20-character access key ID matching AWS format:
// "AKIA" followed by 16 uppercase alphanumeric characters.
func newAccessKeyID() string {
	return "AKIA" + randomAlphanumString(iamEntityIDSuffixLen)
}

// secretKeyBytes is the number of random bytes to generate for a secret access key.
// 30 random bytes produce exactly 40 standard base64 characters (30 * 8/6 = 40).
const secretKeyBytes = 30

// newSecretAccessKey generates a cryptographically secure 40-character secret access key.
// It uses 30 random bytes encoded as standard base64, which produces exactly 40 characters.
func newSecretAccessKey() (string, error) {
	b := make([]byte, secretKeyBytes)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("iam: generate secret access key: %w", err)
	}

	return base64.StdEncoding.EncodeToString(b), nil
}

// ---- Attached Policy Queries ----

// AttachedPolicy is a simplified representation of an attached managed policy.
type AttachedPolicy struct {
	PolicyName string
	PolicyArn  string
}

// ListAttachedUserPolicies returns all policy ARNs attached to the named user.
func (b *InMemoryBackend) ListAttachedUserPolicies(userName string) ([]AttachedPolicy, error) {
	b.mu.RLock("ListAttachedUserPolicies")
	defer b.mu.RUnlock()

	if _, exists := b.users[userName]; !exists {
		return nil, fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	arns := b.userPolicies[userName]
	result := make([]AttachedPolicy, 0, len(arns))

	for _, arn := range arns {
		name := policyNameFromARN(arn)
		result = append(result, AttachedPolicy{PolicyName: name, PolicyArn: arn})
	}

	return result, nil
}

// ListAttachedRolePolicies returns all policy ARNs attached to the named role.
func (b *InMemoryBackend) ListAttachedRolePolicies(roleName string) ([]AttachedPolicy, error) {
	b.mu.RLock("ListAttachedRolePolicies")
	defer b.mu.RUnlock()

	if _, exists := b.roles[roleName]; !exists {
		return nil, fmt.Errorf("%w: role %q not found", ErrRoleNotFound, roleName)
	}

	arns := b.rolePolicies[roleName]
	result := make([]AttachedPolicy, 0, len(arns))

	for _, arn := range arns {
		name := policyNameFromARN(arn)
		result = append(result, AttachedPolicy{PolicyName: name, PolicyArn: arn})
	}

	return result, nil
}

// GetPolicy returns the policy metadata for the given ARN.
func (b *InMemoryBackend) GetPolicy(policyArn string) (*Policy, error) {
	b.mu.RLock("GetPolicy")
	defer b.mu.RUnlock()

	pol, exists := b.getPolicyByARNLocked(policyArn)
	if !exists {
		return nil, fmt.Errorf("%w: policy %q not found", ErrPolicyNotFound, policyArn)
	}

	return &pol, nil
}

// GetPolicyVersion returns a policy document for the requested version.
func (b *InMemoryBackend) GetPolicyVersion(policyArn, versionID string) (*Policy, error) {
	b.mu.RLock("GetPolicyVersion")
	defer b.mu.RUnlock()

	policy, exists := b.getPolicyByARNLocked(policyArn)
	if !exists {
		return nil, fmt.Errorf("%w: policy %q not found", ErrPolicyNotFound, policyArn)
	}

	for _, version := range b.policyVersions[policyArn] {
		if version.VersionID != versionID {
			continue
		}

		policy.PolicyDocument = version.PolicyDocument

		return &policy, nil
	}

	if versionID == "" || versionID == "v1" {
		return &policy, nil
	}

	return nil, fmt.Errorf("%w: policy version %q not found", ErrPolicyNotFound, versionID)
}

// policyNameFromARN extracts the policy name from an ARN.
// arn:aws:iam::<account>:policy/<name> → <name>
func policyNameFromARN(arn string) string {
	const prefix = "policy/"

	if i := strings.LastIndex(arn, prefix); i >= 0 {
		return arn[i+len(prefix):]
	}

	return arn
}

// GetUserByAccessKeyID returns the User associated with the given access key ID.
// Returns ErrAccessKeyNotFound if no key with that ID exists.
func (b *InMemoryBackend) GetUserByAccessKeyID(accessKeyID string) (*User, error) {
	b.mu.RLock("GetUserByAccessKeyID")
	defer b.mu.RUnlock()

	ak, exists := b.accessKeys[accessKeyID]
	if !exists {
		return nil, fmt.Errorf("%w: access key %q not found", ErrAccessKeyNotFound, accessKeyID)
	}

	u, exists := b.users[ak.UserName]
	if !exists {
		return nil, fmt.Errorf("%w: user %q not found for access key", ErrUserNotFound, ak.UserName)
	}

	return &u, nil
}

// GetPoliciesForUser returns the policy documents for all policies attached to the named user.
// Policies that are referenced but not found in the backend are silently skipped.
func (b *InMemoryBackend) GetPoliciesForUser(userName string) ([]string, error) {
	b.mu.RLock("GetPoliciesForUser")
	defer b.mu.RUnlock()

	if _, exists := b.users[userName]; !exists {
		return nil, fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	arns := b.userPolicies[userName]
	docs := make([]string, 0, len(arns))

	for _, policyArn := range arns {
		policy, exists := b.getPolicyByARNLocked(policyArn)
		if !exists || policy.PolicyDocument == "" {
			continue
		}

		docs = append(docs, policy.PolicyDocument)
	}

	return docs, nil
}

// ---- Inline Policies ----

// PutUserPolicy creates or replaces an inline policy on a user.
func (b *InMemoryBackend) PutUserPolicy(userName, policyName, policyDocument string) error {
	b.mu.Lock("PutUserPolicy")
	defer b.mu.Unlock()

	if _, exists := b.users[userName]; !exists {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	if policyDocument != "" && !json.Valid([]byte(policyDocument)) {
		return fmt.Errorf("%w: invalid JSON in PolicyDocument", ErrMalformedPolicyDocument)
	}

	if len(policyDocument) > maxUserPolicySize {
		return fmt.Errorf("%w: inline policy for user %q exceeds %d bytes",
			ErrLimitExceeded, userName, maxUserPolicySize)
	}

	if b.userInlinePolicies[userName] == nil {
		b.userInlinePolicies[userName] = make(map[string]string)
	}

	b.userInlinePolicies[userName][policyName] = policyDocument

	return nil
}

// GetUserPolicy retrieves an inline policy document from a user.
func (b *InMemoryBackend) GetUserPolicy(userName, policyName string) (string, error) {
	b.mu.RLock("GetUserPolicy")
	defer b.mu.RUnlock()

	if _, exists := b.users[userName]; !exists {
		return "", fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	doc, exists := b.userInlinePolicies[userName][policyName]
	if !exists {
		return "", fmt.Errorf(
			"%w: inline policy %q not found on user %q",
			ErrInlinePolicyNotFound,
			policyName,
			userName,
		)
	}

	return doc, nil
}

// DeleteUserPolicy removes an inline policy from a user.
func (b *InMemoryBackend) DeleteUserPolicy(userName, policyName string) error {
	b.mu.Lock("DeleteUserPolicy")
	defer b.mu.Unlock()

	if _, exists := b.users[userName]; !exists {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	if _, exists := b.userInlinePolicies[userName][policyName]; !exists {
		return fmt.Errorf("%w: inline policy %q not found on user %q", ErrInlinePolicyNotFound, policyName, userName)
	}

	delete(b.userInlinePolicies[userName], policyName)

	return nil
}

// ListUserPolicies returns sorted inline policy names for a user.
func (b *InMemoryBackend) ListUserPolicies(userName string) ([]string, error) {
	b.mu.RLock("ListUserPolicies")
	defer b.mu.RUnlock()

	if _, exists := b.users[userName]; !exists {
		return nil, fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	names := make([]string, 0, len(b.userInlinePolicies[userName]))
	for name := range b.userInlinePolicies[userName] {
		names = append(names, name)
	}

	sort.Strings(names)

	return names, nil
}

// PutRolePolicy creates or replaces an inline policy on a role.
func (b *InMemoryBackend) PutRolePolicy(roleName, policyName, policyDocument string) error {
	b.mu.Lock("PutRolePolicy")
	defer b.mu.Unlock()

	if _, exists := b.roles[roleName]; !exists {
		return fmt.Errorf("%w: role %q not found", ErrRoleNotFound, roleName)
	}

	if policyDocument != "" && !json.Valid([]byte(policyDocument)) {
		return fmt.Errorf("%w: invalid JSON in PolicyDocument", ErrMalformedPolicyDocument)
	}

	if len(policyDocument) > maxRolePolicySize {
		return fmt.Errorf("%w: inline policy for role %q exceeds %d bytes",
			ErrLimitExceeded, roleName, maxRolePolicySize)
	}

	if b.roleInlinePolicies[roleName] == nil {
		b.roleInlinePolicies[roleName] = make(map[string]string)
	}

	b.roleInlinePolicies[roleName][policyName] = policyDocument

	return nil
}

// GetRolePolicy retrieves an inline policy document from a role.
func (b *InMemoryBackend) GetRolePolicy(roleName, policyName string) (string, error) {
	b.mu.RLock("GetRolePolicy")
	defer b.mu.RUnlock()

	if _, exists := b.roles[roleName]; !exists {
		return "", fmt.Errorf("%w: role %q not found", ErrRoleNotFound, roleName)
	}

	doc, exists := b.roleInlinePolicies[roleName][policyName]
	if !exists {
		return "", fmt.Errorf(
			"%w: inline policy %q not found on role %q",
			ErrInlinePolicyNotFound,
			policyName,
			roleName,
		)
	}

	return doc, nil
}

// DeleteRolePolicy removes an inline policy from a role.
func (b *InMemoryBackend) DeleteRolePolicy(roleName, policyName string) error {
	b.mu.Lock("DeleteRolePolicy")
	defer b.mu.Unlock()

	if _, exists := b.roles[roleName]; !exists {
		return fmt.Errorf("%w: role %q not found", ErrRoleNotFound, roleName)
	}

	if _, exists := b.roleInlinePolicies[roleName][policyName]; !exists {
		return fmt.Errorf("%w: inline policy %q not found on role %q", ErrInlinePolicyNotFound, policyName, roleName)
	}

	delete(b.roleInlinePolicies[roleName], policyName)

	return nil
}

// ListRolePolicies returns sorted inline policy names for a role.
func (b *InMemoryBackend) ListRolePolicies(roleName string) ([]string, error) {
	b.mu.RLock("ListRolePolicies")
	defer b.mu.RUnlock()

	if _, exists := b.roles[roleName]; !exists {
		return nil, fmt.Errorf("%w: role %q not found", ErrRoleNotFound, roleName)
	}

	names := make([]string, 0, len(b.roleInlinePolicies[roleName]))
	for name := range b.roleInlinePolicies[roleName] {
		names = append(names, name)
	}

	sort.Strings(names)

	return names, nil
}

// PutGroupPolicy creates or replaces an inline policy on a group.
func (b *InMemoryBackend) PutGroupPolicy(groupName, policyName, policyDocument string) error {
	b.mu.Lock("PutGroupPolicy")
	defer b.mu.Unlock()

	if _, exists := b.groups[groupName]; !exists {
		return fmt.Errorf("%w: group %q not found", ErrGroupNotFound, groupName)
	}

	if policyDocument != "" && !json.Valid([]byte(policyDocument)) {
		return fmt.Errorf("%w: invalid JSON in PolicyDocument", ErrMalformedPolicyDocument)
	}

	if len(policyDocument) > maxGroupPolicySize {
		return fmt.Errorf("%w: inline policy for group %q exceeds %d bytes",
			ErrLimitExceeded, groupName, maxGroupPolicySize)
	}

	if b.groupInlinePolicies[groupName] == nil {
		b.groupInlinePolicies[groupName] = make(map[string]string)
	}

	b.groupInlinePolicies[groupName][policyName] = policyDocument

	return nil
}

// GetGroupPolicy retrieves an inline policy document from a group.
func (b *InMemoryBackend) GetGroupPolicy(groupName, policyName string) (string, error) {
	b.mu.RLock("GetGroupPolicy")
	defer b.mu.RUnlock()

	if _, exists := b.groups[groupName]; !exists {
		return "", fmt.Errorf("%w: group %q not found", ErrGroupNotFound, groupName)
	}

	doc, exists := b.groupInlinePolicies[groupName][policyName]
	if !exists {
		return "", fmt.Errorf(
			"%w: inline policy %q not found on group %q",
			ErrInlinePolicyNotFound,
			policyName,
			groupName,
		)
	}

	return doc, nil
}

// DeleteGroupPolicy removes an inline policy from a group.
func (b *InMemoryBackend) DeleteGroupPolicy(groupName, policyName string) error {
	b.mu.Lock("DeleteGroupPolicy")
	defer b.mu.Unlock()

	if _, exists := b.groups[groupName]; !exists {
		return fmt.Errorf("%w: group %q not found", ErrGroupNotFound, groupName)
	}

	if _, exists := b.groupInlinePolicies[groupName][policyName]; !exists {
		return fmt.Errorf("%w: inline policy %q not found on group %q", ErrInlinePolicyNotFound, policyName, groupName)
	}

	delete(b.groupInlinePolicies[groupName], policyName)

	return nil
}

// ListGroupPolicies returns sorted inline policy names for a group.
func (b *InMemoryBackend) ListGroupPolicies(groupName string) ([]string, error) {
	b.mu.RLock("ListGroupPolicies")
	defer b.mu.RUnlock()

	if _, exists := b.groups[groupName]; !exists {
		return nil, fmt.Errorf("%w: group %q not found", ErrGroupNotFound, groupName)
	}

	names := make([]string, 0, len(b.groupInlinePolicies[groupName]))
	for name := range b.groupInlinePolicies[groupName] {
		names = append(names, name)
	}

	sort.Strings(names)

	return names, nil
}

// ---- Permission Boundaries ----

// PutUserPermissionsBoundary sets the permissions boundary on a user.
func (b *InMemoryBackend) PutUserPermissionsBoundary(userName, policyArn string) error {
	b.mu.Lock("PutUserPermissionsBoundary")
	defer b.mu.Unlock()

	u, exists := b.users[userName]
	if !exists {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	u.PermissionsBoundary = policyArn
	b.users[userName] = u

	return nil
}

// DeleteUserPermissionsBoundary clears the permissions boundary on a user.
func (b *InMemoryBackend) DeleteUserPermissionsBoundary(userName string) error {
	b.mu.Lock("DeleteUserPermissionsBoundary")
	defer b.mu.Unlock()

	u, exists := b.users[userName]
	if !exists {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	u.PermissionsBoundary = ""
	b.users[userName] = u

	return nil
}

// PutRolePermissionsBoundary sets the permissions boundary on a role.
func (b *InMemoryBackend) PutRolePermissionsBoundary(roleName, policyArn string) error {
	b.mu.Lock("PutRolePermissionsBoundary")
	defer b.mu.Unlock()

	r, exists := b.roles[roleName]
	if !exists {
		return fmt.Errorf("%w: role %q not found", ErrRoleNotFound, roleName)
	}

	r.PermissionsBoundary = policyArn
	b.roles[roleName] = r

	return nil
}

// DeleteRolePermissionsBoundary clears the permissions boundary on a role.
func (b *InMemoryBackend) DeleteRolePermissionsBoundary(roleName string) error {
	b.mu.Lock("DeleteRolePermissionsBoundary")
	defer b.mu.Unlock()

	r, exists := b.roles[roleName]
	if !exists {
		return fmt.Errorf("%w: role %q not found", ErrRoleNotFound, roleName)
	}

	r.PermissionsBoundary = ""
	b.roles[roleName] = r

	return nil
}

// ---- UpdateAssumeRolePolicy ----

// UpdateAssumeRolePolicy updates the assume-role policy document on a role.
func (b *InMemoryBackend) UpdateAssumeRolePolicy(roleName, policyDocument string) error {
	b.mu.Lock("UpdateAssumeRolePolicy")
	defer b.mu.Unlock()

	r, exists := b.roles[roleName]
	if !exists {
		return fmt.Errorf("%w: role %q not found", ErrRoleNotFound, roleName)
	}

	if policyDocument != "" && !json.Valid([]byte(policyDocument)) {
		return fmt.Errorf("%w: invalid JSON in AssumeRolePolicyDocument", ErrMalformedPolicyDocument)
	}

	if err := validateTrustPolicyPrincipal(policyDocument); err != nil {
		return err
	}

	r.AssumeRolePolicyDocument = policyDocument
	b.roles[roleName] = r

	return nil
}

// ---- AccountAuthorizationDetails and Simulation ----

// InlinePolicyEntry is an inline policy name/document pair used in AccountAuthorizationDetails.
type InlinePolicyEntry struct {
	PolicyName     string
	PolicyDocument string
}

// UserDetail holds user data and all associated policies for GetAccountAuthorizationDetails.
type UserDetail struct {
	User

	AttachedPolicies []AttachedPolicy
	InlinePolicies   []InlinePolicyEntry
}

// GroupDetail holds group data and all associated policies for GetAccountAuthorizationDetails.
type GroupDetail struct {
	Group

	AttachedPolicies []AttachedPolicy
	InlinePolicies   []InlinePolicyEntry
}

// RoleDetail holds role data and all associated policies for GetAccountAuthorizationDetails.
type RoleDetail struct {
	Role

	AttachedPolicies []AttachedPolicy
	InlinePolicies   []InlinePolicyEntry
}

// AccountSummary holds summary counts for GetAccountSummary.
type AccountSummary struct {
	Users             int
	Groups            int
	Roles             int
	Policies          int
	InstanceProfiles  int
	AccessKeysPerUser int
	ActiveAccessKeys  int
	AttachedPolicies  int
	AccountAliases    int
	OIDCProviders     int
	SAMLProviders     int
	MFADevices        int
}

// AccountAuthorizationDetails is the full IAM entity dump returned by GetAccountAuthorizationDetails.
type AccountAuthorizationDetails struct {
	Users    []UserDetail
	Groups   []GroupDetail
	Roles    []RoleDetail
	Policies []Policy
}

// SimulationResult is the outcome of evaluating a single action/resource pair.
type SimulationResult struct {
	ActionName   string
	ResourceName string
	Decision     string // "allowed", "implicitDeny", or "explicitDeny"

	// EvalDecisionDetails maps each policy source ID (ARN for managed, name for inline)
	// to the decision that policy alone would produce.  Populated when caller requests detail.
	EvalDecisionDetails map[string]string

	// AllowedByPermissionsBoundary is non-nil when the principal has a permissions boundary.
	// true → boundary permits this action; false → boundary denies it.
	AllowedByPermissionsBoundary *bool
}

// namedPolicyDoc pairs a policy source ID with its JSON document.
type namedPolicyDoc struct {
	// SourceID is the ARN for managed policies or the inline policy name.
	SourceID string
	Doc      string
}

// GetAccountAuthorizationDetails returns a full dump of all IAM entities and their policies.
func (b *InMemoryBackend) GetAccountAuthorizationDetails() AccountAuthorizationDetails {
	b.mu.RLock("GetAccountAuthorizationDetails")
	defer b.mu.RUnlock()

	// Build user details.
	users := make([]UserDetail, 0, len(b.users))
	for _, u := range b.users {
		user := u
		attached := attachedFromARNs(b.userPolicies[u.UserName])
		inline := inlineEntries(b.userInlinePolicies[u.UserName])
		users = append(users, UserDetail{User: user, AttachedPolicies: attached, InlinePolicies: inline})
	}

	sort.Slice(users, func(i, j int) bool { return users[i].UserName < users[j].UserName })

	// Build group details.
	groups := make([]GroupDetail, 0, len(b.groups))
	for _, g := range b.groups {
		group := g
		attached := attachedFromARNs(b.groupPolicies[g.GroupName])
		inline := inlineEntries(b.groupInlinePolicies[g.GroupName])
		groups = append(groups, GroupDetail{Group: group, AttachedPolicies: attached, InlinePolicies: inline})
	}

	sort.Slice(groups, func(i, j int) bool { return groups[i].GroupName < groups[j].GroupName })

	// Build role details.
	roles := make([]RoleDetail, 0, len(b.roles))
	for _, r := range b.roles {
		role := r
		attached := attachedFromARNs(b.rolePolicies[r.RoleName])
		inline := inlineEntries(b.roleInlinePolicies[r.RoleName])
		roles = append(roles, RoleDetail{Role: role, AttachedPolicies: attached, InlinePolicies: inline})
	}

	sort.Slice(roles, func(i, j int) bool { return roles[i].RoleName < roles[j].RoleName })

	// Build managed policy list.
	policies := make([]Policy, 0, len(b.policies))
	for _, p := range b.policies {
		policies = append(policies, p)
	}

	sort.Slice(policies, func(i, j int) bool { return policies[i].PolicyName < policies[j].PolicyName })

	return AccountAuthorizationDetails{
		Users:    users,
		Groups:   groups,
		Roles:    roles,
		Policies: policies,
	}
}

// attachedFromARNs converts a slice of policy ARNs to AttachedPolicy entries.
func attachedFromARNs(arns []string) []AttachedPolicy {
	result := make([]AttachedPolicy, 0, len(arns))

	for _, a := range arns {
		result = append(result, AttachedPolicy{PolicyName: policyNameFromARN(a), PolicyArn: a})
	}

	return result
}

// inlineEntries converts a policyName→document map to sorted InlinePolicyEntry slices.
func inlineEntries(m map[string]string) []InlinePolicyEntry {
	result := make([]InlinePolicyEntry, 0, len(m))

	for name, doc := range m {
		result = append(result, InlinePolicyEntry{PolicyName: name, PolicyDocument: doc})
	}

	sort.Slice(result, func(i, j int) bool { return result[i].PolicyName < result[j].PolicyName })

	return result
}

// SimulatePrincipalPolicy evaluates a set of actions against a set of resources
// for the given principal ARN, returning a result per action×resource pair.
//
// Supported principal ARN formats:
//   - arn:aws:iam::<account>:user/<name>
//   - arn:aws:iam::<account>:role/<name>
//
// Permission boundaries are enforced: effective permissions = identity policies ∩ boundary.
// An allow is only returned if both the identity policies allow AND the boundary allows.
func (b *InMemoryBackend) SimulatePrincipalPolicy(
	principalArn string, actionNames, resourceArns []string,
) ([]SimulationResult, error) {
	b.mu.RLock("SimulatePrincipalPolicy")
	defer b.mu.RUnlock()

	namedPolicies, err := b.collectNamedPrincipalPolicies(principalArn)
	if err != nil {
		return nil, err
	}

	// Collect permission boundary document (if any).
	boundaryDoc := b.collectBoundaryDoc(principalArn)
	hasBoundary := boundaryDoc != ""

	if len(resourceArns) == 0 {
		resourceArns = []string{"*"}
	}

	// Build plain docs slice for combined evaluation.
	docs := make([]string, 0, len(namedPolicies))
	for _, np := range namedPolicies {
		docs = append(docs, np.Doc)
	}

	results := make([]SimulationResult, 0, len(actionNames)*len(resourceArns))

	for _, action := range actionNames {
		for _, resource := range resourceArns {
			evalResult := EvaluatePolicies(docs, action, resource, ConditionContext{})

			// Per-policy detail map.
			detail := make(map[string]string, len(namedPolicies))
			for _, np := range namedPolicies {
				r := EvaluatePolicies([]string{np.Doc}, action, resource, ConditionContext{})
				detail[np.SourceID] = evalDecisionStr(r)
			}

			// Boundary enforcement.
			var allowedByBoundary *bool

			if hasBoundary {
				boundaryResult := EvaluatePolicies([]string{boundaryDoc}, action, resource, ConditionContext{})
				allowed := boundaryResult == EvalAllow

				allowedByBoundary = &allowed

				if evalResult == EvalAllow && !allowed {
					evalResult = EvalImplicitDeny
				}
			}

			results = append(results, SimulationResult{
				ActionName:                   action,
				ResourceName:                 resource,
				Decision:                     evalDecisionStr(evalResult),
				EvalDecisionDetails:          detail,
				AllowedByPermissionsBoundary: allowedByBoundary,
			})
		}
	}

	return results, nil
}

// evalDecisionStr converts an EvalResult to the AWS-compatible decision string.
func evalDecisionStr(r EvaluationResult) string {
	switch r {
	case EvalAllow:
		return "allowed"
	case EvalExplicitDeny:
		return "explicitDeny"
	default:
		return "implicitDeny"
	}
}

// collectBoundaryDoc returns the policy document for the principal's permission boundary, or "".
// Caller must hold b.mu read-locked.
func (b *InMemoryBackend) collectBoundaryDoc(principalArn string) string {
	const (
		userPrefix = ":user/"
		rolePrefix = ":role/"
	)

	switch {
	case strings.Contains(principalArn, userPrefix):
		idx := strings.LastIndex(principalArn, userPrefix)

		return b.boundaryDocForUser(principalArn[idx+len(userPrefix):])
	case strings.Contains(principalArn, rolePrefix):
		idx := strings.LastIndex(principalArn, rolePrefix)

		return b.boundaryDocForRole(principalArn[idx+len(rolePrefix):])
	}

	return ""
}

// collectPrincipalPolicies returns all policy documents for the given principal ARN.
// It looks at both inline policies and attached managed policies.
func (b *InMemoryBackend) collectPrincipalPolicies(principalArn string) ([]string, error) {
	const (
		userPrefix = ":user/"
		rolePrefix = ":role/"
	)

	switch {
	case strings.Contains(principalArn, userPrefix):
		idx := strings.LastIndex(principalArn, userPrefix)
		userName := principalArn[idx+len(userPrefix):]

		if _, exists := b.users[userName]; !exists {
			return nil, fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
		}

		// Collect direct user policies plus group-inherited policies.
		docs := b.collectEntityPolicies(b.userPolicies[userName], b.userInlinePolicies[userName])
		docs = append(docs, b.collectGroupPoliciesForUser(userName)...)

		return docs, nil

	case strings.Contains(principalArn, rolePrefix):
		idx := strings.LastIndex(principalArn, rolePrefix)
		roleName := principalArn[idx+len(rolePrefix):]

		if _, exists := b.roles[roleName]; !exists {
			return nil, fmt.Errorf("%w: role %q not found", ErrRoleNotFound, roleName)
		}

		return b.collectEntityPolicies(b.rolePolicies[roleName], b.roleInlinePolicies[roleName]), nil

	default:
		return nil, fmt.Errorf("%w: unsupported principal ARN format %q", ErrUserNotFound, principalArn)
	}
}

// collectNamedPrincipalPolicies returns named policy documents for the given principal ARN.
// Each entry contains the policy source ID (ARN for managed, name for inline) and document.
// Caller must hold b.mu read-locked.
func (b *InMemoryBackend) collectNamedPrincipalPolicies(principalArn string) ([]namedPolicyDoc, error) {
	const (
		userPrefix = ":user/"
		rolePrefix = ":role/"
	)

	switch {
	case strings.Contains(principalArn, userPrefix):
		idx := strings.LastIndex(principalArn, userPrefix)
		userName := principalArn[idx+len(userPrefix):]

		if _, exists := b.users[userName]; !exists {
			return nil, fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
		}

		named := b.collectNamedEntityPolicies(b.userPolicies[userName], b.userInlinePolicies[userName])

		// Add group-inherited policies.
		for groupName, members := range b.groupMembers {
			if !slices.Contains(members, userName) {
				continue
			}

			named = append(named,
				b.collectNamedEntityPolicies(b.groupPolicies[groupName], b.groupInlinePolicies[groupName])...)
		}

		return named, nil

	case strings.Contains(principalArn, rolePrefix):
		idx := strings.LastIndex(principalArn, rolePrefix)
		roleName := principalArn[idx+len(rolePrefix):]

		if _, exists := b.roles[roleName]; !exists {
			return nil, fmt.Errorf("%w: role %q not found", ErrRoleNotFound, roleName)
		}

		return b.collectNamedEntityPolicies(b.rolePolicies[roleName], b.roleInlinePolicies[roleName]), nil

	default:
		return nil, fmt.Errorf("%w: unsupported principal ARN format %q", ErrUserNotFound, principalArn)
	}
}

// collectNamedEntityPolicies collects named policy docs from attached ARNs and inline policies.
// Caller must hold b.mu read-locked.
func (b *InMemoryBackend) collectNamedEntityPolicies(
	attachedARNs []string, inlinePols map[string]string,
) []namedPolicyDoc {
	var named []namedPolicyDoc

	for _, policyArn := range attachedARNs {
		for _, p := range b.policies {
			if p.Arn == policyArn && p.PolicyDocument != "" {
				named = append(named, namedPolicyDoc{SourceID: p.Arn, Doc: p.PolicyDocument})

				break
			}
		}
	}

	for name, doc := range inlinePols {
		if doc != "" {
			named = append(named, namedPolicyDoc{SourceID: name, Doc: doc})
		}
	}

	return named
}

// collectEntityPolicies collects policy documents from attached ARNs and inline policies.
func (b *InMemoryBackend) collectEntityPolicies(
	attachedARNs []string, inlinePols map[string]string,
) []string {
	var docs []string

	for _, policyArn := range attachedARNs {
		for _, p := range b.policies {
			if p.Arn == policyArn && p.PolicyDocument != "" {
				docs = append(docs, p.PolicyDocument)

				break
			}
		}
	}

	for _, doc := range inlinePols {
		if doc != "" {
			docs = append(docs, doc)
		}
	}

	return docs
}

// collectGroupPoliciesForUser returns all policy documents inherited via group membership.
// Real AWS evaluates group-attached and group-inline policies as part of the principal's
// effective permissions.  Must be called with b.mu read-lock held.
//
// A seen-set prevents the same document being added twice when multiple groups share
// the same managed policy.
func (b *InMemoryBackend) collectGroupPoliciesForUser(userName string) []string {
	var docs []string
	seen := make(map[string]struct{})

	for groupName, members := range b.groupMembers {
		if !slices.Contains(members, userName) {
			continue
		}

		groupDocs := b.collectEntityPolicies(b.groupPolicies[groupName], b.groupInlinePolicies[groupName])
		for _, doc := range groupDocs {
			if _, dup := seen[doc]; dup {
				continue
			}

			seen[doc] = struct{}{}
			docs = append(docs, doc)
		}
	}

	return docs
}

// Reset clears all in-memory state from the backend. It is used by the
// POST /_gopherstack/reset endpoint for CI pipelines and rapid local development.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.users = make(map[string]User)
	b.roles = make(map[string]Role)
	b.roleByARN = make(map[string]string)
	b.policies = make(map[string]Policy)
	b.policyByARN = make(map[string]string)
	b.policyAttachments = make(map[string]policyAttachmentRefs)
	b.groups = make(map[string]Group)
	b.accessKeys = make(map[string]AccessKey)
	b.instanceProfiles = make(map[string]InstanceProfile)
	b.samlProviders = make(map[string]SAMLProvider)
	b.oidcProviders = make(map[string]OIDCProvider)
	b.loginProfiles = make(map[string]LoginProfile)
	b.userPolicies = make(map[string][]string)
	b.rolePolicies = make(map[string][]string)
	b.groupPolicies = make(map[string][]string)
	b.groupMembers = make(map[string][]string)
	b.userInlinePolicies = make(map[string]map[string]string)
	b.roleInlinePolicies = make(map[string]map[string]string)
	b.groupInlinePolicies = make(map[string]map[string]string)
	b.accountAliases = nil
	b.policyVersions = make(map[string][]StoredPolicyVersion)
	b.policyVersionCounters = make(map[string]int)
	b.serviceSpecificCreds = make(map[string]ServiceSpecificCredential)
	b.virtualMFADevices = make(map[string]VirtualMFADevice)
	b.signingCertificates = make(map[string]SigningCertificate)
	b.serverCertificates = make(map[string]ServerCertificate)
	b.delegationRequests = make(map[string]DelegationRequest)
	b.ResetComprehensiveBackend()
}

// Purge removes all resources older than the given cutoff time.
func (b *InMemoryBackend) Purge(ctx context.Context, cutoff time.Time) {
	if ctx.Err() != nil {
		return
	}

	b.mu.Lock("Purge")
	defer b.mu.Unlock()

	b.purgeUsersLocked(cutoff)
	b.purgeRolesLocked(cutoff)
	b.purgePoliciesLocked(cutoff)
	b.purgeGroupsLocked(cutoff)
	b.purgeAccessKeysLocked(cutoff)
	b.purgeInstanceProfilesLocked(cutoff)
	b.purgeSAMLProvidersLocked(cutoff)
	b.purgeOIDCProvidersLocked(cutoff)
	b.rebuildIndexesLocked()
}

// purgeUsersLocked removes users created before cutoff and cleans up associated data.
// Caller must hold b.mu.
func (b *InMemoryBackend) purgeUsersLocked(cutoff time.Time) {
	for name, u := range b.users {
		if !u.CreateDate.Before(cutoff) {
			continue
		}
		delete(b.users, name)
		delete(b.loginProfiles, name)
		delete(b.userPolicies, name)
		delete(b.userInlinePolicies, name)
		b.removeUserFromGroupsLocked(name)
	}
}

// removeUserFromGroupsLocked removes a user from all group membership lists.
// Caller must hold b.mu.
func (b *InMemoryBackend) removeUserFromGroupsLocked(userName string) {
	for g, members := range b.groupMembers {
		for i, m := range members {
			if m == userName {
				b.groupMembers[g] = append(members[:i], members[i+1:]...)

				break
			}
		}
	}
}

// purgeRolesLocked removes roles created before cutoff.
// Caller must hold b.mu.
func (b *InMemoryBackend) purgeRolesLocked(cutoff time.Time) {
	for name, r := range b.roles {
		if r.CreateDate.Before(cutoff) {
			delete(b.roles, name)
			delete(b.rolePolicies, name)
			delete(b.roleInlinePolicies, name)
		}
	}
}

// purgePoliciesLocked removes policies created before cutoff.
// Caller must hold b.mu.
func (b *InMemoryBackend) purgePoliciesLocked(cutoff time.Time) {
	for name, p := range b.policies {
		if p.CreateDate.Before(cutoff) {
			delete(b.policies, name)
		}
	}
}

// purgeGroupsLocked removes groups created before cutoff.
// Caller must hold b.mu.
func (b *InMemoryBackend) purgeGroupsLocked(cutoff time.Time) {
	for name, g := range b.groups {
		if g.CreateDate.Before(cutoff) {
			delete(b.groups, name)
			delete(b.groupPolicies, name)
			delete(b.groupInlinePolicies, name)
			delete(b.groupMembers, name)
		}
	}
}

// purgeAccessKeysLocked removes access keys created before cutoff.
// Caller must hold b.mu.
func (b *InMemoryBackend) purgeAccessKeysLocked(cutoff time.Time) {
	for id, ak := range b.accessKeys {
		if ak.CreateDate.Before(cutoff) {
			delete(b.accessKeys, id)
		}
	}
}

// purgeInstanceProfilesLocked removes instance profiles created before cutoff.
// Caller must hold b.mu.
func (b *InMemoryBackend) purgeInstanceProfilesLocked(cutoff time.Time) {
	for name, ip := range b.instanceProfiles {
		if ip.CreateDate.Before(cutoff) {
			delete(b.instanceProfiles, name)
		}
	}
}

// purgeSAMLProvidersLocked removes SAML providers created before cutoff.
// Caller must hold b.mu.
func (b *InMemoryBackend) purgeSAMLProvidersLocked(cutoff time.Time) {
	for arnStr, p := range b.samlProviders {
		if p.CreateDate.Before(cutoff) {
			delete(b.samlProviders, arnStr)
		}
	}
}

// purgeOIDCProvidersLocked removes OIDC providers created before cutoff.
// Caller must hold b.mu.
func (b *InMemoryBackend) purgeOIDCProvidersLocked(cutoff time.Time) {
	for arnStr, p := range b.oidcProviders {
		if p.CreateDate.Before(cutoff) {
			delete(b.oidcProviders, arnStr)
		}
	}
}
