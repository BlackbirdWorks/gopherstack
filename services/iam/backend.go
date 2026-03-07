package iam

import (
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
	ListGroups(marker string, maxItems int) (page.Page[Group], error)
	AddUserToGroup(groupName, userName string) error
	AttachGroupPolicy(groupName, policyArn string) error
	DetachGroupPolicy(groupName, policyArn string) error
	ListAttachedGroupPolicies(groupName string) ([]AttachedPolicy, error)

	// Assume Role Policy
	UpdateAssumeRolePolicy(roleName, policyDocument string) error

	// Reporting and simulation
	GetAccountAuthorizationDetails() AccountAuthorizationDetails
	SimulatePrincipalPolicy(principalArn string, actionNames, resourceArns []string) ([]SimulationResult, error)
	GetCredentialReport() string

	// Access Keys
	CreateAccessKey(userName string) (*AccessKey, error)
	DeleteAccessKey(userName, accessKeyID string) error
	ListAccessKeys(userName, marker string, maxItems int) (page.Page[AccessKey], error)

	// Instance Profiles
	CreateInstanceProfile(name, path string) (*InstanceProfile, error)
	DeleteInstanceProfile(name string) error
	ListInstanceProfiles(marker string, maxItems int) (page.Page[InstanceProfile], error)

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
}

// iamDefaultMaxItems is the default page size for IAM list operations.
const iamDefaultMaxItems = 100

// InMemoryBackend implements StorageBackend using in-memory maps.
type InMemoryBackend struct {
	users            map[string]User
	roles            map[string]Role
	policies         map[string]Policy
	groups           map[string]Group
	accessKeys       map[string]AccessKey
	instanceProfiles map[string]InstanceProfile
	// userPolicies, rolePolicies, and groupPolicies track attached policy ARNs keyed by entity name.
	userPolicies        map[string][]string          // userName → []policyArn
	rolePolicies        map[string][]string          // roleName → []policyArn
	groupPolicies       map[string][]string          // groupName → []policyArn
	userInlinePolicies  map[string]map[string]string // userName → policyName → document
	roleInlinePolicies  map[string]map[string]string // roleName → policyName → document
	groupInlinePolicies map[string]map[string]string // groupName → policyName → document
	mu                  *lockmetrics.RWMutex
	accountID           string
}

// NewInMemoryBackend creates a new empty IAM InMemoryBackend with default account ID.
func NewInMemoryBackend() *InMemoryBackend {
	return NewInMemoryBackendWithConfig(IAMAccountID)
}

// NewInMemoryBackendWithConfig creates a new IAM InMemoryBackend with the given account ID.
func NewInMemoryBackendWithConfig(accountID string) *InMemoryBackend {
	return &InMemoryBackend{
		users:               make(map[string]User),
		roles:               make(map[string]Role),
		policies:            make(map[string]Policy),
		groups:              make(map[string]Group),
		accessKeys:          make(map[string]AccessKey),
		instanceProfiles:    make(map[string]InstanceProfile),
		userPolicies:        make(map[string][]string),
		rolePolicies:        make(map[string][]string),
		groupPolicies:       make(map[string][]string),
		userInlinePolicies:  make(map[string]map[string]string),
		roleInlinePolicies:  make(map[string]map[string]string),
		groupInlinePolicies: make(map[string]map[string]string),
		accountID:           accountID,
		mu:                  lockmetrics.New("iam"),
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

// DeleteUser deletes an IAM user by name.
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

	delete(b.roles, roleName)

	return nil
}

// ListRoles returns a paginated list of IAM roles sorted by name.
func (b *InMemoryBackend) ListRoles(marker string, maxItems int) (page.Page[Role], error) {
	b.mu.RLock("ListRoles")
	defer b.mu.RUnlock()

	roles := make([]Role, 0, len(b.roles))
	for _, r := range b.roles {
		roles = append(roles, r)
	}

	sort.Slice(roles, func(i, j int) bool { return roles[i].RoleName < roles[j].RoleName })

	return page.New(roles, marker, maxItems, iamDefaultMaxItems), nil
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

// ---- Policies ----

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

	return &pol, nil
}

// DeletePolicy deletes an IAM policy by ARN.
func (b *InMemoryBackend) DeletePolicy(policyArn string) error {
	b.mu.Lock("DeletePolicy")
	defer b.mu.Unlock()

	// Check for attachment conflicts before deleting — iterating the policy maps
	// directly avoids a redundant join through b.users / b.roles.
	for userName, attached := range b.userPolicies {
		if slices.Contains(attached, policyArn) {
			return fmt.Errorf("%w: policy %q is attached to user %q", ErrDeleteConflict, policyArn, userName)
		}
	}

	for roleName, attached := range b.rolePolicies {
		if slices.Contains(attached, policyArn) {
			return fmt.Errorf("%w: policy %q is attached to role %q", ErrDeleteConflict, policyArn, roleName)
		}
	}

	for name, p := range b.policies {
		if p.Arn == policyArn {
			delete(b.policies, name)

			return nil
		}
	}

	return fmt.Errorf("%w: policy %q not found", ErrPolicyNotFound, policyArn)
}

// ListPolicies returns a paginated list of IAM policies sorted by name.
func (b *InMemoryBackend) ListPolicies(marker string, maxItems int) (page.Page[Policy], error) {
	b.mu.RLock("ListPolicies")
	defer b.mu.RUnlock()

	policies := make([]Policy, 0, len(b.policies))
	for _, p := range b.policies {
		policies = append(policies, p)
	}

	sort.Slice(policies, func(i, j int) bool { return policies[i].PolicyName < policies[j].PolicyName })

	return page.New(policies, marker, maxItems, iamDefaultMaxItems), nil
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

	return nil
}

// AddUserToGroup adds a user to an IAM group (stub — no membership tracking).
func (b *InMemoryBackend) AddUserToGroup(groupName, userName string) error {
	b.mu.RLock("AddUserToGroup")
	defer b.mu.RUnlock()

	if _, exists := b.groups[groupName]; !exists {
		return fmt.Errorf("%w: group %q not found", ErrGroupNotFound, groupName)
	}

	if _, exists := b.users[userName]; !exists {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	return nil
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

	groups := make([]Group, 0, len(b.groups))
	for _, g := range b.groups {
		groups = append(groups, g)
	}

	sort.Slice(groups, func(i, j int) bool { return groups[i].GroupName < groups[j].GroupName })

	return page.New(groups, marker, maxItems, iamDefaultMaxItems), nil
}

// ---- Access Keys ----

// CreateAccessKey creates a new access key for an IAM user.
func (b *InMemoryBackend) CreateAccessKey(userName string) (*AccessKey, error) {
	b.mu.Lock("CreateAccessKey")
	defer b.mu.Unlock()

	if _, exists := b.users[userName]; !exists {
		return nil, fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	ak := AccessKey{
		AccessKeyID:     newAccessKeyID(),
		SecretAccessKey: uuid.New().String(),
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

	keys := make([]AccessKey, 0)
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

	profiles := make([]InstanceProfile, 0, len(b.instanceProfiles))
	for _, ip := range b.instanceProfiles {
		profiles = append(profiles, ip)
	}

	sort.Slice(profiles, func(i, j int) bool {
		return profiles[i].InstanceProfileName < profiles[j].InstanceProfileName
	})

	return page.New(profiles, marker, maxItems, iamDefaultMaxItems), nil
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

// newID generates a short unique identifier with the given prefix.
func newID(prefix string) string {
	id := uuid.New().String()

	return prefix + id[:16]
}

// newAccessKeyID generates a 20-character access key ID.
func newAccessKeyID() string {
	return "AKIA" + uuid.New().String()[:16]
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

	for _, p := range b.policies {
		if p.Arn == policyArn {
			pol := p

			return &pol, nil
		}
	}

	return nil, fmt.Errorf("%w: policy %q not found", ErrPolicyNotFound, policyArn)
}

// GetPolicyVersion returns the default (only) version of a policy document.
// Gopherstack stores a single version per policy; version ID "v1" is always returned.
func (b *InMemoryBackend) GetPolicyVersion(policyArn, _ string) (*Policy, error) {
	return b.GetPolicy(policyArn)
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
		for _, p := range b.policies {
			if p.Arn == policyArn && p.PolicyDocument != "" {
				docs = append(docs, p.PolicyDocument)

				break
			}
		}
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
func (b *InMemoryBackend) SimulatePrincipalPolicy(
	principalArn string, actionNames, resourceArns []string,
) ([]SimulationResult, error) {
	b.mu.RLock("SimulatePrincipalPolicy")
	defer b.mu.RUnlock()

	policyDocs, err := b.collectPrincipalPolicies(principalArn)
	if err != nil {
		return nil, err
	}

	if len(resourceArns) == 0 {
		resourceArns = []string{"*"}
	}

	results := make([]SimulationResult, 0, len(actionNames)*len(resourceArns))
	for _, action := range actionNames {
		for _, resource := range resourceArns {
			evalResult := EvaluatePolicies(policyDocs, action, resource, ConditionContext{})

			var decision string

			switch evalResult {
			case EvalAllow:
				decision = "allowed"
			case EvalExplicitDeny:
				decision = "explicitDeny"
			default:
				decision = "implicitDeny"
			}

			results = append(results, SimulationResult{
				ActionName:   action,
				ResourceName: resource,
				Decision:     decision,
			})
		}
	}

	return results, nil
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

		return b.collectEntityPolicies(b.userPolicies[userName], b.userInlinePolicies[userName]), nil

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

// credentialReportHeader is the CSV header for the credential report.
const credentialReportHeader = "user,arn,user_creation_time,password_enabled,password_last_used," +
	"password_last_changed,password_next_rotation,mfa_active," +
	"access_key_1_active,access_key_1_last_rotated,access_key_1_last_used_date," +
	"access_key_1_last_used_region,access_key_1_last_used_service," +
	"access_key_2_active,access_key_2_last_rotated,access_key_2_last_used_date," +
	"access_key_2_last_used_region,access_key_2_last_used_service," +
	"cert_1_active,cert_1_last_rotated,cert_2_active,cert_2_last_rotated"

// GetCredentialReport generates and returns a base64-encoded CSV credential report.
// The report always includes the root account row followed by all IAM users.
func (b *InMemoryBackend) GetCredentialReport() string {
	b.mu.RLock("GetCredentialReport")
	defer b.mu.RUnlock()

	notApplicable := "N/A"
	falseStr := "false"
	noInfo := "no_information"

	users := sortedUsers(b.users)
	// 2 = header line + root account line
	const extraRows = 2
	lines := make([]string, 0, extraRows+len(users))
	lines = append(lines, credentialReportHeader)

	// Root account row.
	rootArn := "arn:aws:iam::" + b.accountID + ":root"
	lines = append(lines, strings.Join([]string{
		"<root_account>", rootArn, time.Now().UTC().Format(time.RFC3339),
		notApplicable, noInfo, notApplicable, notApplicable, falseStr,
		falseStr, notApplicable, notApplicable, notApplicable, notApplicable,
		falseStr, notApplicable, notApplicable, notApplicable, notApplicable,
		falseStr, notApplicable, falseStr, notApplicable,
	}, ","))

	// One row per user, sorted by name.
	for _, u := range users {
		createdAt := u.CreateDate.UTC().Format(time.RFC3339)
		lines = append(lines, strings.Join([]string{
			u.UserName, u.Arn, createdAt,
			falseStr, noInfo, notApplicable, notApplicable, falseStr,
			falseStr, notApplicable, notApplicable, notApplicable, notApplicable,
			falseStr, notApplicable, notApplicable, notApplicable, notApplicable,
			falseStr, notApplicable, falseStr, notApplicable,
		}, ","))
	}

	return strings.Join(lines, "\n")
}
