package iam

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
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
)

// StorageBackend defines the interface for the IAM in-memory store.
type StorageBackend interface {
	// Users
	CreateUser(userName, path string) (*User, error)
	DeleteUser(userName string) error
	ListUsers() ([]User, error)
	GetUser(userName string) (*User, error)

	// Roles
	CreateRole(roleName, path, assumeRolePolicyDocument string) (*Role, error)
	DeleteRole(roleName string) error
	ListRoles() ([]Role, error)
	GetRole(roleName string) (*Role, error)

	// Policies
	CreatePolicy(policyName, path, policyDocument string) (*Policy, error)
	DeletePolicy(policyArn string) error
	ListPolicies() ([]Policy, error)
	AttachUserPolicy(userName, policyArn string) error
	AttachRolePolicy(roleName, policyArn string) error

	// Groups
	CreateGroup(groupName, path string) (*Group, error)
	DeleteGroup(groupName string) error
	AddUserToGroup(groupName, userName string) error

	// Access Keys
	CreateAccessKey(userName string) (*AccessKey, error)
	DeleteAccessKey(userName, accessKeyID string) error
	ListAccessKeys(userName string) ([]AccessKey, error)

	// Instance Profiles
	CreateInstanceProfile(name, path string) (*InstanceProfile, error)
	DeleteInstanceProfile(name string) error
	ListInstanceProfiles() ([]InstanceProfile, error)

	// Dashboard helpers
	ListAllUsers() []User
	ListAllRoles() []Role
	ListAllPolicies() []Policy
	ListAllGroups() []Group
	ListAllAccessKeys() []AccessKey
	ListAllInstanceProfiles() []InstanceProfile
}

// InMemoryBackend implements StorageBackend using in-memory maps.
type InMemoryBackend struct {
	users            map[string]User
	roles            map[string]Role
	policies         map[string]Policy
	groups           map[string]Group
	accessKeys       map[string]AccessKey // key = AccessKeyId
	instanceProfiles map[string]InstanceProfile
	mu               sync.RWMutex
}

// NewInMemoryBackend creates a new empty IAM InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		users:            make(map[string]User),
		roles:            make(map[string]Role),
		policies:         make(map[string]Policy),
		groups:           make(map[string]Group),
		accessKeys:       make(map[string]AccessKey),
		instanceProfiles: make(map[string]InstanceProfile),
	}
}

// normPath returns a normalized IAM path, defaulting to "/" if empty.
func normPath(path string) string {
	if path == "" {
		return "/"
	}

	return path
}

// ---- Users ----

// CreateUser creates a new IAM user.
func (b *InMemoryBackend) CreateUser(userName, path string) (*User, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.users[userName]; exists {
		return nil, fmt.Errorf("%w: user %q already exists", ErrUserAlreadyExists, userName)
	}

	p := normPath(path)
	u := User{
		UserName:   userName,
		UserID:     newID("AIDA"),
		Arn:        fmt.Sprintf("arn:aws:iam::%s:user%s%s", IAMAccountID, p, userName),
		Path:       p,
		CreateDate: time.Now().UTC(),
	}
	b.users[userName] = u

	return &u, nil
}

// DeleteUser deletes an IAM user by name.
func (b *InMemoryBackend) DeleteUser(userName string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.users[userName]; !exists {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	delete(b.users, userName)

	return nil
}

// ListUsers returns all IAM users sorted by name.
func (b *InMemoryBackend) ListUsers() ([]User, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return sortedUsers(b.users), nil
}

// GetUser retrieves a single IAM user by name.
func (b *InMemoryBackend) GetUser(userName string) (*User, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	u, exists := b.users[userName]
	if !exists {
		return nil, fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	return &u, nil
}

// ---- Roles ----

// CreateRole creates a new IAM role.
func (b *InMemoryBackend) CreateRole(roleName, path, assumeRolePolicyDocument string) (*Role, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.roles[roleName]; exists {
		return nil, fmt.Errorf("%w: role %q already exists", ErrRoleAlreadyExists, roleName)
	}

	p := normPath(path)
	r := Role{
		RoleName:                 roleName,
		RoleID:                   newID("AROA"),
		Arn:                      fmt.Sprintf("arn:aws:iam::%s:role%s%s", IAMAccountID, p, roleName),
		Path:                     p,
		AssumeRolePolicyDocument: assumeRolePolicyDocument,
		CreateDate:               time.Now().UTC(),
	}
	b.roles[roleName] = r

	return &r, nil
}

// DeleteRole deletes an IAM role by name.
func (b *InMemoryBackend) DeleteRole(roleName string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.roles[roleName]; !exists {
		return fmt.Errorf("%w: role %q not found", ErrRoleNotFound, roleName)
	}

	delete(b.roles, roleName)

	return nil
}

// ListRoles returns all IAM roles sorted by name.
func (b *InMemoryBackend) ListRoles() ([]Role, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	roles := make([]Role, 0, len(b.roles))
	for _, r := range b.roles {
		roles = append(roles, r)
	}

	sort.Slice(roles, func(i, j int) bool { return roles[i].RoleName < roles[j].RoleName })

	return roles, nil
}

// GetRole retrieves a single IAM role by name.
func (b *InMemoryBackend) GetRole(roleName string) (*Role, error) {
	b.mu.RLock()
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
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.policies[policyName]; exists {
		return nil, fmt.Errorf("%w: policy %q already exists", ErrPolicyAlreadyExists, policyName)
	}

	p := normPath(path)
	pol := Policy{
		PolicyName:     policyName,
		PolicyID:       newID("ANPA"),
		Arn:            fmt.Sprintf("arn:aws:iam::%s:policy%s%s", IAMAccountID, p, policyName),
		Path:           p,
		PolicyDocument: policyDocument,
		CreateDate:     time.Now().UTC(),
	}
	b.policies[policyName] = pol

	return &pol, nil
}

// DeletePolicy deletes an IAM policy by ARN.
func (b *InMemoryBackend) DeletePolicy(policyArn string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	for name, p := range b.policies {
		if p.Arn == policyArn {
			delete(b.policies, name)

			return nil
		}
	}

	return fmt.Errorf("%w: policy %q not found", ErrPolicyNotFound, policyArn)
}

// ListPolicies returns all IAM policies sorted by name.
func (b *InMemoryBackend) ListPolicies() ([]Policy, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	policies := make([]Policy, 0, len(b.policies))
	for _, p := range b.policies {
		policies = append(policies, p)
	}

	sort.Slice(policies, func(i, j int) bool { return policies[i].PolicyName < policies[j].PolicyName })

	return policies, nil
}

// AttachUserPolicy attaches a policy to a user (stub — no enforcement).
func (b *InMemoryBackend) AttachUserPolicy(userName, _ string) error {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, exists := b.users[userName]; !exists {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	return nil
}

// AttachRolePolicy attaches a policy to a role (stub — no enforcement).
func (b *InMemoryBackend) AttachRolePolicy(roleName, _ string) error {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, exists := b.roles[roleName]; !exists {
		return fmt.Errorf("%w: role %q not found", ErrRoleNotFound, roleName)
	}

	return nil
}

// ---- Groups ----

// CreateGroup creates a new IAM group.
func (b *InMemoryBackend) CreateGroup(groupName, path string) (*Group, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.groups[groupName]; exists {
		return nil, fmt.Errorf("%w: group %q already exists", ErrGroupAlreadyExists, groupName)
	}

	p := normPath(path)
	g := Group{
		GroupName:  groupName,
		GroupID:    newID("AGPA"),
		Arn:        fmt.Sprintf("arn:aws:iam::%s:group%s%s", IAMAccountID, p, groupName),
		Path:       p,
		CreateDate: time.Now().UTC(),
	}
	b.groups[groupName] = g

	return &g, nil
}

// DeleteGroup deletes an IAM group by name.
func (b *InMemoryBackend) DeleteGroup(groupName string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.groups[groupName]; !exists {
		return fmt.Errorf("%w: group %q not found", ErrGroupNotFound, groupName)
	}

	delete(b.groups, groupName)

	return nil
}

// AddUserToGroup adds a user to an IAM group (stub — no membership tracking).
func (b *InMemoryBackend) AddUserToGroup(groupName, userName string) error {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, exists := b.groups[groupName]; !exists {
		return fmt.Errorf("%w: group %q not found", ErrGroupNotFound, groupName)
	}

	if _, exists := b.users[userName]; !exists {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	return nil
}

// ---- Access Keys ----

// CreateAccessKey creates a new access key for an IAM user.
func (b *InMemoryBackend) CreateAccessKey(userName string) (*AccessKey, error) {
	b.mu.Lock()
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
	b.mu.Lock()
	defer b.mu.Unlock()

	ak, exists := b.accessKeys[accessKeyID]
	if !exists || ak.UserName != userName {
		return fmt.Errorf("%w: access key %q not found for user %q", ErrAccessKeyNotFound, accessKeyID, userName)
	}

	delete(b.accessKeys, accessKeyID)

	return nil
}

// ListAccessKeys returns all access keys for an IAM user.
func (b *InMemoryBackend) ListAccessKeys(userName string) ([]AccessKey, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, exists := b.users[userName]; !exists {
		return nil, fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	keys := make([]AccessKey, 0)
	for _, ak := range b.accessKeys {
		if ak.UserName == userName {
			keys = append(keys, ak)
		}
	}

	sort.Slice(keys, func(i, j int) bool { return keys[i].AccessKeyID < keys[j].AccessKeyID })

	return keys, nil
}

// ---- Instance Profiles ----

// CreateInstanceProfile creates a new IAM instance profile.
func (b *InMemoryBackend) CreateInstanceProfile(name, path string) (*InstanceProfile, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.instanceProfiles[name]; exists {
		return nil, fmt.Errorf("%w: instance profile %q already exists", ErrInstanceProfileAlreadyExists, name)
	}

	p := normPath(path)
	ip := InstanceProfile{
		InstanceProfileName: name,
		InstanceProfileID:   newID("AIPA"),
		Arn:                 fmt.Sprintf("arn:aws:iam::%s:instance-profile%s%s", IAMAccountID, p, name),
		Path:                p,
		Roles:               []string{},
		CreateDate:          time.Now().UTC(),
	}
	b.instanceProfiles[name] = ip

	return &ip, nil
}

// DeleteInstanceProfile deletes an IAM instance profile by name.
func (b *InMemoryBackend) DeleteInstanceProfile(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.instanceProfiles[name]; !exists {
		return fmt.Errorf("%w: instance profile %q not found", ErrInstanceProfileNotFound, name)
	}

	delete(b.instanceProfiles, name)

	return nil
}

// ListInstanceProfiles returns all IAM instance profiles sorted by name.
func (b *InMemoryBackend) ListInstanceProfiles() ([]InstanceProfile, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	profiles := make([]InstanceProfile, 0, len(b.instanceProfiles))
	for _, ip := range b.instanceProfiles {
		profiles = append(profiles, ip)
	}

	sort.Slice(profiles, func(i, j int) bool {
		return profiles[i].InstanceProfileName < profiles[j].InstanceProfileName
	})

	return profiles, nil
}

// ---- Dashboard helpers ----

// ListAllUsers returns all users (for dashboard).
func (b *InMemoryBackend) ListAllUsers() []User {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return sortedUsers(b.users)
}

// ListAllRoles returns all roles (for dashboard).
func (b *InMemoryBackend) ListAllRoles() []Role {
	b.mu.RLock()
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
	b.mu.RLock()
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
	b.mu.RLock()
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
	b.mu.RLock()
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
	b.mu.RLock()
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
