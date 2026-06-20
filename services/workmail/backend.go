package workmail

import (
	"errors"
	"fmt"
	"net"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = errors.New("EntityNotFoundException")
	// ErrConflict is returned when a resource already exists.
	ErrConflict = errors.New("EntityAlreadyExistsException")
	// ErrValidation is returned for invalid request parameters.
	ErrValidation = errors.New("InvalidParameterException")
	// ErrLimitExceeded is returned when resource limits are hit.
	ErrLimitExceeded = errors.New("LimitExceededException")
	// ErrMailDomainState is returned for domain state issues.
	ErrMailDomainState = errors.New("MailDomainStateException")
	// ErrEntityState is returned when an operation violates entity state constraints.
	ErrEntityState = errors.New("EntityStateException")
)

const (
	stateEnabled  = "ENABLED"
	stateDisabled = "DISABLED"
	stateDeleted  = "DELETED"
	stateActive   = "ACTIVE"

	memberTypeUser  = "USER"
	memberTypeGroup = "GROUP"

	defaultMailboxQuota = int32(50000)

	effectAllow = "ALLOW"
	effectDeny  = "DENY"
)

// trackedAlias stores an alias along with its entity reference for conflict detection.
type trackedAlias struct {
	orgID    string
	entityID string
}

// InMemoryBackend stores WorkMail state in memory.
type InMemoryBackend struct {
	resources             map[string]map[string]*Resource
	resourcesByEmail      map[string]map[string]string
	mailboxQuotas         map[string]map[string]int32
	organizations         map[string]*Organization
	orgsByAlias           map[string]string
	users                 map[string]map[string]*User
	usersByEmail          map[string]map[string]string
	groups                map[string]map[string]*Group
	groupsByEmail         map[string]map[string]string
	groupMembers          map[string]map[string]map[string]bool
	tags                  map[string][]Tag
	delegates             map[string]map[string]map[string]bool
	impersonation         map[string]map[string]*ImpersonationRole
	aliases               map[string]map[string][]string
	globalAliases         map[string]*trackedAlias
	permissions           map[string]map[string]map[string]*Permission
	mailDomains           map[string]map[string]*MailDomain
	accessRules           map[string]map[string]*AccessControlRule
	availabilityConfigs   map[string]map[string]*AvailabilityConfiguration
	mobileDeviceRules     map[string]map[string]*MobileDeviceAccessRule
	mobileDeviceOverrides map[string]map[string]*MobileDeviceAccessOverride // orgID -> "userID:deviceID" -> override
	emailMonitoring       map[string]*EmailMonitoringConfiguration
	inboundDmarc          map[string]bool
	retentionPolicies     map[string]*RetentionPolicy
	exportJobs            map[string]map[string]*MailboxExportJob
	identityCenterApps    map[string]string // ARN -> name
	idpConfig             map[string]*IdentityProviderConfiguration
	personalTokens        map[string]map[string]*PersonalAccessToken
	accountID             string
	region                string
	mu                    sync.RWMutex
}

// NewInMemoryBackend creates a new in-memory WorkMail backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		accountID:             accountID,
		region:                region,
		organizations:         make(map[string]*Organization),
		orgsByAlias:           make(map[string]string),
		users:                 make(map[string]map[string]*User),
		usersByEmail:          make(map[string]map[string]string),
		groups:                make(map[string]map[string]*Group),
		groupsByEmail:         make(map[string]map[string]string),
		groupMembers:          make(map[string]map[string]map[string]bool),
		resources:             make(map[string]map[string]*Resource),
		resourcesByEmail:      make(map[string]map[string]string),
		delegates:             make(map[string]map[string]map[string]bool),
		aliases:               make(map[string]map[string][]string),
		globalAliases:         make(map[string]*trackedAlias),
		permissions:           make(map[string]map[string]map[string]*Permission),
		mailDomains:           make(map[string]map[string]*MailDomain),
		accessRules:           make(map[string]map[string]*AccessControlRule),
		impersonation:         make(map[string]map[string]*ImpersonationRole),
		tags:                  make(map[string][]Tag),
		mailboxQuotas:         make(map[string]map[string]int32),
		availabilityConfigs:   make(map[string]map[string]*AvailabilityConfiguration),
		mobileDeviceRules:     make(map[string]map[string]*MobileDeviceAccessRule),
		mobileDeviceOverrides: make(map[string]map[string]*MobileDeviceAccessOverride),
		emailMonitoring:       make(map[string]*EmailMonitoringConfiguration),
		inboundDmarc:          make(map[string]bool),
		retentionPolicies:     make(map[string]*RetentionPolicy),
		exportJobs:            make(map[string]map[string]*MailboxExportJob),
		identityCenterApps:    make(map[string]string),
		idpConfig:             make(map[string]*IdentityProviderConfiguration),
		personalTokens:        make(map[string]map[string]*PersonalAccessToken),
	}
}

// AccountID returns the configured account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Region returns the configured region.
func (b *InMemoryBackend) Region() string { return b.region }

// Reset clears all stored state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.organizations = make(map[string]*Organization)
	b.orgsByAlias = make(map[string]string)
	b.users = make(map[string]map[string]*User)
	b.usersByEmail = make(map[string]map[string]string)
	b.groups = make(map[string]map[string]*Group)
	b.groupsByEmail = make(map[string]map[string]string)
	b.groupMembers = make(map[string]map[string]map[string]bool)
	b.resources = make(map[string]map[string]*Resource)
	b.resourcesByEmail = make(map[string]map[string]string)
	b.delegates = make(map[string]map[string]map[string]bool)
	b.aliases = make(map[string]map[string][]string)
	b.globalAliases = make(map[string]*trackedAlias)
	b.permissions = make(map[string]map[string]map[string]*Permission)
	b.mailDomains = make(map[string]map[string]*MailDomain)
	b.accessRules = make(map[string]map[string]*AccessControlRule)
	b.impersonation = make(map[string]map[string]*ImpersonationRole)
	b.tags = make(map[string][]Tag)
	b.mailboxQuotas = make(map[string]map[string]int32)
	b.availabilityConfigs = make(map[string]map[string]*AvailabilityConfiguration)
	b.mobileDeviceRules = make(map[string]map[string]*MobileDeviceAccessRule)
	b.mobileDeviceOverrides = make(map[string]map[string]*MobileDeviceAccessOverride)
	b.emailMonitoring = make(map[string]*EmailMonitoringConfiguration)
	b.inboundDmarc = make(map[string]bool)
	b.retentionPolicies = make(map[string]*RetentionPolicy)
	b.exportJobs = make(map[string]map[string]*MailboxExportJob)
	b.identityCenterApps = make(map[string]string)
	b.idpConfig = make(map[string]*IdentityProviderConfiguration)
	b.personalTokens = make(map[string]map[string]*PersonalAccessToken)
}

func newID() string { return uuid.New().String() }

func (b *InMemoryBackend) orgARN(orgID string) string {
	return arn.Build("workmail", b.region, b.accountID, "organization/"+orgID)
}

func (b *InMemoryBackend) entityARN(orgID, entityType, entityID string) string {
	return arn.Build("workmail", b.region, b.accountID,
		fmt.Sprintf("organization/%s/%s/%s", orgID, entityType, entityID))
}

func (b *InMemoryBackend) ensureOrgMaps(orgID string) {
	if b.users[orgID] == nil {
		b.users[orgID] = make(map[string]*User)
		b.usersByEmail[orgID] = make(map[string]string)
		b.groups[orgID] = make(map[string]*Group)
		b.groupsByEmail[orgID] = make(map[string]string)
		b.groupMembers[orgID] = make(map[string]map[string]bool)
		b.resources[orgID] = make(map[string]*Resource)
		b.resourcesByEmail[orgID] = make(map[string]string)
		b.delegates[orgID] = make(map[string]map[string]bool)
		b.aliases[orgID] = make(map[string][]string)
		b.permissions[orgID] = make(map[string]map[string]*Permission)
		b.mailDomains[orgID] = make(map[string]*MailDomain)
		b.accessRules[orgID] = make(map[string]*AccessControlRule)
		b.impersonation[orgID] = make(map[string]*ImpersonationRole)
		b.mailboxQuotas[orgID] = make(map[string]int32)
		b.availabilityConfigs[orgID] = make(map[string]*AvailabilityConfiguration)
		b.mobileDeviceRules[orgID] = make(map[string]*MobileDeviceAccessRule)
		b.mobileDeviceOverrides[orgID] = make(map[string]*MobileDeviceAccessOverride)
		b.exportJobs[orgID] = make(map[string]*MailboxExportJob)
		b.personalTokens[orgID] = make(map[string]*PersonalAccessToken)
	}
}

// --- Organizations ---

// CreateOrganization creates a new WorkMail organization.
func (b *InMemoryBackend) CreateOrganization(alias string, domains []string) (*Organization, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if alias == "" {
		return nil, fmt.Errorf("%w: Alias is required", ErrValidation)
	}
	if _, exists := b.orgsByAlias[alias]; exists {
		return nil, fmt.Errorf("%w: organization with alias %q already exists", ErrConflict, alias)
	}

	orgID := "m-" + strings.ReplaceAll(newID(), "-", "")[:20]
	defaultDomain := alias + ".awsapps.com"
	now := time.Now().UTC()

	org := &Organization{
		CreatedAt:         now,
		CompletedDate:     now,
		OrgID:             orgID,
		Alias:             alias,
		ARN:               b.orgARN(orgID),
		State:             stateActive,
		DirectoryID:       "d-" + strings.ReplaceAll(newID(), "-", "")[:10],
		DirectoryType:     "SimpleAD",
		DefaultMailDomain: defaultDomain,
	}

	b.organizations[orgID] = org
	b.orgsByAlias[alias] = orgID
	b.ensureOrgMaps(orgID)

	// Register default domain.
	b.mailDomains[orgID][defaultDomain] = &MailDomain{
		DomainName:                  defaultDomain,
		IsDefault:                   true,
		IsTestDomain:                true,
		OwnershipVerificationStatus: "VERIFIED",
	}

	for _, d := range domains {
		if d == "" {
			continue
		}
		b.mailDomains[orgID][d] = &MailDomain{
			DomainName:                  d,
			IsDefault:                   false,
			IsTestDomain:                false,
			OwnershipVerificationStatus: "VERIFIED",
		}
	}

	return org, nil
}

// DescribeOrganization returns details about an organization.
func (b *InMemoryBackend) DescribeOrganization(orgID string) (*Organization, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	org, ok := b.organizations[orgID]
	if !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	return org, nil
}

// DeleteOrganization removes a WorkMail organization.
func (b *InMemoryBackend) DeleteOrganization(orgID string, _ bool) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	org, ok := b.organizations[orgID]
	if !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	delete(b.orgsByAlias, org.Alias)
	delete(b.organizations, orgID)
	delete(b.users, orgID)
	delete(b.usersByEmail, orgID)
	delete(b.groups, orgID)
	delete(b.groupsByEmail, orgID)
	delete(b.groupMembers, orgID)
	delete(b.resources, orgID)
	delete(b.resourcesByEmail, orgID)
	delete(b.delegates, orgID)
	delete(b.aliases, orgID)
	delete(b.permissions, orgID)
	delete(b.mailDomains, orgID)
	delete(b.accessRules, orgID)
	delete(b.impersonation, orgID)
	delete(b.mailboxQuotas, orgID)

	return nil
}

// ListOrganizations returns a paginated list of organizations.
func (b *InMemoryBackend) ListOrganizations(maxResults int32, nextToken string) ([]*OrgSummary, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	orgs := make([]*OrgSummary, 0, len(b.organizations))
	for _, o := range b.organizations {
		orgs = append(orgs, &OrgSummary{
			OrgID:             o.OrgID,
			Alias:             o.Alias,
			DefaultMailDomain: o.DefaultMailDomain,
			State:             o.State,
			ErrorMessage:      o.ErrorMessage,
		})
	}
	sort.Slice(orgs, func(i, j int) bool { return orgs[i].Alias < orgs[j].Alias })

	items, next := paginate(orgs, maxResults, nextToken)

	return items, next, nil
}

// --- Users ---

// CreateUser creates a new WorkMail user.
func (b *InMemoryBackend) CreateUser(orgID, name, displayName, password, role string) (*User, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	org, ok := b.organizations[orgID]
	if !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	_ = org

	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}
	validRoles := map[string]bool{"USER": true, "RESOURCE": true, "SYSTEM_USER": true}
	if role != "" && !validRoles[role] {
		return nil, fmt.Errorf("%w: invalid Role %q, must be USER, RESOURCE, or SYSTEM_USER", ErrValidation, role)
	}

	b.ensureOrgMaps(orgID)
	for _, u := range b.users[orgID] {
		if u.Name == name {
			return nil, fmt.Errorf("%w: user %q already exists", ErrConflict, name)
		}
	}

	userID := newID()
	now := time.Now().UTC()
	_ = password // stored in real AWS but not needed for simulation

	u := &User{
		CreatedAt:   now,
		UserID:      userID,
		Name:        name,
		DisplayName: displayName,
		Role:        role,
		State:       stateDisabled,
		ARN:         b.entityARN(orgID, "user", userID),
	}

	b.users[orgID][userID] = u

	return u, nil
}

// DescribeUser returns details about a user.
func (b *InMemoryBackend) DescribeUser(orgID, entityID string) (*User, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, ok := b.organizations[orgID]; !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	b.ensureOrgMapsRO(orgID)
	u := b.findUser(orgID, entityID)
	if u == nil {
		return nil, fmt.Errorf("%w: user %q not found", ErrNotFound, entityID)
	}

	return u, nil
}

func (b *InMemoryBackend) ensureOrgMapsRO(_ string) {
	// no-op: called under read lock — maps were created at org creation
}

func (b *InMemoryBackend) findUser(orgID, entityID string) *User {
	if users, ok := b.users[orgID]; ok {
		if u, found := users[entityID]; found {
			return u
		}
		// search by name
		for _, u := range users {
			if u.Name == entityID {
				return u
			}
		}
	}

	return nil
}

// UpdateUser updates display name and name fields.
func (b *InMemoryBackend) UpdateUser(orgID, entityID, displayName, firstName, lastName string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.organizations[orgID]; !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	u := b.findUser(orgID, entityID)
	if u == nil {
		return fmt.Errorf("%w: user %q not found", ErrNotFound, entityID)
	}

	if displayName != "" {
		u.DisplayName = displayName
	}
	if firstName != "" {
		u.FirstName = firstName
	}
	if lastName != "" {
		u.LastName = lastName
	}

	return nil
}

// DeleteUser removes a user.
func (b *InMemoryBackend) DeleteUser(orgID, entityID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.organizations[orgID]; !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	u := b.findUser(orgID, entityID)
	if u == nil {
		return fmt.Errorf("%w: user %q not found", ErrNotFound, entityID)
	}

	if u.State == stateEnabled {
		return fmt.Errorf("%w: user %q is in ENABLED state and cannot be deleted; call DeregisterFromWorkMail first", ErrEntityState, entityID)
	}

	actualID := u.UserID
	if u.Email != "" {
		delete(b.usersByEmail[orgID], u.Email)
	}
	delete(b.users[orgID], actualID)

	return nil
}

// ListUsers returns a paginated list of users.
func (b *InMemoryBackend) ListUsers(orgID string, maxResults int32, nextToken string) ([]*UserSummary, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, ok := b.organizations[orgID]; !ok {
		return nil, "", fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	users := make([]*UserSummary, 0)
	for _, u := range b.users[orgID] {
		users = append(users, &UserSummary{
			UserID:      u.UserID,
			Name:        u.Name,
			Email:       u.Email,
			DisplayName: u.DisplayName,
			State:       u.State,
			Role:        u.Role,
		})
	}
	sort.Slice(users, func(i, j int) bool { return users[i].Name < users[j].Name })

	items, next := paginate(users, maxResults, nextToken)

	return items, next, nil
}

// RegisterToWorkMail assigns an email address to a user/group/resource.
func (b *InMemoryBackend) RegisterToWorkMail(orgID, entityID, email string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.organizations[orgID]; !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	if ta, exists := b.globalAliases[email]; exists && ta.orgID == orgID {
		return fmt.Errorf("%w: email %q already in use", ErrConflict, email)
	}

	now := time.Now().UTC()

	if u := b.findUser(orgID, entityID); u != nil {
		if u.Email != "" {
			delete(b.usersByEmail[orgID], u.Email)
			delete(b.globalAliases, u.Email)
		}
		u.Email = email
		u.State = stateEnabled
		u.EnabledDate = now
		b.usersByEmail[orgID][email] = u.UserID
		b.globalAliases[email] = &trackedAlias{orgID: orgID, entityID: u.UserID}

		return nil
	}

	if g := b.findGroup(orgID, entityID); g != nil {
		if g.Email != "" {
			delete(b.groupsByEmail[orgID], g.Email)
			delete(b.globalAliases, g.Email)
		}
		g.Email = email
		g.State = stateEnabled
		g.EnabledDate = now
		b.groupsByEmail[orgID][email] = g.GroupID
		b.globalAliases[email] = &trackedAlias{orgID: orgID, entityID: g.GroupID}

		return nil
	}

	if r := b.findResource(orgID, entityID); r != nil {
		if r.Email != "" {
			delete(b.resourcesByEmail[orgID], r.Email)
			delete(b.globalAliases, r.Email)
		}
		r.Email = email
		r.State = stateEnabled
		r.EnabledDate = now
		b.resourcesByEmail[orgID][email] = r.ResourceID
		b.globalAliases[email] = &trackedAlias{orgID: orgID, entityID: r.ResourceID}

		return nil
	}

	return fmt.Errorf("%w: entity %q not found", ErrNotFound, entityID)
}

// DeregisterFromWorkMail removes an email address assignment.
func (b *InMemoryBackend) DeregisterFromWorkMail(orgID, entityID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.organizations[orgID]; !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	now := time.Now().UTC()

	if u := b.findUser(orgID, entityID); u != nil {
		if u.Email != "" {
			delete(b.usersByEmail[orgID], u.Email)
			delete(b.globalAliases, u.Email)
		}
		u.Email = ""
		u.State = stateDisabled
		u.DisabledDate = now

		return nil
	}

	if g := b.findGroup(orgID, entityID); g != nil {
		if g.Email != "" {
			delete(b.groupsByEmail[orgID], g.Email)
			delete(b.globalAliases, g.Email)
		}
		g.Email = ""
		g.State = stateDisabled
		g.DisabledDate = now

		return nil
	}

	if r := b.findResource(orgID, entityID); r != nil {
		if r.Email != "" {
			delete(b.resourcesByEmail[orgID], r.Email)
			delete(b.globalAliases, r.Email)
		}
		r.Email = ""
		r.State = stateDisabled
		r.DisabledDate = now

		return nil
	}

	return fmt.Errorf("%w: entity %q not found", ErrNotFound, entityID)
}

// ResetPassword updates the user's password (simulated — no-op).
func (b *InMemoryBackend) ResetPassword(orgID, userID, _ string) error {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, ok := b.organizations[orgID]; !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	u := b.findUser(orgID, userID)
	if u == nil {
		return fmt.Errorf("%w: user %q not found", ErrNotFound, userID)
	}

	return nil
}

// GetMailboxDetails returns mailbox quota and usage.
func (b *InMemoryBackend) GetMailboxDetails(orgID, userID string) (*MailboxDetails, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, ok := b.organizations[orgID]; !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	u := b.findUser(orgID, userID)
	if u == nil {
		return nil, fmt.Errorf("%w: user %q not found", ErrNotFound, userID)
	}

	quota := defaultMailboxQuota
	if q, ok := b.mailboxQuotas[orgID][u.UserID]; ok {
		quota = q
	}

	return &MailboxDetails{MailboxQuota: quota, MailboxSize: 0}, nil
}

// UpdateMailboxQuota updates the mailbox quota for a user.
func (b *InMemoryBackend) UpdateMailboxQuota(orgID, userID string, quota int32) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.organizations[orgID]; !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	u := b.findUser(orgID, userID)
	if u == nil {
		return fmt.Errorf("%w: user %q not found", ErrNotFound, userID)
	}

	b.mailboxQuotas[orgID][u.UserID] = quota

	return nil
}

// UpdatePrimaryEmailAddress updates the primary email of an entity.
func (b *InMemoryBackend) UpdatePrimaryEmailAddress(orgID, entityID, email string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.organizations[orgID]; !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	if u := b.findUser(orgID, entityID); u != nil {
		if u.Email != "" {
			delete(b.usersByEmail[orgID], u.Email)
			delete(b.globalAliases, u.Email)
		}
		u.Email = email
		b.usersByEmail[orgID][email] = u.UserID
		b.globalAliases[email] = &trackedAlias{orgID: orgID, entityID: u.UserID}

		return nil
	}

	if g := b.findGroup(orgID, entityID); g != nil {
		if g.Email != "" {
			delete(b.groupsByEmail[orgID], g.Email)
			delete(b.globalAliases, g.Email)
		}
		g.Email = email
		b.groupsByEmail[orgID][email] = g.GroupID
		b.globalAliases[email] = &trackedAlias{orgID: orgID, entityID: g.GroupID}

		return nil
	}

	if r := b.findResource(orgID, entityID); r != nil {
		if r.Email != "" {
			delete(b.resourcesByEmail[orgID], r.Email)
			delete(b.globalAliases, r.Email)
		}
		r.Email = email
		b.resourcesByEmail[orgID][email] = r.ResourceID
		b.globalAliases[email] = &trackedAlias{orgID: orgID, entityID: r.ResourceID}

		return nil
	}

	return fmt.Errorf("%w: entity %q not found", ErrNotFound, entityID)
}

// --- Groups ---

func (b *InMemoryBackend) findGroup(orgID, entityID string) *Group {
	if groups, ok := b.groups[orgID]; ok {
		if g, found := groups[entityID]; found {
			return g
		}
		for _, g := range groups {
			if g.Name == entityID {
				return g
			}
		}
	}

	return nil
}

// CreateGroup creates a new WorkMail group.
func (b *InMemoryBackend) CreateGroup(orgID, name string, hidden bool) (*Group, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.organizations[orgID]; !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	b.ensureOrgMaps(orgID)
	for _, g := range b.groups[orgID] {
		if g.Name == name {
			return nil, fmt.Errorf("%w: group %q already exists", ErrConflict, name)
		}
	}

	groupID := newID()
	now := time.Now().UTC()

	g := &Group{
		CreatedAt: now,
		GroupID:   groupID,
		Name:      name,
		State:     stateDisabled,
		ARN:       b.entityARN(orgID, "group", groupID),
		Hidden:    hidden,
	}

	b.groups[orgID][groupID] = g
	b.groupMembers[orgID][groupID] = make(map[string]bool)

	return g, nil
}

// DescribeGroup returns group details.
func (b *InMemoryBackend) DescribeGroup(orgID, entityID string) (*Group, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, ok := b.organizations[orgID]; !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	g := b.findGroup(orgID, entityID)
	if g == nil {
		return nil, fmt.Errorf("%w: group %q not found", ErrNotFound, entityID)
	}

	return g, nil
}

// UpdateGroup updates a group.
func (b *InMemoryBackend) UpdateGroup(orgID, entityID string, hidden bool) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.organizations[orgID]; !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	g := b.findGroup(orgID, entityID)
	if g == nil {
		return fmt.Errorf("%w: group %q not found", ErrNotFound, entityID)
	}
	g.Hidden = hidden

	return nil
}

// DeleteGroup deletes a group.
func (b *InMemoryBackend) DeleteGroup(orgID, entityID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.organizations[orgID]; !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	g := b.findGroup(orgID, entityID)
	if g == nil {
		return fmt.Errorf("%w: group %q not found", ErrNotFound, entityID)
	}

	if g.State == stateEnabled {
		return fmt.Errorf("%w: group %q is in ENABLED state and cannot be deleted; call DeregisterFromWorkMail first", ErrEntityState, entityID)
	}

	if g.Email != "" {
		delete(b.groupsByEmail[orgID], g.Email)
		delete(b.globalAliases, g.Email)
	}
	delete(b.groups[orgID], g.GroupID)
	delete(b.groupMembers[orgID], g.GroupID)

	return nil
}

// ListGroups returns a paginated list of groups.
func (b *InMemoryBackend) ListGroups(
	orgID string,
	maxResults int32,
	nextToken string,
) ([]*GroupSummary, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, ok := b.organizations[orgID]; !ok {
		return nil, "", fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	gs := make([]*GroupSummary, 0, len(b.groups[orgID]))
	for _, g := range b.groups[orgID] {
		gs = append(gs, &GroupSummary{GroupID: g.GroupID, Name: g.Name, Email: g.Email, State: g.State})
	}
	sort.Slice(gs, func(i, j int) bool { return gs[i].Name < gs[j].Name })

	items, next := paginate(gs, maxResults, nextToken)

	return items, next, nil
}

// AssociateMemberToGroup adds a member to a group.
func (b *InMemoryBackend) AssociateMemberToGroup(orgID, groupID, memberID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.organizations[orgID]; !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	g := b.findGroup(orgID, groupID)
	if g == nil {
		return fmt.Errorf("%w: group %q not found", ErrNotFound, groupID)
	}

	// member must exist as user or group
	if b.findUser(orgID, memberID) == nil && b.findGroup(orgID, memberID) == nil {
		return fmt.Errorf("%w: member entity %q not found", ErrNotFound, memberID)
	}

	if b.groupMembers[orgID][g.GroupID] == nil {
		b.groupMembers[orgID][g.GroupID] = make(map[string]bool)
	}
	b.groupMembers[orgID][g.GroupID][memberID] = true

	return nil
}

// DisassociateMemberFromGroup removes a member from a group.
func (b *InMemoryBackend) DisassociateMemberFromGroup(orgID, groupID, memberID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.organizations[orgID]; !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	g := b.findGroup(orgID, groupID)
	if g == nil {
		return fmt.Errorf("%w: group %q not found", ErrNotFound, groupID)
	}

	members := b.groupMembers[orgID][g.GroupID]
	if members == nil || !members[memberID] {
		return fmt.Errorf("%w: member %q not in group", ErrNotFound, memberID)
	}
	delete(members, memberID)

	return nil
}

// ListGroupMembers returns members of a group.
func (b *InMemoryBackend) ListGroupMembers(
	orgID, groupID string,
	maxResults int32,
	nextToken string,
) ([]*Member, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, ok := b.organizations[orgID]; !ok {
		return nil, "", fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	g := b.findGroup(orgID, groupID)
	if g == nil {
		return nil, "", fmt.Errorf("%w: group %q not found", ErrNotFound, groupID)
	}

	members := make([]*Member, 0)
	for memberID := range b.groupMembers[orgID][g.GroupID] {
		m := &Member{MemberID: memberID, State: stateEnabled}
		if u := b.findUser(orgID, memberID); u != nil {
			m.Name = u.Name
			m.MemberType = memberTypeUser
		} else if grp := b.findGroup(orgID, memberID); grp != nil {
			m.Name = grp.Name
			m.MemberType = memberTypeGroup
		}
		members = append(members, m)
	}
	sort.Slice(members, func(i, j int) bool { return members[i].MemberID < members[j].MemberID })

	items, next := paginate(members, maxResults, nextToken)

	return items, next, nil
}

// ListGroupsForEntity returns groups containing the given entity.
func (b *InMemoryBackend) ListGroupsForEntity(
	orgID, entityID string,
	maxResults int32,
	nextToken string,
) ([]*GroupSummary, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, ok := b.organizations[orgID]; !ok {
		return nil, "", fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	gs := make([]*GroupSummary, 0)
	for _, g := range b.groups[orgID] {
		if b.groupMembers[orgID][g.GroupID][entityID] {
			gs = append(gs, &GroupSummary{GroupID: g.GroupID, Name: g.Name, Email: g.Email, State: g.State})
		}
	}
	sort.Slice(gs, func(i, j int) bool { return gs[i].Name < gs[j].Name })

	items, next := paginate(gs, maxResults, nextToken)

	return items, next, nil
}

// --- Resources ---

func (b *InMemoryBackend) findResource(orgID, entityID string) *Resource {
	if resources, ok := b.resources[orgID]; ok {
		if r, found := resources[entityID]; found {
			return r
		}
		for _, r := range resources {
			if r.Name == entityID {
				return r
			}
		}
	}

	return nil
}

// CreateResource creates a new WorkMail resource.
func (b *InMemoryBackend) CreateResource(orgID, name, resourceType, description string) (*Resource, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.organizations[orgID]; !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}
	validTypes := map[string]bool{"ROOM": true, "EQUIPMENT": true}
	if resourceType != "" && !validTypes[resourceType] {
		return nil, fmt.Errorf("%w: invalid Type %q, must be ROOM or EQUIPMENT", ErrValidation, resourceType)
	}

	b.ensureOrgMaps(orgID)
	for _, r := range b.resources[orgID] {
		if r.Name == name {
			return nil, fmt.Errorf("%w: resource %q already exists", ErrConflict, name)
		}
	}

	resourceID := newID()
	now := time.Now().UTC()

	r := &Resource{
		CreatedAt:    now,
		ResourceID:   resourceID,
		Name:         name,
		ResourceType: resourceType,
		Description:  description,
		State:        stateDisabled,
		ARN:          b.entityARN(orgID, "resource", resourceID),
	}

	b.resources[orgID][resourceID] = r
	b.delegates[orgID][resourceID] = make(map[string]bool)

	return r, nil
}

// DescribeResource returns resource details.
func (b *InMemoryBackend) DescribeResource(orgID, entityID string) (*Resource, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, ok := b.organizations[orgID]; !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	r := b.findResource(orgID, entityID)
	if r == nil {
		return nil, fmt.Errorf("%w: resource %q not found", ErrNotFound, entityID)
	}

	return r, nil
}

// UpdateResource updates resource fields.
func (b *InMemoryBackend) UpdateResource(orgID, entityID, name, description string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.organizations[orgID]; !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	r := b.findResource(orgID, entityID)
	if r == nil {
		return fmt.Errorf("%w: resource %q not found", ErrNotFound, entityID)
	}

	if name != "" {
		r.Name = name
	}
	if description != "" {
		r.Description = description
	}

	return nil
}

// DeleteResource removes a resource.
func (b *InMemoryBackend) DeleteResource(orgID, entityID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.organizations[orgID]; !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	r := b.findResource(orgID, entityID)
	if r == nil {
		return fmt.Errorf("%w: resource %q not found", ErrNotFound, entityID)
	}

	if r.State == stateEnabled {
		return fmt.Errorf("%w: resource %q is in ENABLED state and cannot be deleted; call DeregisterFromWorkMail first", ErrEntityState, entityID)
	}

	if r.Email != "" {
		delete(b.resourcesByEmail[orgID], r.Email)
		delete(b.globalAliases, r.Email)
	}
	delete(b.resources[orgID], r.ResourceID)
	delete(b.delegates[orgID], r.ResourceID)

	return nil
}

// ListResources returns a paginated list of resources.
func (b *InMemoryBackend) ListResources(
	orgID string,
	maxResults int32,
	nextToken string,
) ([]*ResourceSummary, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, ok := b.organizations[orgID]; !ok {
		return nil, "", fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	rs := make([]*ResourceSummary, 0, len(b.resources[orgID]))
	for _, r := range b.resources[orgID] {
		rs = append(rs, &ResourceSummary{
			ResourceID:   r.ResourceID,
			Name:         r.Name,
			Email:        r.Email,
			ResourceType: r.ResourceType,
			State:        r.State,
			Description:  r.Description,
		})
	}
	sort.Slice(rs, func(i, j int) bool { return rs[i].Name < rs[j].Name })

	items, next := paginate(rs, maxResults, nextToken)

	return items, next, nil
}

// AssociateDelegateToResource adds a delegate to a resource.
func (b *InMemoryBackend) AssociateDelegateToResource(orgID, resourceID, entityID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.organizations[orgID]; !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	r := b.findResource(orgID, resourceID)
	if r == nil {
		return fmt.Errorf("%w: resource %q not found", ErrNotFound, resourceID)
	}

	if b.delegates[orgID][r.ResourceID] == nil {
		b.delegates[orgID][r.ResourceID] = make(map[string]bool)
	}
	b.delegates[orgID][r.ResourceID][entityID] = true

	return nil
}

// DisassociateDelegateFromResource removes a delegate from a resource.
func (b *InMemoryBackend) DisassociateDelegateFromResource(orgID, resourceID, entityID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.organizations[orgID]; !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	r := b.findResource(orgID, resourceID)
	if r == nil {
		return fmt.Errorf("%w: resource %q not found", ErrNotFound, resourceID)
	}

	delegates := b.delegates[orgID][r.ResourceID]
	if delegates == nil || !delegates[entityID] {
		return fmt.Errorf("%w: delegate %q not found on resource", ErrNotFound, entityID)
	}
	delete(delegates, entityID)

	return nil
}

// ListResourceDelegates returns delegates of a resource.
func (b *InMemoryBackend) ListResourceDelegates(
	orgID, resourceID string,
	maxResults int32,
	nextToken string,
) ([]*Delegate, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, ok := b.organizations[orgID]; !ok {
		return nil, "", fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	r := b.findResource(orgID, resourceID)
	if r == nil {
		return nil, "", fmt.Errorf("%w: resource %q not found", ErrNotFound, resourceID)
	}

	delegates := make([]*Delegate, 0)
	for entityID := range b.delegates[orgID][r.ResourceID] {
		dt := memberTypeUser
		if b.findGroup(orgID, entityID) != nil {
			dt = memberTypeGroup
		}
		delegates = append(delegates, &Delegate{DelegateID: entityID, DelegateType: dt})
	}
	sort.Slice(delegates, func(i, j int) bool { return delegates[i].DelegateID < delegates[j].DelegateID })

	items, next := paginate(delegates, maxResults, nextToken)

	return items, next, nil
}

// --- Aliases ---

// CreateAlias creates an email alias for an entity.
func (b *InMemoryBackend) CreateAlias(orgID, entityID, alias string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.organizations[orgID]; !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	if ta, exists := b.globalAliases[alias]; exists && ta.orgID == orgID {
		return fmt.Errorf("%w: alias %q already in use", ErrConflict, alias)
	}

	// Verify entity exists.
	var actualID string
	if u := b.findUser(orgID, entityID); u != nil {
		actualID = u.UserID
	} else if g := b.findGroup(orgID, entityID); g != nil {
		actualID = g.GroupID
	} else if r := b.findResource(orgID, entityID); r != nil {
		actualID = r.ResourceID
	} else {
		return fmt.Errorf("%w: entity %q not found", ErrNotFound, entityID)
	}

	b.aliases[orgID][actualID] = append(b.aliases[orgID][actualID], alias)
	b.globalAliases[alias] = &trackedAlias{orgID: orgID, entityID: actualID}

	return nil
}

// DeleteAlias removes an email alias.
func (b *InMemoryBackend) DeleteAlias(orgID, entityID, alias string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.organizations[orgID]; !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	var actualID string
	if u := b.findUser(orgID, entityID); u != nil {
		actualID = u.UserID
	} else if g := b.findGroup(orgID, entityID); g != nil {
		actualID = g.GroupID
	} else if r := b.findResource(orgID, entityID); r != nil {
		actualID = r.ResourceID
	} else {
		return fmt.Errorf("%w: entity %q not found", ErrNotFound, entityID)
	}

	aliases := b.aliases[orgID][actualID]
	found := false
	newAliases := make([]string, 0, len(aliases))
	for _, a := range aliases {
		if a == alias {
			found = true

			continue
		}
		newAliases = append(newAliases, a)
	}
	if !found {
		return fmt.Errorf("%w: alias %q not found", ErrNotFound, alias)
	}
	b.aliases[orgID][actualID] = newAliases
	delete(b.globalAliases, alias)

	return nil
}

// ListAliases returns aliases for an entity.
func (b *InMemoryBackend) ListAliases(
	orgID, entityID string,
	maxResults int32,
	nextToken string,
) ([]string, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, ok := b.organizations[orgID]; !ok {
		return nil, "", fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	var actualID string
	var primaryEmail string
	if u := b.findUser(orgID, entityID); u != nil {
		actualID = u.UserID
		primaryEmail = u.Email
	} else if g := b.findGroup(orgID, entityID); g != nil {
		actualID = g.GroupID
		primaryEmail = g.Email
	} else if r := b.findResource(orgID, entityID); r != nil {
		actualID = r.ResourceID
		primaryEmail = r.Email
	} else {
		return nil, "", fmt.Errorf("%w: entity %q not found", ErrNotFound, entityID)
	}

	all := make([]string, 0)
	if primaryEmail != "" {
		all = append(all, primaryEmail)
	}
	all = append(all, b.aliases[orgID][actualID]...)
	sort.Strings(all)

	items, next := paginate(all, maxResults, nextToken)

	return items, next, nil
}

// --- Mailbox Permissions ---

// PutMailboxPermissions creates or updates mailbox permissions.
func (b *InMemoryBackend) PutMailboxPermissions(orgID, entityID, granteeID string, perms []string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.organizations[orgID]; !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	var actualID string
	if u := b.findUser(orgID, entityID); u != nil {
		actualID = u.UserID
	} else if g := b.findGroup(orgID, entityID); g != nil {
		actualID = g.GroupID
	} else if r := b.findResource(orgID, entityID); r != nil {
		actualID = r.ResourceID
	} else {
		return fmt.Errorf("%w: entity %q not found", ErrNotFound, entityID)
	}

	granteeType := memberTypeUser
	if b.findGroup(orgID, granteeID) != nil {
		granteeType = memberTypeGroup
	}

	if b.permissions[orgID][actualID] == nil {
		b.permissions[orgID][actualID] = make(map[string]*Permission)
	}
	b.permissions[orgID][actualID][granteeID] = &Permission{
		GranteeID:   granteeID,
		GranteeType: granteeType,
		Permissions: perms,
	}

	return nil
}

// DeleteMailboxPermissions removes mailbox permissions.
func (b *InMemoryBackend) DeleteMailboxPermissions(orgID, entityID, granteeID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.organizations[orgID]; !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	var actualID string
	if u := b.findUser(orgID, entityID); u != nil {
		actualID = u.UserID
	} else if g := b.findGroup(orgID, entityID); g != nil {
		actualID = g.GroupID
	} else {
		return fmt.Errorf("%w: entity %q not found", ErrNotFound, entityID)
	}

	if b.permissions[orgID][actualID] == nil || b.permissions[orgID][actualID][granteeID] == nil {
		return fmt.Errorf("%w: permission for grantee %q not found", ErrNotFound, granteeID)
	}
	delete(b.permissions[orgID][actualID], granteeID)

	return nil
}

// ListMailboxPermissions returns mailbox permissions for an entity.
func (b *InMemoryBackend) ListMailboxPermissions(
	orgID, entityID string,
	maxResults int32,
	nextToken string,
) ([]*Permission, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, ok := b.organizations[orgID]; !ok {
		return nil, "", fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	var actualID string
	if u := b.findUser(orgID, entityID); u != nil {
		actualID = u.UserID
	} else if g := b.findGroup(orgID, entityID); g != nil {
		actualID = g.GroupID
	} else if r := b.findResource(orgID, entityID); r != nil {
		actualID = r.ResourceID
	} else {
		return nil, "", fmt.Errorf("%w: entity %q not found", ErrNotFound, entityID)
	}

	perms := make([]*Permission, 0)
	for _, p := range b.permissions[orgID][actualID] {
		perms = append(perms, p)
	}
	sort.Slice(perms, func(i, j int) bool { return perms[i].GranteeID < perms[j].GranteeID })

	items, next := paginate(perms, maxResults, nextToken)

	return items, next, nil
}

// --- Mail Domains ---

// RegisterMailDomain registers a domain with the organization.
func (b *InMemoryBackend) RegisterMailDomain(orgID, domainName string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.organizations[orgID]; !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	b.ensureOrgMaps(orgID)
	if _, exists := b.mailDomains[orgID][domainName]; exists {
		return fmt.Errorf("%w: domain %q already registered", ErrConflict, domainName)
	}

	b.mailDomains[orgID][domainName] = &MailDomain{
		DomainName:                  domainName,
		IsDefault:                   false,
		IsTestDomain:                false,
		OwnershipVerificationStatus: "PENDING",
	}

	return nil
}

// DeregisterMailDomain removes a domain from the organization.
func (b *InMemoryBackend) DeregisterMailDomain(orgID, domainName string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.organizations[orgID]; !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	domain, exists := b.mailDomains[orgID][domainName]
	if !exists {
		return fmt.Errorf("%w: domain %q not found", ErrNotFound, domainName)
	}
	if domain.IsDefault {
		return fmt.Errorf("%w: cannot deregister the default domain", ErrMailDomainState)
	}
	delete(b.mailDomains[orgID], domainName)

	return nil
}

// GetMailDomain returns details about a registered domain.
func (b *InMemoryBackend) GetMailDomain(orgID, domainName string) (*MailDomain, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, ok := b.organizations[orgID]; !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	d, exists := b.mailDomains[orgID][domainName]
	if !exists {
		return nil, fmt.Errorf("%w: domain %q not found", ErrNotFound, domainName)
	}

	return d, nil
}

// ListMailDomains returns a paginated list of mail domains.
func (b *InMemoryBackend) ListMailDomains(
	orgID string,
	maxResults int32,
	nextToken string,
) ([]*MailDomainSummary, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, ok := b.organizations[orgID]; !ok {
		return nil, "", fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	domains := make([]*MailDomainSummary, 0, len(b.mailDomains[orgID]))
	for _, d := range b.mailDomains[orgID] {
		domains = append(domains, &MailDomainSummary{
			DomainName:   d.DomainName,
			IsDefault:    d.IsDefault,
			IsTestDomain: d.IsTestDomain,
		})
	}
	sort.Slice(domains, func(i, j int) bool { return domains[i].DomainName < domains[j].DomainName })

	items, next := paginate(domains, maxResults, nextToken)

	return items, next, nil
}

// UpdateDefaultMailDomain changes the default mail domain.
func (b *InMemoryBackend) UpdateDefaultMailDomain(orgID, domainName string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	org, ok := b.organizations[orgID]
	if !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	d, exists := b.mailDomains[orgID][domainName]
	if !exists {
		return fmt.Errorf("%w: domain %q not found", ErrNotFound, domainName)
	}
	// clear old default
	for _, dom := range b.mailDomains[orgID] {
		dom.IsDefault = false
	}
	d.IsDefault = true
	org.DefaultMailDomain = domainName

	return nil
}

// --- Access Control Rules ---

// PutAccessControlRule creates or updates an access control rule.
func (b *InMemoryBackend) PutAccessControlRule(
	orgID, name, effect, description string,
	ipRanges, notIPRanges []string,
	actions, notActions []string,
	userIDs, notUserIDs []string,
) (*AccessControlRule, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.organizations[orgID]; !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	b.ensureOrgMaps(orgID)

	now := time.Now().UTC()
	existing := b.accessRules[orgID][name]

	rule := &AccessControlRule{
		DateCreated:  now,
		DateModified: now,
		Name:         name,
		Effect:       effect,
		Description:  description,
		IPRanges:     ipRanges,
		NotIPRanges:  notIPRanges,
		Actions:      actions,
		NotActions:   notActions,
		UserIDs:      userIDs,
		NotUserIDs:   notUserIDs,
	}
	if existing != nil {
		rule.DateCreated = existing.DateCreated
	}

	b.accessRules[orgID][name] = rule

	return rule, nil
}

// DeleteAccessControlRule removes an access control rule.
func (b *InMemoryBackend) DeleteAccessControlRule(orgID, name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.organizations[orgID]; !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	if _, exists := b.accessRules[orgID][name]; !exists {
		return fmt.Errorf("%w: access control rule %q not found", ErrNotFound, name)
	}
	delete(b.accessRules[orgID], name)

	return nil
}

// GetAccessControlEffect evaluates access control rules.
func (b *InMemoryBackend) GetAccessControlEffect(orgID, ipAddr, action, userID string) (string, []string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, ok := b.organizations[orgID]; !ok {
		return "", nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	rules := make([]*AccessControlRule, 0, len(b.accessRules[orgID]))
	for _, r := range b.accessRules[orgID] {
		rules = append(rules, r)
	}
	// AWS evaluates rules in creation order; sort by DateCreated for determinism
	sort.Slice(rules, func(i, j int) bool {
		return rules[i].DateCreated.Before(rules[j].DateCreated)
	})

	for _, rule := range rules {
		if !ruleMatchesRequest(rule, ipAddr, action, userID) {
			continue
		}

		return rule.Effect, []string{rule.Name}, nil
	}

	return effectAllow, []string{}, nil
}

// ruleMatchesRequest returns true when ALL non-empty condition lists match.
func ruleMatchesRequest(rule *AccessControlRule, ipAddr, action, userID string) bool {
	if len(rule.IPRanges) > 0 && !matchesCIDRList(ipAddr, rule.IPRanges) {
		return false
	}
	if len(rule.NotIPRanges) > 0 && matchesCIDRList(ipAddr, rule.NotIPRanges) {
		return false
	}
	if len(rule.Actions) > 0 && !slices.Contains(rule.Actions, action) {
		return false
	}
	if len(rule.NotActions) > 0 && slices.Contains(rule.NotActions, action) {
		return false
	}
	if len(rule.UserIDs) > 0 && !slices.Contains(rule.UserIDs, userID) {
		return false
	}
	if len(rule.NotUserIDs) > 0 && slices.Contains(rule.NotUserIDs, userID) {
		return false
	}

	return true
}

func matchesCIDRList(ipAddr string, cidrs []string) bool {
	ip := net.ParseIP(ipAddr)
	if ip == nil {
		return false
	}
	for _, cidr := range cidrs {
		if !strings.Contains(cidr, "/") {
			cidr += "/32"
		}
		_, network, err := net.ParseCIDR(cidr)
		if err != nil {
			continue
		}
		if network.Contains(ip) {
			return true
		}
	}

	return false
}

// ListAccessControlRules returns all access control rules.
func (b *InMemoryBackend) ListAccessControlRules(orgID string) ([]*AccessControlRule, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, ok := b.organizations[orgID]; !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	rules := make([]*AccessControlRule, 0, len(b.accessRules[orgID]))
	for _, r := range b.accessRules[orgID] {
		rules = append(rules, r)
	}
	sort.Slice(rules, func(i, j int) bool { return rules[i].Name < rules[j].Name })

	return rules, nil
}

// --- Impersonation Roles ---

// CreateImpersonationRole creates a new impersonation role.
func (b *InMemoryBackend) CreateImpersonationRole(
	orgID, name, roleType, description string,
	rules []ImpersonationRule,
) (*ImpersonationRole, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.organizations[orgID]; !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	b.ensureOrgMaps(orgID)

	for _, r := range b.impersonation[orgID] {
		if r.Name == name {
			return nil, fmt.Errorf("%w: impersonation role %q already exists", ErrConflict, name)
		}
	}

	roleID := newID()
	now := time.Now().UTC()

	role := &ImpersonationRole{
		DateCreated:  now,
		DateModified: now,
		RoleID:       roleID,
		Name:         name,
		RoleType:     roleType,
		Description:  description,
		Rules:        rules,
	}

	b.impersonation[orgID][roleID] = role

	return role, nil
}

// GetImpersonationRole returns an impersonation role.
func (b *InMemoryBackend) GetImpersonationRole(orgID, roleID string) (*ImpersonationRole, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, ok := b.organizations[orgID]; !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	role, ok := b.impersonation[orgID][roleID]
	if !ok {
		return nil, fmt.Errorf("%w: impersonation role %q not found", ErrNotFound, roleID)
	}

	return role, nil
}

// UpdateImpersonationRole updates an impersonation role.
func (b *InMemoryBackend) UpdateImpersonationRole(
	orgID, roleID, name, roleType, description string,
	rules []ImpersonationRule,
) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.organizations[orgID]; !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	role, ok := b.impersonation[orgID][roleID]
	if !ok {
		return fmt.Errorf("%w: impersonation role %q not found", ErrNotFound, roleID)
	}

	if name != "" {
		role.Name = name
	}
	if roleType != "" {
		role.RoleType = roleType
	}
	if description != "" {
		role.Description = description
	}
	if rules != nil {
		role.Rules = rules
	}
	role.DateModified = time.Now().UTC()

	return nil
}

// DeleteImpersonationRole removes an impersonation role.
func (b *InMemoryBackend) DeleteImpersonationRole(orgID, roleID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.organizations[orgID]; !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	if _, ok := b.impersonation[orgID][roleID]; !ok {
		return fmt.Errorf("%w: impersonation role %q not found", ErrNotFound, roleID)
	}
	delete(b.impersonation[orgID], roleID)

	return nil
}

// ListImpersonationRoles returns impersonation roles.
func (b *InMemoryBackend) ListImpersonationRoles(
	orgID string,
	maxResults int32,
	nextToken string,
) ([]*ImpersonationRole, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, ok := b.organizations[orgID]; !ok {
		return nil, "", fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	roles := make([]*ImpersonationRole, 0, len(b.impersonation[orgID]))
	for _, r := range b.impersonation[orgID] {
		roles = append(roles, r)
	}
	sort.Slice(roles, func(i, j int) bool { return roles[i].Name < roles[j].Name })

	items, next := paginate(roles, maxResults, nextToken)

	return items, next, nil
}

// --- Tags ---

// TagResource adds tags to a resource.
func (b *InMemoryBackend) TagResource(resourceARN string, tags []Tag) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	existing := b.tags[resourceARN]
	merged := make([]Tag, 0, len(existing)+len(tags))
	merged = append(merged, existing...)
	for _, newTag := range tags {
		found := false
		for i, t := range merged {
			if t.Key == newTag.Key {
				merged[i].Value = newTag.Value
				found = true

				break
			}
		}
		if !found {
			merged = append(merged, newTag)
		}
	}
	b.tags[resourceARN] = merged

	return nil
}

// UntagResource removes tags from a resource.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	existing := b.tags[resourceARN]
	keySet := make(map[string]bool, len(tagKeys))
	for _, k := range tagKeys {
		keySet[k] = true
	}
	filtered := existing[:0]
	for _, t := range existing {
		if !keySet[t.Key] {
			filtered = append(filtered, t)
		}
	}
	b.tags[resourceARN] = filtered

	return nil
}

// ListTagsForResource returns tags for a resource.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) ([]Tag, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.tags[resourceARN], nil
}

// --- DescribeEntity ---

// DescribeEntity describes an entity by email or ID.
func (b *InMemoryBackend) DescribeEntity(orgID, entityID string) (*EntityDescription, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, ok := b.organizations[orgID]; !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	if u := b.findUser(orgID, entityID); u != nil {
		return &EntityDescription{EntityID: u.UserID, Name: u.Name, Type: "USER", State: u.State}, nil
	}
	if g := b.findGroup(orgID, entityID); g != nil {
		return &EntityDescription{EntityID: g.GroupID, Name: g.Name, Type: "GROUP", State: g.State}, nil
	}
	if r := b.findResource(orgID, entityID); r != nil {
		return &EntityDescription{EntityID: r.ResourceID, Name: r.Name, Type: "RESOURCE", State: r.State}, nil
	}

	return nil, fmt.Errorf("%w: entity %q not found", ErrNotFound, entityID)
}

// --- Availability Configurations ---

// CreateAvailabilityConfiguration creates an availability configuration for a domain.
func (b *InMemoryBackend) CreateAvailabilityConfiguration(
	orgID, domainName string, ewsProvider *AvailabilityEwsProvider, lambdaARN string,
) (*AvailabilityConfiguration, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.organizations[orgID]; !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	b.ensureOrgMaps(orgID)
	if _, ok := b.availabilityConfigs[orgID][domainName]; ok {
		return nil, fmt.Errorf("%w: availability configuration for %q already exists", ErrConflict, domainName)
	}
	now := time.Now()
	cfg := &AvailabilityConfiguration{
		DateCreated:  now,
		DateModified: now,
		DomainName:   domainName,
	}
	if ewsProvider != nil {
		cfg.ProviderType = "EWS" //nolint:goconst // existing issue.
		cfg.EwsEndpoint = ewsProvider.EwsEndpoint
		cfg.EwsUsername = ewsProvider.EwsUsername
	} else {
		cfg.ProviderType = "LAMBDA"
		cfg.LambdaARN = lambdaARN
	}
	b.availabilityConfigs[orgID][domainName] = cfg

	return cfg, nil
}

// DeleteAvailabilityConfiguration deletes an availability configuration.
func (b *InMemoryBackend) DeleteAvailabilityConfiguration(orgID, domainName string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.organizations[orgID]; !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	b.ensureOrgMaps(orgID)
	if _, ok := b.availabilityConfigs[orgID][domainName]; !ok {
		return fmt.Errorf("%w: availability configuration for %q not found", ErrNotFound, domainName)
	}
	delete(b.availabilityConfigs[orgID], domainName)

	return nil
}

// UpdateAvailabilityConfiguration updates an existing availability configuration.
func (b *InMemoryBackend) UpdateAvailabilityConfiguration(
	orgID, domainName string, ewsProvider *AvailabilityEwsProvider, lambdaARN string,
) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.organizations[orgID]; !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	b.ensureOrgMaps(orgID)
	cfg, ok := b.availabilityConfigs[orgID][domainName]
	if !ok {
		return fmt.Errorf("%w: availability configuration for %q not found", ErrNotFound, domainName)
	}
	cfg.DateModified = time.Now()
	if ewsProvider != nil {
		cfg.ProviderType = "EWS"
		cfg.EwsEndpoint = ewsProvider.EwsEndpoint
		cfg.EwsUsername = ewsProvider.EwsUsername
		cfg.LambdaARN = ""
	} else {
		cfg.ProviderType = "LAMBDA"
		cfg.LambdaARN = lambdaARN
		cfg.EwsEndpoint = ""
		cfg.EwsUsername = ""
	}

	return nil
}

// ListAvailabilityConfigurations lists availability configurations for an org.
func (b *InMemoryBackend) ListAvailabilityConfigurations(
	orgID string, maxResults int32, nextToken string,
) ([]*AvailabilityConfiguration, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, ok := b.organizations[orgID]; !ok {
		return nil, "", fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	cfgs := make([]*AvailabilityConfiguration, 0)
	for _, cfg := range b.availabilityConfigs[orgID] {
		cfgs = append(cfgs, cfg)
	}
	sort.Slice(cfgs, func(i, j int) bool { return cfgs[i].DomainName < cfgs[j].DomainName })
	page, next := paginate(cfgs, maxResults, nextToken)

	return page, next, nil
}

// TestAvailabilityConfiguration simulates testing a configuration.
func (b *InMemoryBackend) TestAvailabilityConfiguration(orgID, domainName string) (bool, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, ok := b.organizations[orgID]; !ok {
		return false, "", fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	if domainName != "" {
		if _, ok := b.availabilityConfigs[orgID][domainName]; !ok {
			return false, "", fmt.Errorf("%w: availability configuration for %q not found", ErrNotFound, domainName)
		}
	}

	return true, "", nil
}

// --- Mobile Device Access Rules ---

// CreateMobileDeviceAccessRule creates a mobile device access rule.
func (b *InMemoryBackend) CreateMobileDeviceAccessRule(
	orgID, name, effect, description string,
	deviceModels, notDeviceModels, deviceTypes, notDeviceTypes,
	deviceOperatingSystems, notDeviceOperatingSystems, deviceUserAgents, notDeviceUserAgents []string,
) (*MobileDeviceAccessRule, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.organizations[orgID]; !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	b.ensureOrgMaps(orgID)
	now := time.Now()
	rule := &MobileDeviceAccessRule{
		DateCreated:               now,
		DateModified:              now,
		RuleID:                    newID(),
		Name:                      name,
		Effect:                    effect,
		Description:               description,
		DeviceModels:              deviceModels,
		NotDeviceModels:           notDeviceModels,
		DeviceTypes:               deviceTypes,
		NotDeviceTypes:            notDeviceTypes,
		DeviceOperatingSystems:    deviceOperatingSystems,
		NotDeviceOperatingSystems: notDeviceOperatingSystems,
		DeviceUserAgents:          deviceUserAgents,
		NotDeviceUserAgents:       notDeviceUserAgents,
	}
	b.mobileDeviceRules[orgID][rule.RuleID] = rule

	return rule, nil
}

// DeleteMobileDeviceAccessRule deletes a mobile device access rule.
func (b *InMemoryBackend) DeleteMobileDeviceAccessRule(orgID, ruleID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.organizations[orgID]; !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	b.ensureOrgMaps(orgID)
	if _, ok := b.mobileDeviceRules[orgID][ruleID]; !ok {
		return fmt.Errorf("%w: mobile device access rule %q not found", ErrNotFound, ruleID)
	}
	delete(b.mobileDeviceRules[orgID], ruleID)

	return nil
}

// UpdateMobileDeviceAccessRule updates a mobile device access rule.
func (b *InMemoryBackend) UpdateMobileDeviceAccessRule(
	orgID, ruleID, name, effect, description string,
	deviceModels, notDeviceModels, deviceTypes, notDeviceTypes,
	deviceOperatingSystems, notDeviceOperatingSystems, deviceUserAgents, notDeviceUserAgents []string,
) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.organizations[orgID]; !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	b.ensureOrgMaps(orgID)
	rule, ok := b.mobileDeviceRules[orgID][ruleID]
	if !ok {
		return fmt.Errorf("%w: mobile device access rule %q not found", ErrNotFound, ruleID)
	}
	rule.DateModified = time.Now()
	rule.Name = name
	rule.Effect = effect
	rule.Description = description
	rule.DeviceModels = deviceModels
	rule.NotDeviceModels = notDeviceModels
	rule.DeviceTypes = deviceTypes
	rule.NotDeviceTypes = notDeviceTypes
	rule.DeviceOperatingSystems = deviceOperatingSystems
	rule.NotDeviceOperatingSystems = notDeviceOperatingSystems
	rule.DeviceUserAgents = deviceUserAgents
	rule.NotDeviceUserAgents = notDeviceUserAgents

	return nil
}

// ListMobileDeviceAccessRules lists all mobile device access rules for an org.
func (b *InMemoryBackend) ListMobileDeviceAccessRules(orgID string) ([]*MobileDeviceAccessRule, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, ok := b.organizations[orgID]; !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	rules := make([]*MobileDeviceAccessRule, 0, len(b.mobileDeviceRules[orgID]))
	for _, r := range b.mobileDeviceRules[orgID] {
		rules = append(rules, r)
	}
	sort.Slice(rules, func(i, j int) bool { return rules[i].RuleID < rules[j].RuleID })

	return rules, nil
}

func matchesFilter(value string, allow, deny []string) bool {
	if len(allow) > 0 {
		found := false
		for _, v := range allow {
			if strings.EqualFold(v, value) {
				found = true

				break
			}
		}
		if !found {
			return false
		}
	}
	for _, v := range deny {
		if strings.EqualFold(v, value) {
			return false
		}
	}

	return true
}

// GetMobileDeviceAccessEffect evaluates rules for a simulated device.
func (b *InMemoryBackend) GetMobileDeviceAccessEffect(
	orgID, deviceType, deviceModel, deviceOS, deviceUserAgent string,
) (string, []*MobileDeviceMatchedRule, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, ok := b.organizations[orgID]; !ok {
		return "", nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	rules := make([]*MobileDeviceAccessRule, 0, len(b.mobileDeviceRules[orgID]))
	for _, r := range b.mobileDeviceRules[orgID] {
		rules = append(rules, r)
	}
	sort.Slice(rules, func(i, j int) bool { return rules[i].RuleID < rules[j].RuleID })

	effect := "ALLOW"
	matched := []*MobileDeviceMatchedRule{}
	for _, rule := range rules {
		if !matchesFilter(deviceType, rule.DeviceTypes, rule.NotDeviceTypes) {
			continue
		}
		if !matchesFilter(deviceModel, rule.DeviceModels, rule.NotDeviceModels) {
			continue
		}
		if !matchesFilter(deviceOS, rule.DeviceOperatingSystems, rule.NotDeviceOperatingSystems) {
			continue
		}
		if !matchesFilter(deviceUserAgent, rule.DeviceUserAgents, rule.NotDeviceUserAgents) {
			continue
		}
		effect = rule.Effect
		matched = append(matched, &MobileDeviceMatchedRule{RuleID: rule.RuleID, Name: rule.Name})
	}

	return effect, matched, nil
}

// --- Mobile Device Access Overrides ---

func mobileOverrideKey(userID, deviceID string) string {
	return userID + ":" + strings.ToLower(deviceID)
}

// PutMobileDeviceAccessOverride creates or updates a per-user per-device override.
func (b *InMemoryBackend) PutMobileDeviceAccessOverride(
	orgID, userID, deviceID, effect, description string,
) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.organizations[orgID]; !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	b.ensureOrgMaps(orgID)
	key := mobileOverrideKey(userID, deviceID)
	now := time.Now()
	if existing, ok := b.mobileDeviceOverrides[orgID][key]; ok {
		existing.Effect = effect
		existing.Description = description
		existing.DateModified = now
	} else {
		b.mobileDeviceOverrides[orgID][key] = &MobileDeviceAccessOverride{
			DateCreated:  now,
			DateModified: now,
			UserID:       userID,
			DeviceID:     strings.ToLower(deviceID),
			Effect:       effect,
			Description:  description,
		}
	}

	return nil
}

// DeleteMobileDeviceAccessOverride removes a per-user per-device override.
func (b *InMemoryBackend) DeleteMobileDeviceAccessOverride(orgID, userID, deviceID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.organizations[orgID]; !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	b.ensureOrgMaps(orgID)
	key := mobileOverrideKey(userID, deviceID)
	if _, ok := b.mobileDeviceOverrides[orgID][key]; !ok {
		return fmt.Errorf("%w: mobile device access override not found", ErrNotFound)
	}
	delete(b.mobileDeviceOverrides[orgID], key)

	return nil
}

// GetMobileDeviceAccessOverride retrieves a per-user per-device override.
func (b *InMemoryBackend) GetMobileDeviceAccessOverride(
	orgID, userID, deviceID string,
) (*MobileDeviceAccessOverride, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, ok := b.organizations[orgID]; !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	key := mobileOverrideKey(userID, deviceID)
	ov, ok := b.mobileDeviceOverrides[orgID][key]
	if !ok {
		return nil, fmt.Errorf("%w: mobile device access override not found", ErrNotFound)
	}

	return ov, nil
}

// ListMobileDeviceAccessOverrides lists overrides filtered by userID and/or deviceID.
func (b *InMemoryBackend) ListMobileDeviceAccessOverrides(
	orgID, userID, deviceID string, maxResults int32, nextToken string,
) ([]*MobileDeviceAccessOverride, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, ok := b.organizations[orgID]; !ok {
		return nil, "", fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	all := make([]*MobileDeviceAccessOverride, 0)
	for _, ov := range b.mobileDeviceOverrides[orgID] {
		if userID != "" && ov.UserID != userID {
			continue
		}
		if deviceID != "" && !strings.EqualFold(ov.DeviceID, deviceID) {
			continue
		}
		all = append(all, ov)
	}
	sort.Slice(all, func(i, j int) bool {
		return all[i].UserID+all[i].DeviceID < all[j].UserID+all[j].DeviceID
	})
	page, next := paginate(all, maxResults, nextToken)

	return page, next, nil
}

// --- Email Monitoring Configuration ---

// PutEmailMonitoringConfiguration sets email monitoring config for an org.
func (b *InMemoryBackend) PutEmailMonitoringConfiguration(orgID, roleARN, logGroupARN string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.organizations[orgID]; !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	b.emailMonitoring[orgID] = &EmailMonitoringConfiguration{RoleARN: roleARN, LogGroupARN: logGroupARN}

	return nil
}

// DeleteEmailMonitoringConfiguration removes email monitoring config for an org.
func (b *InMemoryBackend) DeleteEmailMonitoringConfiguration(orgID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.organizations[orgID]; !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	delete(b.emailMonitoring, orgID)

	return nil
}

// DescribeEmailMonitoringConfiguration returns email monitoring config for an org.
func (b *InMemoryBackend) DescribeEmailMonitoringConfiguration(orgID string) (*EmailMonitoringConfiguration, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, ok := b.organizations[orgID]; !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	cfg, ok := b.emailMonitoring[orgID]
	if !ok {
		return &EmailMonitoringConfiguration{}, nil
	}

	return cfg, nil
}

// --- Inbound DMARC Settings ---

// PutInboundDmarcSettings sets inbound DMARC enforcement for an org.
func (b *InMemoryBackend) PutInboundDmarcSettings(orgID string, enforced bool) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.organizations[orgID]; !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	b.inboundDmarc[orgID] = enforced

	return nil
}

// DescribeInboundDmarcSettings returns whether inbound DMARC is enforced for an org.
func (b *InMemoryBackend) DescribeInboundDmarcSettings(orgID string) (bool, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, ok := b.organizations[orgID]; !ok {
		return false, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	return b.inboundDmarc[orgID], nil
}

// --- Retention Policies ---

// PutRetentionPolicy creates or updates a retention policy for an org.
func (b *InMemoryBackend) PutRetentionPolicy(
	orgID, id, name, description string, folderConfigurations []*FolderConfiguration,
) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.organizations[orgID]; !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	if id == "" {
		id = newID()
	}
	b.retentionPolicies[orgID] = &RetentionPolicy{
		ID:                   id,
		Name:                 name,
		Description:          description,
		FolderConfigurations: folderConfigurations,
	}

	return nil
}

// DeleteRetentionPolicy removes the retention policy from an org.
func (b *InMemoryBackend) DeleteRetentionPolicy(orgID, id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.organizations[orgID]; !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	existing := b.retentionPolicies[orgID]
	if existing == nil || existing.ID != id {
		return fmt.Errorf("%w: retention policy %q not found", ErrNotFound, id)
	}
	delete(b.retentionPolicies, orgID)

	return nil
}

// GetDefaultRetentionPolicy returns the retention policy for an org.
func (b *InMemoryBackend) GetDefaultRetentionPolicy(orgID string) (*RetentionPolicy, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, ok := b.organizations[orgID]; !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	pol, ok := b.retentionPolicies[orgID]
	if !ok {
		return nil, fmt.Errorf("%w: no retention policy configured", ErrNotFound)
	}

	return pol, nil
}

// --- Mailbox Export Jobs ---

// StartMailboxExportJob starts a mailbox export job.
func (b *InMemoryBackend) StartMailboxExportJob(
	orgID, entityID, description, roleARN, kmsKeyARN, s3BucketName, s3Prefix string,
) (*MailboxExportJob, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.organizations[orgID]; !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	b.ensureOrgMaps(orgID)
	job := &MailboxExportJob{
		JobID:        newID(),
		EntityID:     entityID,
		Description:  description,
		RoleARN:      roleARN,
		KmsKeyARN:    kmsKeyARN,
		S3BucketName: s3BucketName,
		S3Prefix:     s3Prefix,
		S3Path:       s3Prefix + "/export.zip",
		State:        "RUNNING",
		StartTime:    time.Now(),
	}
	b.exportJobs[orgID][job.JobID] = job

	return job, nil
}

// CancelMailboxExportJob cancels a running mailbox export job.
func (b *InMemoryBackend) CancelMailboxExportJob(orgID, jobID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.organizations[orgID]; !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	b.ensureOrgMaps(orgID)
	job, ok := b.exportJobs[orgID][jobID]
	if !ok {
		return fmt.Errorf("%w: mailbox export job %q not found", ErrNotFound, jobID)
	}
	job.State = "CANCELLED"
	job.EndTime = time.Now()

	return nil
}

// DescribeMailboxExportJob returns details of a mailbox export job.
func (b *InMemoryBackend) DescribeMailboxExportJob(orgID, jobID string) (*MailboxExportJob, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, ok := b.organizations[orgID]; !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	job, ok := b.exportJobs[orgID][jobID]
	if !ok {
		return nil, fmt.Errorf("%w: mailbox export job %q not found", ErrNotFound, jobID)
	}

	return job, nil
}

// ListMailboxExportJobs lists mailbox export jobs for an org.
func (b *InMemoryBackend) ListMailboxExportJobs(
	orgID string, maxResults int32, nextToken string,
) ([]*MailboxExportJob, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, ok := b.organizations[orgID]; !ok {
		return nil, "", fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	jobs := make([]*MailboxExportJob, 0, len(b.exportJobs[orgID]))
	for _, j := range b.exportJobs[orgID] {
		jobs = append(jobs, j)
	}
	sort.Slice(jobs, func(i, k int) bool { return jobs[i].JobID < jobs[k].JobID })
	page, next := paginate(jobs, maxResults, nextToken)

	return page, next, nil
}

// --- Identity Center Applications ---

// CreateIdentityCenterApplication creates a new IAM Identity Center application.
func (b *InMemoryBackend) CreateIdentityCenterApplication(
	instanceARN, name string, //nolint:revive // existing issue.
) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	appARN := arn.Build("sso", b.region, b.accountID, "application/"+newID())
	b.identityCenterApps[appARN] = name

	return appARN, nil
}

// DeleteIdentityCenterApplication removes an IAM Identity Center application.
func (b *InMemoryBackend) DeleteIdentityCenterApplication(applicationARN string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.identityCenterApps[applicationARN]; !ok {
		return fmt.Errorf("%w: identity center application %q not found", ErrNotFound, applicationARN)
	}
	delete(b.identityCenterApps, applicationARN)

	return nil
}

// --- Identity Provider Configuration ---

// PutIdentityProviderConfiguration creates or updates IdP configuration.
func (b *InMemoryBackend) PutIdentityProviderConfiguration(
	orgID, authMode, identityCenterAppARN, identityCenterInstanceARN, patStatus string, patLifetimeDays int32,
) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.organizations[orgID]; !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	cfg := &IdentityProviderConfiguration{
		AuthMode:                  authMode,
		IdentityCenterAppARN:      identityCenterAppARN,
		IdentityCenterInstanceARN: identityCenterInstanceARN,
		PATStatus:                 patStatus,
	}
	if patLifetimeDays > 0 {
		cfg.PATLifetimeDays = &patLifetimeDays
	}
	b.idpConfig[orgID] = cfg

	return nil
}

// DeleteIdentityProviderConfiguration removes IdP configuration.
func (b *InMemoryBackend) DeleteIdentityProviderConfiguration(orgID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.organizations[orgID]; !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	delete(b.idpConfig, orgID)

	return nil
}

// DescribeIdentityProviderConfiguration returns IdP configuration for an org.
func (b *InMemoryBackend) DescribeIdentityProviderConfiguration(orgID string) (*IdentityProviderConfiguration, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, ok := b.organizations[orgID]; !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	cfg, ok := b.idpConfig[orgID]
	if !ok {
		return nil, fmt.Errorf("%w: identity provider configuration not found", ErrNotFound)
	}

	return cfg, nil
}

// --- Personal Access Tokens ---

// DeletePersonalAccessToken removes a personal access token.
func (b *InMemoryBackend) DeletePersonalAccessToken(orgID, tokenID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.organizations[orgID]; !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	b.ensureOrgMaps(orgID)
	if _, ok := b.personalTokens[orgID][tokenID]; !ok {
		return fmt.Errorf("%w: personal access token %q not found", ErrNotFound, tokenID)
	}
	delete(b.personalTokens[orgID], tokenID)

	return nil
}

// GetPersonalAccessTokenMetadata returns metadata for a personal access token.
func (b *InMemoryBackend) GetPersonalAccessTokenMetadata(orgID, tokenID string) (*PersonalAccessToken, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, ok := b.organizations[orgID]; !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	tok, ok := b.personalTokens[orgID][tokenID]
	if !ok {
		return nil, fmt.Errorf("%w: personal access token %q not found", ErrNotFound, tokenID)
	}

	return tok, nil
}

// ListPersonalAccessTokens lists personal access tokens, optionally filtered by userID.
func (b *InMemoryBackend) ListPersonalAccessTokens(
	orgID, userID string, maxResults int32, nextToken string,
) ([]*PersonalAccessToken, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, ok := b.organizations[orgID]; !ok {
		return nil, "", fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	all := make([]*PersonalAccessToken, 0)
	for _, tok := range b.personalTokens[orgID] {
		if userID != "" && tok.UserID != userID {
			continue
		}
		all = append(all, tok)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].TokenID < all[j].TokenID })
	page, next := paginate(all, maxResults, nextToken)

	return page, next, nil
}

// CreatePersonalAccessToken creates a new personal access token (for testing).
func (b *InMemoryBackend) CreatePersonalAccessToken(
	orgID, userID, name string,
	scopes []string,
) (*PersonalAccessToken, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.organizations[orgID]; !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	b.ensureOrgMaps(orgID)
	now := time.Now()
	tok := &PersonalAccessToken{
		TokenID:     newID(),
		UserID:      userID,
		Name:        name,
		Scopes:      scopes,
		DateCreated: now,
		ExpiresTime: now.Add(365 * 24 * time.Hour),
	}
	b.personalTokens[orgID][tok.TokenID] = tok

	return tok, nil
}

// --- Impersonation Role Effect ---

// GetImpersonationRoleEffect evaluates impersonation rules for a target user.
func (b *InMemoryBackend) GetImpersonationRoleEffect(
	orgID, roleID, targetUser string,
) (string, string, []*ImpersonationMatchedRule, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, ok := b.organizations[orgID]; !ok {
		return "", "", nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	role, ok := b.impersonation[orgID][roleID]
	if !ok {
		return "", "", nil, fmt.Errorf("%w: impersonation role %q not found", ErrNotFound, roleID)
	}
	effect := "DENY"
	matched := []*ImpersonationMatchedRule{}
	for _, rule := range role.Rules {
		inTarget := slices.Contains(rule.TargetUsers, targetUser)
		inNotTarget := slices.Contains(rule.NotTargetUsers, targetUser)
		if len(rule.TargetUsers) > 0 && !inTarget {
			continue
		}
		if inNotTarget {
			continue
		}
		effect = rule.Effect
		matched = append(matched, &ImpersonationMatchedRule{RuleID: rule.RuleID, Name: rule.Name})
	}

	return effect, role.RoleType, matched, nil
}

// --- Assume Impersonation Role ---

// AssumeImpersonationRole returns a synthetic token for an impersonation role.
func (b *InMemoryBackend) AssumeImpersonationRole(orgID, roleID string) (string, int64, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, ok := b.organizations[orgID]; !ok {
		return "", 0, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	if _, ok := b.impersonation[orgID][roleID]; !ok {
		return "", 0, fmt.Errorf("%w: impersonation role %q not found", ErrNotFound, roleID)
	}
	token := "imp-token-" + roleID
	expiresIn := int64(3600) //nolint:mnd // existing issue.

	return token, expiresIn, nil
}

// paginate returns a page of items and a next token using index-based paging.
func paginate[T any](items []T, maxResults int32, nextToken string) ([]T, string) {
	if len(items) == 0 {
		return []T{}, ""
	}

	start := 0
	if nextToken != "" {
		if idx, err := strconv.Atoi(nextToken); err == nil && idx > 0 && idx < len(items) {
			start = idx
		}
	}

	if maxResults <= 0 {
		maxResults = 100
	}

	end := start + int(maxResults)
	if end >= len(items) {
		return items[start:], ""
	}

	return items[start:end], strconv.Itoa(end)
}
