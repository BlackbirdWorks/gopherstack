package workmail

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New("EntityNotFoundException", awserr.ErrNotFound)
	// ErrConflict is returned when a resource already exists.
	ErrConflict = awserr.New("EntityAlreadyExistsException", awserr.ErrAlreadyExists)
	// ErrValidation is returned for invalid request parameters.
	ErrValidation = awserr.New("InvalidParameterException", awserr.ErrInvalidParameter)
	// ErrLimitExceeded is returned when resource limits are hit.
	ErrLimitExceeded = awserr.New("LimitExceededException", awserr.ErrConflict)
	// ErrMailDomainState is returned for domain state issues.
	ErrMailDomainState = awserr.New("MailDomainStateException", awserr.ErrConflict)
	// ErrEntityState is returned when an operation violates entity state constraints.
	ErrEntityState = awserr.New("EntityStateException", awserr.ErrConflict)
)

const (
	stateEnabled  = "ENABLED"
	stateDisabled = "DISABLED"
	stateDeleted  = "DELETED"
	stateActive   = "ACTIVE"

	memberTypeUser  = "USER"
	memberTypeGroup = "GROUP"

	roleUser       = "USER"
	roleResource   = "RESOURCE"
	roleSystemUser = "SYSTEM_USER"

	providerEWS    = "EWS"
	providerLambda = "LAMBDA"

	defaultMailboxQuota = int32(50000)

	effectAllow = "ALLOW"
	effectDeny  = "DENY"
)

// trackedAlias stores an alias along with its entity reference for conflict
// detection. It is an unexported (package-private) type, so its fields are
// exported purely for JSON round-tripping through store.Table -- see
// pkgs/store's package doc: encoding/json silently drops unexported fields,
// and since trackedAlias itself is never visible outside this package,
// exporting its fields does not change the package's public API.
type trackedAlias struct {
	Alias    string
	OrgID    string
	EntityID string
}

// issuedImpersonationToken stores metadata for an issued impersonation
// token. Like trackedAlias, it is unexported, so its fields are exported
// purely to survive JSON round-tripping.
type issuedImpersonationToken struct {
	Token     string
	ExpiresAt time.Time
	OrgID     string
	RoleID    string
}

// orgKey builds the composite store.Table primary key ("orgID|id") shared by
// every org-nested resource table (see store_setup.go's registerAllTables).
func orgKey(orgID, id string) string { return orgID + "|" + id }

// permissionKey builds the composite store.Table primary key
// ("orgID|entityID|granteeID") for the permissions table, which nests one
// level deeper than the other org-nested tables (org -> entity -> grantee).
func permissionKey(orgID, entityID, granteeID string) string {
	return orgID + "|" + entityID + "|" + granteeID
}

// permissionOrgEntityKey builds the secondary-index key grouping permissions
// by (orgID, entityID) -- the pair ListMailboxPermissions/
// PutMailboxPermissions/DeleteMailboxPermissions always know up front.
func permissionOrgEntityKey(orgID, entityID string) string { return orgID + "|" + entityID }

// InMemoryBackend stores WorkMail state in memory.
type InMemoryBackend struct {
	// registry holds the "clean" tables only (organizations, globalAliases,
	// issuedTokens): each already carries a real, wire-visible-or-otherwise
	// unhidden identity field, so registry.SnapshotAll/RestoreAll can
	// round-trip them directly. Every org-nested / composite-keyed table
	// below hides at least one field (orgID, and for permissions also
	// entityID) that plain JSON marshaling would silently drop, so those are
	// deliberately NOT on this registry -- see store_setup.go's
	// registerAllTables doc and persistence.go's DTO registry.
	registry      *store.Registry
	organizations *store.Table[Organization]
	orgsByAlias   map[string]string

	users        *store.Table[User]
	usersByOrg   *store.Index[User]
	usersByEmail map[string]map[string]string

	groups        *store.Table[Group]
	groupsByOrg   *store.Index[Group]
	groupsByEmail map[string]map[string]string
	groupMembers  map[string]map[string]map[string]bool

	resources        *store.Table[Resource]
	resourcesByOrg   *store.Index[Resource]
	resourcesByEmail map[string]map[string]string

	mailboxQuotas map[string]map[string]int32
	tags          map[string][]Tag
	delegates     map[string]map[string]map[string]bool

	impersonation      *store.Table[ImpersonationRole]
	impersonationByOrg *store.Index[ImpersonationRole]

	aliases       map[string]map[string][]string
	globalAliases *store.Table[trackedAlias]

	permissions            *store.Table[Permission]
	permissionsByOrgEntity *store.Index[Permission]

	mailDomains      *store.Table[MailDomain]
	mailDomainsByOrg *store.Index[MailDomain]

	accessRules      *store.Table[AccessControlRule]
	accessRulesByOrg *store.Index[AccessControlRule]

	availabilityConfigs      *store.Table[AvailabilityConfiguration]
	availabilityConfigsByOrg *store.Index[AvailabilityConfiguration]

	mobileDeviceRules      *store.Table[MobileDeviceAccessRule]
	mobileDeviceRulesByOrg *store.Index[MobileDeviceAccessRule]

	// mobileDeviceOverrides is keyed by orgID -> "userID:deviceID" (see
	// mobileOverrideKey), composited via orgKey into the flat table below.
	mobileDeviceOverrides      *store.Table[MobileDeviceAccessOverride]
	mobileDeviceOverridesByOrg *store.Index[MobileDeviceAccessOverride]

	emailMonitoring *store.Table[EmailMonitoringConfiguration]
	inboundDmarc    map[string]bool

	retentionPolicies *store.Table[RetentionPolicy]

	exportJobs      *store.Table[MailboxExportJob]
	exportJobsByOrg *store.Index[MailboxExportJob]

	identityCenterApps map[string]string // ARN -> name

	idpConfig *store.Table[IdentityProviderConfiguration]

	personalTokens      *store.Table[PersonalAccessToken]
	personalTokensByOrg *store.Index[PersonalAccessToken]

	issuedTokens *store.Table[issuedImpersonationToken]

	mu        *lockmetrics.RWMutex
	accountID string
	region    string
}

// NewInMemoryBackend creates a new in-memory WorkMail backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		accountID: accountID,
		region:    region,
		mu:        lockmetrics.New("workmail"),
		registry:  store.NewRegistry(),
	}
	registerAllTables(b)
	b.resetRawMaps()

	return b
}

// AccountID returns the configured account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Region returns the configured region.
func (b *InMemoryBackend) Region() string { return b.region }

// regionFor returns the request region from ctx, falling back to b.region.
func (b *InMemoryBackend) regionFor(ctx context.Context) string {
	if r := awsmeta.Region(ctx); r != "" {
		return r
	}

	return b.region
}

// Reset clears all stored state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.resetLocked()
}

// resetLocked clears every table (registered and org-nested/composite-keyed)
// plus every raw map. Callers must hold b.mu.Lock.
func (b *InMemoryBackend) resetLocked() {
	b.registry.ResetAll()
	// The org-nested / composite-keyed tables (see the InMemoryBackend field
	// comments above) are deliberately NOT on b.registry, so each needs its
	// own Reset() call here.
	b.users.Reset()
	b.groups.Reset()
	b.resources.Reset()
	b.impersonation.Reset()
	b.permissions.Reset()
	b.mailDomains.Reset()
	b.accessRules.Reset()
	b.availabilityConfigs.Reset()
	b.mobileDeviceRules.Reset()
	b.mobileDeviceOverrides.Reset()
	b.emailMonitoring.Reset()
	b.retentionPolicies.Reset()
	b.exportJobs.Reset()
	b.idpConfig.Reset()
	b.personalTokens.Reset()
	b.resetRawMaps()
}

// resetRawMaps (re)allocates every plain (non-store.Table) map to empty. It
// is shared by NewInMemoryBackend and resetLocked.
func (b *InMemoryBackend) resetRawMaps() {
	b.orgsByAlias = make(map[string]string)
	b.usersByEmail = make(map[string]map[string]string)
	b.groupsByEmail = make(map[string]map[string]string)
	b.groupMembers = make(map[string]map[string]map[string]bool)
	b.resourcesByEmail = make(map[string]map[string]string)
	b.mailboxQuotas = make(map[string]map[string]int32)
	b.tags = make(map[string][]Tag)
	b.delegates = make(map[string]map[string]map[string]bool)
	b.aliases = make(map[string]map[string][]string)
	b.inboundDmarc = make(map[string]bool)
	b.identityCenterApps = make(map[string]string)
}

func newID() string { return uuid.New().String() }

func (b *InMemoryBackend) orgARN(orgID, region string) string {
	return arn.Build("workmail", region, b.accountID, "organization/"+orgID)
}

func (b *InMemoryBackend) entityARN(orgID, entityType, entityID string) string {
	org, _ := b.organizations.Get(orgID)
	region := b.region
	if org != nil && org.Region != "" {
		region = org.Region
	}

	return arn.Build("workmail", region, b.accountID,
		fmt.Sprintf("organization/%s/%s/%s", orgID, entityType, entityID))
}

// initOrgMaps preallocates the per-org submaps of every raw (non-store.Table)
// collection nested under orgID. The org-nested store.Table collections need
// no such preallocation: entries are created on demand via Put.
func (b *InMemoryBackend) initOrgMaps(orgID string) {
	b.usersByEmail[orgID] = make(map[string]string)
	b.groupsByEmail[orgID] = make(map[string]string)
	b.groupMembers[orgID] = make(map[string]map[string]bool)
	b.resourcesByEmail[orgID] = make(map[string]string)
	b.delegates[orgID] = make(map[string]map[string]bool)
	b.aliases[orgID] = make(map[string][]string)
	b.mailboxQuotas[orgID] = make(map[string]int32)
}

// deleteAllForOrg removes every entry belonging to orgID from t, found via
// idx. idx's result slice is cloned before the delete loop since Index
// slices mutate under Table.Delete (see pkgs/store's Index doc).
func deleteAllForOrg[V any](t *store.Table[V], idx *store.Index[V], keyFn func(*V) string, orgID string) {
	for _, v := range slices.Clone(idx.Get(orgID)) {
		t.Delete(keyFn(v))
	}
}

// --- Organizations ---

// CreateOrganization creates a new WorkMail organization.
func (b *InMemoryBackend) CreateOrganization(
	ctx context.Context,
	alias string,
	domains []string,
) (*Organization, error) {
	region := b.regionFor(ctx)

	b.mu.Lock("CreateOrganization")
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
		ARN:               b.orgARN(orgID, region),
		State:             stateActive,
		DirectoryID:       "d-" + strings.ReplaceAll(newID(), "-", "")[:10],
		DirectoryType:     "SimpleAD",
		DefaultMailDomain: defaultDomain,
		Region:            region,
	}

	b.organizations.Put(org)
	b.orgsByAlias[alias] = orgID
	b.initOrgMaps(orgID)

	// Register default domain.
	b.mailDomains.Put(&MailDomain{
		DomainName:                  defaultDomain,
		IsDefault:                   true,
		IsTestDomain:                true,
		OwnershipVerificationStatus: "VERIFIED",
		orgID:                       orgID,
	})

	for _, d := range domains {
		if d == "" {
			continue
		}
		b.mailDomains.Put(&MailDomain{
			DomainName:                  d,
			IsDefault:                   false,
			IsTestDomain:                false,
			OwnershipVerificationStatus: "VERIFIED",
			orgID:                       orgID,
		})
	}

	return org, nil
}

// DescribeOrganization returns details about an organization.
func (b *InMemoryBackend) DescribeOrganization(orgID string) (*Organization, error) {
	b.mu.RLock("DescribeOrganization")
	defer b.mu.RUnlock()

	org, ok := b.organizations.Get(orgID)
	if !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	return org, nil
}

// DeleteOrganization removes a WorkMail organization. It clears exactly the
// same set of collections the pre-Phase-3.3 map-based implementation did --
// globalAliases, availabilityConfigs, mobileDeviceRules,
// mobileDeviceOverrides, emailMonitoring, inboundDmarc, retentionPolicies,
// exportJobs, identityCenterApps, idpConfig, personalTokens, tags, and
// issuedTokens are deliberately left untouched, matching prior behavior.
func (b *InMemoryBackend) DeleteOrganization(orgID string, _ bool) error {
	b.mu.Lock("DeleteOrganization")
	defer b.mu.Unlock()

	org, ok := b.organizations.Get(orgID)
	if !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	delete(b.orgsByAlias, org.Alias)
	b.organizations.Delete(orgID)
	deleteAllForOrg(b.users, b.usersByOrg, userKeyFn, orgID)
	delete(b.usersByEmail, orgID)
	deleteAllForOrg(b.groups, b.groupsByOrg, groupKeyFn, orgID)
	delete(b.groupsByEmail, orgID)
	delete(b.groupMembers, orgID)
	deleteAllForOrg(b.resources, b.resourcesByOrg, resourceKeyFn, orgID)
	delete(b.resourcesByEmail, orgID)
	delete(b.delegates, orgID)
	delete(b.aliases, orgID)
	b.deletePermissionsForOrg(orgID)
	deleteAllForOrg(b.mailDomains, b.mailDomainsByOrg, mailDomainKeyFn, orgID)
	deleteAllForOrg(b.accessRules, b.accessRulesByOrg, accessRuleKeyFn, orgID)
	deleteAllForOrg(b.impersonation, b.impersonationByOrg, impersonationKeyFn, orgID)
	delete(b.mailboxQuotas, orgID)

	return nil
}

// deletePermissionsForOrg removes every permission entry belonging to orgID.
// permissions has no byOrg index (only byOrgEntity, used by the hot-path
// per-entity lookups); org-wide deletion only happens here, on
// DeleteOrganization, so a full Range scan is acceptable.
func (b *InMemoryBackend) deletePermissionsForOrg(orgID string) {
	var toDelete []string

	b.permissions.Range(func(p *Permission) bool {
		if p.orgID == orgID {
			toDelete = append(toDelete, permissionKey(p.orgID, p.entityID, p.GranteeID))
		}

		return true
	})

	for _, k := range toDelete {
		b.permissions.Delete(k)
	}
}

// ListOrganizations returns a paginated list of organizations.
func (b *InMemoryBackend) ListOrganizations(
	ctx context.Context,
	maxResults int32,
	nextToken string,
) ([]*OrgSummary, string, error) {
	requestRegion := b.regionFor(ctx)

	b.mu.RLock("ListOrganizations")
	defer b.mu.RUnlock()

	orgs := make([]*OrgSummary, 0, b.organizations.Len())
	for _, o := range b.organizations.All() {
		if o.Region != "" && requestRegion != "" && o.Region != requestRegion {
			continue
		}
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
func (b *InMemoryBackend) CreateUser(
	orgID, name, displayName, password, role string,
) (*User, error) {
	b.mu.Lock("CreateUser")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}
	validRoles := map[string]bool{roleUser: true, roleResource: true, roleSystemUser: true}
	if role != "" && !validRoles[role] {
		return nil, fmt.Errorf(
			"%w: invalid Role %q, must be USER, RESOURCE, or SYSTEM_USER",
			ErrValidation,
			role,
		)
	}

	for _, u := range b.usersByOrg.Get(orgID) {
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
		orgID:       orgID,
	}

	b.users.Put(u)

	return u, nil
}

// DescribeUser returns details about a user.
func (b *InMemoryBackend) DescribeUser(orgID, entityID string) (*User, error) {
	b.mu.RLock("DescribeUser")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	u := b.findUser(orgID, entityID)
	if u == nil {
		return nil, fmt.Errorf("%w: user %q not found", ErrNotFound, entityID)
	}

	return u, nil
}

func (b *InMemoryBackend) findUser(orgID, entityID string) *User {
	if u, ok := b.users.Get(orgKey(orgID, entityID)); ok {
		return u
	}
	// search by name
	for _, u := range b.usersByOrg.Get(orgID) {
		if u.Name == entityID {
			return u
		}
	}

	return nil
}

// UpdateUser updates display name and name fields.
func (b *InMemoryBackend) UpdateUser(
	orgID, entityID, displayName, firstName, lastName string,
) error {
	b.mu.Lock("UpdateUser")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
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
	b.mu.Lock("DeleteUser")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	u := b.findUser(orgID, entityID)
	if u == nil {
		return fmt.Errorf("%w: user %q not found", ErrNotFound, entityID)
	}

	if u.State == stateEnabled {
		return fmt.Errorf(
			"%w: user %q is in ENABLED state and cannot be deleted; call DeregisterFromWorkMail first",
			ErrEntityState,
			entityID,
		)
	}

	actualID := u.UserID
	if u.Email != "" {
		delete(b.usersByEmail[orgID], u.Email)
	}
	b.users.Delete(orgKey(orgID, actualID))

	return nil
}

// ListUsers returns a paginated list of users.
func (b *InMemoryBackend) ListUsers(
	orgID string,
	maxResults int32,
	nextToken string,
) ([]*UserSummary, string, error) {
	b.mu.RLock("ListUsers")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, "", fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	users := make([]*UserSummary, 0)
	for _, u := range b.usersByOrg.Get(orgID) {
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
	b.mu.Lock("RegisterToWorkMail")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	if ta, exists := b.globalAliases.Get(email); exists && ta.OrgID == orgID {
		return fmt.Errorf("%w: email %q already in use", ErrConflict, email)
	}

	now := time.Now().UTC()

	if u := b.findUser(orgID, entityID); u != nil {
		if u.Email != "" {
			delete(b.usersByEmail[orgID], u.Email)
			b.globalAliases.Delete(u.Email)
		}
		u.Email = email
		u.State = stateEnabled
		u.EnabledDate = now
		b.usersByEmail[orgID][email] = u.UserID
		b.globalAliases.Put(&trackedAlias{Alias: email, OrgID: orgID, EntityID: u.UserID})

		return nil
	}

	if g := b.findGroup(orgID, entityID); g != nil {
		if g.Email != "" {
			delete(b.groupsByEmail[orgID], g.Email)
			b.globalAliases.Delete(g.Email)
		}
		g.Email = email
		g.State = stateEnabled
		g.EnabledDate = now
		b.groupsByEmail[orgID][email] = g.GroupID
		b.globalAliases.Put(&trackedAlias{Alias: email, OrgID: orgID, EntityID: g.GroupID})

		return nil
	}

	if r := b.findResource(orgID, entityID); r != nil {
		if r.Email != "" {
			delete(b.resourcesByEmail[orgID], r.Email)
			b.globalAliases.Delete(r.Email)
		}
		r.Email = email
		r.State = stateEnabled
		r.EnabledDate = now
		b.resourcesByEmail[orgID][email] = r.ResourceID
		b.globalAliases.Put(&trackedAlias{Alias: email, OrgID: orgID, EntityID: r.ResourceID})

		return nil
	}

	return fmt.Errorf("%w: entity %q not found", ErrNotFound, entityID)
}

// DeregisterFromWorkMail removes an email address assignment.
func (b *InMemoryBackend) DeregisterFromWorkMail(orgID, entityID string) error {
	b.mu.Lock("DeregisterFromWorkMail")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	now := time.Now().UTC()

	if u := b.findUser(orgID, entityID); u != nil {
		if u.Email != "" {
			delete(b.usersByEmail[orgID], u.Email)
			b.globalAliases.Delete(u.Email)
		}
		u.Email = ""
		u.State = stateDisabled
		u.DisabledDate = now

		return nil
	}

	if g := b.findGroup(orgID, entityID); g != nil {
		if g.Email != "" {
			delete(b.groupsByEmail[orgID], g.Email)
			b.globalAliases.Delete(g.Email)
		}
		g.Email = ""
		g.State = stateDisabled
		g.DisabledDate = now

		return nil
	}

	if r := b.findResource(orgID, entityID); r != nil {
		if r.Email != "" {
			delete(b.resourcesByEmail[orgID], r.Email)
			b.globalAliases.Delete(r.Email)
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
	b.mu.RLock("ResetPassword")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
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
	b.mu.RLock("GetMailboxDetails")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
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
	b.mu.Lock("UpdateMailboxQuota")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
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
	b.mu.Lock("UpdatePrimaryEmailAddress")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	if u := b.findUser(orgID, entityID); u != nil {
		if u.Email != "" {
			delete(b.usersByEmail[orgID], u.Email)
			b.globalAliases.Delete(u.Email)
		}
		u.Email = email
		b.usersByEmail[orgID][email] = u.UserID
		b.globalAliases.Put(&trackedAlias{Alias: email, OrgID: orgID, EntityID: u.UserID})

		return nil
	}

	if g := b.findGroup(orgID, entityID); g != nil {
		if g.Email != "" {
			delete(b.groupsByEmail[orgID], g.Email)
			b.globalAliases.Delete(g.Email)
		}
		g.Email = email
		b.groupsByEmail[orgID][email] = g.GroupID
		b.globalAliases.Put(&trackedAlias{Alias: email, OrgID: orgID, EntityID: g.GroupID})

		return nil
	}

	if r := b.findResource(orgID, entityID); r != nil {
		if r.Email != "" {
			delete(b.resourcesByEmail[orgID], r.Email)
			b.globalAliases.Delete(r.Email)
		}
		r.Email = email
		b.resourcesByEmail[orgID][email] = r.ResourceID
		b.globalAliases.Put(&trackedAlias{Alias: email, OrgID: orgID, EntityID: r.ResourceID})

		return nil
	}

	return fmt.Errorf("%w: entity %q not found", ErrNotFound, entityID)
}

// --- Groups ---

func (b *InMemoryBackend) findGroup(orgID, entityID string) *Group {
	if g, ok := b.groups.Get(orgKey(orgID, entityID)); ok {
		return g
	}
	for _, g := range b.groupsByOrg.Get(orgID) {
		if g.Name == entityID {
			return g
		}
	}

	return nil
}

// CreateGroup creates a new WorkMail group.
func (b *InMemoryBackend) CreateGroup(orgID, name string, hidden bool) (*Group, error) {
	b.mu.Lock("CreateGroup")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	for _, g := range b.groupsByOrg.Get(orgID) {
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
		orgID:     orgID,
	}

	b.groups.Put(g)
	b.groupMembers[orgID][groupID] = make(map[string]bool)

	return g, nil
}

// DescribeGroup returns group details.
func (b *InMemoryBackend) DescribeGroup(orgID, entityID string) (*Group, error) {
	b.mu.RLock("DescribeGroup")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
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
	b.mu.Lock("UpdateGroup")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
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
	b.mu.Lock("DeleteGroup")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	g := b.findGroup(orgID, entityID)
	if g == nil {
		return fmt.Errorf("%w: group %q not found", ErrNotFound, entityID)
	}

	if g.State == stateEnabled {
		return fmt.Errorf(
			"%w: group %q is in ENABLED state and cannot be deleted; call DeregisterFromWorkMail first",
			ErrEntityState,
			entityID,
		)
	}

	if g.Email != "" {
		delete(b.groupsByEmail[orgID], g.Email)
		b.globalAliases.Delete(g.Email)
	}
	b.groups.Delete(orgKey(orgID, g.GroupID))
	delete(b.groupMembers[orgID], g.GroupID)

	return nil
}

// ListGroups returns a paginated list of groups.
func (b *InMemoryBackend) ListGroups(
	orgID string,
	maxResults int32,
	nextToken string,
) ([]*GroupSummary, string, error) {
	b.mu.RLock("ListGroups")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, "", fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	gs := make([]*GroupSummary, 0, len(b.groupsByOrg.Get(orgID)))
	for _, g := range b.groupsByOrg.Get(orgID) {
		gs = append(
			gs,
			&GroupSummary{GroupID: g.GroupID, Name: g.Name, Email: g.Email, State: g.State},
		)
	}
	sort.Slice(gs, func(i, j int) bool { return gs[i].Name < gs[j].Name })

	items, next := paginate(gs, maxResults, nextToken)

	return items, next, nil
}

// AssociateMemberToGroup adds a member to a group.
func (b *InMemoryBackend) AssociateMemberToGroup(orgID, groupID, memberID string) error {
	b.mu.Lock("AssociateMemberToGroup")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
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
	b.mu.Lock("DisassociateMemberFromGroup")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
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
	b.mu.RLock("ListGroupMembers")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
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
	b.mu.RLock("ListGroupsForEntity")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, "", fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	gs := make([]*GroupSummary, 0)
	for _, g := range b.groupsByOrg.Get(orgID) {
		if b.groupMembers[orgID][g.GroupID][entityID] {
			gs = append(
				gs,
				&GroupSummary{GroupID: g.GroupID, Name: g.Name, Email: g.Email, State: g.State},
			)
		}
	}
	sort.Slice(gs, func(i, j int) bool { return gs[i].Name < gs[j].Name })

	items, next := paginate(gs, maxResults, nextToken)

	return items, next, nil
}

// --- Resources ---

func (b *InMemoryBackend) findResource(orgID, entityID string) *Resource {
	if r, ok := b.resources.Get(orgKey(orgID, entityID)); ok {
		return r
	}
	for _, r := range b.resourcesByOrg.Get(orgID) {
		if r.Name == entityID {
			return r
		}
	}

	return nil
}

// CreateResource creates a new WorkMail resource.
func (b *InMemoryBackend) CreateResource(
	orgID, name, resourceType, description string,
) (*Resource, error) {
	b.mu.Lock("CreateResource")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}
	validTypes := map[string]bool{"ROOM": true, "EQUIPMENT": true}
	if resourceType != "" && !validTypes[resourceType] {
		return nil, fmt.Errorf(
			"%w: invalid Type %q, must be ROOM or EQUIPMENT",
			ErrValidation,
			resourceType,
		)
	}

	for _, r := range b.resourcesByOrg.Get(orgID) {
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
		orgID:        orgID,
	}

	b.resources.Put(r)
	b.delegates[orgID][resourceID] = make(map[string]bool)

	return r, nil
}

// DescribeResource returns resource details.
func (b *InMemoryBackend) DescribeResource(orgID, entityID string) (*Resource, error) {
	b.mu.RLock("DescribeResource")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
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
	b.mu.Lock("UpdateResource")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
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
	b.mu.Lock("DeleteResource")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	r := b.findResource(orgID, entityID)
	if r == nil {
		return fmt.Errorf("%w: resource %q not found", ErrNotFound, entityID)
	}

	if r.State == stateEnabled {
		return fmt.Errorf(
			"%w: resource %q is in ENABLED state and cannot be deleted; call DeregisterFromWorkMail first",
			ErrEntityState,
			entityID,
		)
	}

	if r.Email != "" {
		delete(b.resourcesByEmail[orgID], r.Email)
		b.globalAliases.Delete(r.Email)
	}
	b.resources.Delete(orgKey(orgID, r.ResourceID))
	delete(b.delegates[orgID], r.ResourceID)

	return nil
}

// ListResources returns a paginated list of resources.
func (b *InMemoryBackend) ListResources(
	orgID string,
	maxResults int32,
	nextToken string,
) ([]*ResourceSummary, string, error) {
	b.mu.RLock("ListResources")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, "", fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	rs := make([]*ResourceSummary, 0, len(b.resourcesByOrg.Get(orgID)))
	for _, r := range b.resourcesByOrg.Get(orgID) {
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
	b.mu.Lock("AssociateDelegateToResource")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
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
func (b *InMemoryBackend) DisassociateDelegateFromResource(
	orgID, resourceID, entityID string,
) error {
	b.mu.Lock("DisassociateDelegateFromResource")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
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
	b.mu.RLock("ListResourceDelegates")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
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
	sort.Slice(
		delegates,
		func(i, j int) bool { return delegates[i].DelegateID < delegates[j].DelegateID },
	)

	items, next := paginate(delegates, maxResults, nextToken)

	return items, next, nil
}

// --- Aliases ---

// CreateAlias creates an email alias for an entity.
func (b *InMemoryBackend) CreateAlias(orgID, entityID, alias string) error {
	b.mu.Lock("CreateAlias")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	if ta, exists := b.globalAliases.Get(alias); exists && ta.OrgID == orgID {
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
	b.globalAliases.Put(&trackedAlias{Alias: alias, OrgID: orgID, EntityID: actualID})

	return nil
}

// DeleteAlias removes an email alias.
func (b *InMemoryBackend) DeleteAlias(orgID, entityID, alias string) error {
	b.mu.Lock("DeleteAlias")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
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
	b.globalAliases.Delete(alias)

	return nil
}

// ListAliases returns aliases for an entity.
func (b *InMemoryBackend) ListAliases(
	orgID, entityID string,
	maxResults int32,
	nextToken string,
) ([]string, string, error) {
	b.mu.RLock("ListAliases")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
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
func (b *InMemoryBackend) PutMailboxPermissions(
	orgID, entityID, granteeID string,
	perms []string,
) error {
	b.mu.Lock("PutMailboxPermissions")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
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

	b.permissions.Put(&Permission{
		GranteeID:   granteeID,
		GranteeType: granteeType,
		Permissions: perms,
		orgID:       orgID,
		entityID:    actualID,
	})

	return nil
}

// DeleteMailboxPermissions removes mailbox permissions.
func (b *InMemoryBackend) DeleteMailboxPermissions(orgID, entityID, granteeID string) error {
	b.mu.Lock("DeleteMailboxPermissions")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
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

	if !b.permissions.Delete(permissionKey(orgID, actualID, granteeID)) {
		return fmt.Errorf("%w: permission for grantee %q not found", ErrNotFound, granteeID)
	}

	return nil
}

// ListMailboxPermissions returns mailbox permissions for an entity.
func (b *InMemoryBackend) ListMailboxPermissions(
	orgID, entityID string,
	maxResults int32,
	nextToken string,
) ([]*Permission, string, error) {
	b.mu.RLock("ListMailboxPermissions")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
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

	perms := slices.Clone(b.permissionsByOrgEntity.Get(permissionOrgEntityKey(orgID, actualID)))
	sort.Slice(perms, func(i, j int) bool { return perms[i].GranteeID < perms[j].GranteeID })

	items, next := paginate(perms, maxResults, nextToken)

	return items, next, nil
}

// --- Mail Domains ---

// RegisterMailDomain registers a domain with the organization.
func (b *InMemoryBackend) RegisterMailDomain(orgID, domainName string) error {
	b.mu.Lock("RegisterMailDomain")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	if b.mailDomains.Has(orgKey(orgID, domainName)) {
		return fmt.Errorf("%w: domain %q already registered", ErrConflict, domainName)
	}

	b.mailDomains.Put(&MailDomain{
		DomainName:                  domainName,
		IsDefault:                   false,
		IsTestDomain:                false,
		OwnershipVerificationStatus: "PENDING",
		orgID:                       orgID,
	})

	return nil
}

// DeregisterMailDomain removes a domain from the organization.
func (b *InMemoryBackend) DeregisterMailDomain(orgID, domainName string) error {
	b.mu.Lock("DeregisterMailDomain")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	domain, exists := b.mailDomains.Get(orgKey(orgID, domainName))
	if !exists {
		return fmt.Errorf("%w: domain %q not found", ErrNotFound, domainName)
	}
	if domain.IsDefault {
		return fmt.Errorf("%w: cannot deregister the default domain", ErrMailDomainState)
	}
	b.mailDomains.Delete(orgKey(orgID, domainName))

	return nil
}

// GetMailDomain returns details about a registered domain.
func (b *InMemoryBackend) GetMailDomain(orgID, domainName string) (*MailDomain, error) {
	b.mu.RLock("GetMailDomain")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	d, exists := b.mailDomains.Get(orgKey(orgID, domainName))
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
	b.mu.RLock("ListMailDomains")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, "", fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	domainsByOrg := b.mailDomainsByOrg.Get(orgID)
	domains := make([]*MailDomainSummary, 0, len(domainsByOrg))
	for _, d := range domainsByOrg {
		domains = append(domains, &MailDomainSummary{
			DomainName:   d.DomainName,
			IsDefault:    d.IsDefault,
			IsTestDomain: d.IsTestDomain,
		})
	}
	sort.Slice(
		domains,
		func(i, j int) bool { return domains[i].DomainName < domains[j].DomainName },
	)

	items, next := paginate(domains, maxResults, nextToken)

	return items, next, nil
}

// UpdateDefaultMailDomain changes the default mail domain.
func (b *InMemoryBackend) UpdateDefaultMailDomain(orgID, domainName string) error {
	b.mu.Lock("UpdateDefaultMailDomain")
	defer b.mu.Unlock()

	org, ok := b.organizations.Get(orgID)
	if !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	d, exists := b.mailDomains.Get(orgKey(orgID, domainName))
	if !exists {
		return fmt.Errorf("%w: domain %q not found", ErrNotFound, domainName)
	}
	// clear old default
	for _, dom := range b.mailDomainsByOrg.Get(orgID) {
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
	b.mu.Lock("PutAccessControlRule")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	now := time.Now().UTC()
	existing, _ := b.accessRules.Get(orgKey(orgID, name))

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
		orgID:        orgID,
	}
	if existing != nil {
		rule.DateCreated = existing.DateCreated
	}

	b.accessRules.Put(rule)

	return rule, nil
}

// DeleteAccessControlRule removes an access control rule.
func (b *InMemoryBackend) DeleteAccessControlRule(orgID, name string) error {
	b.mu.Lock("DeleteAccessControlRule")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	if !b.accessRules.Delete(orgKey(orgID, name)) {
		return fmt.Errorf("%w: access control rule %q not found", ErrNotFound, name)
	}

	return nil
}

// GetAccessControlEffect evaluates access control rules.
func (b *InMemoryBackend) GetAccessControlEffect(
	orgID, ipAddr, action, userID string,
) (string, []string, error) {
	b.mu.RLock("GetAccessControlEffect")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return "", nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	byOrg := b.accessRulesByOrg.Get(orgID)
	rules := make([]*AccessControlRule, 0, len(byOrg))
	rules = append(rules, byOrg...)
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
	b.mu.RLock("ListAccessControlRules")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	byOrg := b.accessRulesByOrg.Get(orgID)
	rules := make([]*AccessControlRule, 0, len(byOrg))
	rules = append(rules, byOrg...)
	sort.Slice(rules, func(i, j int) bool { return rules[i].Name < rules[j].Name })

	return rules, nil
}

// --- Impersonation Roles ---

// CreateImpersonationRole creates a new impersonation role.
func (b *InMemoryBackend) CreateImpersonationRole(
	orgID, name, roleType, description string,
	rules []ImpersonationRule,
) (*ImpersonationRole, error) {
	b.mu.Lock("CreateImpersonationRole")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	for _, r := range b.impersonationByOrg.Get(orgID) {
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
		orgID:        orgID,
	}

	b.impersonation.Put(role)

	return role, nil
}

// GetImpersonationRole returns an impersonation role.
func (b *InMemoryBackend) GetImpersonationRole(orgID, roleID string) (*ImpersonationRole, error) {
	b.mu.RLock("GetImpersonationRole")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	role, ok := b.impersonation.Get(orgKey(orgID, roleID))
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
	b.mu.Lock("UpdateImpersonationRole")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	role, ok := b.impersonation.Get(orgKey(orgID, roleID))
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
	b.mu.Lock("DeleteImpersonationRole")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	if !b.impersonation.Delete(orgKey(orgID, roleID)) {
		return fmt.Errorf("%w: impersonation role %q not found", ErrNotFound, roleID)
	}

	return nil
}

// ListImpersonationRoles returns impersonation roles.
func (b *InMemoryBackend) ListImpersonationRoles(
	orgID string,
	maxResults int32,
	nextToken string,
) ([]*ImpersonationRole, string, error) {
	b.mu.RLock("ListImpersonationRoles")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, "", fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	byOrg := b.impersonationByOrg.Get(orgID)
	roles := make([]*ImpersonationRole, 0, len(byOrg))
	roles = append(roles, byOrg...)
	sort.Slice(roles, func(i, j int) bool { return roles[i].Name < roles[j].Name })

	items, next := paginate(roles, maxResults, nextToken)

	return items, next, nil
}

// --- Tags ---

// TagResource adds tags to a resource.
func (b *InMemoryBackend) TagResource(resourceARN string, tags []Tag) error {
	b.mu.Lock("TagResource")
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
	b.mu.Lock("UntagResource")
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
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	return b.tags[resourceARN], nil
}

// --- DescribeEntity ---

// DescribeEntity describes an entity looked up by its primary email address
// (DescribeEntityInput.Email is the only lookup field the real API accepts --
// see aws-sdk-go-v2/service/workmail/api_op_DescribeEntity.go). The plain
// findUser/findGroup/findResource ID-or-name lookup never matches an email
// (they only check the composite-key ID and the Name field), so email lookup
// is checked first via the byEmail reverse-index maps; ID/name is kept as a
// fallback for backward compatibility with any caller that already has the
// entity's own ID.
func (b *InMemoryBackend) DescribeEntity(orgID, identifier string) (*EntityDescription, error) {
	b.mu.RLock("DescribeEntity")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	if desc := b.describeEntityByEmail(orgID, identifier); desc != nil {
		return desc, nil
	}

	if u := b.findUser(orgID, identifier); u != nil {
		return &EntityDescription{EntityID: u.UserID, Name: u.Name, Type: "USER", State: u.State}, nil
	}
	if g := b.findGroup(orgID, identifier); g != nil {
		return &EntityDescription{EntityID: g.GroupID, Name: g.Name, Type: "GROUP", State: g.State}, nil
	}
	if r := b.findResource(orgID, identifier); r != nil {
		return &EntityDescription{EntityID: r.ResourceID, Name: r.Name, Type: "RESOURCE", State: r.State}, nil
	}

	return nil, fmt.Errorf("%w: entity %q not found", ErrNotFound, identifier)
}

// describeEntityByEmail resolves email against the byEmail reverse-index
// maps. Callers must hold at least b.mu.RLock. Returns nil (not an error)
// when email doesn't match any entity, so DescribeEntity can fall through to
// its ID/name lookup.
func (b *InMemoryBackend) describeEntityByEmail(orgID, email string) *EntityDescription {
	if userID, found := b.usersByEmail[orgID][email]; found {
		if u, exists := b.users.Get(orgKey(orgID, userID)); exists {
			return &EntityDescription{EntityID: u.UserID, Name: u.Name, Type: "USER", State: u.State}
		}
	}
	if groupID, found := b.groupsByEmail[orgID][email]; found {
		if g, exists := b.groups.Get(orgKey(orgID, groupID)); exists {
			return &EntityDescription{EntityID: g.GroupID, Name: g.Name, Type: "GROUP", State: g.State}
		}
	}
	if resourceID, found := b.resourcesByEmail[orgID][email]; found {
		if r, exists := b.resources.Get(orgKey(orgID, resourceID)); exists {
			return &EntityDescription{EntityID: r.ResourceID, Name: r.Name, Type: "RESOURCE", State: r.State}
		}
	}

	return nil
}

// --- Availability Configurations ---

// CreateAvailabilityConfiguration creates an availability configuration for a domain.
func (b *InMemoryBackend) CreateAvailabilityConfiguration(
	orgID, domainName string, ewsProvider *AvailabilityEwsProvider, lambdaARN string,
) (*AvailabilityConfiguration, error) {
	b.mu.Lock("CreateAvailabilityConfiguration")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	if b.availabilityConfigs.Has(orgKey(orgID, domainName)) {
		return nil, fmt.Errorf(
			"%w: availability configuration for %q already exists",
			ErrConflict,
			domainName,
		)
	}
	now := time.Now()
	cfg := &AvailabilityConfiguration{
		DateCreated:  now,
		DateModified: now,
		DomainName:   domainName,
		orgID:        orgID,
	}
	if ewsProvider != nil {
		cfg.ProviderType = providerEWS
		cfg.EwsEndpoint = ewsProvider.EwsEndpoint
		cfg.EwsUsername = ewsProvider.EwsUsername
	} else {
		cfg.ProviderType = providerLambda
		cfg.LambdaARN = lambdaARN
	}
	b.availabilityConfigs.Put(cfg)

	return cfg, nil
}

// DeleteAvailabilityConfiguration deletes an availability configuration.
func (b *InMemoryBackend) DeleteAvailabilityConfiguration(orgID, domainName string) error {
	b.mu.Lock("DeleteAvailabilityConfiguration")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	if !b.availabilityConfigs.Delete(orgKey(orgID, domainName)) {
		return fmt.Errorf(
			"%w: availability configuration for %q not found",
			ErrNotFound,
			domainName,
		)
	}

	return nil
}

// UpdateAvailabilityConfiguration updates an existing availability configuration.
func (b *InMemoryBackend) UpdateAvailabilityConfiguration(
	orgID, domainName string, ewsProvider *AvailabilityEwsProvider, lambdaARN string,
) error {
	b.mu.Lock("UpdateAvailabilityConfiguration")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	cfg, ok := b.availabilityConfigs.Get(orgKey(orgID, domainName))
	if !ok {
		return fmt.Errorf(
			"%w: availability configuration for %q not found",
			ErrNotFound,
			domainName,
		)
	}
	cfg.DateModified = time.Now()
	if ewsProvider != nil {
		cfg.ProviderType = providerEWS
		cfg.EwsEndpoint = ewsProvider.EwsEndpoint
		cfg.EwsUsername = ewsProvider.EwsUsername
		cfg.LambdaARN = ""
	} else {
		cfg.ProviderType = providerLambda
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
	b.mu.RLock("ListAvailabilityConfigurations")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, "", fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	byOrg := b.availabilityConfigsByOrg.Get(orgID)
	cfgs := make([]*AvailabilityConfiguration, 0, len(byOrg))
	cfgs = append(cfgs, byOrg...)
	sort.Slice(cfgs, func(i, j int) bool { return cfgs[i].DomainName < cfgs[j].DomainName })
	page, next := paginate(cfgs, maxResults, nextToken)

	return page, next, nil
}

// TestAvailabilityConfiguration simulates testing a configuration.
func (b *InMemoryBackend) TestAvailabilityConfiguration(
	orgID, domainName string,
) (bool, string, error) {
	b.mu.RLock("TestAvailabilityConfiguration")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return false, "", fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	if domainName == "" {
		return false, "", fmt.Errorf("%w: domainName is required", ErrValidation)
	}

	cfg, ok := b.availabilityConfigs.Get(orgKey(orgID, domainName))
	if !ok {
		return false, "", fmt.Errorf(
			"%w: availability configuration for %q not found",
			ErrNotFound,
			domainName,
		)
	}

	switch cfg.ProviderType {
	case providerEWS:
		if cfg.EwsEndpoint == "" {
			return false, "EwsEndpoint is required", nil
		}
		parsed, err := url.Parse(cfg.EwsEndpoint)
		if err != nil || parsed.Hostname() == "" {
			return false, fmt.Sprintf("invalid EwsEndpoint: %v", err), nil
		}
		if parsed.Scheme != "https" && parsed.Scheme != "http" {
			return false, "EwsEndpoint must use http or https scheme", nil
		}
		if cfg.EwsUsername == "" {
			return false, "EwsUsername is required", nil
		}
	case providerLambda:
		if cfg.LambdaARN == "" {
			return false, "LambdaArn is required", nil
		}
		if !strings.HasPrefix(cfg.LambdaARN, "arn:") {
			return false, fmt.Sprintf("invalid LambdaArn %q: must begin with arn:", cfg.LambdaARN), nil
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
	b.mu.Lock("CreateMobileDeviceAccessRule")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
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
		orgID:                     orgID,
	}
	b.mobileDeviceRules.Put(rule)

	return rule, nil
}

// DeleteMobileDeviceAccessRule deletes a mobile device access rule.
func (b *InMemoryBackend) DeleteMobileDeviceAccessRule(orgID, ruleID string) error {
	b.mu.Lock("DeleteMobileDeviceAccessRule")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	if !b.mobileDeviceRules.Delete(orgKey(orgID, ruleID)) {
		return fmt.Errorf("%w: mobile device access rule %q not found", ErrNotFound, ruleID)
	}

	return nil
}

// UpdateMobileDeviceAccessRule updates a mobile device access rule.
func (b *InMemoryBackend) UpdateMobileDeviceAccessRule(
	orgID, ruleID, name, effect, description string,
	deviceModels, notDeviceModels, deviceTypes, notDeviceTypes,
	deviceOperatingSystems, notDeviceOperatingSystems, deviceUserAgents, notDeviceUserAgents []string,
) error {
	b.mu.Lock("UpdateMobileDeviceAccessRule")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	rule, ok := b.mobileDeviceRules.Get(orgKey(orgID, ruleID))
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
func (b *InMemoryBackend) ListMobileDeviceAccessRules(
	orgID string,
) ([]*MobileDeviceAccessRule, error) {
	b.mu.RLock("ListMobileDeviceAccessRules")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	byOrg := b.mobileDeviceRulesByOrg.Get(orgID)
	rules := make([]*MobileDeviceAccessRule, 0, len(byOrg))
	rules = append(rules, byOrg...)
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
	b.mu.RLock("GetMobileDeviceAccessEffect")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return "", nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	byOrg := b.mobileDeviceRulesByOrg.Get(orgID)
	rules := make([]*MobileDeviceAccessRule, 0, len(byOrg))
	rules = append(rules, byOrg...)
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
	b.mu.Lock("PutMobileDeviceAccessOverride")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	key := orgKey(orgID, mobileOverrideKey(userID, deviceID))
	now := time.Now()
	if existing, ok := b.mobileDeviceOverrides.Get(key); ok {
		existing.Effect = effect
		existing.Description = description
		existing.DateModified = now
	} else {
		b.mobileDeviceOverrides.Put(&MobileDeviceAccessOverride{
			DateCreated:  now,
			DateModified: now,
			UserID:       userID,
			DeviceID:     strings.ToLower(deviceID),
			Effect:       effect,
			Description:  description,
			orgID:        orgID,
		})
	}

	return nil
}

// DeleteMobileDeviceAccessOverride removes a per-user per-device override.
func (b *InMemoryBackend) DeleteMobileDeviceAccessOverride(orgID, userID, deviceID string) error {
	b.mu.Lock("DeleteMobileDeviceAccessOverride")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	key := orgKey(orgID, mobileOverrideKey(userID, deviceID))
	if !b.mobileDeviceOverrides.Delete(key) {
		return fmt.Errorf("%w: mobile device access override not found", ErrNotFound)
	}

	return nil
}

// GetMobileDeviceAccessOverride retrieves a per-user per-device override.
func (b *InMemoryBackend) GetMobileDeviceAccessOverride(
	orgID, userID, deviceID string,
) (*MobileDeviceAccessOverride, error) {
	b.mu.RLock("GetMobileDeviceAccessOverride")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	key := orgKey(orgID, mobileOverrideKey(userID, deviceID))
	ov, ok := b.mobileDeviceOverrides.Get(key)
	if !ok {
		return nil, fmt.Errorf("%w: mobile device access override not found", ErrNotFound)
	}

	return ov, nil
}

// ListMobileDeviceAccessOverrides lists overrides filtered by userID and/or deviceID.
func (b *InMemoryBackend) ListMobileDeviceAccessOverrides(
	orgID, userID, deviceID string, maxResults int32, nextToken string,
) ([]*MobileDeviceAccessOverride, string, error) {
	b.mu.RLock("ListMobileDeviceAccessOverrides")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, "", fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	all := make([]*MobileDeviceAccessOverride, 0)
	for _, ov := range b.mobileDeviceOverridesByOrg.Get(orgID) {
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
func (b *InMemoryBackend) PutEmailMonitoringConfiguration(
	orgID, roleARN, logGroupARN string,
) error {
	b.mu.Lock("PutEmailMonitoringConfiguration")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	b.emailMonitoring.Put(&EmailMonitoringConfiguration{
		RoleARN:     roleARN,
		LogGroupARN: logGroupARN,
		orgID:       orgID,
	})

	return nil
}

// DeleteEmailMonitoringConfiguration removes email monitoring config for an org.
func (b *InMemoryBackend) DeleteEmailMonitoringConfiguration(orgID string) error {
	b.mu.Lock("DeleteEmailMonitoringConfiguration")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	b.emailMonitoring.Delete(orgID)

	return nil
}

// DescribeEmailMonitoringConfiguration returns email monitoring config for an org.
func (b *InMemoryBackend) DescribeEmailMonitoringConfiguration(
	orgID string,
) (*EmailMonitoringConfiguration, error) {
	b.mu.RLock("DescribeEmailMonitoringConfiguration")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	cfg, ok := b.emailMonitoring.Get(orgID)
	if !ok {
		return &EmailMonitoringConfiguration{}, nil
	}

	return cfg, nil
}

// --- Inbound DMARC Settings ---

// PutInboundDmarcSettings sets inbound DMARC enforcement for an org.
func (b *InMemoryBackend) PutInboundDmarcSettings(orgID string, enforced bool) error {
	b.mu.Lock("PutInboundDmarcSettings")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	b.inboundDmarc[orgID] = enforced

	return nil
}

// DescribeInboundDmarcSettings returns whether inbound DMARC is enforced for an org.
func (b *InMemoryBackend) DescribeInboundDmarcSettings(orgID string) (bool, error) {
	b.mu.RLock("DescribeInboundDmarcSettings")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return false, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	return b.inboundDmarc[orgID], nil
}

// --- Retention Policies ---

// PutRetentionPolicy creates or updates a retention policy for an org.
func (b *InMemoryBackend) PutRetentionPolicy(
	orgID, id, name, description string, folderConfigurations []*FolderConfiguration,
) error {
	b.mu.Lock("PutRetentionPolicy")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	if id == "" {
		id = newID()
	}
	b.retentionPolicies.Put(&RetentionPolicy{
		ID:                   id,
		Name:                 name,
		Description:          description,
		FolderConfigurations: folderConfigurations,
		orgID:                orgID,
	})

	return nil
}

// DeleteRetentionPolicy removes the retention policy from an org.
func (b *InMemoryBackend) DeleteRetentionPolicy(orgID, id string) error {
	b.mu.Lock("DeleteRetentionPolicy")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	existing, ok := b.retentionPolicies.Get(orgID)
	if !ok || existing.ID != id {
		return fmt.Errorf("%w: retention policy %q not found", ErrNotFound, id)
	}
	b.retentionPolicies.Delete(orgID)

	return nil
}

// GetDefaultRetentionPolicy returns the retention policy for an org.
func (b *InMemoryBackend) GetDefaultRetentionPolicy(orgID string) (*RetentionPolicy, error) {
	b.mu.RLock("GetDefaultRetentionPolicy")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	pol, ok := b.retentionPolicies.Get(orgID)
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
	b.mu.Lock("StartMailboxExportJob")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
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
		orgID:        orgID,
	}
	b.exportJobs.Put(job)

	return job, nil
}

// CancelMailboxExportJob cancels a running mailbox export job.
func (b *InMemoryBackend) CancelMailboxExportJob(orgID, jobID string) error {
	b.mu.Lock("CancelMailboxExportJob")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	job, ok := b.exportJobs.Get(orgKey(orgID, jobID))
	if !ok {
		return fmt.Errorf("%w: mailbox export job %q not found", ErrNotFound, jobID)
	}
	job.State = "CANCELLED"
	job.EndTime = time.Now()

	return nil
}

// DescribeMailboxExportJob returns details of a mailbox export job.
func (b *InMemoryBackend) DescribeMailboxExportJob(orgID, jobID string) (*MailboxExportJob, error) {
	b.mu.RLock("DescribeMailboxExportJob")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	job, ok := b.exportJobs.Get(orgKey(orgID, jobID))
	if !ok {
		return nil, fmt.Errorf("%w: mailbox export job %q not found", ErrNotFound, jobID)
	}

	return job, nil
}

// ListMailboxExportJobs lists mailbox export jobs for an org.
func (b *InMemoryBackend) ListMailboxExportJobs(
	orgID string, maxResults int32, nextToken string,
) ([]*MailboxExportJob, string, error) {
	b.mu.RLock("ListMailboxExportJobs")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, "", fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	byOrg := b.exportJobsByOrg.Get(orgID)
	jobs := make([]*MailboxExportJob, 0, len(byOrg))
	jobs = append(jobs, byOrg...)
	sort.Slice(jobs, func(i, k int) bool { return jobs[i].JobID < jobs[k].JobID })
	page, next := paginate(jobs, maxResults, nextToken)

	return page, next, nil
}

// --- Identity Center Applications ---

// CreateIdentityCenterApplication creates a new IAM Identity Center application.
func (b *InMemoryBackend) CreateIdentityCenterApplication(
	instanceARN, name string, //nolint:revive // existing issue.
) (string, error) {
	b.mu.Lock("CreateIdentityCenterApplication")
	defer b.mu.Unlock()

	appARN := arn.Build("sso", b.region, b.accountID, "application/"+newID())
	b.identityCenterApps[appARN] = name

	return appARN, nil
}

// DeleteIdentityCenterApplication removes an IAM Identity Center application.
func (b *InMemoryBackend) DeleteIdentityCenterApplication(applicationARN string) error {
	b.mu.Lock("DeleteIdentityCenterApplication")
	defer b.mu.Unlock()

	if _, ok := b.identityCenterApps[applicationARN]; !ok {
		return fmt.Errorf(
			"%w: identity center application %q not found",
			ErrNotFound,
			applicationARN,
		)
	}
	delete(b.identityCenterApps, applicationARN)

	return nil
}

// --- Identity Provider Configuration ---

// PutIdentityProviderConfiguration creates or updates IdP configuration.
func (b *InMemoryBackend) PutIdentityProviderConfiguration(
	orgID, authMode, identityCenterAppARN, identityCenterInstanceARN, patStatus string,
	patLifetimeDays int32,
) error {
	b.mu.Lock("PutIdentityProviderConfiguration")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	cfg := &IdentityProviderConfiguration{
		AuthMode:                  authMode,
		IdentityCenterAppARN:      identityCenterAppARN,
		IdentityCenterInstanceARN: identityCenterInstanceARN,
		PATStatus:                 patStatus,
		orgID:                     orgID,
	}
	if patLifetimeDays > 0 {
		cfg.PATLifetimeDays = &patLifetimeDays
	}
	b.idpConfig.Put(cfg)

	return nil
}

// DeleteIdentityProviderConfiguration removes IdP configuration.
func (b *InMemoryBackend) DeleteIdentityProviderConfiguration(orgID string) error {
	b.mu.Lock("DeleteIdentityProviderConfiguration")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	b.idpConfig.Delete(orgID)

	return nil
}

// DescribeIdentityProviderConfiguration returns IdP configuration for an org.
func (b *InMemoryBackend) DescribeIdentityProviderConfiguration(
	orgID string,
) (*IdentityProviderConfiguration, error) {
	b.mu.RLock("DescribeIdentityProviderConfiguration")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	cfg, ok := b.idpConfig.Get(orgID)
	if !ok {
		return nil, fmt.Errorf("%w: identity provider configuration not found", ErrNotFound)
	}

	return cfg, nil
}

// --- Personal Access Tokens ---

// DeletePersonalAccessToken removes a personal access token.
func (b *InMemoryBackend) DeletePersonalAccessToken(orgID, tokenID string) error {
	b.mu.Lock("DeletePersonalAccessToken")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	if !b.personalTokens.Delete(orgKey(orgID, tokenID)) {
		return fmt.Errorf("%w: personal access token %q not found", ErrNotFound, tokenID)
	}

	return nil
}

// GetPersonalAccessTokenMetadata returns metadata for a personal access token.
func (b *InMemoryBackend) GetPersonalAccessTokenMetadata(
	orgID, tokenID string,
) (*PersonalAccessToken, error) {
	b.mu.RLock("GetPersonalAccessTokenMetadata")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	tok, ok := b.personalTokens.Get(orgKey(orgID, tokenID))
	if !ok {
		return nil, fmt.Errorf("%w: personal access token %q not found", ErrNotFound, tokenID)
	}

	return tok, nil
}

// ListPersonalAccessTokens lists personal access tokens, optionally filtered by userID.
func (b *InMemoryBackend) ListPersonalAccessTokens(
	orgID, userID string, maxResults int32, nextToken string,
) ([]*PersonalAccessToken, string, error) {
	b.mu.RLock("ListPersonalAccessTokens")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, "", fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	all := make([]*PersonalAccessToken, 0)
	for _, tok := range b.personalTokensByOrg.Get(orgID) {
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
	b.mu.Lock("CreatePersonalAccessToken")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	now := time.Now()
	tok := &PersonalAccessToken{
		TokenID:     newID(),
		UserID:      userID,
		Name:        name,
		Scopes:      scopes,
		DateCreated: now,
		ExpiresTime: now.Add(365 * 24 * time.Hour),
		orgID:       orgID,
	}
	b.personalTokens.Put(tok)

	return tok, nil
}

// --- Impersonation Role Effect ---

// GetImpersonationRoleEffect evaluates impersonation rules for a target user.
func (b *InMemoryBackend) GetImpersonationRoleEffect(
	orgID, roleID, targetUser string,
) (string, string, []*ImpersonationMatchedRule, error) {
	b.mu.RLock("GetImpersonationRoleEffect")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return "", "", nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	role, ok := b.impersonation.Get(orgKey(orgID, roleID))
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

// AssumeImpersonationRole issues a validated token for an impersonation role.
func (b *InMemoryBackend) AssumeImpersonationRole(orgID, roleID string) (string, int64, error) {
	b.mu.Lock("AssumeImpersonationRole")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return "", 0, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	if !b.impersonation.Has(orgKey(orgID, roleID)) {
		return "", 0, fmt.Errorf("%w: impersonation role %q not found", ErrNotFound, roleID)
	}

	const ttl = int64(3600)
	token := newID()
	b.issuedTokens.Put(&issuedImpersonationToken{
		Token:     token,
		OrgID:     orgID,
		RoleID:    roleID,
		ExpiresAt: time.Now().Add(time.Duration(ttl) * time.Second),
	})

	return token, ttl, nil
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
