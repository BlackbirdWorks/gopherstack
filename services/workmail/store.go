package workmail

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

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

// cascadeCleanEntity removes every trace of entityID (a user, group, or
// resource identified by its own store ID) from the collections that
// reference entities by ID but are not covered by the entity's own table
// delete: aliases the entity owns (plus their globalAliases reverse-index
// rows), tags keyed by its ARN, mailbox permissions where it is either the
// target entity or the grantee, its membership in every group, and its
// listing as a delegate on every resource. Callers must hold b.mu.Lock; the
// entity's own row in its own table (users/groups/resources) and its own
// primary-email index entries are the caller's responsibility (see
// DeleteUser/DeleteGroup/DeleteResource).
func (b *InMemoryBackend) cascadeCleanEntity(orgID, entityID, arn string) {
	for _, a := range b.aliases[orgID][entityID] {
		b.globalAliases.Delete(a)
	}
	delete(b.aliases[orgID], entityID)

	delete(b.tags, arn)

	b.cascadeCleanPermissions(orgID, entityID)

	for _, members := range b.groupMembers[orgID] {
		delete(members, entityID)
	}
	for _, delegateSet := range b.delegates[orgID] {
		delete(delegateSet, entityID)
	}
}

// cascadeCleanPermissions removes every mailbox-permission row where
// entityID is either the target entity or the grantee. Split out of
// cascadeCleanEntity to keep it small. Callers must hold b.mu.Lock.
func (b *InMemoryBackend) cascadeCleanPermissions(orgID, entityID string) {
	for _, p := range slices.Clone(b.permissionsByOrgEntity.Get(permissionOrgEntityKey(orgID, entityID))) {
		b.permissions.Delete(permissionKey(orgID, p.entityID, p.GranteeID))
	}

	var granteeKeys []string
	b.permissions.Range(func(p *Permission) bool {
		if p.orgID == orgID && p.GranteeID == entityID {
			granteeKeys = append(granteeKeys, permissionKey(p.orgID, p.entityID, p.GranteeID))
		}

		return true
	})
	for _, k := range granteeKeys {
		b.permissions.Delete(k)
	}
}

// deleteTagsForOrg removes the organization's own tag entry plus every tag
// entry belonging to a resource ARN nested under it (users/groups/resources
// all mint ARNs of the form "<orgARN>/<type>/<id>" -- see entityARN).
// Callers must hold b.mu.Lock.
func (b *InMemoryBackend) deleteTagsForOrg(orgARN string) {
	delete(b.tags, orgARN)
	prefix := orgARN + "/"
	for arn := range b.tags {
		if strings.HasPrefix(arn, prefix) {
			delete(b.tags, arn)
		}
	}
}

// deleteGlobalAliasesForOrg removes every globalAliases row (primary emails
// and CreateAlias-created aliases alike) belonging to orgID. Callers must
// hold b.mu.Lock.
func (b *InMemoryBackend) deleteGlobalAliasesForOrg(orgID string) {
	var keys []string
	b.globalAliases.Range(func(ta *trackedAlias) bool {
		if ta.OrgID == orgID {
			keys = append(keys, ta.Alias)
		}

		return true
	})
	for _, k := range keys {
		b.globalAliases.Delete(k)
	}
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
