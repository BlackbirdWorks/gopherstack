package workmail

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// workmailSnapshotVersion identifies the shape of [backendSnapshot]. It must
// be bumped whenever a change to a DTO type, a registered table's value
// type, or backendSnapshot itself would make an older snapshot unsafe to
// decode as the current shape. Restore compares this against the persisted
// value and discards (resetLocked, not a partial decode) any mismatch -- see
// Restore below. This is the first version: WorkMail had no persistence at
// all before Phase 3.3 (Handler had no Snapshot/Restore, so cli.go's generic
// setupPersistence never picked it up even though nothing on the backend
// implemented persistence.Persistable either -- see the Handler.Snapshot/
// Restore delegation at the bottom of this file), so there is no legacy
// snapshot shape to be compatible with -- any snapshot without a matching
// Version (including one with no version field, which decodes as 0) is
// discarded the same way any other incompatible snapshot is.
const workmailSnapshotVersion = 1

// orgDTO wraps an org-nested resource for JSON round-tripping through the
// ephemeral persistence registry below. It is shared by every converted
// resource whose only hidden field is orgID (User, Group, Resource,
// ImpersonationRole, MailDomain, AccessControlRule,
// AvailabilityConfiguration, MobileDeviceAccessRule,
// MobileDeviceAccessOverride, MailboxExportJob, PersonalAccessToken) -- the
// same technique services/emr's regionalDTO[V] uses, with orgID standing in
// for region. ID mirrors the resource's own composite-key component (its own
// identifier, or the mobileOverrideKey composite for
// MobileDeviceAccessOverride) so the DTO table itself has a stable composite
// key ("OrgID|ID").
type orgDTO[V any] struct {
	Value *V     `json:"value"`
	OrgID string `json:"orgID"`
	ID    string `json:"id"`
}

func orgDTOKeyFn[V any](d *orgDTO[V]) string { return orgKey(d.OrgID, d.ID) }

// orgOnlyDTO wraps a resource that is keyed directly by orgID (one value per
// org, not per-resource) -- EmailMonitoringConfiguration, RetentionPolicy,
// IdentityProviderConfiguration. Unlike orgDTO there is no separate ID: orgID
// itself is the whole primary key.
type orgOnlyDTO[V any] struct {
	Value *V     `json:"value"`
	OrgID string `json:"orgID"`
}

func orgOnlyDTOKeyFn[V any](d *orgOnlyDTO[V]) string { return d.OrgID }

// permissionDTO wraps a Permission for JSON round-tripping. Permission hides
// two fields (orgID, entityID) rather than orgDTO's one, so it gets its own
// DTO instead of reusing orgDTO.
type permissionDTO struct {
	Value     *Permission `json:"value"`
	OrgID     string      `json:"orgID"`
	EntityID  string      `json:"entityID"`
	GranteeID string      `json:"granteeID"`
}

func permissionDTOKeyFn(d *permissionDTO) string {
	return permissionKey(d.OrgID, d.EntityID, d.GranteeID)
}

// persistenceDTOTables groups every ephemeral DTO table
// buildPersistenceDTORegistry constructs, so Snapshot/Restore can pass them
// around as one value instead of sixteen separate return values.
type persistenceDTOTables struct {
	registry              *store.Registry
	users                 *store.Table[orgDTO[User]]
	groups                *store.Table[orgDTO[Group]]
	resources             *store.Table[orgDTO[Resource]]
	impersonation         *store.Table[orgDTO[ImpersonationRole]]
	mailDomains           *store.Table[orgDTO[MailDomain]]
	accessRules           *store.Table[orgDTO[AccessControlRule]]
	availabilityConfigs   *store.Table[orgDTO[AvailabilityConfiguration]]
	mobileDeviceRules     *store.Table[orgDTO[MobileDeviceAccessRule]]
	mobileDeviceOverrides *store.Table[orgDTO[MobileDeviceAccessOverride]]
	exportJobs            *store.Table[orgDTO[MailboxExportJob]]
	personalTokens        *store.Table[orgDTO[PersonalAccessToken]]
	emailMonitoring       *store.Table[orgOnlyDTO[EmailMonitoringConfiguration]]
	retentionPolicies     *store.Table[orgOnlyDTO[RetentionPolicy]]
	idpConfig             *store.Table[orgOnlyDTO[IdentityProviderConfiguration]]
	permissions           *store.Table[permissionDTO]
}

// buildPersistenceDTORegistry constructs the ephemeral DTO registry used by
// both Snapshot and Restore for the fifteen tables that are NOT on
// b.registry (see store_setup.go). It is built fresh on every call, mirroring
// services/codecommit's and services/emr's buildPersistenceDTORegistry.
func buildPersistenceDTORegistry() persistenceDTOTables {
	dtoReg := store.NewRegistry()

	return persistenceDTOTables{
		registry:      dtoReg,
		users:         store.Register(dtoReg, "users", store.New(orgDTOKeyFn[User])),
		groups:        store.Register(dtoReg, "groups", store.New(orgDTOKeyFn[Group])),
		resources:     store.Register(dtoReg, "resources", store.New(orgDTOKeyFn[Resource])),
		impersonation: store.Register(dtoReg, "impersonation", store.New(orgDTOKeyFn[ImpersonationRole])),
		mailDomains:   store.Register(dtoReg, "mailDomains", store.New(orgDTOKeyFn[MailDomain])),
		accessRules:   store.Register(dtoReg, "accessRules", store.New(orgDTOKeyFn[AccessControlRule])),
		availabilityConfigs: store.Register(
			dtoReg, "availabilityConfigs", store.New(orgDTOKeyFn[AvailabilityConfiguration]),
		),
		mobileDeviceRules: store.Register(
			dtoReg, "mobileDeviceRules", store.New(orgDTOKeyFn[MobileDeviceAccessRule]),
		),
		mobileDeviceOverrides: store.Register(
			dtoReg, "mobileDeviceOverrides", store.New(orgDTOKeyFn[MobileDeviceAccessOverride]),
		),
		exportJobs:     store.Register(dtoReg, "exportJobs", store.New(orgDTOKeyFn[MailboxExportJob])),
		personalTokens: store.Register(dtoReg, "personalTokens", store.New(orgDTOKeyFn[PersonalAccessToken])),
		emailMonitoring: store.Register(
			dtoReg, "emailMonitoring", store.New(orgOnlyDTOKeyFn[EmailMonitoringConfiguration]),
		),
		retentionPolicies: store.Register(
			dtoReg, "retentionPolicies", store.New(orgOnlyDTOKeyFn[RetentionPolicy]),
		),
		idpConfig: store.Register(
			dtoReg, "idpConfig", store.New(orgOnlyDTOKeyFn[IdentityProviderConfiguration]),
		),
		permissions: store.Register(dtoReg, "permissions", store.New(permissionDTOKeyFn)),
	}
}

// backendSnapshot is the top-level on-disk shape for the WorkMail backend.
//
// Tables holds one JSON-encoded array per table name: "organizations",
// "globalAliases", and "issuedTokens" produced directly by
// b.registry.SnapshotAll() (none of the three hide any field from JSON), plus
// the fifteen DTO-wrapped tables built by buildPersistenceDTORegistry above
// (each hides orgID, and for permissions also entityID).
//
// OrgsByAlias, UsersByEmail, GroupsByEmail, ResourcesByEmail, GroupMembers,
// Delegates, Aliases, MailboxQuotas, Tags, InboundDmarc, and
// IdentityCenterApps are the plain (non-store.Table) maps left unconverted
// by Phase 3.3 -- see the field comments on InMemoryBackend in backend.go:
// each holds a value with no identity of its own (a set, a bare scalar, a
// slice, or a reverse-lookup string), so there is nothing for store.Table to
// key on. All are persisted directly here. Version guards against decoding a
// snapshot from an incompatible (older or newer) build of this backend as
// though it were the current shape; see Restore.
type backendSnapshot struct {
	Tables             map[string]json.RawMessage            `json:"tables"`
	OrgsByAlias        map[string]string                     `json:"orgsByAlias"`
	UsersByEmail       map[string]map[string]string          `json:"usersByEmail"`
	GroupsByEmail      map[string]map[string]string          `json:"groupsByEmail"`
	ResourcesByEmail   map[string]map[string]string          `json:"resourcesByEmail"`
	GroupMembers       map[string]map[string]map[string]bool `json:"groupMembers"`
	Delegates          map[string]map[string]map[string]bool `json:"delegates"`
	Aliases            map[string]map[string][]string        `json:"aliases"`
	MailboxQuotas      map[string]map[string]int32           `json:"mailboxQuotas"`
	Tags               map[string][]Tag                      `json:"tags"`
	InboundDmarc       map[string]bool                       `json:"inboundDmarc"`
	IdentityCenterApps map[string]string                     `json:"identityCenterApps"`
	AccountID          string                                `json:"accountID"`
	Region             string                                `json:"region"`
	Version            int                                   `json:"version"`
}

// Snapshot serializes the backend state to JSON. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "workmail: snapshot table marshal failed", "error", err)

		return nil
	}

	dtos := buildPersistenceDTORegistry()
	b.fillPersistenceDTOs(dtos)

	dtoTables, err := dtos.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "workmail: snapshot DTO table marshal failed", "error", err)

		return nil
	}
	maps.Copy(tables, dtoTables)

	snap := backendSnapshot{
		Version:            workmailSnapshotVersion,
		Tables:             tables,
		OrgsByAlias:        b.orgsByAlias,
		UsersByEmail:       b.usersByEmail,
		GroupsByEmail:      b.groupsByEmail,
		ResourcesByEmail:   b.resourcesByEmail,
		GroupMembers:       b.groupMembers,
		Delegates:          b.delegates,
		Aliases:            b.aliases,
		MailboxQuotas:      b.mailboxQuotas,
		Tags:               b.tags,
		InboundDmarc:       b.inboundDmarc,
		IdentityCenterApps: b.identityCenterApps,
		AccountID:          b.accountID,
		Region:             b.region,
	}

	return persistence.MarshalSnapshot(ctx, "workmail", snap)
}

// fillPersistenceDTOs populates dtos from every live org-nested / composite
// -keyed table, factored out of Snapshot to keep Snapshot's own cognitive
// complexity low. Split into two halves (fillEntityDTOs/fillConfigDTOs) to
// stay under the per-function cyclomatic-complexity budget. Callers must
// hold at least b.mu.RLock.
func (b *InMemoryBackend) fillPersistenceDTOs(dtos persistenceDTOTables) {
	b.fillEntityDTOs(dtos)
	b.fillConfigDTOs(dtos)
}

// fillEntityDTOs populates the per-org-entity DTO tables (users through
// personalTokens).
func (b *InMemoryBackend) fillEntityDTOs(dtos persistenceDTOTables) {
	for _, v := range b.users.Snapshot() {
		dtos.users.Put(&orgDTO[User]{Value: v, OrgID: v.orgID, ID: v.UserID})
	}
	for _, v := range b.groups.Snapshot() {
		dtos.groups.Put(&orgDTO[Group]{Value: v, OrgID: v.orgID, ID: v.GroupID})
	}
	for _, v := range b.resources.Snapshot() {
		dtos.resources.Put(&orgDTO[Resource]{Value: v, OrgID: v.orgID, ID: v.ResourceID})
	}
	for _, v := range b.impersonation.Snapshot() {
		dtos.impersonation.Put(&orgDTO[ImpersonationRole]{Value: v, OrgID: v.orgID, ID: v.RoleID})
	}
	for _, v := range b.mailDomains.Snapshot() {
		dtos.mailDomains.Put(&orgDTO[MailDomain]{Value: v, OrgID: v.orgID, ID: v.DomainName})
	}
	for _, v := range b.accessRules.Snapshot() {
		dtos.accessRules.Put(&orgDTO[AccessControlRule]{Value: v, OrgID: v.orgID, ID: v.Name})
	}
	for _, v := range b.availabilityConfigs.Snapshot() {
		dtos.availabilityConfigs.Put(
			&orgDTO[AvailabilityConfiguration]{Value: v, OrgID: v.orgID, ID: v.DomainName},
		)
	}
	for _, v := range b.mobileDeviceRules.Snapshot() {
		dtos.mobileDeviceRules.Put(&orgDTO[MobileDeviceAccessRule]{Value: v, OrgID: v.orgID, ID: v.RuleID})
	}
	for _, v := range b.mobileDeviceOverrides.Snapshot() {
		id := mobileOverrideKey(v.UserID, v.DeviceID)
		dtos.mobileDeviceOverrides.Put(&orgDTO[MobileDeviceAccessOverride]{Value: v, OrgID: v.orgID, ID: id})
	}
	for _, v := range b.exportJobs.Snapshot() {
		dtos.exportJobs.Put(&orgDTO[MailboxExportJob]{Value: v, OrgID: v.orgID, ID: v.JobID})
	}
	for _, v := range b.personalTokens.Snapshot() {
		dtos.personalTokens.Put(&orgDTO[PersonalAccessToken]{Value: v, OrgID: v.orgID, ID: v.TokenID})
	}
}

// fillConfigDTOs populates the one-per-org config DTO tables plus
// permissions.
func (b *InMemoryBackend) fillConfigDTOs(dtos persistenceDTOTables) {
	for _, v := range b.emailMonitoring.Snapshot() {
		dtos.emailMonitoring.Put(&orgOnlyDTO[EmailMonitoringConfiguration]{Value: v, OrgID: v.orgID})
	}
	for _, v := range b.retentionPolicies.Snapshot() {
		dtos.retentionPolicies.Put(&orgOnlyDTO[RetentionPolicy]{Value: v, OrgID: v.orgID})
	}
	for _, v := range b.idpConfig.Snapshot() {
		dtos.idpConfig.Put(&orgOnlyDTO[IdentityProviderConfiguration]{Value: v, OrgID: v.orgID})
	}
	for _, v := range b.permissions.Snapshot() {
		dtos.permissions.Put(&permissionDTO{Value: v, OrgID: v.orgID, EntityID: v.entityID, GranteeID: v.GranteeID})
	}
}

// Restore deserializes backend state from a snapshot. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot
	if err := persistence.UnmarshalSnapshot(ctx, "workmail", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != workmailSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"workmail: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", workmailSnapshotVersion)

		b.resetLocked()

		return nil
	}

	if err := b.restoreResourceTables(snap.Tables); err != nil {
		return err
	}

	b.restoreRawMaps(&snap)

	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// restoreResourceTables rebuilds every store.Table on b from snap's
// per-table JSON, factored out of Restore to keep Restore's own cognitive
// complexity low. Callers must hold b.mu.Lock.
func (b *InMemoryBackend) restoreResourceTables(tables map[string]json.RawMessage) error {
	if err := b.registry.RestoreAll(tables); err != nil {
		return fmt.Errorf("workmail: restore clean tables: %w", err)
	}

	dtos := buildPersistenceDTORegistry()
	if err := dtos.registry.RestoreAll(tables); err != nil {
		return fmt.Errorf("workmail: restore DTO tables: %w", err)
	}

	b.users.Restore(unwrapOrgDTOs(dtos.users, func(v *User, orgID string) { v.orgID = orgID }))
	b.groups.Restore(unwrapOrgDTOs(dtos.groups, func(v *Group, orgID string) { v.orgID = orgID }))
	b.resources.Restore(unwrapOrgDTOs(dtos.resources, func(v *Resource, orgID string) { v.orgID = orgID }))
	b.impersonation.Restore(
		unwrapOrgDTOs(dtos.impersonation, func(v *ImpersonationRole, orgID string) { v.orgID = orgID }),
	)
	b.mailDomains.Restore(unwrapOrgDTOs(dtos.mailDomains, func(v *MailDomain, orgID string) { v.orgID = orgID }))
	b.accessRules.Restore(
		unwrapOrgDTOs(dtos.accessRules, func(v *AccessControlRule, orgID string) { v.orgID = orgID }),
	)
	b.availabilityConfigs.Restore(unwrapOrgDTOs(
		dtos.availabilityConfigs, func(v *AvailabilityConfiguration, orgID string) { v.orgID = orgID },
	))
	b.mobileDeviceRules.Restore(unwrapOrgDTOs(
		dtos.mobileDeviceRules, func(v *MobileDeviceAccessRule, orgID string) { v.orgID = orgID },
	))
	b.mobileDeviceOverrides.Restore(unwrapOrgDTOs(
		dtos.mobileDeviceOverrides, func(v *MobileDeviceAccessOverride, orgID string) { v.orgID = orgID },
	))
	b.exportJobs.Restore(
		unwrapOrgDTOs(dtos.exportJobs, func(v *MailboxExportJob, orgID string) { v.orgID = orgID }),
	)
	b.personalTokens.Restore(unwrapOrgDTOs(
		dtos.personalTokens, func(v *PersonalAccessToken, orgID string) { v.orgID = orgID },
	))
	b.emailMonitoring.Restore(unwrapOrgOnlyDTOs(
		dtos.emailMonitoring, func(v *EmailMonitoringConfiguration, orgID string) { v.orgID = orgID },
	))
	b.retentionPolicies.Restore(unwrapOrgOnlyDTOs(
		dtos.retentionPolicies, func(v *RetentionPolicy, orgID string) { v.orgID = orgID },
	))
	b.idpConfig.Restore(unwrapOrgOnlyDTOs(
		dtos.idpConfig, func(v *IdentityProviderConfiguration, orgID string) { v.orgID = orgID },
	))
	b.permissions.Restore(unwrapPermissionDTOs(dtos.permissions))

	return nil
}

// unwrapOrgDTOs converts every orgDTO[V] in dtos into its live *V, restoring
// the unexported orgID field each carries via setOrgID (a plain generic type
// parameter V has no field access, so the assignment is supplied by the
// caller, one per concrete V; see restoreResourceTables). A DTO whose Value
// is nil -- not producible by Snapshot, but a defensive guard against a
// hand-edited or corrupted snapshot -- is skipped rather than dereferenced.
func unwrapOrgDTOs[V any](dtos *store.Table[orgDTO[V]], setOrgID func(*V, string)) []*V {
	items := make([]*V, 0, dtos.Len())

	for _, d := range dtos.All() {
		if d.Value == nil {
			continue
		}

		setOrgID(d.Value, d.OrgID)
		items = append(items, d.Value)
	}

	return items
}

// unwrapOrgOnlyDTOs is unwrapOrgDTOs' counterpart for the three
// keyed-directly-by-orgID tables (EmailMonitoringConfiguration,
// RetentionPolicy, IdentityProviderConfiguration).
func unwrapOrgOnlyDTOs[V any](dtos *store.Table[orgOnlyDTO[V]], setOrgID func(*V, string)) []*V {
	items := make([]*V, 0, dtos.Len())

	for _, d := range dtos.All() {
		if d.Value == nil {
			continue
		}

		setOrgID(d.Value, d.OrgID)
		items = append(items, d.Value)
	}

	return items
}

// unwrapPermissionDTOs converts every permissionDTO in dtos into its live
// *Permission, restoring both hidden fields (orgID, entityID).
func unwrapPermissionDTOs(dtos *store.Table[permissionDTO]) []*Permission {
	items := make([]*Permission, 0, dtos.Len())

	for _, d := range dtos.All() {
		if d.Value == nil {
			continue
		}

		d.Value.orgID = d.OrgID
		d.Value.entityID = d.EntityID
		items = append(items, d.Value)
	}

	return items
}

// restoreRawMaps restores every plain (non-store.Table) persisted map from
// snap, defaulting each to an empty map when absent from the snapshot.
// Factored out of Restore to keep Restore's own cognitive complexity low.
// Callers must hold b.mu.Lock.
func (b *InMemoryBackend) restoreRawMaps(snap *backendSnapshot) {
	b.restoreReverseLookupMaps(snap)
	b.restoreCollectionMaps(snap)
}

// restoreReverseLookupMaps restores the string-keyed reverse-lookup maps.
// Split out of restoreRawMaps to keep each half small.
func (b *InMemoryBackend) restoreReverseLookupMaps(snap *backendSnapshot) {
	b.orgsByAlias = snap.OrgsByAlias
	if b.orgsByAlias == nil {
		b.orgsByAlias = make(map[string]string)
	}

	b.usersByEmail = snap.UsersByEmail
	if b.usersByEmail == nil {
		b.usersByEmail = make(map[string]map[string]string)
	}

	b.groupsByEmail = snap.GroupsByEmail
	if b.groupsByEmail == nil {
		b.groupsByEmail = make(map[string]map[string]string)
	}

	b.resourcesByEmail = snap.ResourcesByEmail
	if b.resourcesByEmail == nil {
		b.resourcesByEmail = make(map[string]map[string]string)
	}

	b.identityCenterApps = snap.IdentityCenterApps
	if b.identityCenterApps == nil {
		b.identityCenterApps = make(map[string]string)
	}
}

// restoreCollectionMaps restores the remaining raw collection maps (sets,
// scalars, and slices with no identity of their own). Split out of
// restoreRawMaps to keep each half small.
func (b *InMemoryBackend) restoreCollectionMaps(snap *backendSnapshot) {
	b.groupMembers = snap.GroupMembers
	if b.groupMembers == nil {
		b.groupMembers = make(map[string]map[string]map[string]bool)
	}

	b.delegates = snap.Delegates
	if b.delegates == nil {
		b.delegates = make(map[string]map[string]map[string]bool)
	}

	b.aliases = snap.Aliases
	if b.aliases == nil {
		b.aliases = make(map[string]map[string][]string)
	}

	b.mailboxQuotas = snap.MailboxQuotas
	if b.mailboxQuotas == nil {
		b.mailboxQuotas = make(map[string]map[string]int32)
	}

	b.tags = snap.Tags
	if b.tags == nil {
		b.tags = make(map[string][]Tag)
	}

	b.inboundDmarc = snap.InboundDmarc
	if b.inboundDmarc == nil {
		b.inboundDmarc = make(map[string]bool)
	}
}

// Snapshot implements persistence.Persistable by delegating to the backend.
// Handler previously had no Snapshot/Restore of its own -- and neither did
// InMemoryBackend -- so cli.go's generic setupPersistence (which
// type-asserts the registered service.Registerable, i.e. the Handler, for a
// Snapshot/Restore pair) never picked WorkMail up at all: dead wiring, with
// no persistence underneath it either. This delegation (matching the
// codecommit/codepipeline/emr pattern) is what wires WorkMail into
// persistence for the first time.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
