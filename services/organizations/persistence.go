package organizations

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// organizationsSnapshotVersion identifies the shape of [backendSnapshot]. It
// must be bumped whenever a change to delegatedAdminSnapshot or
// backendSnapshot itself would make an older snapshot unsafe to decode as
// the current shape. Restore compares this against the persisted value and
// discards (resetStateLocked, not a partial decode) any mismatch -- see
// Restore. The pre-Phase-3.3 snapshot format had no version field at all, so
// an old snapshot decodes with Version == 0, which is guaranteed to
// mismatch organizationsSnapshotVersion and is discarded the same way any
// other incompatible snapshot is.
const organizationsSnapshotVersion = 1

// delegatedAdminSnapshot is the DTO used only for Snapshot/Restore of the
// "dirty" delegatedAdmins table -- see store_setup.go's file doc comment for
// why delegatedAdmins can't be registered directly on b.registry. It wraps
// the live DelegatedAdmin alongside its ServicePrincipal, which the live
// type intentionally excludes from JSON (`json:"-"`), so the identity
// survives the round trip.
type delegatedAdminSnapshot struct {
	ServicePrincipal string         `json:"servicePrincipal"`
	Admin            DelegatedAdmin `json:"admin"`
}

func delegatedAdminSnapshotKey(v *delegatedAdminSnapshot) string {
	return delegatedAdminKey(v.ServicePrincipal, v.Admin.AccountID)
}

// newDirtyDTORegistry builds the ephemeral DTO [store.Registry] shared by
// Snapshot and Restore for the delegatedAdmins table, which can't be
// registered on b.registry directly (see store_setup.go).
func newDirtyDTORegistry() (*store.Registry, *store.Table[delegatedAdminSnapshot]) {
	dtoReg := store.NewRegistry()
	daDTOs := store.Register(dtoReg, "delegatedAdmins", store.New(delegatedAdminSnapshotKey))

	return dtoReg, daDTOs
}

// backendSnapshot is the top-level on-disk shape for the Organizations
// backend.
//
// Tables holds one JSON-encoded array per registered table name, produced by
// merging b.registry.SnapshotAll() (the "clean" tables: accounts, ous,
// policies, createStatuses, handshakes, serviceAccess,
// responsibilityTransfers) with the ephemeral
// DTO registry's SnapshotAll() (the "dirty" delegatedAdmins table). Version
// guards against decoding a snapshot from an incompatible (older or newer)
// build of this backend as though it were the current shape; see Restore.
//
// The remaining fields mirror the plain map fields left un-converted on
// InMemoryBackend (see its doc comment in backend.go for why each one stays
// a raw map). ousByParent and accountChildrenByParent are deliberately
// absent: they were never part of the pre-Phase-3.3 snapshot either (they
// are rebuilt-on-write derived indexes, not source-of-truth state), so this
// preserves that existing gap rather than fixing it as a side effect of the
// datalayer swap.
type backendSnapshot struct {
	Tables           map[string]json.RawMessage   `json:"tables"`
	Org              *Organization                `json:"org"`
	Root             *Root                        `json:"root"`
	ResourcePolicy   *ResourcePolicy              `json:"resourcePolicy,omitempty"`
	TargetPolicies   map[string][]string          `json:"targetPolicies"`
	PolicyTargets    map[string][]string          `json:"policyTargets"`
	AccountParent    map[string]string            `json:"accountParent"`
	OUParent         map[string]string            `json:"ouParent"`
	Tags             map[string]map[string]string `json:"tags"`
	EmailToAccountID map[string]string            `json:"emailToAccountID"`
	AccountID        string                       `json:"accountID"`
	Region           string                       `json:"region"`
	AccountCounter   int                          `json:"accountCounter"`
	StatusCounter    int                          `json:"statusCounter"`
	Version          int                          `json:"version"`
}

// ensureNonNilMaps replaces nil maps in snap with empty allocations. Only
// the plain-map fields need this: the Table-backed collections in Tables
// are handled by store.Registry.RestoreAll, which never leaves a registered
// table nil.
func ensureNonNilMaps(snap *backendSnapshot) {
	if snap.TargetPolicies == nil {
		snap.TargetPolicies = make(map[string][]string)
	}

	if snap.PolicyTargets == nil {
		snap.PolicyTargets = make(map[string][]string)
	}

	if snap.AccountParent == nil {
		snap.AccountParent = make(map[string]string)
	}

	if snap.OUParent == nil {
		snap.OUParent = make(map[string]string)
	}

	if snap.Tags == nil {
		snap.Tags = make(map[string]map[string]string)
	}

	if snap.EmailToAccountID == nil {
		snap.EmailToAccountID = make(map[string]string)
	}
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "organizations: snapshot table marshal failed", "error", err)

		return nil
	}

	dtoReg, daDTOs := newDirtyDTORegistry()

	for _, da := range b.delegatedAdmins.Snapshot() {
		daDTOs.Put(&delegatedAdminSnapshot{ServicePrincipal: da.ServicePrincipal, Admin: *da})
	}

	dirtyTables, err := dtoReg.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "organizations: snapshot DTO table marshal failed", "error", err)

		return nil
	}

	maps.Copy(tables, dirtyTables)

	snap := backendSnapshot{
		Version:          organizationsSnapshotVersion,
		Tables:           tables,
		Org:              b.org,
		Root:             b.root,
		ResourcePolicy:   b.resourcePolicy,
		TargetPolicies:   b.targetPolicies,
		PolicyTargets:    b.policyTargets,
		AccountParent:    b.accountParent,
		OUParent:         b.ouParent,
		Tags:             b.tags,
		EmailToAccountID: b.emailToAccountID,
		AccountID:        b.accountID,
		Region:           b.region,
		AccountCounter:   b.accountCounter,
		StatusCounter:    b.statusCounter,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "organizations: failed to marshal snapshot", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "organizations", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != organizationsSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"organizations: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", organizationsSnapshotVersion)

		b.resetStateLocked()
		b.statusCounter = 0

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("organizations: restore snapshot tables: %w", err)
	}

	if err := b.restoreDelegatedAdminsLocked(snap.Tables); err != nil {
		return err
	}

	ensureNonNilMaps(&snap)

	b.org = snap.Org
	b.root = snap.Root
	b.resourcePolicy = snap.ResourcePolicy
	b.targetPolicies = snap.TargetPolicies
	b.policyTargets = snap.PolicyTargets
	b.accountParent = snap.AccountParent
	b.ouParent = snap.OUParent
	b.tags = snap.Tags
	b.emailToAccountID = snap.EmailToAccountID
	b.accountID = snap.AccountID
	b.region = snap.Region
	b.accountCounter = snap.AccountCounter
	b.statusCounter = snap.StatusCounter

	return nil
}

// restoreDelegatedAdminsLocked restores the "dirty" delegatedAdmins table
// from tables via the ephemeral DTO registry, converting each DTO back to
// its live *DelegatedAdmin. The caller MUST hold b.mu for writing.
func (b *InMemoryBackend) restoreDelegatedAdminsLocked(tables map[string]json.RawMessage) error {
	dtoReg, daDTOs := newDirtyDTORegistry()

	if err := dtoReg.RestoreAll(tables); err != nil {
		return fmt.Errorf("organizations: restore delegated admins: %w", err)
	}

	live := make([]*DelegatedAdmin, 0, daDTOs.Len())

	for _, dto := range daDTOs.All() {
		da := dto.Admin
		da.ServicePrincipal = dto.ServicePrincipal
		live = append(live, &da)
	}

	b.delegatedAdmins.Restore(live)

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
