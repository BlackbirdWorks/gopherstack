package ssoadmin

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// ssoadminSnapshotVersion identifies the shape of [backendSnapshot]. It must
// be bumped whenever a change to a DTO type or backendSnapshot itself would
// make an older snapshot unsafe to decode as the current shape. Restore
// compares this against the persisted value and discards (ResetAll, not a
// partial decode) any mismatch -- see Restore. The pre-Phase-3.3 snapshot
// format had no version field at all, so an old snapshot decodes with
// Version == 0, which is guaranteed to mismatch ssoadminSnapshotVersion and
// is discarded the same way any other incompatible snapshot is.
const ssoadminSnapshotVersion = 2

// instanceACASnapshot and permissionsBoundarySnapshot are DTOs used only for
// Snapshot/Restore of the "dirty" tables -- see store_setup.go's file doc
// comment for why instanceACAs/permissionBoundaries can't be registered
// directly on b.registry. Each wraps the live value alongside the identity
// field that value intentionally excludes from JSON (`json:"-"`), so the
// identity survives the round trip.
type instanceACASnapshot struct {
	InstanceArn string     `json:"instanceArn"`
	Config      ABACConfig `json:"config"`
}

func instanceACASnapshotKey(v *instanceACASnapshot) string { return v.InstanceArn }

type permissionsBoundarySnapshot struct {
	PermissionSetArn string              `json:"permissionSetArn"`
	Boundary         PermissionsBoundary `json:"boundary"`
}

func permissionsBoundarySnapshotKey(v *permissionsBoundarySnapshot) string { return v.PermissionSetArn }

// newDirtyDTORegistry builds the ephemeral DTO [store.Registry] shared by
// Snapshot and Restore for the tables that can't be registered on
// b.registry directly (see store_setup.go).
func newDirtyDTORegistry() (
	*store.Registry,
	*store.Table[instanceACASnapshot],
	*store.Table[permissionsBoundarySnapshot],
) {
	dtoReg := store.NewRegistry()
	acaDTOs := store.Register(dtoReg, "instanceACAs", store.New(instanceACASnapshotKey))
	pbDTOs := store.Register(dtoReg, "permissionBoundaries", store.New(permissionsBoundarySnapshotKey))

	return dtoReg, acaDTOs, pbDTOs
}

// backendSnapshot is the top-level on-disk shape for the SSO Admin backend.
//
// Tables holds one JSON-encoded array per registered table name, produced by
// merging b.registry.SnapshotAll() (the "clean" tables: instances,
// permissionSets, creationStatuses, deletionStatuses, provisioningStatuses,
// applications, trustedTokenIssuers) with the ephemeral DTO registry's
// SnapshotAll() (the "dirty" tables: instanceACAs, permissionBoundaries).
// Version guards against decoding a snapshot from an incompatible (older or
// newer) build of this backend as though it were the current shape; see
// Restore.
type backendSnapshot struct {
	Tables                  map[string]json.RawMessage                  `json:"tables"`
	Assignments             map[string][]*AccountAssignment             `json:"assignments"`
	AssignmentCreationIDs   map[string]string                           `json:"assignmentCreationIDs"`
	InstanceRegions         map[string][]RegionMetadata                 `json:"instanceRegions"`
	CustomerManagedPolicies map[string][]CustomerManagedPolicyReference `json:"customerManagedPolicies"`
	ApplicationAssignments  map[string][]*ApplicationAssignment         `json:"applicationAssignments"`
	ApplicationScopes       map[string]map[string][]string              `json:"applicationScopes"`
	ApplicationAuthMethods  map[string]map[string]json.RawMessage       `json:"applicationAuthMethods"`
	ApplicationGrants       map[string]map[string]json.RawMessage       `json:"applicationGrants"`
	ApplicationAssignConfig map[string]bool                             `json:"applicationAssignConfig"`
	ApplicationSessions     map[string]string                           `json:"applicationSessions"`
	AccountID               string                                      `json:"accountID"`
	Region                  string                                      `json:"region"`
	Version                 int                                         `json:"version"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "ssoadmin: snapshot table marshal failed", "error", err)

		return nil
	}

	dtoReg, acaDTOs, pbDTOs := newDirtyDTORegistry()

	for _, v := range b.instanceACAs.Snapshot() {
		acaDTOs.Put(&instanceACASnapshot{InstanceArn: v.InstanceArn, Config: *v})
	}

	for _, v := range b.permissionBoundaries.Snapshot() {
		pbDTOs.Put(&permissionsBoundarySnapshot{PermissionSetArn: v.PermissionSetArn, Boundary: *v})
	}

	dirtyTables, err := dtoReg.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "ssoadmin: snapshot DTO table marshal failed", "error", err)

		return nil
	}

	maps.Copy(tables, dirtyTables)

	snap := backendSnapshot{
		Version:                 ssoadminSnapshotVersion,
		Tables:                  tables,
		Assignments:             b.assignments,
		AssignmentCreationIDs:   b.assignmentCreationIDs,
		InstanceRegions:         b.instanceRegions,
		CustomerManagedPolicies: b.customerManagedPolicies,
		ApplicationAssignments:  b.applicationAssignments,
		ApplicationScopes:       b.applicationScopes,
		ApplicationAuthMethods:  b.applicationAuthMethods,
		ApplicationGrants:       b.applicationGrants,
		ApplicationAssignConfig: b.applicationAssignConfig,
		ApplicationSessions:     b.applicationSessions,
		AccountID:               b.accountID,
		Region:                  b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "ssoadmin: failed to marshal snapshot", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "ssoadmin", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != ssoadminSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"ssoadmin: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", ssoadminSnapshotVersion)

		b.registry.ResetAll()
		b.instanceACAs.Reset()
		b.permissionBoundaries.Reset()
		b.assignments = make(map[string][]*AccountAssignment)
		b.assignmentCreationIDs = make(map[string]string)
		b.instanceRegions = make(map[string][]RegionMetadata)
		b.customerManagedPolicies = make(map[string][]CustomerManagedPolicyReference)
		b.applicationAssignments = make(map[string][]*ApplicationAssignment)
		b.applicationScopes = make(map[string]map[string][]string)
		b.applicationAuthMethods = make(map[string]map[string]json.RawMessage)
		b.applicationGrants = make(map[string]map[string]json.RawMessage)
		b.applicationAssignConfig = make(map[string]bool)
		b.applicationSessions = make(map[string]string)
		b.seedDefaultInstance()

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("ssoadmin: restore snapshot tables: %w", err)
	}

	if err := b.restoreDirtyTablesLocked(snap.Tables); err != nil {
		return err
	}

	b.assignments = ensureNonNilMap(snap.Assignments)
	b.assignmentCreationIDs = ensureNonNilMap(snap.AssignmentCreationIDs)
	b.instanceRegions = ensureNonNilMap(snap.InstanceRegions)
	b.customerManagedPolicies = ensureNonNilMap(snap.CustomerManagedPolicies)
	b.applicationAssignments = ensureNonNilMap(snap.ApplicationAssignments)
	b.applicationScopes = ensureNonNilMap(snap.ApplicationScopes)
	b.applicationAuthMethods = ensureNonNilMap(snap.ApplicationAuthMethods)
	b.applicationGrants = ensureNonNilMap(snap.ApplicationGrants)
	b.applicationAssignConfig = ensureNonNilMap(snap.ApplicationAssignConfig)
	b.applicationSessions = ensureNonNilMap(snap.ApplicationSessions)
	b.accountID = snap.AccountID
	b.region = snap.Region

	fixNilTagMaps(b)

	return nil
}

// restoreDirtyTablesLocked restores the "dirty" tables (instanceACAs,
// permissionBoundaries) from tables via the ephemeral DTO registry,
// converting each DTO back to its live type. The caller MUST hold b.mu for
// writing.
func (b *InMemoryBackend) restoreDirtyTablesLocked(tables map[string]json.RawMessage) error {
	dtoReg, acaDTOs, pbDTOs := newDirtyDTORegistry()

	if err := dtoReg.RestoreAll(tables); err != nil {
		return fmt.Errorf("ssoadmin: restore snapshot DTO tables: %w", err)
	}

	liveACAs := make([]*ABACConfig, 0, acaDTOs.Len())

	for _, dto := range acaDTOs.All() {
		cfg := dto.Config
		cfg.InstanceArn = dto.InstanceArn
		liveACAs = append(liveACAs, &cfg)
	}

	b.instanceACAs.Restore(liveACAs)

	liveBoundaries := make([]*PermissionsBoundary, 0, pbDTOs.Len())

	for _, dto := range pbDTOs.All() {
		boundary := dto.Boundary
		boundary.PermissionSetArn = dto.PermissionSetArn
		liveBoundaries = append(liveBoundaries, &boundary)
	}

	b.permissionBoundaries.Restore(liveBoundaries)

	return nil
}

// ensureNonNilMap initialises a nil map read from a snapshot to an empty map.
func ensureNonNilMap[K comparable, V any](m map[K]V) map[K]V {
	if m == nil {
		return make(map[K]V)
	}

	return m
}

// fixNilTagMaps ensures restored resources have non-nil tag maps.
func fixNilTagMaps(b *InMemoryBackend) {
	for _, inst := range b.instances.All() {
		if inst.Tags == nil {
			inst.Tags = make(map[string]string)
		}
	}

	for _, ps := range b.permissionSets.All() {
		if ps.Tags == nil {
			ps.Tags = make(map[string]string)
		}
	}

	for _, app := range b.applications.All() {
		if app.Tags == nil {
			app.Tags = make(map[string]string)
		}
	}

	for _, tti := range b.trustedTokenIssuers.All() {
		if tti.Tags == nil {
			tti.Tags = make(map[string]string)
		}
	}
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
