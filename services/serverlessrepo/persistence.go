package serverlessrepo

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// serverlessrepoSnapshotVersion identifies the shape of [backendSnapshot]. It
// must be bumped whenever a change to a DTO type or backendSnapshot itself
// would make an older snapshot unsafe to decode as the current shape.
// Restore compares this against the persisted value and discards
// (registry.ResetAll() + resetting the dirty tables and raw maps, not a
// partial decode) any mismatch -- see Restore. The pre-Phase-3.3 snapshot
// format had no version field at all, so an old snapshot decodes with
// Version == 0, which is guaranteed to mismatch serverlessrepoSnapshotVersion
// and is discarded the same way any other incompatible snapshot is.
const serverlessrepoSnapshotVersion = 1

// versionSnapshot, cfTemplateSnapshot, and cfChangeSetSnapshot are DTOs used
// only for Snapshot/Restore of the "dirty" tables -- see store_setup.go's
// file doc comment for why appVersions/cfTemplates/cfChangeSets can't be
// registered directly on b.registry. Each wraps the live value alongside the
// AppName field that value intentionally excludes from JSON (`json:"-"`), so
// it survives the round trip.
type versionSnapshot struct {
	AppName string             `json:"appName"`
	Version ApplicationVersion `json:"version"`
}

func versionSnapshotKey(v *versionSnapshot) string {
	return versionKey(v.AppName, v.Version.SemanticVersion)
}

type cfTemplateSnapshot struct {
	AppName  string                 `json:"appName"`
	Template CloudFormationTemplate `json:"template"`
}

func cfTemplateSnapshotKey(v *cfTemplateSnapshot) string { return v.Template.TemplateID }

type cfChangeSetSnapshot struct {
	AppName   string                  `json:"appName"`
	ChangeSet CloudFormationChangeSet `json:"changeSet"`
}

func cfChangeSetSnapshotKey(v *cfChangeSetSnapshot) string {
	return changeSetKey(v.AppName, v.ChangeSet.ChangeSetID)
}

// newDirtyDTORegistry builds the ephemeral DTO [store.Registry] shared by
// Snapshot and Restore for the tables that can't be registered on b.registry
// directly (see store_setup.go).
func newDirtyDTORegistry() (
	*store.Registry,
	*store.Table[versionSnapshot],
	*store.Table[cfTemplateSnapshot],
	*store.Table[cfChangeSetSnapshot],
) {
	dtoReg := store.NewRegistry()
	vDTOs := store.Register(dtoReg, "appVersions", store.New(versionSnapshotKey))
	tDTOs := store.Register(dtoReg, "cfTemplates", store.New(cfTemplateSnapshotKey))
	csDTOs := store.Register(dtoReg, "cfChangeSets", store.New(cfChangeSetSnapshotKey))

	return dtoReg, vDTOs, tDTOs, csDTOs
}

// backendSnapshot is the top-level on-disk shape for the ServerlessRepo
// backend.
//
// Tables holds one JSON-encoded array per registered table name, produced by
// merging b.registry.SnapshotAll() (the "clean" applications table) with the
// ephemeral DTO registry's SnapshotAll() (the "dirty" appVersions,
// cfTemplates, cfChangeSets tables). Version guards against decoding a
// snapshot from an incompatible (older or newer) build of this backend as
// though it were the current shape; see Restore.
type backendSnapshot struct {
	Tables          map[string]json.RawMessage                     `json:"tables"`
	AppPolicies     map[string][]*ApplicationPolicyStatement       `json:"appPolicies"`
	AppDependencies map[string]map[string][]*ApplicationDependency `json:"appDependencies"`
	AccountID       string                                         `json:"accountID"`
	Region          string                                         `json:"region"`
	Version         int                                            `json:"version"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "serverlessrepo: snapshot table marshal failed", "error", err)

		return nil
	}

	dtoReg, vDTOs, tDTOs, csDTOs := newDirtyDTORegistry()

	for _, v := range b.appVersions.Snapshot() {
		vDTOs.Put(&versionSnapshot{AppName: v.AppName, Version: *v})
	}

	for _, t := range b.cfTemplates.Snapshot() {
		tDTOs.Put(&cfTemplateSnapshot{AppName: t.AppName, Template: *t})
	}

	for _, cs := range b.cfChangeSets.Snapshot() {
		csDTOs.Put(&cfChangeSetSnapshot{AppName: cs.AppName, ChangeSet: *cs})
	}

	dirtyTables, err := dtoReg.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "serverlessrepo: snapshot DTO table marshal failed", "error", err)

		return nil
	}

	maps.Copy(tables, dirtyTables)

	// Deep-copy policy statements and dependencies to prevent shared-pointer
	// mutations after snapshot.
	appPoliciesCopy := make(map[string][]*ApplicationPolicyStatement, len(b.appPolicies))
	for appName, stmts := range b.appPolicies {
		appPoliciesCopy[appName] = clonePolicyStatements(stmts)
	}

	appDependenciesCopy := make(map[string]map[string][]*ApplicationDependency, len(b.appDependencies))

	for appName, versions := range b.appDependencies {
		versionsCopy := make(map[string][]*ApplicationDependency, len(versions))

		for semVer, deps := range versions {
			depsCopy := make([]*ApplicationDependency, len(deps))
			for i, d := range deps {
				cp := *d
				depsCopy[i] = &cp
			}

			versionsCopy[semVer] = depsCopy
		}

		appDependenciesCopy[appName] = versionsCopy
	}

	snap := backendSnapshot{
		Version:         serverlessrepoSnapshotVersion,
		Tables:          tables,
		AppPolicies:     appPoliciesCopy,
		AppDependencies: appDependenciesCopy,
		AccountID:       b.accountID,
		Region:          b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "serverlessrepo: failed to marshal snapshot", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "serverlessrepo", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != serverlessrepoSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"serverlessrepo: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", serverlessrepoSnapshotVersion)

		b.resetTablesLocked()
		b.appPolicies = make(map[string][]*ApplicationPolicyStatement)
		b.appDependencies = make(map[string]map[string][]*ApplicationDependency)

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("serverlessrepo: restore snapshot tables: %w", err)
	}

	if err := b.restoreDirtyTablesLocked(snap.Tables); err != nil {
		return err
	}

	if snap.AppPolicies == nil {
		snap.AppPolicies = make(map[string][]*ApplicationPolicyStatement)
	}

	b.appPolicies = snap.AppPolicies

	if snap.AppDependencies == nil {
		snap.AppDependencies = make(map[string]map[string][]*ApplicationDependency)
	}

	b.appDependencies = snap.AppDependencies
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// restoreDirtyTablesLocked restores the "dirty" tables (appVersions,
// cfTemplates, cfChangeSets) from tables via the ephemeral DTO registry,
// converting each DTO back to its live type. The caller MUST hold b.mu for
// writing.
func (b *InMemoryBackend) restoreDirtyTablesLocked(tables map[string]json.RawMessage) error {
	dtoReg, vDTOs, tDTOs, csDTOs := newDirtyDTORegistry()

	if err := dtoReg.RestoreAll(tables); err != nil {
		return fmt.Errorf("serverlessrepo: restore snapshot DTO tables: %w", err)
	}

	liveVersions := make([]*ApplicationVersion, 0, vDTOs.Len())

	for _, dto := range vDTOs.All() {
		v := dto.Version
		v.AppName = dto.AppName

		if v.ParameterDefinitions == nil {
			v.ParameterDefinitions = []ParameterDefinition{}
		}

		if v.RequiredCapabilities == nil {
			v.RequiredCapabilities = []string{}
		}

		liveVersions = append(liveVersions, &v)
	}

	b.appVersions.Restore(liveVersions)

	liveTemplates := make([]*CloudFormationTemplate, 0, tDTOs.Len())

	for _, dto := range tDTOs.All() {
		t := dto.Template
		t.AppName = dto.AppName
		liveTemplates = append(liveTemplates, &t)
	}

	b.cfTemplates.Restore(liveTemplates)

	liveChangeSets := make([]*CloudFormationChangeSet, 0, csDTOs.Len())

	for _, dto := range csDTOs.All() {
		cs := dto.ChangeSet
		cs.AppName = dto.AppName
		liveChangeSets = append(liveChangeSets, &cs)
	}

	b.cfChangeSets.Restore(liveChangeSets)

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
