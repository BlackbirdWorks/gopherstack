package codedeploy

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// codedeploySnapshotVersion identifies the shape of [backendSnapshot]. It
// must be bumped whenever a change to a DTO type, a registered table's value
// type, or backendSnapshot itself would make an older snapshot unsafe to
// decode as the current shape. Restore compares this against the persisted
// value and discards (registry.ResetAll plus the dirty tables' own Reset,
// not a partial decode) any mismatch -- see Restore below. This is the
// first version: the pre-Phase-3.3 snapshot carried no version field at
// all, so it also fails this check and is discarded rather than misread,
// which is the safe behaviour across any snapshot-format change.
const codedeploySnapshotVersion = 1

// applicationSnapshot is the JSON-serializable form of Application. Its
// live Tags field is marked json:"-" (see backend.go), so TagsMap carries
// the tag data through the round-trip instead -- the same technique
// services/resourcegroups' groupSnapshot uses.
type applicationSnapshot struct {
	TagsMap map[string]string `json:"tagsMap"`
	Application
}

func applicationSnapshotKeyFn(v *applicationSnapshot) string { return v.ApplicationName }

func toApplicationSnapshot(a *Application) *applicationSnapshot {
	var tagsMap map[string]string
	if a.Tags != nil {
		tagsMap = a.Tags.Clone()
	}

	return &applicationSnapshot{Application: *a, TagsMap: tagsMap}
}

// applicationFromSnapshot rebuilds a live Application from its persisted
// representation, giving it a freshly (and correctly) named Tags instance.
func applicationFromSnapshot(s *applicationSnapshot) *Application {
	app := s.Application
	app.Tags = tags.New("codedeploy.application." + app.ApplicationName + ".tags")
	if len(s.TagsMap) > 0 {
		app.Tags.Merge(s.TagsMap)
	}

	return &app
}

// deploymentGroupSnapshot is the JSON-serializable form of DeploymentGroup
// (with tags map); see applicationSnapshot's comment for why.
type deploymentGroupSnapshot struct {
	TagsMap map[string]string `json:"tagsMap"`
	DeploymentGroup
}

func deploymentGroupSnapshotKeyFn(v *deploymentGroupSnapshot) string {
	return dgKey(v.ApplicationName, v.DeploymentGroupName)
}

func toDeploymentGroupSnapshot(dg *DeploymentGroup) *deploymentGroupSnapshot {
	var tagsMap map[string]string
	if dg.Tags != nil {
		tagsMap = dg.Tags.Clone()
	}

	return &deploymentGroupSnapshot{DeploymentGroup: *dg, TagsMap: tagsMap}
}

func deploymentGroupFromSnapshot(s *deploymentGroupSnapshot) *DeploymentGroup {
	dg := s.DeploymentGroup
	dg.Tags = tags.New("codedeploy.dg." + dg.ApplicationName + "." + dg.DeploymentGroupName + ".tags")
	if len(s.TagsMap) > 0 {
		dg.Tags.Merge(s.TagsMap)
	}

	return &dg
}

// onPremisesInstanceSnapshot is the JSON-serializable form of
// OnPremisesInstance (with tags map); see applicationSnapshot's comment for why.
type onPremisesInstanceSnapshot struct {
	TagsMap map[string]string `json:"tagsMap"`
	OnPremisesInstance
}

func onPremisesInstanceSnapshotKeyFn(v *onPremisesInstanceSnapshot) string { return v.InstanceName }

func toOnPremisesInstanceSnapshot(inst *OnPremisesInstance) *onPremisesInstanceSnapshot {
	var tagsMap map[string]string
	if inst.Tags != nil {
		tagsMap = inst.Tags.Clone()
	}

	return &onPremisesInstanceSnapshot{OnPremisesInstance: *inst, TagsMap: tagsMap}
}

func onPremisesInstanceFromSnapshot(s *onPremisesInstanceSnapshot) *OnPremisesInstance {
	inst := s.OnPremisesInstance
	inst.Tags = tags.New("codedeploy.onprem." + inst.InstanceName + ".tags")
	if len(s.TagsMap) > 0 {
		inst.Tags.Merge(s.TagsMap)
	}

	return &inst
}

// persistenceDTOTables groups every ephemeral DTO table
// buildPersistenceDTORegistry constructs, so Snapshot/Restore can pass them
// around as one value instead of three separate return values.
type persistenceDTOTables struct {
	registry            *store.Registry
	applications        *store.Table[applicationSnapshot]
	deploymentGroups    *store.Table[deploymentGroupSnapshot]
	onPremisesInstances *store.Table[onPremisesInstanceSnapshot]
}

// buildPersistenceDTORegistry constructs the ephemeral DTO registry used by
// both Snapshot and Restore for the three "dirty" tables (applications,
// deploymentGroups, onPremisesInstances -- see store_setup.go's
// registerAllTables doc). It is built fresh on every call (rather than
// reusing b.registry) because the DTO value types differ from the live
// table value types (e.g. applicationSnapshot vs Application).
func buildPersistenceDTORegistry() persistenceDTOTables {
	dtoReg := store.NewRegistry()

	return persistenceDTOTables{
		registry:            dtoReg,
		applications:        store.Register(dtoReg, "applications", store.New(applicationSnapshotKeyFn)),
		deploymentGroups:    store.Register(dtoReg, "deploymentGroups", store.New(deploymentGroupSnapshotKeyFn)),
		onPremisesInstances: store.Register(dtoReg, "onPremisesInstances", store.New(onPremisesInstanceSnapshotKeyFn)),
	}
}

// backendSnapshot is the top-level on-disk shape for the CodeDeploy backend.
//
// Tables holds one JSON-encoded array per registered table name: the two
// "clean" tables on b.registry (deployments, deploymentConfigs) plus the
// three DTO tables built above for the "dirty" ones (applications,
// deploymentGroups, onPremisesInstances). GitHubTokens is the plain
// identity-less set left unconverted (see store_setup.go) and is persisted
// here directly, as before.
type backendSnapshot struct {
	Tables       map[string]json.RawMessage `json:"tables"`
	AccountID    string                     `json:"accountID"`
	Region       string                     `json:"region"`
	GitHubTokens []string                   `json:"githubTokens,omitempty"`
	Version      int                        `json:"version"`
}

// Snapshot serialises the backend state to JSON. Returns nil on marshal failure.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "codedeploy: snapshot table marshal failed", "error", err)

		return nil
	}

	dtos := buildPersistenceDTORegistry()

	for _, a := range b.applications.Snapshot() {
		dtos.applications.Put(toApplicationSnapshot(a))
	}

	for _, dg := range b.deploymentGroups.Snapshot() {
		dtos.deploymentGroups.Put(toDeploymentGroupSnapshot(dg))
	}

	for _, inst := range b.onPremisesInstances.Snapshot() {
		dtos.onPremisesInstances.Put(toOnPremisesInstanceSnapshot(inst))
	}

	dtoTables, err := dtos.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "codedeploy: snapshot DTO table marshal failed", "error", err)

		return nil
	}
	maps.Copy(tables, dtoTables)

	var githubTokens []string
	for name := range b.githubTokens {
		githubTokens = append(githubTokens, name)
	}

	snap := backendSnapshot{
		Version:      codedeploySnapshotVersion,
		Tables:       tables,
		AccountID:    b.accountID,
		Region:       b.region,
		GitHubTokens: githubTokens,
	}

	return persistence.MarshalSnapshot(ctx, "codedeploy", snap)
}

// Restore loads backend state from a JSON snapshot produced by Snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "codedeploy", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	for _, app := range b.applications.All() {
		app.Tags.Close()
	}
	for _, dg := range b.deploymentGroups.All() {
		dg.Tags.Close()
	}
	for _, inst := range b.onPremisesInstances.All() {
		inst.Tags.Close()
	}

	if snap.Version != codedeploySnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"codedeploy: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", codedeploySnapshotVersion)

		b.registry.ResetAll()
		b.applications.Reset()
		b.deploymentGroups.Reset()
		b.onPremisesInstances.Reset()
		b.githubTokens = make(map[string]struct{})
		b.accountID = snap.AccountID
		b.region = snap.Region
		b.seedDefaultConfigs()

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("codedeploy: restore snapshot tables: %w", err)
	}

	if err := b.restoreDirtyTables(snap.Tables); err != nil {
		return err
	}

	githubTokens := make(map[string]struct{}, len(snap.GitHubTokens))
	for _, name := range snap.GitHubTokens {
		githubTokens[name] = struct{}{}
	}

	b.githubTokens = githubTokens
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// restoreDirtyTables rebuilds the three "dirty" tables (applications,
// deploymentGroups, onPremisesInstances; see store_setup.go's
// registerAllTables doc) from tables via the ephemeral DTO registry,
// factored out of Restore to keep Restore's own cognitive complexity low.
// Callers must hold b.mu.Lock.
func (b *InMemoryBackend) restoreDirtyTables(tables map[string]json.RawMessage) error {
	dtos := buildPersistenceDTORegistry()
	if err := dtos.registry.RestoreAll(tables); err != nil {
		return fmt.Errorf("codedeploy: restore snapshot DTO tables: %w", err)
	}

	b.applications.Reset()
	for _, s := range dtos.applications.Snapshot() {
		b.applications.Put(applicationFromSnapshot(s))
	}

	b.deploymentGroups.Reset()
	for _, s := range dtos.deploymentGroups.Snapshot() {
		b.deploymentGroups.Put(deploymentGroupFromSnapshot(s))
	}

	b.onPremisesInstances.Reset()
	for _, s := range dtos.onPremisesInstances.Snapshot() {
		b.onPremisesInstances.Put(onPremisesInstanceFromSnapshot(s))
	}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
