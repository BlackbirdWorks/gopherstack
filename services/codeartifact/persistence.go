package codeartifact

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// codeartifactSnapshotVersion identifies the shape of [backendSnapshot]. It
// must be bumped whenever a change to a DTO type, a registered table's value
// type, or backendSnapshot itself would make an older snapshot unsafe to
// decode as the current shape. Restore compares this against the persisted
// value and discards (registry.ResetAll plus the dirty tables' own Reset, not
// a partial decode) any mismatch -- see Restore below. This is the first
// version: the pre-Phase-3.3 snapshot carried no version field at all, so it
// also fails this check and is discarded rather than misread, which is the
// safe behaviour across any snapshot-format change.
const codeartifactSnapshotVersion = 1

// regionalDTO wraps a region-nested resource whose only hidden field is
// region (PackageGroup, Package, PackageVersion) for JSON round-tripping
// through the ephemeral persistence registry below -- the same technique
// services/emr's regionalDTO[V] uses. ID mirrors the resource's own
// composite-key component (see store_setup.go's *TableKeyFn) so the DTO
// table itself has a stable composite key ("Region|ID").
type regionalDTO[V any] struct {
	Value  *V     `json:"value"`
	Region string `json:"region"`
	ID     string `json:"id"`
}

func regionalDTOKeyFn[V any](d *regionalDTO[V]) string { return regionKey(d.Region, d.ID) }

// repositoryPolicyDTO wraps a RepositoryPermissionsPolicy, which hides three
// fields (region, domainName, repoName) rather than regionalDTO's one, so it
// gets its own DTO instead of reusing regionalDTO -- mirrors
// services/workmail's permissionDTO.
type repositoryPolicyDTO struct {
	Value      *RepositoryPermissionsPolicy `json:"value"`
	Region     string                       `json:"region"`
	DomainName string                       `json:"domainName"`
	RepoName   string                       `json:"repoName"`
}

func repositoryPolicyDTOKeyFn(d *repositoryPolicyDTO) string {
	return regionKey(d.Region, repoKey(d.DomainName, d.RepoName))
}

// domainPolicyDTO wraps a DomainPermissionsPolicy, which hides two fields
// (region, domainName).
type domainPolicyDTO struct {
	Value      *DomainPermissionsPolicy `json:"value"`
	Region     string                   `json:"region"`
	DomainName string                   `json:"domainName"`
}

func domainPolicyDTOKeyFn(d *domainPolicyDTO) string { return regionKey(d.Region, d.DomainName) }

// persistenceDTOTables groups every ephemeral DTO table
// buildPersistenceDTORegistry constructs, so Snapshot/Restore can pass them
// around as one value instead of five separate return values.
type persistenceDTOTables struct {
	registry           *store.Registry
	packageGroups      *store.Table[regionalDTO[PackageGroup]]
	packages           *store.Table[regionalDTO[Package]]
	packageVersions    *store.Table[regionalDTO[PackageVersion]]
	repositoryPolicies *store.Table[repositoryPolicyDTO]
	domainPolicies     *store.Table[domainPolicyDTO]
}

// buildPersistenceDTORegistry constructs the ephemeral DTO registry used by
// both Snapshot and Restore for the five "dirty" tables (packageGroups,
// packages, packageVersions, repositoryPolicies, domainPolicies -- see
// store_setup.go's registerAllTables doc). It is built fresh on every call
// (rather than reusing b.registry) because the DTO value types differ from
// the live table value types (e.g. regionalDTO[V] vs V).
func buildPersistenceDTORegistry() persistenceDTOTables {
	dtoReg := store.NewRegistry()

	return persistenceDTOTables{
		registry:           dtoReg,
		packageGroups:      store.Register(dtoReg, "packageGroups", store.New(regionalDTOKeyFn[PackageGroup])),
		packages:           store.Register(dtoReg, "packages", store.New(regionalDTOKeyFn[Package])),
		packageVersions:    store.Register(dtoReg, "packageVersions", store.New(regionalDTOKeyFn[PackageVersion])),
		repositoryPolicies: store.Register(dtoReg, "repositoryPolicies", store.New(repositoryPolicyDTOKeyFn)),
		domainPolicies:     store.Register(dtoReg, "domainPolicies", store.New(domainPolicyDTOKeyFn)),
	}
}

// backendSnapshot is the top-level on-disk shape for the CodeArtifact
// backend.
//
// Tables holds one JSON-encoded array per registered table name: the two
// "clean" tables on b.registry (domains, repositories) plus the five DTO
// tables built above for the "dirty" ones (packageGroups, packages,
// packageVersions, repositoryPolicies, domainPolicies).
// ExternalConnections is a plain region-nested map left unconverted (its
// value, []ExternalConnection, has no identity of its own -- see
// store_setup.go) and is persisted here directly, as before.
type backendSnapshot struct {
	Tables              map[string]json.RawMessage                 `json:"tables"`
	ExternalConnections map[string]map[string][]ExternalConnection `json:"externalConnections"`
	AccountID           string                                     `json:"accountID"`
	Region              string                                     `json:"region"`
	Version             int                                        `json:"version"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "codeartifact: snapshot table marshal failed", "error", err)

		return nil
	}

	dtos := buildPersistenceDTORegistry()

	for _, v := range b.packageGroups.Snapshot() {
		dtos.packageGroups.Put(&regionalDTO[PackageGroup]{
			Value: v, Region: v.region, ID: packageGroupKey(v.DomainName, v.Pattern),
		})
	}

	for _, v := range b.packages.Snapshot() {
		dtos.packages.Put(&regionalDTO[Package]{
			Value: v, Region: v.region,
			ID: packageKey(v.DomainName, v.Repository, v.Format, v.Namespace, v.Name),
		})
	}

	for _, v := range b.packageVersions.Snapshot() {
		dtos.packageVersions.Put(&regionalDTO[PackageVersion]{
			Value: v, Region: v.region,
			ID: packageVersionKey(v.DomainName, v.Repository, v.Format, v.Namespace, v.PackageName, v.Version),
		})
	}

	for _, v := range b.repositoryPolicies.Snapshot() {
		dtos.repositoryPolicies.Put(&repositoryPolicyDTO{
			Value: v, Region: v.region, DomainName: v.domainName, RepoName: v.repoName,
		})
	}

	for _, v := range b.domainPolicies.Snapshot() {
		dtos.domainPolicies.Put(&domainPolicyDTO{Value: v, Region: v.region, DomainName: v.domainName})
	}

	dtoTables, err := dtos.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "codeartifact: snapshot DTO table marshal failed", "error", err)

		return nil
	}
	maps.Copy(tables, dtoTables)

	snap := backendSnapshot{
		Version:             codeartifactSnapshotVersion,
		Tables:              tables,
		ExternalConnections: b.externalConnections,
		AccountID:           b.accountID,
		Region:              b.region,
	}

	return persistence.MarshalSnapshot(ctx, "codeartifact", snap)
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "codeartifact", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != codeartifactSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"codeartifact: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", codeartifactSnapshotVersion)

		b.registry.ResetAll()
		b.packageGroups.Reset()
		b.packages.Reset()
		b.packageVersions.Reset()
		b.repositoryPolicies.Reset()
		b.domainPolicies.Reset()
		b.externalConnections = make(map[string]map[string][]ExternalConnection)
		b.accountID = snap.AccountID
		b.region = snap.Region

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("codeartifact: restore snapshot tables: %w", err)
	}

	if err := b.restoreDirtyTables(snap.Tables); err != nil {
		return err
	}

	if snap.ExternalConnections == nil {
		snap.ExternalConnections = make(map[string]map[string][]ExternalConnection)
	}
	b.externalConnections = snap.ExternalConnections

	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// restoreDirtyTables rebuilds the five "dirty" tables (packageGroups,
// packages, packageVersions, repositoryPolicies, domainPolicies; see
// store_setup.go's registerAllTables doc) from tables via the ephemeral DTO
// registry, factored out of Restore to keep Restore's own cognitive
// complexity low. Callers must hold b.mu.Lock.
func (b *InMemoryBackend) restoreDirtyTables(tables map[string]json.RawMessage) error {
	dtos := buildPersistenceDTORegistry()
	if err := dtos.registry.RestoreAll(tables); err != nil {
		return fmt.Errorf("codeartifact: restore snapshot DTO tables: %w", err)
	}

	b.packageGroups.Reset()
	for _, d := range dtos.packageGroups.Snapshot() {
		if d.Value == nil {
			continue
		}
		d.Value.region = d.Region
		b.packageGroups.Put(d.Value)
	}

	b.packages.Reset()
	for _, d := range dtos.packages.Snapshot() {
		if d.Value == nil {
			continue
		}
		d.Value.region = d.Region
		b.packages.Put(d.Value)
	}

	b.packageVersions.Reset()
	for _, d := range dtos.packageVersions.Snapshot() {
		if d.Value == nil {
			continue
		}
		d.Value.region = d.Region
		b.packageVersions.Put(d.Value)
	}

	b.repositoryPolicies.Reset()
	for _, d := range dtos.repositoryPolicies.Snapshot() {
		if d.Value == nil {
			continue
		}
		d.Value.region = d.Region
		d.Value.domainName = d.DomainName
		d.Value.repoName = d.RepoName
		b.repositoryPolicies.Put(d.Value)
	}

	b.domainPolicies.Reset()
	for _, d := range dtos.domainPolicies.Snapshot() {
		if d.Value == nil {
			continue
		}
		d.Value.region = d.Region
		d.Value.domainName = d.DomainName
		b.domainPolicies.Put(d.Value)
	}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
