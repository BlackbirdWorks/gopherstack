package verifiedpermissions

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"

	cedar "github.com/cedar-policy/cedar-go"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// verifiedpermissionsSnapshotVersion identifies the shape of [backendSnapshot].
// It must be bumped whenever a change to a DTO type, a registered table's
// value type, or backendSnapshot itself would make an older snapshot unsafe
// to decode as the current shape. Restore compares this against the
// persisted value and discards (registry.ResetAll plus schemas' own Reset,
// not a partial decode) any mismatch -- see Restore below. This is the first
// version: the pre-Phase-3.3 snapshot carried no version field at all, so it
// also fails this check and is discarded rather than misread, which is the
// safe behaviour across any snapshot-format change.
const verifiedpermissionsSnapshotVersion = 1

// policyStoreSchemaDTO wraps a PolicyStoreSchema, whose only hidden field is
// policyStoreID, for JSON round-tripping through the ephemeral persistence
// registry below -- the same technique services/codeartifact's regionalDTO[V]
// uses.
type policyStoreSchemaDTO struct {
	Value         *PolicyStoreSchema `json:"value"`
	PolicyStoreID string             `json:"policyStoreId"`
}

func policyStoreSchemaDTOKeyFn(d *policyStoreSchemaDTO) string { return d.PolicyStoreID }

// buildSchemaDTORegistry constructs the ephemeral one-table DTO registry used
// by both Snapshot and Restore for the "dirty" schemas table (see
// store_setup.go's registerAllTables doc). It is built fresh on every call
// (rather than reusing b.registry) because the DTO value type differs from
// the live table value type (policyStoreSchemaDTO vs PolicyStoreSchema).
func buildSchemaDTORegistry() (*store.Registry, *store.Table[policyStoreSchemaDTO]) {
	dtoReg := store.NewRegistry()
	tbl := store.Register(dtoReg, "schemas", store.New(policyStoreSchemaDTOKeyFn))

	return dtoReg, tbl
}

// backendSnapshot is the top-level on-disk shape for the Verified Permissions
// backend.
//
// Tables holds one JSON-encoded array per registered table name: the four
// "clean" tables on b.registry (policyStores, policies, policyTemplates,
// identitySources) plus the one DTO table built above for the "dirty" one
// (schemas). ResourceTags is a non-*T value map left unconverted (see
// InMemoryBackend's doc comment in backend.go) and is persisted here
// directly, as before.
type backendSnapshot struct {
	Tables       map[string]json.RawMessage   `json:"tables"`
	ResourceTags map[string]map[string]string `json:"resourceTags,omitempty"`
	AccountID    string                       `json:"accountID"`
	Region       string                       `json:"region"`
	Version      int                          `json:"version"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "verifiedpermissions: snapshot table marshal failed", "err", err)

		return nil
	}

	dtoReg, dtoTable := buildSchemaDTORegistry()

	for _, s := range b.schemas.Snapshot() {
		dtoTable.Put(&policyStoreSchemaDTO{Value: s, PolicyStoreID: s.policyStoreID})
	}

	dtoTables, err := dtoReg.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "verifiedpermissions: snapshot DTO table marshal failed", "err", err)

		return nil
	}
	maps.Copy(tables, dtoTables)

	snap := backendSnapshot{
		Version:      verifiedpermissionsSnapshotVersion,
		Tables:       tables,
		ResourceTags: b.resourceTags,
		AccountID:    b.accountID,
		Region:       b.region,
	}

	return persistence.MarshalSnapshot(ctx, "verifiedpermissions", snap)
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "verifiedpermissions", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != verifiedpermissionsSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"verifiedpermissions: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", verifiedpermissionsSnapshotVersion)

		b.registry.ResetAll()
		b.schemas.Reset()
		b.arnIndex = make(map[string]string)
		b.resourceTags = make(map[string]map[string]string)
		b.policySetCache = make(map[string]*cedar.PolicySet)
		b.policySetDirty = make(map[string]bool)
		b.accountID = snap.AccountID
		b.region = snap.Region

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("verifiedpermissions: restore snapshot tables: %w", err)
	}

	if err := b.restoreSchemas(snap.Tables); err != nil {
		return err
	}

	if snap.ResourceTags == nil {
		snap.ResourceTags = make(map[string]map[string]string)
	}
	b.resourceTags = snap.ResourceTags
	b.policySetCache = make(map[string]*cedar.PolicySet)
	b.policySetDirty = make(map[string]bool)
	b.accountID = snap.AccountID
	b.region = snap.Region

	b.rebuildARNIndex()

	return nil
}

// restoreSchemas rebuilds the "dirty" schemas table (see store_setup.go's
// registerAllTables doc) from tables via the ephemeral DTO registry, factored
// out of Restore to keep Restore's own cognitive complexity low. Callers must
// hold b.mu.Lock.
func (b *InMemoryBackend) restoreSchemas(tables map[string]json.RawMessage) error {
	dtoReg, dtoTable := buildSchemaDTORegistry()
	if err := dtoReg.RestoreAll(tables); err != nil {
		return fmt.Errorf("verifiedpermissions: restore snapshot schema DTO table: %w", err)
	}

	b.schemas.Reset()

	for _, d := range dtoTable.Snapshot() {
		if d.Value == nil {
			continue
		}

		d.Value.policyStoreID = d.PolicyStoreID
		b.schemas.Put(d.Value)
	}

	return nil
}

// rebuildARNIndex rebuilds the ARN index from all restored resources.
// Callers must hold b.mu.Lock.
func (b *InMemoryBackend) rebuildARNIndex() {
	policyStores := b.policyStores.All()
	policies := b.policies.All()
	policyTemplates := b.policyTemplates.All()
	identitySources := b.identitySources.All()

	count := len(policyStores) + len(policies) + len(policyTemplates) + len(identitySources)
	b.arnIndex = make(map[string]string, count)

	for _, ps := range policyStores {
		b.arnIndex[ps.Arn] = arnKindPolicyStore + ":" + ps.PolicyStoreID
	}

	for _, p := range policies {
		b.arnIndex[policyARN(b.accountID, p.PolicyStoreID, p.PolicyID)] =
			arnKindPolicy + ":" + p.PolicyStoreID + ":" + p.PolicyID
	}

	for _, pt := range policyTemplates {
		b.arnIndex[policyTemplateARN(b.accountID, pt.PolicyStoreID, pt.PolicyTemplateID)] =
			arnKindPolicyTemplate + ":" + pt.PolicyStoreID + ":" + pt.PolicyTemplateID
	}

	for _, is := range identitySources {
		b.arnIndex[identitySourceARN(b.accountID, is.PolicyStoreID, is.IdentitySourceID)] =
			arnKindIdentitySource + ":" + is.PolicyStoreID + ":" + is.IdentitySourceID
	}
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
