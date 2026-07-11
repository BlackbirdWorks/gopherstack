package cognitoidentity

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// cognitoidentitySnapshotVersion identifies the shape of [backendSnapshot]. It must be
// bumped whenever a change to the DTOs/backendSnapshot itself would make an older
// snapshot unsafe to decode as the current shape (e.g. a field's meaning or type
// changes). Restore compares this against the persisted value and discards (rather than
// attempts to partially decode) any mismatch -- see Restore below. This is the first
// version: earlier (pre-Phase-3.3) snapshots carried no version field at all, so they
// also fail this check and are discarded rather than misread, which is the safe
// behaviour across any snapshot-format change.
const cognitoidentitySnapshotVersion = 1

// regionalDTO wraps a region-nested resource for JSON round-tripping through
// store.Registry. It is shared by every converted resource collection whose only
// hidden field(s) beyond a stable identifier are region -- IdentityPool (ID =
// IdentityPoolID), Identity (ID = IdentityID), and IdentityRoles (ID = poolID). ID
// mirrors that identifier so the DTO table has a stable composite key ("region|ID")
// independent of Value's own (unmarshaled) fields. PrincipalTagMapping needs a
// dedicated DTO instead (see principalTagDTO below) because its identity is the
// two-part (poolID, providerName) pair, not a single field.
type regionalDTO[V any] struct {
	Value  *V     `json:"value"`
	Region string `json:"region"`
	ID     string `json:"id"`
}

// regionalDTOKeyFn is the shared [store.Table] key function for every regionalDTO[V]
// table below; it mirrors the "region|id" composite key each live table uses (see
// regionKey in backend.go).
func regionalDTOKeyFn[V any](d *regionalDTO[V]) string { return regionKey(d.Region, d.ID) }

// principalTagDTO wraps a PrincipalTagMapping for JSON round-tripping through
// store.Registry, carrying its hidden region/poolID/providerName fields alongside Value.
type principalTagDTO struct {
	Value        *PrincipalTagMapping `json:"value"`
	Region       string               `json:"region"`
	PoolID       string               `json:"poolID"`
	ProviderName string               `json:"providerName"`
}

func principalTagDTOKeyFn(d *principalTagDTO) string {
	return regionKey(d.Region, principalTagKey(d.PoolID, d.ProviderName))
}

// backendSnapshot is the top-level on-disk shape for the Cognito Identity backend.
//
// Tables holds one JSON-encoded array per registered DTO table, produced by
// [store.Registry.SnapshotAll] -- the four converted resource collections, each
// wrapped in a DTO to carry its hidden field(s) through. Version guards against
// decoding a snapshot from an incompatible (older or newer) build of this backend as
// though it were the current shape; see Restore.
type backendSnapshot struct {
	Tables    map[string]json.RawMessage `json:"tables"`
	AccountID string                     `json:"accountID"`
	Region    string                     `json:"region"`
	Version   int                        `json:"version"`
}

// persistenceDTOTables groups every ephemeral DTO table buildPersistenceDTORegistry
// constructs, so Snapshot/Restore can pass them around as one value instead of four
// separate return values.
type persistenceDTOTables struct {
	registry      *store.Registry
	pools         *store.Table[regionalDTO[IdentityPool]]
	identities    *store.Table[regionalDTO[Identity]]
	roles         *store.Table[regionalDTO[IdentityRoles]]
	principalTags *store.Table[principalTagDTO]
}

// buildPersistenceDTORegistry constructs the ephemeral DTO registry used by both
// Snapshot and Restore. It is built fresh on every call (rather than reusing
// b.registry) because the DTO value types differ from the live table value types
// (e.g. regionalDTO[V] vs V).
func buildPersistenceDTORegistry() persistenceDTOTables {
	dtoReg := store.NewRegistry()

	return persistenceDTOTables{
		registry:      dtoReg,
		pools:         store.Register(dtoReg, "pools", store.New(regionalDTOKeyFn[IdentityPool])),
		identities:    store.Register(dtoReg, "identities", store.New(regionalDTOKeyFn[Identity])),
		roles:         store.Register(dtoReg, "roles", store.New(regionalDTOKeyFn[IdentityRoles])),
		principalTags: store.Register(dtoReg, "principalTags", store.New(principalTagDTOKeyFn)),
	}
}

// Snapshot serialises the backend state to JSON. It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	dtos := buildPersistenceDTORegistry()

	for _, v := range b.pools.Snapshot() {
		dtos.pools.Put(&regionalDTO[IdentityPool]{Region: v.region, ID: v.IdentityPoolID, Value: v})
	}

	for _, v := range b.identities.Snapshot() {
		dtos.identities.Put(&regionalDTO[Identity]{Region: v.region, ID: v.IdentityID, Value: v})
	}

	for _, v := range b.roles.Snapshot() {
		dtos.roles.Put(&regionalDTO[IdentityRoles]{Region: v.region, ID: v.poolID, Value: v})
	}

	for _, v := range b.principalTags.Snapshot() {
		dtos.principalTags.Put(&principalTagDTO{
			Region: v.region, PoolID: v.poolID, ProviderName: v.providerName, Value: v,
		})
	}

	tables, err := dtos.registry.SnapshotAll()
	if err != nil {
		// The DTOs above are plain JSON-friendly structs, so a marshal failure here
		// would indicate a programming error rather than bad input data. Log and skip
		// the snapshot rather than panic, matching the persistence.Persistable
		// contract (nil is skipped by the Manager).
		logger.Load(ctx).WarnContext(ctx, "cognitoidentity: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:   cognitoidentitySnapshotVersion,
		Tables:    tables,
		AccountID: b.accountID,
		Region:    b.region,
	}

	return persistence.MarshalSnapshot(ctx, "cognitoidentity", snap)
}

// Restore loads backend state from a JSON snapshot. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "cognitoidentity", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != cognitoidentitySnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"cognitoidentity: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", cognitoidentitySnapshotVersion)

		b.registry.ResetAll()
		b.accountID = snap.AccountID
		b.region = snap.Region

		return nil
	}

	if err := b.restoreResourceTables(snap.Tables); err != nil {
		return err
	}

	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// unwrapRegionalDTOs converts every regionalDTO[V] in dtos into its live *V, restoring
// the unexported region field (and any other hidden field) each carries via setFields
// (a plain generic type parameter V has no field access, so the field assignment is
// supplied by the caller, one per concrete V; see restoreResourceTables). A DTO whose
// Value is nil -- not producible by Snapshot, but a defensive guard against a
// hand-edited or corrupted snapshot -- is skipped rather than dereferenced.
func unwrapRegionalDTOs[V any](
	dtos *store.Table[regionalDTO[V]],
	setFields func(v *V, region, id string),
) []*V {
	items := make([]*V, 0, dtos.Len())

	for _, d := range dtos.All() {
		if d.Value == nil {
			continue
		}

		setFields(d.Value, d.Region, d.ID)
		items = append(items, d.Value)
	}

	return items
}

// unwrapPrincipalTagDTOs converts every principalTagDTO in dtos into its live
// *PrincipalTagMapping, restoring its three hidden fields.
func unwrapPrincipalTagDTOs(dtos *store.Table[principalTagDTO]) []*PrincipalTagMapping {
	items := make([]*PrincipalTagMapping, 0, dtos.Len())

	for _, d := range dtos.All() {
		if d.Value == nil {
			continue
		}

		d.Value.region = d.Region
		d.Value.poolID = d.PoolID
		d.Value.providerName = d.ProviderName
		items = append(items, d.Value)
	}

	return items
}

// restoreResourceTables rebuilds every store.Table on b from snap's per-table JSON,
// factored out of Restore to keep Restore's own cognitive complexity low. Callers must
// hold b.mu.Lock.
func (b *InMemoryBackend) restoreResourceTables(tables map[string]json.RawMessage) error {
	dtos := buildPersistenceDTORegistry()

	if err := dtos.registry.RestoreAll(tables); err != nil {
		return fmt.Errorf("cognitoidentity: restore snapshot tables: %w", err)
	}

	b.pools.Restore(unwrapRegionalDTOs(dtos.pools, func(v *IdentityPool, region, _ string) {
		v.region = region
	}))
	b.identities.Restore(unwrapRegionalDTOs(dtos.identities, func(v *Identity, region, _ string) {
		v.region = region
	}))
	b.roles.Restore(unwrapRegionalDTOs(dtos.roles, func(v *IdentityRoles, region, id string) {
		v.region = region
		v.poolID = id
	}))
	b.principalTags.Restore(unwrapPrincipalTagDTOs(dtos.principalTags))

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
