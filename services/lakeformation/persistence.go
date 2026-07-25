package lakeformation

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// Snapshot implements persistence.Persistable by delegating to the backend.
//
// h.Backend is the StorageBackend interface, which does not declare
// Snapshot/Restore, so InMemoryBackend.Snapshot/Restore (above) are not
// promoted to Handler automatically. Without this delegation, cli.go's
// setupPersistence type-asserts the registered service.Registerable (this
// *Handler) against persistence.Persistable, fails silently, and never
// registers lakeformation for snapshot/restore despite the backend being
// fully capable. Mirrors services/securityhub's Handler-level delegation.
//
// InMemoryBackend.Snapshot has a different shape than persistence.Persistable
// (no ctx parameter, and it returns an error instead of logging one itself),
// so this adapts: it calls the backend's Snapshot() and logs+swallows any
// marshal error, matching the Persistable contract (a nil snapshot is skipped
// by the persistence Manager).
func (h *Handler) Snapshot(ctx context.Context) []byte {
	type snapshotter interface {
		Snapshot() ([]byte, error)
	}

	s, ok := h.Backend.(snapshotter)
	if !ok {
		return nil
	}

	data, err := s.Snapshot()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "lakeformation: Handler snapshot failed", "error", err)

		return nil
	}

	return data
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	type restorer interface {
		Restore(context.Context, []byte) error
	}

	if r, ok := h.Backend.(restorer); ok {
		return r.Restore(ctx, data)
	}

	return nil
}

// Compile-time proof Handler satisfies the persistence layer's contract.
var _ persistence.Persistable = (*Handler)(nil)

// lakeformationSnapshotVersion identifies the shape of [backendSnapshot]. It
// must be bumped whenever a change to backendSnapshot (or a value type held
// by one of the registered tables) would make an older snapshot unsafe to
// decode as the current shape. Restore compares this against the persisted
// value and discards (registry.ResetAll, not a partial decode) any mismatch
// -- see Restore. The pre-Phase-3.3 snapshot format had no version field at
// all, so an old snapshot decodes with Version == 0, which is guaranteed to
// mismatch lakeformationSnapshotVersion and is discarded the same way any
// other incompatible snapshot is.
const lakeformationSnapshotVersion = 1

// transactionSnapshot is the DTO used only for Snapshot/Restore of the
// "dirty" transactions table -- transactionInfo carries no field of its own
// naming the transaction ID (it lives only as the store.Table key), so a
// dedicated DTO carries the ID as a real JSON field for the round trip. See
// store_setup.go's file doc comment.
type transactionSnapshot struct {
	ID   string          `json:"id"`
	Info transactionInfo `json:"info"`
}

func transactionSnapshotKey(v *transactionSnapshot) string { return v.ID }

// newTransactionDTORegistry builds the ephemeral DTO [store.Registry] shared
// by Snapshot and Restore for the transactions table (see
// store_setup.go's file doc comment).
func newTransactionDTORegistry() (*store.Registry, *store.Table[transactionSnapshot]) {
	dtoReg := store.NewRegistry()
	txDTOs := store.Register(dtoReg, "transactions", store.New(transactionSnapshotKey))

	return dtoReg, txDTOs
}

// backendSnapshot is the serialisable form of InMemoryBackend state.
//
// Tables holds one JSON-encoded array per registered table name, produced by
// merging b.registry.SnapshotAll() (the "clean" tables: resources,
// identityCenterConfigs, lfTags, dataCellsFilters, lfTagExpressions) with the
// ephemeral transactions DTO registry's SnapshotAll(). permissionsMap is
// deliberately absent from Tables: it is a derived cache rebuilt from
// Permissions (b.permissionsList) after every Restore, so persisting it
// separately would be redundant -- see store_setup.go's file doc comment and
// rebuildPermissionsMapLocked below. Version guards against decoding a
// snapshot from an incompatible (older or newer) build of this backend as
// though it were the current shape; see Restore.
type backendSnapshot struct {
	Tables                 map[string]json.RawMessage    `json:"tables"`
	ResourceLFTags         map[string][]LFTagPair        `json:"resourceLFTags"`
	Queries                map[string]string             `json:"queries,omitempty"`
	TableStorageOptimizers map[string][]StorageOptimizer `json:"tableStorageOptimizers,omitempty"`
	DataLakeSettings       *DataLakeSettings             `json:"dataLakeSettings"`
	Permissions            []*PermissionEntry            `json:"permissions"`
	LakeFormationOptIns    []*LFOptIn                    `json:"lakeFormationOptIns"`
	Version                int                           `json:"version"`
}

// Snapshot serialises the backend state to JSON for persistence.
func (b *InMemoryBackend) Snapshot() ([]byte, error) {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		return nil, fmt.Errorf("lakeformation: snapshot table marshal failed: %w", err)
	}

	dtoReg, txDTOs := newTransactionDTORegistry()

	for _, info := range b.transactions.Snapshot() {
		txDTOs.Put(&transactionSnapshot{ID: info.ID, Info: *info})
	}

	txTables, err := dtoReg.SnapshotAll()
	if err != nil {
		return nil, fmt.Errorf("lakeformation: snapshot transaction DTO marshal failed: %w", err)
	}

	maps.Copy(tables, txTables)

	resourceLFTags := make(map[string][]LFTagPair, len(b.resourceLFTags))
	for k, v := range b.resourceLFTags {
		pairs := make([]LFTagPair, len(v))
		copy(pairs, v)
		resourceLFTags[k] = pairs
	}

	queries := make(map[string]string, len(b.queries))
	maps.Copy(queries, b.queries)

	tableStorageOptimizers := make(map[string][]StorageOptimizer, len(b.tableStorageOptimizers))
	for k, v := range b.tableStorageOptimizers {
		cp := make([]StorageOptimizer, len(v))
		copy(cp, v)
		tableStorageOptimizers[k] = cp
	}

	permissions := make([]*PermissionEntry, len(b.permissionsList))
	for i, p := range b.permissionsList {
		permissions[i] = deepCopyPermissionEntry(p)
	}

	optIns := make([]*LFOptIn, len(b.lakeFormationOptIns))

	for i, o := range b.lakeFormationOptIns {
		cp := &LFOptIn{LastModified: o.LastModified, LastUpdatedBy: o.LastUpdatedBy}

		if o.Principal != nil {
			p := *o.Principal
			cp.Principal = &p
		}

		if o.Resource != nil {
			cp.Resource = copyResource(o.Resource)
		}

		if o.Condition != nil {
			cond := *o.Condition
			cp.Condition = &cond
		}

		optIns[i] = cp
	}

	snap := backendSnapshot{
		Version:                lakeformationSnapshotVersion,
		Tables:                 tables,
		DataLakeSettings:       copyDataLakeSettings(b.dataLakeSettings),
		ResourceLFTags:         resourceLFTags,
		Queries:                queries,
		TableStorageOptimizers: tableStorageOptimizers,
		Permissions:            permissions,
		LakeFormationOptIns:    optIns,
	}

	// Marshal failure is returned to the caller (the Persistable wrapper logs
	// it via the context-aware logger); do not log through slog.Default() here.
	data, err := json.Marshal(snap)
	if err != nil {
		return nil, fmt.Errorf("lakeformation: marshal snapshot: %w", err)
	}

	return data, nil
}

// Restore deserialises a snapshot produced by Snapshot back into the backend.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot
	if err := persistence.UnmarshalSnapshot(ctx, "lakeformation", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != lakeformationSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"lakeformation: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", lakeformationSnapshotVersion)

		b.resetTablesLocked()
		b.dataLakeSettings = &DataLakeSettings{}
		b.resourceLFTags = make(map[string][]LFTagPair)
		b.queries = make(map[string]string)
		b.tableStorageOptimizers = make(map[string][]StorageOptimizer)
		b.permissionsList = make([]*PermissionEntry, 0)
		b.lakeFormationOptIns = make([]*LFOptIn, 0)

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("lakeformation: restore snapshot tables: %w", err)
	}

	if err := b.restoreTransactionsLocked(snap.Tables); err != nil {
		return err
	}

	b.dataLakeSettings = snap.DataLakeSettings
	if b.dataLakeSettings == nil {
		b.dataLakeSettings = &DataLakeSettings{}
	}

	b.resourceLFTags = snap.ResourceLFTags
	if b.resourceLFTags == nil {
		b.resourceLFTags = make(map[string][]LFTagPair)
	}

	b.queries = snap.Queries
	if b.queries == nil {
		b.queries = make(map[string]string)
	}

	b.tableStorageOptimizers = snap.TableStorageOptimizers
	if b.tableStorageOptimizers == nil {
		b.tableStorageOptimizers = make(map[string][]StorageOptimizer)
	}

	b.permissionsList = snap.Permissions
	if b.permissionsList == nil {
		b.permissionsList = make([]*PermissionEntry, 0)
	}

	b.rebuildPermissionsMapLocked()

	b.lakeFormationOptIns = snap.LakeFormationOptIns
	if b.lakeFormationOptIns == nil {
		b.lakeFormationOptIns = make([]*LFOptIn, 0)
	}

	return nil
}

// restoreTransactionsLocked restores the "dirty" transactions table from
// tables via the ephemeral DTO registry, converting each DTO back to its
// live type. The caller MUST hold b.mu for writing.
func (b *InMemoryBackend) restoreTransactionsLocked(tables map[string]json.RawMessage) error {
	dtoReg, txDTOs := newTransactionDTORegistry()

	if err := dtoReg.RestoreAll(tables); err != nil {
		return fmt.Errorf("lakeformation: restore transaction DTO tables: %w", err)
	}

	live := make([]*transactionInfo, 0, txDTOs.Len())

	for _, dto := range txDTOs.All() {
		info := dto.Info
		info.ID = dto.ID
		live = append(live, &info)
	}

	b.transactions.Restore(live)

	return nil
}

// rebuildPermissionsMapLocked rebuilds the permissionsMap derived cache from
// b.permissionsList, the source of truth for permission ordering and
// persistence (see store_setup.go's file doc comment). The caller MUST hold
// b.mu for writing.
func (b *InMemoryBackend) rebuildPermissionsMapLocked() {
	b.permissionsMap.Reset()

	for _, p := range b.permissionsList {
		b.permissionsMap.Put(p)
	}
}
