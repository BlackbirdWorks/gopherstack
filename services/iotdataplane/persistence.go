package iotdataplane

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// ErrNoSnapshot is returned when a backend does not support snapshot/restore.
var ErrNoSnapshot = errors.New("backend does not support restore")

// iotdataplaneSnapshotVersion identifies the shape of [backendSnapshot]. It
// must be bumped whenever a change to a DTO type, a registered table's value
// type, or backendSnapshot itself would make an older snapshot unsafe to
// decode as the current shape. Restore compares this against the persisted
// value and discards (registry.ResetAll plus resetting the dirty tables, not
// a partial decode) any mismatch -- see Restore. The pre-Phase-3.3 snapshot
// format had no version field at all, so an old snapshot decodes with
// Version == 0, which is guaranteed to mismatch iotdataplaneSnapshotVersion
// and is discarded the same way any other incompatible snapshot is.
const iotdataplaneSnapshotVersion = 1

// shadowEntrySnap is the serialisable form of a shadowEntry. ThingName and
// ShadowName are given real JSON tags here purely to round-trip shadowEntry's
// unexported identity fields (see store_setup.go) -- shadowEntry itself
// carries no exported fields.
type shadowEntrySnap struct {
	UpdatedAt    time.Time                  `json:"updatedAt"`
	Desired      map[string]json.RawMessage `json:"desired,omitempty"`
	Reported     map[string]json.RawMessage `json:"reported,omitempty"`
	MetaDesired  map[string]int64           `json:"metaDesired,omitempty"`
	MetaReported map[string]int64           `json:"metaReported,omitempty"`
	ThingName    string                     `json:"thingName"`
	ShadowName   string                     `json:"shadowName"`
	Version      int                        `json:"version"`
	// Deleted round-trips the tombstone flag (see shadowEntry.deleted) so a
	// restored snapshot preserves delete-then-recreate version continuity.
	// Omitted (defaults to false) for pre-existing snapshots, which is the
	// correct interpretation since tombstones didn't exist before this field.
	Deleted bool `json:"deleted,omitempty"`
}

func shadowEntrySnapKeyFn(v *shadowEntrySnap) string { return shadowKey(v.ThingName, v.ShadowName) }

// connectionEntrySnap is the serialisable form of a connectionEntry. ClientID
// is given a real JSON tag here purely to round-trip connectionEntry's
// unexported identity field (see store_setup.go) -- connectionEntry itself
// carries no exported fields.
type connectionEntrySnap struct {
	ConnectedAt time.Time `json:"connectedAt"`
	ClientID    string    `json:"clientId"`
	SourceIP    string    `json:"sourceIp,omitempty"`
}

func connectionEntrySnapKeyFn(v *connectionEntrySnap) string { return v.ClientID }

// newDirtyDTORegistry builds the ephemeral DTO [store.Registry] shared by
// Snapshot and Restore for the tables that can't be registered on b.registry
// directly (see store_setup.go): shadows and connections.
func newDirtyDTORegistry() (
	*store.Registry,
	*store.Table[shadowEntrySnap],
	*store.Table[connectionEntrySnap],
) {
	dtoReg := store.NewRegistry()
	shadowDTOs := store.Register(dtoReg, "shadows", store.New(shadowEntrySnapKeyFn))
	connDTOs := store.Register(dtoReg, "connections", store.New(connectionEntrySnapKeyFn))

	return dtoReg, shadowDTOs, connDTOs
}

// copyRawMessages returns a deep copy of a map[string]json.RawMessage.
func copyRawMessages(src map[string]json.RawMessage) map[string]json.RawMessage {
	if src == nil {
		return nil
	}

	dst := make(map[string]json.RawMessage, len(src))
	for k, v := range src {
		cp := make(json.RawMessage, len(v))
		copy(cp, v)
		dst[k] = cp
	}

	return dst
}

// copyInt64Map returns a shallow copy of a map[string]int64.
func copyInt64Map(src map[string]int64) map[string]int64 {
	if src == nil {
		return nil
	}

	dst := make(map[string]int64, len(src))
	maps.Copy(dst, src)

	return dst
}

// entryToSnap converts a shadowEntry to its serialisable form.
func entryToSnap(entry *shadowEntry) *shadowEntrySnap {
	return &shadowEntrySnap{
		ThingName:    entry.thingName,
		ShadowName:   entry.shadowName,
		Version:      entry.version,
		UpdatedAt:    entry.updatedAt,
		Desired:      copyRawMessages(entry.desired),
		Reported:     copyRawMessages(entry.reported),
		MetaDesired:  copyInt64Map(entry.metaDesired),
		MetaReported: copyInt64Map(entry.metaReported),
		Deleted:      entry.deleted,
	}
}

// snapToEntry converts a shadowEntrySnap back to its runtime form.
func snapToEntry(es *shadowEntrySnap) *shadowEntry {
	return &shadowEntry{
		thingName:    es.ThingName,
		shadowName:   es.ShadowName,
		version:      es.Version,
		updatedAt:    es.UpdatedAt,
		desired:      copyRawMessages(es.Desired),
		reported:     copyRawMessages(es.Reported),
		metaDesired:  copyInt64Map(es.MetaDesired),
		metaReported: copyInt64Map(es.MetaReported),
		deleted:      es.Deleted,
	}
}

// backendSnapshot is the top-level on-disk shape for the IoT Data Plane
// backend.
//
// Tables holds one JSON-encoded array per registered table name: the one
// "clean" table on b.registry (retainedMessages) plus the two DTO tables
// built above for the "dirty" ones (shadows, connections) -- see
// store_setup.go's registerAllTables doc for the clean/dirty split. Version
// guards against decoding a snapshot from an incompatible (older or newer)
// build of this backend as though it were the current shape; see Restore.
type backendSnapshot struct {
	Tables  map[string]json.RawMessage `json:"tables"`
	Version int                        `json:"version"`
}

// Snapshot serialises backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "iotdataplane: snapshot table marshal failed", "error", err)

		return nil
	}

	dtoReg, shadowDTOs, connDTOs := newDirtyDTORegistry()

	for _, entry := range b.shadows.Snapshot() {
		shadowDTOs.Put(entryToSnap(entry))
	}

	for _, entry := range b.connections.Snapshot() {
		connDTOs.Put(&connectionEntrySnap{
			ClientID:    entry.clientID,
			ConnectedAt: entry.connectedAt,
			SourceIP:    entry.sourceIP,
		})
	}

	dirtyTables, err := dtoReg.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "iotdataplane: snapshot DTO table marshal failed", "error", err)

		return nil
	}

	maps.Copy(tables, dirtyTables)

	snap := backendSnapshot{
		Version: iotdataplaneSnapshotVersion,
		Tables:  tables,
	}

	return persistence.MarshalSnapshot(ctx, "iotdataplane", snap)
}

// Restore deserialises backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "iotdataplane", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != iotdataplaneSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"iotdataplane: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", iotdataplaneSnapshotVersion)

		b.registry.ResetAll()
		b.shadows.Reset()
		b.connections.Reset()

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("iotdataplane: restore snapshot tables: %w", err)
	}

	return b.restoreDirtyTablesLocked(snap.Tables)
}

// restoreDirtyTablesLocked restores the "dirty" tables (shadows,
// connections) from tables via the ephemeral DTO registry, converting each
// DTO back to its live type. The caller MUST hold b.mu for writing.
func (b *InMemoryBackend) restoreDirtyTablesLocked(tables map[string]json.RawMessage) error {
	dtoReg, shadowDTOs, connDTOs := newDirtyDTORegistry()

	if err := dtoReg.RestoreAll(tables); err != nil {
		return fmt.Errorf("iotdataplane: restore snapshot DTO tables: %w", err)
	}

	liveShadows := make([]*shadowEntry, 0, shadowDTOs.Len())
	for _, dto := range shadowDTOs.All() {
		liveShadows = append(liveShadows, snapToEntry(dto))
	}

	b.shadows.Restore(liveShadows)

	liveConnections := make([]*connectionEntry, 0, connDTOs.Len())

	for _, dto := range connDTOs.All() {
		liveConnections = append(liveConnections, &connectionEntry{
			clientID:    dto.ClientID,
			connectedAt: dto.ConnectedAt,
			sourceIP:    dto.SourceIP,
		})
	}

	b.connections.Restore(liveConnections)

	return nil
}

// Snapshot implements persistence by delegating to the backend if it supports it.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	s, ok := h.Backend.(Snapshottable)
	if !ok {
		return nil
	}

	return s.Snapshot(ctx)
}

// Restore implements persistence by delegating to the backend if it supports it.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	s, ok := h.Backend.(Snapshottable)
	if !ok {
		return ErrNoSnapshot
	}

	return s.Restore(ctx, data)
}
