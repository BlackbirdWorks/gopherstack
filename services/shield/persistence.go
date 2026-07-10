package shield

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// shieldSnapshotVersion identifies the shape of [backendSnapshot]. It must be
// bumped whenever a change to backendSnapshot (or a value type held by one of
// the registered tables, or alarConfigDTO) would make an older snapshot
// unsafe to decode as the current shape. Restore compares this against the
// persisted value and discards (ResetAll, not a partial decode) any mismatch
// -- see Restore. The pre-Phase-3.3 snapshot format had no version field at
// all, so an old snapshot decodes with Version == 0, which is guaranteed to
// mismatch shieldSnapshotVersion and is discarded the same way any other
// incompatible snapshot is.
const shieldSnapshotVersion = 1

// alarConfigDTO is the on-disk shape of one alarConfigs entry. ALARConfig's
// ResourceARN field is tagged json:"-" (see backend.go), so a direct
// json.Marshal of *ALARConfig would silently drop the very field the value
// is keyed by; alarConfigDTO carries it as a real JSON field instead so
// Snapshot/Restore round-trips correctly. See store_setup.go's file doc
// comment for why alarConfigs is not registered on b.registry alongside the
// "clean" tables.
type alarConfigDTO struct {
	ResourceARN string     `json:"resourceARN"`
	Config      ALARConfig `json:"config"`
}

// backendSnapshot is the top-level on-disk shape for the Shield backend.
//
// Tables holds one JSON-encoded array per registered table name, produced by
// b.registry.SnapshotAll() (the "clean" tables: protections, protectionGroups,
// attacks). ALARConfigs is the one table left un-registered (see
// alarConfigDTO). Version guards against decoding a snapshot from an
// incompatible (older or newer) build of this backend as though it were the
// current shape; see Restore.
type backendSnapshot struct {
	Tables                    map[string]json.RawMessage `json:"tables"`
	Subscription              *Subscription              `json:"subscription,omitempty"`
	DRTAccess                 *DRTAccess                 `json:"drtAccess,omitempty"`
	AccountID                 string                     `json:"accountID"`
	Region                    string                     `json:"region"`
	ProactiveEngagementStatus string                     `json:"proactiveEngagementStatus,omitempty"`
	ALARConfigs               []alarConfigDTO            `json:"alarConfigs,omitempty"`
	EmergencyContacts         []EmergencyContact         `json:"emergencyContacts,omitempty"`
	Version                   int                        `json:"version"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "shield: snapshot table marshal failed", "error", err)

		return nil
	}

	alarItems := b.alarConfigs.Snapshot()
	alarDTOs := make([]alarConfigDTO, 0, len(alarItems))

	for _, cfg := range alarItems {
		alarDTOs = append(alarDTOs, alarConfigDTO{ResourceARN: cfg.ResourceARN, Config: *cfg})
	}

	snap := backendSnapshot{
		Version:                   shieldSnapshotVersion,
		Tables:                    tables,
		ALARConfigs:               alarDTOs,
		Subscription:              b.subscription,
		DRTAccess:                 b.drtAccess,
		EmergencyContacts:         append([]EmergencyContact(nil), b.emergencyContacts...),
		ProactiveEngagementStatus: b.proactiveEngagementStatus,
		AccountID:                 b.accountID,
		Region:                    b.region,
	}

	return persistence.MarshalSnapshot(ctx, "shield", &snap)
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "shield", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != shieldSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"shield: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", shieldSnapshotVersion)

		b.registry.ResetAll()
		b.alarConfigs.Reset()
		b.subscription = nil
		b.drtAccess = nil
		b.emergencyContacts = nil
		b.proactiveEngagementStatus = ""
		b.accountID = snap.AccountID
		b.region = snap.Region

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("shield: restore snapshot tables: %w", err)
	}

	alarConfigs := make([]*ALARConfig, 0, len(snap.ALARConfigs))

	for _, dto := range snap.ALARConfigs {
		cfg := dto.Config
		cfg.ResourceARN = dto.ResourceARN
		alarConfigs = append(alarConfigs, &cfg)
	}

	b.alarConfigs.Restore(alarConfigs)

	b.subscription = snap.Subscription
	b.drtAccess = snap.DRTAccess
	b.emergencyContacts = snap.EmergencyContacts
	b.proactiveEngagementStatus = snap.ProactiveEngagementStatus
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
