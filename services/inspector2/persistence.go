package inspector2

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// inspector2SnapshotVersion identifies the shape of [backendSnapshot]. It
// must be bumped whenever a change to backendSnapshot (or a value type held
// by one of the registered tables) would make an older snapshot unsafe to
// decode as the current shape. Restore compares this against the persisted
// value and discards (ResetAll plus a reset of the raw maps/structs below,
// not a partial decode) any mismatch -- see Restore. The pre-Phase-3.3
// snapshot format had no version field at all (it also had a different
// top-level shape, nesting appendix-A state under an "appendix" key), so an
// old snapshot decodes with Version == 0, which is guaranteed to mismatch
// inspector2SnapshotVersion and is discarded the same way any other
// incompatible snapshot is.
const inspector2SnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the Inspector2 backend.
//
// Tables holds one JSON-encoded array per registered table name, produced by
// b.registry.SnapshotAll() -- every store.Table-backed resource field is a
// "clean" table (see store_setup.go's file doc comment), so no ephemeral DTO
// registry is needed here. Tags, EnabledTypes, and CodeSecurityScans are the
// raw maps left un-converted (their values are not *T). Config,
// Ec2DeepConfig, OrgEc2Config, and OrgConfig are single structs, not
// collections, so they were never map-shaped and are simply carried
// alongside the tables. Version guards against decoding a snapshot from an
// incompatible (older or newer) build of this backend as though it were the
// current shape; see Restore.
type backendSnapshot struct {
	Tables            map[string]json.RawMessage   `json:"tables"`
	Tags              map[string]map[string]string `json:"tags"`
	EnabledTypes      map[string]bool              `json:"enabledTypes"`
	CodeSecurityScans map[string]map[string]any    `json:"codeSecurityScans"`
	Config            Configuration                `json:"config"`
	AccountID         string                       `json:"accountId"`
	Region            string                       `json:"region"`
	OrgConfig         OrgConfiguration             `json:"orgConfig"`
	Ec2DeepConfig     Ec2DeepInspectionConfig      `json:"ec2DeepConfig"`
	OrgEc2Config      OrgEc2DeepInspectionConfig   `json:"orgEc2Config"`
	Version           int                          `json:"version"`
}

// Snapshot serializes the backend state. It implements
// persistence.Persistable via Handler.Snapshot (see below).
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		// The registered tables are all plain JSON-friendly structs, so a
		// marshal failure here would indicate a programming error rather
		// than bad input data. Log and skip the snapshot rather than panic,
		// matching the persistence.Persistable contract (nil is skipped by
		// the Manager).
		logger.Load(ctx).WarnContext(ctx, "inspector2: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:           inspector2SnapshotVersion,
		Tables:            tables,
		Tags:              b.tags,
		EnabledTypes:      b.enabledTypes,
		CodeSecurityScans: b.codeSecurityScans,
		Config:            b.config,
		Ec2DeepConfig:     b.ec2DeepConfig,
		OrgEc2Config:      b.orgEc2Config,
		OrgConfig:         b.orgConfig,
		AccountID:         b.accountID,
		Region:            b.region,
	}

	return persistence.MarshalSnapshot(ctx, "inspector2", &snap)
}

// Restore deserializes the backend state.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "inspector2", data, &snap); err != nil {
		return fmt.Errorf("inspector2: restore: %w", err)
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != inspector2SnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never
		// be partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"inspector2: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", inspector2SnapshotVersion)

		b.registry.ResetAll()
		b.resetRawState()
		b.accountID = snap.AccountID
		b.region = snap.Region

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("inspector2: restore snapshot tables: %w", err)
	}

	b.restoreRawState(&snap)
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// restoreRawState loads the raw (non-store.Table) maps and structs from snap,
// defaulting any nil map to an empty one so callers never observe a nil map
// after a successful Restore. Factored out of Restore to keep Restore's own
// cognitive complexity low. Callers must hold b.mu.Lock.
func (b *InMemoryBackend) restoreRawState(snap *backendSnapshot) {
	b.tags = snap.Tags
	if b.tags == nil {
		b.tags = make(map[string]map[string]string)
	}

	b.enabledTypes = snap.EnabledTypes
	if b.enabledTypes == nil {
		b.enabledTypes = make(map[string]bool)
	}

	b.codeSecurityScans = snap.CodeSecurityScans
	if b.codeSecurityScans == nil {
		b.codeSecurityScans = make(map[string]map[string]any)
	}

	b.config = snap.Config
	b.ec2DeepConfig = snap.Ec2DeepConfig
	b.orgEc2Config = snap.OrgEc2Config
	b.orgConfig = snap.OrgConfig
}

// resetRawState resets every raw (non-store.Table) map and struct field to
// the same defaults NewInMemoryBackend uses. Callers must hold b.mu.Lock.
func (b *InMemoryBackend) resetRawState() {
	b.tags = make(map[string]map[string]string)
	b.enabledTypes = make(map[string]bool)
	b.codeSecurityScans = make(map[string]map[string]any)
	b.config = defaultConfiguration()
	b.ec2DeepConfig = defaultEc2DeepInspectionConfig()
	b.orgEc2Config = OrgEc2DeepInspectionConfig{}
	b.orgConfig = OrgConfiguration{}
}

// Snapshot implements persistence.Persistable by delegating to the backend.
// Without this, cli.go's generic setupPersistence (which type-asserts the
// registered service.Registerable, i.e. *Handler, for Snapshot/Restore) never
// finds a persistable Inspector2 service even though InMemoryBackend itself
// has always implemented Snapshot/Restore -- the backend methods were dead
// wiring until Handler delegated to them.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
