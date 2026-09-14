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

// findingSnapshot is storedFinding's persisted twin (gopherstack-2slev).
// ResourceID/ResourceType carry `json:"-"` on Finding: the real wire type has
// no such top-level members (inspector2@v1.54.1 types/types.go:3319-3399 --
// only nested inside Resources), and findingToWire builds the response as a
// map rather than marshaling Finding directly, so the tag is correct for the
// wire. But storedFinding is registered directly on the registry with no
// DTO, so SnapshotAll honored that tag and dropped both fields from
// persistence too: findingFilterCriteria.matches (findings.go) compares
// ListFindings' resourceId/resourceType criteria against them, so every
// restored finding became unmatchable by either filter.
//
// Embedding Finding and redeclaring the two fields at depth 0 shadows the
// embedded json:"-" copies for both encode and decode, so every other
// Finding field flows through unmodified.
type findingSnapshot struct {
	ResourceID   string `json:"resourceId,omitempty"`
	ResourceType string `json:"resourceType,omitempty"`
	Finding
}

func toFindingSnapshot(sf *storedFinding) *findingSnapshot {
	return &findingSnapshot{
		Finding:      sf.Finding,
		ResourceID:   sf.Finding.ResourceID,
		ResourceType: sf.Finding.ResourceType,
	}
}

func fromFindingSnapshot(fs *findingSnapshot) *storedFinding {
	f := fs.Finding
	f.ResourceID = fs.ResourceID
	f.ResourceType = fs.ResourceType

	return &storedFinding{Finding: f}
}

// findingsTableName is the name storedFinding is registered under
// (store_setup.go's registerAllTables) -- Snapshot/Restore special-case this
// one table to route it through findingSnapshot instead of the registry's
// generic per-table encoding.
const findingsTableName = "findings"

// backendSnapshot is the top-level on-disk shape for the Inspector2 backend.
//
// Tables holds one JSON-encoded array per registered table name, produced by
// b.registry.SnapshotAll() -- every store.Table-backed resource field is a
// "clean" table (see store_setup.go's file doc comment) except findings,
// whose entry Snapshot/Restore overwrite with the findingSnapshot DTO shape
// (gopherstack-2slev). Tags, EnabledTypes, and CodeSecurityScans are the
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

	// Overwrite the generic findings table encoding (which would drop
	// ResourceID/ResourceType, since both carry `json:"-"`) with the
	// findingSnapshot DTO shape.
	findings := b.findings.Snapshot()
	dtos := make([]*findingSnapshot, len(findings))

	for i, f := range findings {
		dtos[i] = toFindingSnapshot(f)
	}

	findingsData, err := json.Marshal(dtos)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "inspector2: snapshot findings table marshal failed", "error", err)

		return nil
	}

	tables[findingsTableName] = findingsData

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

	// findingSnapshot decodes findings (see Snapshot); pull the raw entry out
	// of snap.Tables first so registry.RestoreAll doesn't decode it as a bare
	// storedFinding and silently drop ResourceID/ResourceType via their
	// `json:"-"` tags. RestoreAll resets any table absent from the map it's
	// given, so removing this key here is what makes the manual restore
	// below authoritative rather than racing a stale decode.
	findingsRaw, hasFindings := snap.Tables[findingsTableName]
	delete(snap.Tables, findingsTableName)

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("inspector2: restore snapshot tables: %w", err)
	}

	if hasFindings {
		var dtos []*findingSnapshot

		if err := json.Unmarshal(findingsRaw, &dtos); err != nil {
			return fmt.Errorf("inspector2: restore findings table: %w", err)
		}

		items := make([]*storedFinding, len(dtos))
		for i, d := range dtos {
			items[i] = fromFindingSnapshot(d)
		}

		b.findings.Restore(items)
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
