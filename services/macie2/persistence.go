package macie2

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// macie2SnapshotVersion identifies the shape of [backendSnapshot]. It must be
// bumped whenever a change to a registered table's value type or to
// backendSnapshot itself would make an older snapshot unsafe to decode as the
// current shape. Restore compares this against the persisted value and
// discards (registry.ResetAll, not a partial decode) any mismatch -- see
// Restore below. This is the first version: Macie2 had no persistence at all
// before Phase 3.3 (Handler never delegated Snapshot/Restore to the backend,
// so cli.go's generic setupPersistence never picked this service up even
// though InMemoryBackend itself already implemented persistence.Persistable
// -- see the Handler.Snapshot/Restore delegates at the bottom of this file),
// so there is no legacy snapshot shape to be compatible with: any snapshot
// without a matching Version (including one with no version field, which
// decodes as 0) is discarded the same way any other incompatible snapshot
// is.
const macie2SnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the Macie2 backend.
//
// Tables holds one JSON-encoded array per registered table name, produced by
// b.registry.SnapshotAll() -- every store.Table-backed resource field is a
// "clean" table (see store_setup.go's file doc comment), so no ephemeral
// DTO registry is needed here.
//
// Tags and ResourceDetections are the two plain maps left unconverted by
// Phase 3.3 (see the field comments on InMemoryBackend in backend.go): Tags
// holds a plain map[string]string per resource ARN (no *T identity to key a
// Table on), and ResourceDetections holds a []ResourceProfileDetection slice
// per bucket ARN, whose elements have no identity of their own within that
// slice. Session, Administrator, OrgConfig, AutoDiscoveryConfig,
// ClassExportConfig, FindingsPubConfig, and RevealConfig are single pointers,
// not collections, and are persisted directly alongside the tables.
type backendSnapshot struct {
	Tables              map[string]json.RawMessage            `json:"tables"`
	Session             *Session                              `json:"session,omitempty"`
	Tags                map[string]map[string]string          `json:"tags"`
	Administrator       *AdministratorAccount                 `json:"administrator,omitempty"`
	OrgConfig           *OrgConfig                            `json:"orgConfig,omitempty"`
	AutoDiscoveryConfig *AutoDiscoveryConfig                  `json:"autoDiscoveryConfig,omitempty"`
	ClassExportConfig   *ClassificationExportConfig           `json:"classExportConfig,omitempty"`
	FindingsPubConfig   *FindingsPublicationConfig            `json:"findingsPubConfig,omitempty"`
	ResourceDetections  map[string][]ResourceProfileDetection `json:"resourceDetections"`
	RevealConfig        *RevealConfiguration                  `json:"revealConfig,omitempty"`
	AccountID           string                                `json:"accountId"`
	Region              string                                `json:"region"`
	Version             int                                   `json:"version"`
}

// Snapshot serializes backend state to JSON. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "macie2: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:             macie2SnapshotVersion,
		Tables:              tables,
		Session:             b.session,
		Tags:                b.tags,
		Administrator:       b.administrator,
		OrgConfig:           b.orgConfig,
		AutoDiscoveryConfig: b.autoDiscoveryConfig,
		ClassExportConfig:   b.classExportConfig,
		FindingsPubConfig:   b.findingsPubConfig,
		ResourceDetections:  b.resourceDetections,
		RevealConfig:        b.revealConfig,
		AccountID:           b.accountID,
		Region:              b.region,
	}

	return persistence.MarshalSnapshot(ctx, "macie2", snap)
}

// Restore deserializes backend state from JSON. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot
	if err := persistence.UnmarshalSnapshot(ctx, "macie2", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != macie2SnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"macie2: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", macie2SnapshotVersion)

		b.session = nil
		b.registry.ResetAll()
		b.tags = make(map[string]map[string]string)
		b.administrator = nil
		b.orgConfig = &OrgConfig{AutoEnable: false}
		b.autoDiscoveryConfig = &AutoDiscoveryConfig{Status: "DISABLED"} //nolint:goconst // existing issue.
		b.classExportConfig = nil
		b.findingsPubConfig = nil
		b.resourceDetections = make(map[string][]ResourceProfileDetection)
		b.revealConfig = nil

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("macie2: restore snapshot tables: %w", err)
	}

	b.restorePlainFields(&snap)

	return nil
}

// restorePlainFields restores every plain (non-store.Table) persisted field
// from snap, defaulting each map to an empty map when absent from the
// snapshot (e.g. an older snapshot predating a field, though
// macie2SnapshotVersion's own guard makes that unreachable today). Factored
// out of Restore to keep Restore's own cognitive complexity low. Callers
// must hold b.mu.Lock.
func (b *InMemoryBackend) restorePlainFields(snap *backendSnapshot) {
	b.session = snap.Session

	b.tags = snap.Tags
	if b.tags == nil {
		b.tags = make(map[string]map[string]string)
	}

	b.administrator = snap.Administrator
	b.orgConfig = snap.OrgConfig
	b.autoDiscoveryConfig = snap.AutoDiscoveryConfig
	b.classExportConfig = snap.ClassExportConfig
	b.findingsPubConfig = snap.FindingsPubConfig

	b.resourceDetections = snap.ResourceDetections
	if b.resourceDetections == nil {
		b.resourceDetections = make(map[string][]ResourceProfileDetection)
	}

	b.revealConfig = snap.RevealConfig
	b.accountID = snap.AccountID
	b.region = snap.Region
}

// Snapshot implements persistence.Persistable by delegating to the backend.
//
// Handler did not previously implement Snapshot/Restore at all, even though
// InMemoryBackend already did: cli.go's generic setupPersistence
// type-asserts the service.Registerable returned by Provider.Init (the
// *Handler, not the backend) against a persistable interface, so without
// this delegate Macie2 was never registered with the persistence manager and
// its state was silently dropped across restarts/demo reloads. This is the
// codecommit/codepipeline/emr dead-wiring fix, applied here.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
