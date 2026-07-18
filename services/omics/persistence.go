package omics

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// omicsSnapshotVersion identifies the shape of [backendSnapshot]. It must be
// bumped whenever a change to backendSnapshot itself would make an older
// snapshot unsafe to decode as the current shape (e.g. a field's meaning or
// type changes). Restore compares this against the persisted value and
// discards (rather than attempts to partially decode) any mismatch -- see
// Restore below. This is the first version: earlier (pre-Phase-3.3)
// snapshots carried no version field at all (the old Snapshot/Restore pair
// marshaled b.regions directly via encoding/json, which -- since every
// regionState field was unexported -- silently produced empty {} objects and
// never actually round-tripped any data), so they also fail this check and
// are discarded rather than misread, which is the safe behaviour across any
// snapshot-format change.
const omicsSnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the HealthOmics backend.
//
// Tables holds one JSON-encoded array per registered [store.Table], produced
// by [store.Registry.SnapshotAll]. Every converted resource type here already
// carries its own identity/parent fields as exported JSON members (unlike
// e.g. EMR's Cluster), so no DTO wrapper is needed: b.registry itself -- the
// same registry the live tables are registered on -- is snapshotted and
// restored directly. The five left-raw maps (see the InMemoryBackend doc
// comment in store.go) are persisted as plain fields alongside Tables.
type backendSnapshot struct {
	Tables         map[string]json.RawMessage                      `json:"tables"`
	UploadParts    map[string]map[string][]*ReadSetUploadPart      `json:"uploadParts"`
	UploadPartData map[string]map[string]map[string]map[int][]byte `json:"uploadPartData"`
	ReadSetBytes   map[string]map[string][]byte                    `json:"readSetBytes"`
	ReferenceBytes map[string]map[string][]byte                    `json:"referenceBytes"`
	Tags           map[string]map[string]string                    `json:"tags"`
	AccountID      string                                          `json:"accountID"`
	Region         string                                          `json:"region"`
	Version        int                                             `json:"version"`
}

// Snapshot serialises the backend state to JSON. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		// Every registered table holds a plain JSON-friendly struct, so a
		// marshal failure here would indicate a programming error rather
		// than bad input data. Log and skip the snapshot rather than panic,
		// matching the persistence.Persistable contract (nil is skipped by
		// the Manager).
		logger.Load(ctx).WarnContext(ctx, "omics: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:        omicsSnapshotVersion,
		Tables:         tables,
		UploadParts:    b.uploadParts,
		UploadPartData: b.uploadPartData,
		ReadSetBytes:   b.readSetBytes,
		ReferenceBytes: b.referenceBytes,
		Tags:           b.tags,
		AccountID:      b.accountID,
		Region:         b.defaultRegion,
	}

	return persistence.MarshalSnapshot(ctx, "omics", snap)
}

// Restore loads backend state from a JSON snapshot. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "omics", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != omicsSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never
		// be partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"omics: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", omicsSnapshotVersion)

		b.registry.ResetAll()
		b.uploadParts = make(map[string]map[string][]*ReadSetUploadPart)
		b.uploadPartData = make(map[string]map[string]map[string]map[int][]byte)
		b.readSetBytes = make(map[string]map[string][]byte)
		b.referenceBytes = make(map[string]map[string][]byte)
		b.tags = make(map[string]map[string]string)
		b.accountID = snap.AccountID
		b.defaultRegion = snap.Region

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("omics: restore snapshot tables: %w", err)
	}

	b.uploadParts = snap.UploadParts
	if b.uploadParts == nil {
		b.uploadParts = make(map[string]map[string][]*ReadSetUploadPart)
	}

	b.uploadPartData = snap.UploadPartData
	if b.uploadPartData == nil {
		b.uploadPartData = make(map[string]map[string]map[string]map[int][]byte)
	}

	b.readSetBytes = snap.ReadSetBytes
	if b.readSetBytes == nil {
		b.readSetBytes = make(map[string]map[string][]byte)
	}

	b.referenceBytes = snap.ReferenceBytes
	if b.referenceBytes == nil {
		b.referenceBytes = make(map[string]map[string][]byte)
	}

	b.tags = snap.Tags
	if b.tags == nil {
		b.tags = make(map[string]map[string]string)
	}

	b.accountID = snap.AccountID
	b.defaultRegion = snap.Region

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
