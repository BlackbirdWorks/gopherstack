package ecr

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// ecrSnapshotVersion identifies the shape of [backendSnapshot]. It must be
// bumped whenever a change to any store.Table DTO or backendSnapshot itself
// would make an older snapshot unsafe to decode as the current shape --
// Restore compares this against the persisted value and discards (rather
// than attempts to partially decode) any mismatch, see Restore below.
//
// This is the first versioned ecr snapshot: Phase 3.3 (the pkgs/store
// conversion) moved every *T resource map registered on b.registry from a
// nested map[string]map[string]*T (or bare map[string]T) shape directly under
// backendSnapshot to a flat store.Table JSON array nested under Tables, so any
// snapshot written before this conversion is unconditionally incompatible --
// it decodes as Version 0 via Go's json zero value, which never equals
// ecrSnapshotVersion.
//
// Bumped 1 -> 2 (gopherstack-hjdd) for d83f4b5d3: ImageScanFinding.Attributes
// (nested inside the registered ImageScanFindingsResult table) changed from
// map[string]string to []Attribute, matching the real deserializer. A
// Version-1 snapshot's "attributes" object no longer unmarshals into the new
// array field at all -- an outright decode error, not silent loss -- so it
// must be discarded like any other shape-incompatible snapshot.
const ecrSnapshotVersion = 2

// backendSnapshot is the top-level on-disk shape for the ECR backend.
//
// Tables holds one JSON-encoded array per store.Table registered on
// b.registry, produced by [store.Registry.SnapshotAll] -- see store_setup.go
// for the full list ("repos", "images", "imageScanFindings",
// "pullThroughCacheRules", "repositoryCreationTemplates", "lifecyclePolicies",
// "lifecyclePolicyPreviews", "repositoryPolicies", "accountSettings",
// "pullTimeUpdateExclusions").
//
// The remaining fields persist state deliberately left off b.registry -- see
// the comment above registerAllTables in store_setup.go for why each one is
// exempt. RepoTags and UploadedLayers were persisted before this conversion
// and still are (unchanged shape/JSON tag, so an old snapshot's values for
// these two fields would in isolation still decode correctly -- but Restore
// discards the whole snapshot on a Version mismatch regardless, matching the
// services/sqs precedent that a snapshot must never be partially decoded).
// tagIndex, digestTagsIndex, layerUploads, repoUploadIndex, and
// lifecycleLastEvaluated were never persisted before this conversion and
// still aren't.
type backendSnapshot struct {
	Tables                 map[string]json.RawMessage   `json:"tables"`
	RepoTags               map[string]map[string]string `json:"repoTags,omitempty"`
	UploadedLayers         map[string]map[string]int64  `json:"uploadedLayers,omitempty"`
	RegistryScanningConfig *RegistryScanningSettings    `json:"registryScanningConfig,omitempty"`
	ReplicationConfig      *ReplicationConfig           `json:"replicationConfig,omitempty"`
	SigningConfig          *SigningSettings             `json:"signingConfig,omitempty"`
	RegistryPolicy         string                       `json:"registryPolicy"`
	Version                int                          `json:"version"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		// Every registered table's value type is a plain JSON-friendly struct,
		// so a marshal failure here would indicate a programming error rather
		// than bad input data. Log and skip the snapshot rather than panic,
		// matching the persistence.Persistable contract (nil is skipped by the
		// Manager).
		logger.Load(ctx).WarnContext(ctx, "ecr: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:                ecrSnapshotVersion,
		Tables:                 tables,
		RepoTags:               copyNestedMap(b.repoTags),
		UploadedLayers:         copyNestedMap(b.uploadedLayers),
		RegistryScanningConfig: copyRegistryScanningSettings(b.registryScanningConfig),
		ReplicationConfig:      copyReplicationConfig(b.replicationConfig),
		SigningConfig:          copySigningSettings(b.signingConfig),
		RegistryPolicy:         b.registryPolicy,
	}

	return persistence.MarshalSnapshot(ctx, "ecr", snap)
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot
	if err := persistence.UnmarshalSnapshot(ctx, "ecr", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != ecrSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across this snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"ecr: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", ecrSnapshotVersion)

		b.registry.ResetAll()

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("ecr: restore snapshot tables: %w", err)
	}

	b.repoTags = copyNestedMap(snap.RepoTags)
	b.uploadedLayers = copyNestedMap(snap.UploadedLayers)
	b.registryPolicy = snap.RegistryPolicy
	b.registryScanningConfig = copyRegistryScanningSettings(snap.RegistryScanningConfig)
	b.replicationConfig = copyReplicationConfig(snap.ReplicationConfig)
	b.signingConfig = copySigningSettings(snap.SigningConfig)
	b.layerUploads = make(map[string]*layerUploadState)
	b.repoUploadIndex = make(map[string]map[string]struct{})
	b.layerUploadQueue = make([]layerUploadQueueEntry, 0)

	return nil
}

func copyMap[T any](in map[string]T) map[string]T {
	if in == nil {
		return make(map[string]T)
	}

	cp := make(map[string]T, len(in))
	maps.Copy(cp, in)

	return cp
}

func copyNestedMap[T any](in map[string]map[string]T) map[string]map[string]T {
	cp := make(map[string]map[string]T, len(in))
	for key, value := range in {
		cp[key] = copyMap(value)
	}

	return cp
}

// Snapshot implements persistence.Persistable by delegating to the backend
// when it implements Snapshottable. Returns nil for non-snapshottable backends.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	if s, ok := h.Backend.(Snapshottable); ok {
		return s.Snapshot(ctx)
	}

	return nil
}

// Restore implements persistence.Persistable by delegating to the backend
// when it implements Snapshottable. Non-snapshottable backends are skipped.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if s, ok := h.Backend.(Snapshottable); ok {
		return s.Restore(ctx, data)
	}

	return nil
}
