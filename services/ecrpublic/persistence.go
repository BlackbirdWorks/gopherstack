package ecrpublic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// ErrNoSnapshot is returned when a backend does not support snapshot/restore.
var ErrNoSnapshot = errors.New("backend does not support restore")

// Snapshottable is an optional interface a Backend may implement to support
// snapshot/restore for persistence or test isolation.
type Snapshottable interface {
	Snapshot(ctx context.Context) []byte
	Restore(context.Context, []byte) error
}

// ecrPublicSnapshotVersion identifies the shape of [backendSnapshot]. Bump it
// whenever a change would make an older snapshot unsafe to decode as the
// current shape; Restore discards (rather than partially decodes) any mismatch.
const ecrPublicSnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the backend. Tables
// holds one JSON-encoded array per registered table name ("repos", "images"
// -- see store_setup.go), produced by b.registry.SnapshotAll(). TagIndex and
// UploadedLayers carry no identity field of their own (see store.go's doc)
// and so are persisted directly here instead of as registered tables.
// LayerUploads (in-flight sessions) is deliberately NOT persisted, matching
// AWS: an in-progress upload does not survive a restart.
type backendSnapshot struct {
	Tables              map[string]json.RawMessage       `json:"tables"`
	TagIndex            map[string]map[string]tagBinding `json:"tagIndex,omitempty"`
	UploadedLayers      map[string]map[string]int64      `json:"uploadedLayers,omitempty"`
	RegistryCatalogData RegistryCatalogData              `json:"registryCatalogData"`
	Version             int                              `json:"version"`
}

// Snapshot serializes backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "ecrpublic: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:             ecrPublicSnapshotVersion,
		Tables:              tables,
		TagIndex:            copyTagIndex(b.tagIndex),
		UploadedLayers:      copyLayerSizes(b.uploadedLayers),
		RegistryCatalogData: b.registryCatalogData,
	}

	return persistence.MarshalSnapshot(ctx, "ecrpublic", &snap)
}

// Restore deserializes backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "ecrpublic", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != ecrPublicSnapshotVersion {
		logger.Load(ctx).WarnContext(ctx,
			"ecrpublic: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", ecrPublicSnapshotVersion)

		b.registry.ResetAll()
		b.tagIndex = make(map[string]map[string]tagBinding)
		b.uploadedLayers = make(map[string]map[string]int64)
		b.registryCatalogData = RegistryCatalogData{}

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("ecrpublic: restore snapshot tables: %w", err)
	}

	b.tagIndex = copyTagIndex(snap.TagIndex)
	b.uploadedLayers = copyLayerSizes(snap.UploadedLayers)
	b.registryCatalogData = snap.RegistryCatalogData
	b.layerUploads = make(map[string]*layerUploadState)

	return nil
}

func copyTagIndex(in map[string]map[string]tagBinding) map[string]map[string]tagBinding {
	out := make(map[string]map[string]tagBinding, len(in))
	for repo, tags := range in {
		inner := make(map[string]tagBinding, len(tags))
		maps.Copy(inner, tags)

		out[repo] = inner
	}

	return out
}

func copyLayerSizes(in map[string]map[string]int64) map[string]map[string]int64 {
	out := make(map[string]map[string]int64, len(in))
	for repo, layers := range in {
		inner := make(map[string]int64, len(layers))
		maps.Copy(inner, layers)

		out[repo] = inner
	}

	return out
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
