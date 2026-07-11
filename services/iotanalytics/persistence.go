package iotanalytics

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// ErrNoSnapshot is returned when a backend does not support snapshot/restore.
var ErrNoSnapshot = errors.New("backend does not support restore")

// Snapshottable is an optional interface that a StorageBackend may implement to
// support snapshot/restore for persistence or test isolation.
type Snapshottable interface {
	Snapshot(ctx context.Context) []byte
	Restore(context.Context, []byte) error
}

// iotanalyticsSnapshotVersion identifies the shape of [backendSnapshot]. It
// must be bumped whenever a change to backendSnapshot (or a value type held
// by one of the registered tables) would make an older snapshot unsafe to
// decode as the current shape. Restore compares this against the persisted
// value and discards (registry.ResetAll, not a partial decode) any mismatch
// -- see Restore. The pre-Phase-3.3 snapshot format had no version field at
// all, so an old snapshot decodes with Version == 0, which is guaranteed to
// mismatch iotanalyticsSnapshotVersion and is discarded the same way any
// other incompatible snapshot is.
const iotanalyticsSnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the IoT Analytics backend.
//
// Tables holds one JSON-encoded array per registered table name, produced by
// b.registry.SnapshotAll() (channels, datastores, datasets, pipelines -- see
// store_setup.go); all four are "clean" tables (see store_setup.go's file doc
// comment), so no ephemeral DTO registry is needed here. Tags,
// ChannelMessages, and DatasetContents are the three maps left un-converted
// (their values are not *T -- see store_setup.go). Version guards against
// decoding a snapshot from an incompatible (older or newer) build of this
// backend as though it were the current shape; see Restore.
type backendSnapshot struct {
	Tables          map[string]json.RawMessage   `json:"tables"`
	Tags            map[string]map[string]string `json:"tags"`
	ChannelMessages map[string][][]byte          `json:"channelMessages"`
	DatasetContents map[string][]*DatasetContent `json:"datasetContents"`
	LoggingOptions  *LoggingOptions              `json:"loggingOptions"`
	Version         int                          `json:"version"`
}

// Snapshot serializes backend state to JSON.
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
		logger.Load(ctx).WarnContext(ctx, "iotanalytics: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:         iotanalyticsSnapshotVersion,
		Tables:          tables,
		Tags:            b.tags,
		ChannelMessages: b.channelMessages,
		DatasetContents: b.datasetContents,
		LoggingOptions:  b.loggingOptions,
	}

	return persistence.MarshalSnapshot(ctx, "iotanalytics", &snap)
}

// Restore deserializes backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "iotanalytics", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != iotanalyticsSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"iotanalytics: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", iotanalyticsSnapshotVersion)

		b.registry.ResetAll()
		b.tags = make(map[string]map[string]string)
		b.channelMessages = make(map[string][][]byte)
		b.datasetContents = make(map[string][]*DatasetContent)
		b.loggingOptions = nil

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("iotanalytics: restore snapshot tables: %w", err)
	}

	if snap.Tags == nil {
		snap.Tags = make(map[string]map[string]string)
	}

	if snap.ChannelMessages == nil {
		snap.ChannelMessages = make(map[string][][]byte)
	}

	if snap.DatasetContents == nil {
		snap.DatasetContents = make(map[string][]*DatasetContent)
	}

	b.tags = snap.Tags
	b.channelMessages = snap.ChannelMessages
	b.datasetContents = snap.DatasetContents
	b.loggingOptions = snap.LoggingOptions

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
