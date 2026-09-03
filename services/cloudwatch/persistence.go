package cloudwatch

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// cloudwatchSnapshotVersion identifies the shape of backendSnapshot's Tables
// blob (i.e. the set/shape of resources registered on b.registry -- see
// registerAllTables in store_setup.go). It must be bumped whenever a change
// there would make an older snapshot unsafe to decode as the current shape.
// Restore compares this against the persisted value and discards (rather than
// attempts to partially decode) any mismatch -- see Restore below. This
// mirrors the services/sqs pilot (commit 0f09d77c) and the services/ec2
// conversion (commit 12e611a4).
const cloudwatchSnapshotVersion = 1

type backendSnapshot struct {
	Tables       map[string]json.RawMessage          `json:"tables"`
	Metrics      map[string]map[string]*metricRecord `json:"metrics"`
	AlarmHistory map[string][]AlarmHistoryItem       `json:"alarmHistory"`
	AccountID    string                              `json:"accountID"`
	Region       string                              `json:"region"`
	Version      int                                 `json:"version"`
	TotalMetrics int                                 `json:"totalMetrics"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		// The registered tables are plain JSON-friendly structs, so a marshal
		// failure here would indicate a programming error rather than bad
		// input data. Log and skip the snapshot rather than panic, matching
		// the persistence.Persistable contract (nil is skipped by the Manager).
		logger.Load(ctx).WarnContext(ctx, "cloudwatch: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:      cloudwatchSnapshotVersion,
		Tables:       tables,
		Metrics:      b.metrics,
		AlarmHistory: b.alarmHistory,
		AccountID:    b.accountID,
		Region:       b.region,
		TotalMetrics: b.totalMetrics,
	}

	return persistence.MarshalSnapshot(ctx, "cloudwatch", snap)
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "cloudwatch", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != cloudwatchSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption. Mirrors the services/sqs pilot (commit 0f09d77c).
		logger.Load(ctx).WarnContext(ctx,
			"cloudwatch: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", cloudwatchSnapshotVersion)

		b.metrics = make(map[string]map[string]*metricRecord)
		b.alarmHistory = make(map[string][]AlarmHistoryItem)
		b.totalMetrics = 0
		b.registry.ResetAll()

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("cloudwatch: restore snapshot tables: %w", err)
	}

	if snap.Metrics == nil {
		snap.Metrics = make(map[string]map[string]*metricRecord)
	}

	if snap.AlarmHistory == nil {
		snap.AlarmHistory = make(map[string][]AlarmHistoryItem)
	}

	b.metrics = snap.Metrics
	b.alarmHistory = snap.AlarmHistory
	b.accountID = snap.AccountID
	b.region = snap.Region
	b.totalMetrics = snap.TotalMetrics
	b.reindexAlarmHistorySeqLocked()

	// #60: recompute running total from restored metrics.
	total := 0
	for _, nsMap := range b.metrics {
		total += len(nsMap)
	}

	b.totalMetrics = total

	return nil
}

// handlerSnapshot wraps the backend snapshot with the handler-level tags map.
type handlerSnapshot struct {
	Tags    map[string]map[string]string `json:"tags,omitempty"`
	Backend []byte                       `json:"backend"`
}

// Snapshot implements persistence.Persistable by delegating to the backend and
// also persisting the handler-level resource tags.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	type snapshotter interface {
		Snapshot(ctx context.Context) []byte
	}

	var backendData []byte
	if s, ok := h.Backend.(snapshotter); ok {
		backendData = s.Snapshot(ctx)
	}

	h.tagsMu.RLock("Snapshot")
	tagsCopy := make(map[string]map[string]string, len(h.tags))
	for k, t := range h.tags {
		if t != nil {
			tagsCopy[k] = t.Clone()
		}
	}
	h.tagsMu.RUnlock()

	data, err := json.Marshal(handlerSnapshot{Backend: backendData, Tags: tagsCopy})
	if err != nil {
		return backendData
	}

	return data
}

// Restore implements persistence.Persistable by delegating to the backend and
// restoring the handler-level resource tags.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	// Attempt to unmarshal as a handlerSnapshot first.
	var hs handlerSnapshot
	if unmarshalErr := json.Unmarshal(data, &hs); unmarshalErr == nil && hs.Backend != nil {
		type restorer interface {
			Restore(context.Context, []byte) error
		}
		if r, ok := h.Backend.(restorer); ok {
			if err := r.Restore(ctx, hs.Backend); err != nil {
				return err
			}
		}

		h.tagsMu.Lock("Restore")
		h.tags = make(map[string]*tags.Tags, len(hs.Tags))
		for k, kv := range hs.Tags {
			h.tags[k] = tags.FromMap("cloudwatch."+k+".tags", kv)
		}
		h.tagsMu.Unlock()

		return nil
	}

	// Fall back: treat data as a bare backend snapshot (backward compatibility).
	type restorer interface {
		Restore(context.Context, []byte) error
	}
	if r, ok := h.Backend.(restorer); ok {
		return r.Restore(ctx, data)
	}

	return nil
}
