package ocilogging

import (
	"context"
	"fmt"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/ociauth"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// snapshotVersion must be bumped when a change to the stored types would make an older snapshot unsafe to decode.
const snapshotVersion = 1

type backendSnapshot struct {
	Groups   map[string]*LogGroup    `json:"logGroups"`
	Logs     map[string]*Log         `json:"logs"`
	Agents   map[string]*AgentConfig `json:"agentConfigurations"`
	WorkReqs map[string]*WorkRequest `json:"workRequests"`
	Entries  map[string][]*Entry     `json:"entries"`
	Auth     []ociauth.KeyRecord     `json:"authKeys"`
	Version  int                     `json:"version"`
	Seq      int64                   `json:"seq"`
}

// Snapshot serialises the backend state, including registered API keys.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	keys := b.Auth.Records()

	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	return persistence.MarshalSnapshot(ctx, "ocilogging", backendSnapshot{
		Version: snapshotVersion, Groups: b.groups, Logs: b.logs, Agents: b.agents, WorkReqs: b.workReqs,
		Entries: b.entries, Auth: keys, Seq: b.seq,
	})
}

// Restore loads backend state from a snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "ocilogging", data, &snap); err != nil {
		return err
	}

	if snap.Version != snapshotVersion {
		logger.Load(ctx).WarnContext(ctx, "ocilogging: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", snapshotVersion)
		b.Reset()

		return nil
	}

	if err := checkSnapshot(&snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.groups, b.logs, b.agents, b.workReqs = snap.Groups, snap.Logs, snap.Agents, snap.WorkReqs
	b.entries, b.seq = snap.Entries, snap.Seq
	b.Auth.Load(snap.Auth)

	return nil
}

func nullCheck[T any](kind string, m *map[string]*T) error {
	if *m == nil {
		*m = map[string]*T{}
	}

	for id, v := range *m {
		if v == nil {
			return fmt.Errorf("%w: %s %q", ErrSnapshotNull, kind, id)
		}
	}

	return nil
}

func checkSnapshot(snap *backendSnapshot) error {
	for _, err := range []error{
		nullCheck("log group", &snap.Groups), nullCheck("log", &snap.Logs),
		nullCheck("agent configuration", &snap.Agents), nullCheck("work request", &snap.WorkReqs),
	} {
		if err != nil {
			return err
		}
	}

	if snap.Entries == nil {
		snap.Entries = map[string][]*Entry{}
	}

	for id, es := range snap.Entries {
		if slices.Contains(es, nil) {
			return fmt.Errorf("%w: entry of log %q", ErrSnapshotNull, id)
		}
	}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if err := h.Backend.Restore(ctx, data); err != nil {
		return fmt.Errorf("ocilogging: restore snapshot: %w", err)
	}

	return nil
}
