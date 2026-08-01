package outposts

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// outpostsSnapshotVersion identifies the shape of backendSnapshot's Tables
// blob (the set of resources registered on b.registry -- see
// registerAllTables in store_setup.go). Bump whenever a change there would
// make an older snapshot unsafe to decode as the current shape; Restore
// discards (rather than partially decodes) any mismatch -- see Restore.
const outpostsSnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the Outposts backend.
type backendSnapshot struct {
	Tables    map[string]json.RawMessage `json:"tables"`
	AccountID string                     `json:"accountId"`
	Region    string                     `json:"region"`
	Version   int                        `json:"version"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "outposts: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:   outpostsSnapshotVersion,
		Tables:    tables,
		AccountID: b.accountID,
		Region:    b.region,
	}

	return persistence.MarshalSnapshot(ctx, "outposts", snap)
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "outposts", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != outpostsSnapshotVersion {
		logger.Load(ctx).WarnContext(ctx,
			"outposts: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", outpostsSnapshotVersion)

		b.registry.ResetAll()
		clear(b.renewalIdempotency)

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("outposts: restore tables: %w", err)
	}

	restoreOutpostAndSiteTagsLocked(b)

	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// restoreOutpostAndSiteTagsLocked rebuilds a Prometheus-named tags.Tags
// instance for every Outpost/Site after a JSON round-trip, which
// deserializes every tags.Tags with the generic "json.tags" name (see
// tags.Tags.UnmarshalJSON's doc comment) -- matching services/grafana's
// restoreWorkspaceTagsLocked. Callers must hold b.mu.
func restoreOutpostAndSiteTagsLocked(b *InMemoryBackend) {
	for _, o := range b.outposts.All() {
		if o.Tags == nil {
			o.Tags = tags.New("outposts.outpost." + o.ID + ".tags")

			continue
		}

		raw := o.Tags.Clone()
		o.Tags.Close()
		o.Tags = tags.FromMap("outposts.outpost."+o.ID+".tags", raw)
	}

	for _, s := range b.sites.All() {
		if s.Tags == nil {
			s.Tags = tags.New("outposts.site." + s.ID + ".tags")

			continue
		}

		raw := s.Tags.Clone()
		s.Tags.Close()
		s.Tags = tags.FromMap("outposts.site."+s.ID+".tags", raw)
	}
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
