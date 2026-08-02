package resiliencehub

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// resiliencehubSnapshotVersion identifies the shape of backendSnapshot's
// Tables blob (the set of resources registered on b.registry -- see
// registerAllTables in store_setup.go). Bump whenever a change there would
// make an older snapshot unsafe to decode as the current shape; Restore
// discards (rather than partially decodes) any mismatch -- see Restore.
const resiliencehubSnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the Resilience Hub
// backend.
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
		logger.Load(ctx).WarnContext(ctx, "resiliencehub: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:   resiliencehubSnapshotVersion,
		Tables:    tables,
		AccountID: b.accountID,
		Region:    b.region,
	}

	return persistence.MarshalSnapshot(ctx, "resiliencehub", snap)
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "resiliencehub", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != resiliencehubSnapshotVersion {
		logger.Load(ctx).WarnContext(ctx,
			"resiliencehub: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", resiliencehubSnapshotVersion)

		b.registry.ResetAll()

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("resiliencehub: restore tables: %w", err)
	}

	restoreTagsLocked(b)

	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// restoreTagsLocked rebuilds a Prometheus-named tags.Tags instance for every
// App/ResiliencyPolicy/AppAssessment/RecommendationTemplate after a JSON
// round-trip, which deserializes every tags.Tags with the generic
// "json.tags" name (see tags.Tags.UnmarshalJSON's doc comment) -- matching
// services/outposts' restoreOutpostAndSiteTagsLocked. Callers must hold b.mu.
func restoreTagsLocked(b *InMemoryBackend) {
	for _, a := range b.apps.All() {
		a.Tags = rebuildTags(a.Tags, "resiliencehub.app."+a.ID+".tags")
	}

	for _, p := range b.policies.All() {
		id, _ := resourceIDFromARN(p.ARN, ":resiliency-policy/")
		p.Tags = rebuildTags(p.Tags, "resiliencehub.policy."+id+".tags")
	}

	for _, a := range b.assessments.All() {
		id, _ := resourceIDFromARN(a.ARN, ":app-assessment/")
		a.Tags = rebuildTags(a.Tags, "resiliencehub.assessment."+id+".tags")
	}

	for _, t := range b.templates.All() {
		id, _ := resourceIDFromARN(t.ARN, ":recommendation-template/")
		t.Tags = rebuildTags(t.Tags, "resiliencehub.template."+id+".tags")
	}
}

// rebuildTags returns a fresh, correctly-named tags.Tags carrying the same
// entries as old (which may be nil after a JSON round-trip lost its name).
func rebuildTags(old *tags.Tags, name string) *tags.Tags {
	if old == nil {
		return tags.New(name)
	}

	raw := old.Clone()
	old.Close()

	return tags.FromMap(name, raw)
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
