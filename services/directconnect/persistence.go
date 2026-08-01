package directconnect

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// directConnectSnapshotVersion identifies the shape of backendSnapshot's
// Tables blob (the set of resources registered on b.registry -- see
// registerAllTables in store_setup.go). Bump whenever a change there would
// make an older snapshot unsafe to decode as the current shape; Restore
// discards (rather than partially decodes) any mismatch -- see Restore.
const directConnectSnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the Direct Connect
// backend.
type backendSnapshot struct {
	Tables    map[string]json.RawMessage `json:"tables"`
	AccountID string                     `json:"accountId"`
	Region    string                     `json:"region"`
	Version   int                        `json:"version"`
	NextASN   int64                      `json:"nextAmazonSideAsn"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "directconnect: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:   directConnectSnapshotVersion,
		Tables:    tables,
		AccountID: b.accountID,
		Region:    b.region,
		NextASN:   b.nextPrivateAsn,
	}

	return persistence.MarshalSnapshot(ctx, "directconnect", snap)
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "directconnect", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != directConnectSnapshotVersion {
		logger.Load(ctx).WarnContext(ctx,
			"directconnect: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", directConnectSnapshotVersion)

		b.registry.ResetAll()

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("directconnect: restore tables: %w", err)
	}

	restoreTagsLocked(b)

	b.accountID = snap.AccountID
	b.region = snap.Region
	b.nextPrivateAsn = snap.NextASN

	if b.nextPrivateAsn == 0 {
		b.nextPrivateAsn = privateAsnRangeStart
	}

	return nil
}

// restoreTagsLocked rebuilds a Prometheus-named tags.Tags instance for every
// Connection/Lag/Interconnect/VirtualInterface/DirectConnectGateway after a
// JSON round-trip, which deserializes every tags.Tags with the generic
// "json.tags" name (see tags.Tags.UnmarshalJSON's doc comment) -- matching
// services/outposts/services/resiliencehub's identical restoreTagsLocked.
// Callers must hold b.mu.
func restoreTagsLocked(b *InMemoryBackend) {
	for _, c := range b.connections.All() {
		c.Tags = rebuildTags(c.Tags, "directconnect.connection."+c.ConnectionID+".tags")
	}

	for _, l := range b.lags.All() {
		l.Tags = rebuildTags(l.Tags, "directconnect.lag."+l.LagID+".tags")
	}

	for _, i := range b.interconnects.All() {
		i.Tags = rebuildTags(i.Tags, "directconnect.interconnect."+i.InterconnectID+".tags")
	}

	for _, v := range b.virtualInterfaces.All() {
		v.Tags = rebuildTags(v.Tags, "directconnect.vif."+v.VirtualInterfaceID+".tags")
	}

	for _, g := range b.gateways.All() {
		g.Tags = rebuildTags(g.Tags, "directconnect.gateway."+g.DirectConnectGatewayID+".tags")
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
