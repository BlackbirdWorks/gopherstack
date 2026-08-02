package networkmanager

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// networkManagerSnapshotVersion identifies the shape of backendSnapshot's
// Tables blob (the set of resources registered on b.registry -- see
// registerAllTables in store_setup.go). Bump whenever a change there would
// make an older snapshot unsafe to decode as the current shape; Restore
// discards (rather than partially decodes) any mismatch -- see Restore.
const networkManagerSnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the Network Manager
// backend.
type backendSnapshot struct {
	Tables    map[string]json.RawMessage `json:"tables"`
	OrgStatus *organizationStatus        `json:"orgStatus"`
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
		logger.Load(ctx).WarnContext(ctx, "networkmanager: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:   networkManagerSnapshotVersion,
		Tables:    tables,
		OrgStatus: b.orgStatus,
		AccountID: b.accountID,
		Region:    b.region,
	}

	return persistence.MarshalSnapshot(ctx, "networkmanager", snap)
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "networkmanager", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != networkManagerSnapshotVersion {
		logger.Load(ctx).WarnContext(ctx,
			"networkmanager: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", networkManagerSnapshotVersion)

		b.registry.ResetAll()
		b.orgStatus = &organizationStatus{OrganizationAwsServiceAccessStatus: orgAccessStatusDisabled}

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("networkmanager: restore tables: %w", err)
	}

	restoreTagsLocked(b)

	if snap.OrgStatus != nil {
		b.orgStatus = snap.OrgStatus
	} else {
		b.orgStatus = &organizationStatus{OrganizationAwsServiceAccessStatus: orgAccessStatusDisabled}
	}

	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// restoreTagsLocked rebuilds a Prometheus-named tags.Tags instance for every
// tagged resource kind after a JSON round-trip, which deserializes every
// tags.Tags with the generic "json.tags" name (see tags.Tags.UnmarshalJSON's
// doc comment) -- matching every other service in this campaign's identical
// restoreTagsLocked. Callers must hold b.mu.
func restoreTagsLocked(b *InMemoryBackend) {
	for _, v := range b.globalNetworks.All() {
		v.Tags = rebuildTags(v.Tags, "networkmanager.globalnetwork."+v.GlobalNetworkID+".tags")
	}

	for _, v := range b.sites.All() {
		v.Tags = rebuildTags(v.Tags, "networkmanager.site."+v.SiteID+".tags")
	}

	for _, v := range b.devices.All() {
		v.Tags = rebuildTags(v.Tags, "networkmanager.device."+v.DeviceID+".tags")
	}

	for _, v := range b.links.All() {
		v.Tags = rebuildTags(v.Tags, "networkmanager.link."+v.LinkID+".tags")
	}

	for _, v := range b.connections.All() {
		v.Tags = rebuildTags(v.Tags, "networkmanager.connection."+v.ConnectionID+".tags")
	}

	for _, v := range b.connectPeers.All() {
		v.Tags = rebuildTags(v.Tags, "networkmanager.connectpeer."+v.ConnectPeerID+".tags")
	}

	for _, v := range b.coreNetworks.All() {
		v.Tags = rebuildTags(v.Tags, "networkmanager.corenetwork."+v.CoreNetworkID+".tags")
	}

	for _, v := range b.attachments.All() {
		v.Tags = rebuildTags(v.Tags, "networkmanager.attachment."+v.AttachmentID+".tags")
	}

	for _, v := range b.peerings.All() {
		v.Tags = rebuildTags(v.Tags, "networkmanager.peering."+v.PeeringID+".tags")
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
