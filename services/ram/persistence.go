package ram

import (
	"context"
	"encoding/json"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

type backendSnapshot struct {
	ResourceShares   map[string]*ResourceShare           `json:"resourceShares"`
	Permissions      map[string]*Permission              `json:"permissions"`
	SharePermissions map[string]map[string]int32         `json:"sharePermissions"`
	Invitations      map[string]*ResourceShareInvitation `json:"invitations"`
	AccountID        string                              `json:"accountID"`
	Region           string                              `json:"region"`
	Associations     []*ResourceShareAssociation         `json:"associations"`
}

// Snapshot serialises the backend state to JSON.
// It acquires a read-lock and takes a deep copy of the maps before marshalling
// to avoid serialising data that is concurrently mutated.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")

	snap := backendSnapshot{
		ResourceShares: maps.Clone(b.resourceShares),
		Permissions:    maps.Clone(b.permissions),
		Invitations:    maps.Clone(b.invitations),
		AccountID:      b.accountID,
		Region:         b.region,
	}

	// Deep-copy nested sharePermissions map.
	snap.SharePermissions = make(map[string]map[string]int32, len(b.sharePermissions))

	for k, v := range b.sharePermissions {
		snap.SharePermissions[k] = maps.Clone(v)
	}

	// Copy associations slice.
	snap.Associations = make([]*ResourceShareAssociation, len(b.associations))
	copy(snap.Associations, b.associations)

	b.mu.RUnlock()

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "ram: failed to snapshot backend state", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	ensureNonNilMaps(&snap)

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.resourceShares = snap.ResourceShares
	b.associations = snap.Associations
	b.permissions = snap.Permissions
	b.sharePermissions = snap.SharePermissions
	b.invitations = snap.Invitations
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// ensureNonNilMaps initialises nil maps in the snapshot to empty collections.
func ensureNonNilMaps(snap *backendSnapshot) {
	if snap.ResourceShares == nil {
		snap.ResourceShares = make(map[string]*ResourceShare)
	}

	if snap.Associations == nil {
		snap.Associations = make([]*ResourceShareAssociation, 0)
	}

	if snap.Permissions == nil {
		snap.Permissions = make(map[string]*Permission)
	}

	if snap.SharePermissions == nil {
		snap.SharePermissions = make(map[string]map[string]int32)
	}

	if snap.Invitations == nil {
		snap.Invitations = make(map[string]*ResourceShareInvitation)
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
