package kms

import (
	"encoding/json"
	"fmt"
	"log/slog"
)

type backendSnapshot struct {
	Keys         map[string]*Key                  `json:"keys"`
	Aliases      map[string]*Alias                `json:"aliases"`
	Grants       map[string]*Grant                `json:"grants"`
	Policies     map[string]string                `json:"policies"`
	KeyMaterials map[string]serializedKeyMaterial `json:"key_materials,omitempty"`
	AccountID    string                           `json:"accountID"`
	Region       string                           `json:"region"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
// Key materials that cannot be serialized are omitted from the snapshot with a warning log.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	serialized := make(map[string]serializedKeyMaterial, len(b.keyMaterials))

	for keyID, km := range b.keyMaterials {
		s, err := marshalKeyMaterial(km)
		if err != nil {
			slog.Default().Warn("KMS snapshot: skipping key material that could not be serialized",
				"keyID", keyID, "error", err)

			continue
		}

		serialized[keyID] = s
	}

	snap := backendSnapshot{
		Keys:         b.keys,
		Aliases:      b.aliases,
		Grants:       b.grants,
		Policies:     b.policies,
		KeyMaterials: serialized,
		AccountID:    b.accountID,
		Region:       b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
// If a key in the snapshot does not have corresponding key material (e.g. from an older snapshot
// format), a warning is logged. Callers of Encrypt/Sign/etc. will receive ErrKeyMaterialUnavailable.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Keys == nil {
		snap.Keys = make(map[string]*Key)
	}

	if snap.Aliases == nil {
		snap.Aliases = make(map[string]*Alias)
	}

	if snap.Grants == nil {
		snap.Grants = make(map[string]*Grant)
	}

	if snap.Policies == nil {
		snap.Policies = make(map[string]string)
	}

	// Restore key materials.
	restored := make(map[string]*keyMaterial, len(snap.KeyMaterials))

	for keyID, s := range snap.KeyMaterials {
		km, err := unmarshalKeyMaterial(s)
		if err != nil {
			return fmt.Errorf("restoring key material for %s: %w", keyID, err)
		}

		restored[keyID] = km
	}

	// Warn about keys that lack material in the snapshot (older snapshots may omit key_materials).
	for keyID := range snap.Keys {
		if _, hasMaterial := restored[keyID]; !hasMaterial {
			slog.Default().Warn("KMS restore: key has no material in snapshot; crypto operations will fail",
				"keyID", keyID)
		}
	}

	b.keys = snap.Keys
	b.aliases = snap.Aliases
	b.grants = snap.Grants
	b.policies = snap.Policies
	b.keyMaterials = restored
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte {
	type snapshotter interface{ Snapshot() []byte }
	if s, ok := h.Backend.(snapshotter); ok {
		return s.Snapshot()
	}

	return nil
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error {
	type restorer interface{ Restore([]byte) error }
	if r, ok := h.Backend.(restorer); ok {
		return r.Restore(data)
	}

	return nil
}
