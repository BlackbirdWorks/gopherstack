package kms

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// errFlatSnapshotFormat is returned by tryRestoreNested when the snapshot uses the legacy flat format.
var errFlatSnapshotFormat = errors.New("detected flat snapshot format")

type backendSnapshot struct {
	Keys               map[string]map[string]*Key                    `json:"keys"`
	Aliases            map[string]map[string]*Alias                  `json:"aliases"`
	Grants             map[string]map[string]*Grant                  `json:"grants"`
	Policies           map[string]map[string]string                  `json:"policies"`
	KeyMaterials       map[string]map[string]serializedKeyMaterial   `json:"key_materials,omitempty"`
	KeyMaterialHistory map[string]map[string][]serializedKeyMaterial `json:"key_material_history,omitempty"`
	CustomKeyStores    map[string]map[string]*CustomKeyStore         `json:"custom_key_stores,omitempty"`
	AccountID          string                                        `json:"accountID"`
	Region             string                                        `json:"region"`
}

// backendSnapshotFlat is the legacy flat (pre-region-nested) snapshot format for backwards compat.
type backendSnapshotFlat struct {
	Keys               map[string]*Key                    `json:"keys"`
	Aliases            map[string]*Alias                  `json:"aliases"`
	Grants             map[string]*Grant                  `json:"grants"`
	Policies           map[string]string                  `json:"policies"`
	KeyMaterials       map[string]serializedKeyMaterial   `json:"key_materials,omitempty"`
	KeyMaterialHistory map[string][]serializedKeyMaterial `json:"key_material_history,omitempty"`
	CustomKeyStores    map[string]*CustomKeyStore         `json:"custom_key_stores,omitempty"`
	AccountID          string                             `json:"accountID"`
	Region             string                             `json:"region"`
}

// snapshotRegionKeyMaterials serializes b.keyMaterials into region-nested form.
func snapshotRegionKeyMaterials(
	ctx context.Context,
	km map[string]map[string]*keyMaterial,
) map[string]map[string]serializedKeyMaterial {
	out := make(map[string]map[string]serializedKeyMaterial, len(km))

	for region, regionKMs := range km {
		regionOut := make(map[string]serializedKeyMaterial, len(regionKMs))

		for keyID, m := range regionKMs {
			s, err := marshalKeyMaterial(m)
			if err != nil {
				logger.Load(ctx).WarnContext(ctx, "KMS snapshot: skipping key material",
					"region", region, "keyID", keyID, "error", err)

				continue
			}

			regionOut[keyID] = s
		}

		if len(regionOut) > 0 {
			out[region] = regionOut
		}
	}

	return out
}

// snapshotRegionKeyMaterialHistory serializes b.keyMaterialHistory into region-nested form.
func snapshotRegionKeyMaterialHistory(
	ctx context.Context,
	hist map[string]map[string][]*keyMaterial,
) map[string]map[string][]serializedKeyMaterial {
	out := make(map[string]map[string][]serializedKeyMaterial, len(hist))

	for region, regionHist := range hist {
		regionOut := make(map[string][]serializedKeyMaterial, len(regionHist))

		for keyID, history := range regionHist {
			entries := snapshotKeyMaterialHistory(ctx, region, keyID, history)
			if len(entries) > 0 {
				regionOut[keyID] = entries
			}
		}

		if len(regionOut) > 0 {
			out[region] = regionOut
		}
	}

	return out
}

// snapshotKeyMaterialHistory serializes a single key's material history slice.
func snapshotKeyMaterialHistory(
	ctx context.Context,
	region, keyID string,
	history []*keyMaterial,
) []serializedKeyMaterial {
	entries := make([]serializedKeyMaterial, 0, len(history))

	for _, m := range history {
		s, err := marshalKeyMaterial(m)
		if err != nil {
			logger.Load(ctx).WarnContext(ctx, "KMS snapshot: skipping historical key material",
				"region", region, "keyID", keyID, "error", err)

			continue
		}

		entries = append(entries, s)
	}

	return entries
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
// Key materials that cannot be serialized are omitted from the snapshot with a warning log.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Keys:               b.keys,
		Aliases:            b.aliases,
		Grants:             b.grants,
		Policies:           b.policies,
		KeyMaterials:       snapshotRegionKeyMaterials(ctx, b.keyMaterials),
		KeyMaterialHistory: snapshotRegionKeyMaterialHistory(ctx, b.keyMaterialHistory),
		CustomKeyStores:    b.customKeyStores,
		AccountID:          b.accountID,
		Region:             b.defaultRegion,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
// Supports both region-nested (current) and flat (legacy) snapshot formats.
// If a key in the snapshot does not have corresponding key material (e.g. from an older snapshot
// format), a warning is logged. Callers of Encrypt/Sign/etc. will receive ErrKeyMaterialUnavailable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	// Two-pass restore: try nested format first, fall back to flat legacy format.
	snap, err := tryRestoreNested(data)
	if err != nil {
		// Fall back to flat format: migrate everything under defaultRegion.
		return b.restoreFlat(ctx, data)
	}

	return b.applySnapshot(ctx, snap)
}

// tryRestoreNested attempts to unmarshal data as the region-nested snapshot format.
// Returns an error if the data does not match the nested format (e.g. Keys is flat).
func tryRestoreNested(data []byte) (*backendSnapshot, error) {
	var snap backendSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, err
	}

	// Heuristic: if Keys is nil or empty it could be either format — accept as nested.
	// If Keys has entries, check that values are map[string]*Key (nested) not *Key (flat).
	// We detect flat format by trying to unmarshal Keys as map[string]*Key.
	// Since both formats use the same "keys" JSON tag, we use a raw probe.
	var probe struct {
		Keys json.RawMessage `json:"keys"`
	}

	if err := json.Unmarshal(data, &probe); err != nil {
		return nil, err
	}

	if len(probe.Keys) > 0 && string(probe.Keys) != "null" {
		// Try to unmarshal as flat map[string]*Key.
		var flatKeys map[string]*Key
		if err := json.Unmarshal(probe.Keys, &flatKeys); err == nil {
			// Successfully parsed as flat — check if any value looks like a *Key
			// (has a "keyId" field) rather than map[string]*Key (has string sub-keys
			// that themselves contain "keyId"). If the flat parse succeeded without error
			// and snap.Keys (nested parse) is non-nil, we need to determine which
			// interpretation is correct.
			//
			// Nested format: {"us-east-1": {"uuid": {Key...}}}
			// Flat format:   {"uuid": {Key...}}
			//
			// Try nested parse; if any value in snap.Keys can be parsed as a *Key
			// (i.e., has KeyID field), it's flat format.
			for _, regionOrKey := range flatKeys {
				if regionOrKey != nil && regionOrKey.KeyID != "" {
					// This is a flat map: the "region" key has a KeyID → it's actually a keyID key.
					return nil, errFlatSnapshotFormat
				}
			}
		}
	}

	return &snap, nil
}

// restoreRegionKeyMaterials deserializes the region-nested key materials map.
func restoreRegionKeyMaterials(
	serialized map[string]map[string]serializedKeyMaterial,
) (map[string]map[string]*keyMaterial, error) {
	out := make(map[string]map[string]*keyMaterial, len(serialized))

	for region, regionKMs := range serialized {
		regionOut := make(map[string]*keyMaterial, len(regionKMs))

		for keyID, s := range regionKMs {
			km, err := unmarshalKeyMaterial(s)
			if err != nil {
				return nil, fmt.Errorf("restoring key material for region %s key %s: %w", region, keyID, err)
			}

			regionOut[keyID] = km
		}

		out[region] = regionOut
	}

	return out, nil
}

// restoreRegionKeyMaterialHistory deserializes the region-nested key material history map.
func restoreRegionKeyMaterialHistory(
	serialized map[string]map[string][]serializedKeyMaterial,
) (map[string]map[string][]*keyMaterial, error) {
	out := make(map[string]map[string][]*keyMaterial, len(serialized))

	for region, regionHist := range serialized {
		regionOut := make(map[string][]*keyMaterial, len(regionHist))

		for keyID, entries := range regionHist {
			history, err := restoreKeyMaterialHistory(region, keyID, entries)
			if err != nil {
				return nil, err
			}

			regionOut[keyID] = history
		}

		out[region] = regionOut
	}

	return out, nil
}

// restoreKeyMaterialHistory deserializes a single key's material history.
func restoreKeyMaterialHistory(region, keyID string, entries []serializedKeyMaterial) ([]*keyMaterial, error) {
	history := make([]*keyMaterial, 0, len(entries))

	for i, s := range entries {
		km, err := unmarshalKeyMaterial(s)
		if err != nil {
			return nil, fmt.Errorf(
				"restoring historical key material[%d] for region %s key %s: %w",
				i,
				region,
				keyID,
				err,
			)
		}

		history = append(history, km)
	}

	return history, nil
}

// warnMissingKeyMaterials logs a warning for any key that lacks key material after restore.
func warnMissingKeyMaterials(
	ctx context.Context,
	keys map[string]map[string]*Key,
	materials map[string]map[string]*keyMaterial,
) {
	for region, regionKeys := range keys {
		regionMaterials := materials[region]

		for keyID, key := range regionKeys {
			if key.KeyState == KeyStatePendingImport {
				continue
			}

			if regionMaterials == nil {
				logger.Load(ctx).WarnContext(ctx,
					"KMS restore: key has no material in snapshot; crypto operations will fail",
					"region", region, "keyID", keyID)

				continue
			}

			if _, hasMaterial := regionMaterials[keyID]; !hasMaterial {
				logger.Load(ctx).WarnContext(ctx,
					"KMS restore: key has no material in snapshot; crypto operations will fail",
					"region", region, "keyID", keyID)
			}
		}
	}
}

// ensureSnapDefaults fills nil maps in a snapshot with empty initialized versions.
func ensureSnapDefaults(snap *backendSnapshot) {
	if snap.Keys == nil {
		snap.Keys = make(map[string]map[string]*Key)
	}

	if snap.Aliases == nil {
		snap.Aliases = make(map[string]map[string]*Alias)
	}

	if snap.Grants == nil {
		snap.Grants = make(map[string]map[string]*Grant)
	}

	if snap.Policies == nil {
		snap.Policies = make(map[string]map[string]string)
	}

	if snap.CustomKeyStores == nil {
		snap.CustomKeyStores = make(map[string]map[string]*CustomKeyStore)
	}
}

// applySnapshot applies a nested backendSnapshot to the backend.
// Must be called without any lock held (acquires write lock internally).
func (b *InMemoryBackend) applySnapshot(ctx context.Context, snap *backendSnapshot) error {
	ensureSnapDefaults(snap)

	restored, err := restoreRegionKeyMaterials(snap.KeyMaterials)
	if err != nil {
		return err
	}

	restoredHistory, err := restoreRegionKeyMaterialHistory(snap.KeyMaterialHistory)
	if err != nil {
		return err
	}

	warnMissingKeyMaterials(ctx, snap.Keys, restored)

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.keys = snap.Keys
	b.aliases = snap.Aliases
	b.grants = snap.Grants
	b.policies = snap.Policies
	b.keyMaterials = restored
	b.keyMaterialHistory = restoredHistory
	b.customKeyStores = snap.CustomKeyStores
	b.accountID = snap.AccountID
	b.defaultRegion = snap.Region
	b.clearResolutionCache()
	b.rebuildGrantIndexesLocked()

	return nil
}

// rebuildGrantIndexesLocked rebuilds grantsByToken and grantsByKey from the
// canonical grants map. Must be called with the write lock held.
func (b *InMemoryBackend) rebuildGrantIndexesLocked() {
	b.grantsByToken = make(map[string]map[string]*Grant)
	b.grantsByKey = make(map[string]map[string]map[string]*Grant)

	for region, regionGrants := range b.grants {
		for _, g := range regionGrants {
			b.grantsByTokenStore(region)[g.GrantToken] = g
			b.grantsByKeyStore(region, g.KeyID)[g.GrantID] = g
		}
	}
}

// restoreFlat handles the legacy flat snapshot format by migrating all data under defaultRegion.
func (b *InMemoryBackend) restoreFlat(ctx context.Context, data []byte) error {
	var flat backendSnapshotFlat

	if err := json.Unmarshal(data, &flat); err != nil {
		return err
	}

	// Determine the region to migrate flat data into.
	region := flat.Region
	if region == "" {
		region = b.defaultRegion
	}

	// Lift flat maps into nested maps under the single region.
	snap := &backendSnapshot{
		AccountID: flat.AccountID,
		Region:    region,
	}

	if flat.Keys != nil {
		snap.Keys = map[string]map[string]*Key{region: flat.Keys}
	}

	if flat.Aliases != nil {
		snap.Aliases = map[string]map[string]*Alias{region: flat.Aliases}
	}

	if flat.Grants != nil {
		snap.Grants = map[string]map[string]*Grant{region: flat.Grants}
	}

	if flat.Policies != nil {
		snap.Policies = map[string]map[string]string{region: flat.Policies}
	}

	if flat.KeyMaterials != nil {
		snap.KeyMaterials = map[string]map[string]serializedKeyMaterial{region: flat.KeyMaterials}
	}

	if flat.KeyMaterialHistory != nil {
		snap.KeyMaterialHistory = map[string]map[string][]serializedKeyMaterial{region: flat.KeyMaterialHistory}
	}

	if flat.CustomKeyStores != nil {
		snap.CustomKeyStores = map[string]map[string]*CustomKeyStore{region: flat.CustomKeyStores}
	}

	return b.applySnapshot(ctx, snap)
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	type snapshotter interface {
		Snapshot(ctx context.Context) []byte
	}
	if s, ok := h.Backend.(snapshotter); ok {
		return s.Snapshot(ctx)
	}

	return nil
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	type restorer interface {
		Restore(context.Context, []byte) error
	}
	if r, ok := h.Backend.(restorer); ok {
		return r.Restore(ctx, data)
	}

	return nil
}
