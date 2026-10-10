package kms

import (
	"fmt"
	"slices"
	"time"
)

const keyMaterialPendingMultiRegion = "PENDING_MULTI_REGION_IMPORT_AND_ROTATION"

func isPendingMaterialState(state string) bool {
	return state == keyMaterialPendingRotation || state == keyMaterialPendingMultiRegion
}

func findPendingImportedMaterial(mats []ImportedMaterial) int {
	return slices.IndexFunc(mats, func(m ImportedMaterial) bool { return isPendingMaterialState(m.State) })
}

// isMultiRegionReplica reports whether key is a replica (a multi-Region key whose primary lives elsewhere).
func isMultiRegionReplica(key *Key) bool {
	return key.MultiRegion && key.PrimaryRegion != "" && extractRegionFromARN(key.Arn) != key.PrimaryRegion
}

// materialIDKey is the key ID imported key material IDs are derived from: every key of a multi-Region set
// shares its primary's, so the same material carries the same ID in each Region. Must hold b.mu.
func (b *InMemoryBackend) materialIDKey(key *Key) string {
	if isMultiRegionReplica(key) {
		if primary := b.findPrimaryKeyForReplica(key); primary != nil {
			return primary.KeyID
		}
	}

	return key.KeyID
}

// replicaKeysLocked returns the live replicas of a primary. Must hold b.mu.
func (b *InMemoryBackend) replicaKeysLocked(primary *Key) []*Key {
	out := make([]*Key, 0, len(primary.ReplicaKeyIDs))

	for _, id := range primary.ReplicaKeyIDs {
		if r := b.findKeyInAnyRegion(id); r != nil {
			out = append(out, r)
		}
	}

	return out
}

// pendingStateForNewMaterialLocked is the state of newly imported material on key: on a multi-Region primary it
// waits for every replica to import it too.
func (b *InMemoryBackend) pendingStateForNewMaterialLocked(key *Key) string {
	if key.MultiRegion && !isMultiRegionReplica(key) && len(b.replicaKeysLocked(key)) > 0 {
		return keyMaterialPendingMultiRegion
	}

	return keyMaterialPendingRotation
}

// importExistingIntoReplicaLocked imports material the primary holds pending into an enabled replica, then
// promotes the primary's material to PENDING_ROTATION once every replica has it. handled is false when the key
// is not an enabled replica or the primary has no matching pending material.
func (b *InMemoryBackend) importExistingIntoReplicaLocked(
	region string, key *Key, km *keyMaterial, id string, entry ImportedMaterial,
) (bool, error) {
	if !isMultiRegionReplica(key) || key.KeyState != KeyStateEnabled {
		return false, nil
	}

	primary := b.findPrimaryKeyForReplica(key)
	if primary == nil {
		return false, nil
	}

	pIdx := findImportedMaterial(primary.ImportedMaterials, id)
	if pIdx < 0 || !isPendingMaterialState(primary.ImportedMaterials[pIdx].State) {
		return false, nil
	}

	if findImportedMaterial(key.ImportedMaterials, id) >= 0 {
		return false, nil
	}

	if findPendingImportedMaterial(key.ImportedMaterials) >= 0 {
		return true, fmt.Errorf(
			"%w: key %q already has key material pending rotation; rotate or delete it first",
			ErrKeyInvalidState, key.KeyID,
		)
	}

	entry.State = keyMaterialPendingRotation
	key.ImportedMaterials = append(key.ImportedMaterials, entry)
	b.pendingMaterialsStore(region)[key.KeyID] = km
	b.syncMultiRegionPendingLocked(primary, id)

	return true, nil
}

// syncMultiRegionPendingLocked moves the primary's pending material to PENDING_ROTATION once all replicas hold it.
func (b *InMemoryBackend) syncMultiRegionPendingLocked(primary *Key, id string) {
	idx := findImportedMaterial(primary.ImportedMaterials, id)
	if idx < 0 || primary.ImportedMaterials[idx].State != keyMaterialPendingMultiRegion {
		return
	}

	for _, r := range b.replicaKeysLocked(primary) {
		i := findImportedMaterial(r.ImportedMaterials, id)
		if i < 0 || r.ImportedMaterials[i].State != keyMaterialPendingRotation {
			return
		}
	}

	primary.ImportedMaterials[idx].State = keyMaterialPendingRotation
}

// rotateMultiRegionLocked rotates a multi-Region primary and all its replicas onto the pending material.
func (b *InMemoryBackend) rotateMultiRegionLocked(region string, primary *Key) error {
	primary.ImportedMaterials = b.importedMaterialsView(primary, b.keyMaterialsStore(region)[primary.KeyID])

	idx := findImportedMaterialByState(primary.ImportedMaterials, keyMaterialPendingRotation)
	if idx < 0 {
		return fmt.Errorf(
			"%w: key %q has no key material in %s state; import new key material into the primary and every "+
				"replica first", ErrKeyInvalidState, primary.KeyID, keyMaterialPendingRotation,
		)
	}

	id := primary.ImportedMaterials[idx].ID
	ts := UnixTimeFloat(time.Now())

	if err := b.promotePendingLocked(region, primary, id, ts); err != nil {
		return err
	}

	for _, r := range b.replicaKeysLocked(primary) {
		if err := b.promotePendingLocked(extractRegionFromARN(r.Arn), r, id, ts); err != nil {
			return err
		}
	}

	return nil
}

// promotePendingLocked makes the pending material id current on key and retires the previous one.
func (b *InMemoryBackend) promotePendingLocked(region string, key *Key, id string, ts float64) error {
	idx := findImportedMaterial(key.ImportedMaterials, id)
	pending := b.pendingMaterialsStore(region)[key.KeyID]

	if idx < 0 || pending == nil || !isPendingMaterialState(key.ImportedMaterials[idx].State) {
		return fmt.Errorf("%w: key %q lacks the pending key material", ErrKeyInvalidState, key.KeyID)
	}

	kms := b.keyMaterialsStore(region)
	kmh := b.keyMaterialHistoryStore(region)

	if current := kms[key.KeyID]; current != nil {
		kmh[key.KeyID] = append(kmh[key.KeyID], current)
	}

	kms[key.KeyID] = pending
	delete(b.pendingMaterialsStore(region), key.KeyID)

	for i := range key.ImportedMaterials {
		if key.ImportedMaterials[i].State == keyMaterialCurrent {
			key.ImportedMaterials[i].State = keyMaterialNonCurrent
		}
	}

	key.ImportedMaterials[idx].State = keyMaterialCurrent
	key.ImportedMaterials[idx].RotationDate = ts
	key.ImportedMaterials[idx].RotationType = rotationTypeOnDemand
	key.RotationDates = append(key.RotationDates, ts)
	key.Rotations = append(key.Rotations, RotationRecord{Date: ts, RotationType: rotationTypeOnDemand})
	syncCurrentExpiry(key)

	return nil
}
