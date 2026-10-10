package kms

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"time"
)

const (
	importTypeNew      = "NEW_KEY_MATERIAL"
	importTypeExisting = "EXISTING_KEY_MATERIAL"

	keyMaterialPendingRotation = "PENDING_ROTATION"
	importStatePendingImport   = "PENDING_IMPORT"
)

// ImportedMaterial is one generation of imported key material of an EXTERNAL symmetric key.
// The bytes live in the key material stores; this records identity and lifecycle.
type ImportedMaterial struct {
	ID              string  `json:"Id"`
	Description     string  `json:"Description,omitempty"`
	ExpirationModel string  `json:"ExpirationModel,omitempty"`
	State           string  `json:"State"`
	RotationType    string  `json:"RotationType,omitempty"`
	ValidTo         float64 `json:"ValidTo,omitempty"`
	RotationDate    float64 `json:"RotationDate,omitempty"`
	Imported        bool    `json:"Imported"`
}

// importedMaterialID derives the key material ID from the key and the material bytes.
func importedMaterialID(keyID string, material []byte) string {
	h := sha256.New()
	h.Write([]byte(keyID))
	h.Write([]byte{0})
	h.Write(material)

	return hex.EncodeToString(h.Sum(nil))
}

// importedMaterialsView returns key's material generations, synthesizing the single current entry for
// keys imported before generations were tracked.
func (b *InMemoryBackend) importedMaterialsView(key *Key, current *keyMaterial) []ImportedMaterial {
	if len(key.ImportedMaterials) > 0 || current == nil || key.KeyState != KeyStateEnabled {
		return key.ImportedMaterials
	}

	return []ImportedMaterial{{
		ID:              importedMaterialID(b.materialIDKey(key), current.symmetricKey),
		ExpirationModel: key.ExpirationModel,
		ValidTo:         key.ValidTo,
		State:           keyMaterialCurrent,
		Imported:        true,
	}}
}

func findImportedMaterial(mats []ImportedMaterial, id string) int {
	return slices.IndexFunc(mats, func(m ImportedMaterial) bool { return m.ID == id })
}

func findImportedMaterialByState(mats []ImportedMaterial, state string) int {
	return slices.IndexFunc(mats, func(m ImportedMaterial) bool { return m.State == state })
}

func syncCurrentExpiry(key *Key) {
	key.ValidTo = 0
	key.ExpirationModel = ""

	if i := findImportedMaterialByState(key.ImportedMaterials, keyMaterialCurrent); i >= 0 {
		key.ValidTo = key.ImportedMaterials[i].ValidTo
		key.ExpirationModel = key.ImportedMaterials[i].ExpirationModel
	}
}

func (b *InMemoryBackend) pendingMaterialsStore(region string) map[string]*keyMaterial {
	b.tableMu.Lock()
	defer b.tableMu.Unlock()

	if m, ok := b.pendingMaterials[region]; ok {
		return m
	}

	m := make(map[string]*keyMaterial)
	b.pendingMaterials[region] = m

	return m
}

// markCurrentMaterialGone records that the current imported material is no longer held by KMS.
func markCurrentMaterialGone(key *Key) {
	if i := findImportedMaterialByState(key.ImportedMaterials, keyMaterialCurrent); i >= 0 {
		key.ImportedMaterials[i].Imported = false
	}
}

func resolveImportType(input *ImportKeyMaterialInput, hasMaterials bool) (string, error) {
	t := input.ImportType

	switch t {
	case "":
		t = importTypeExisting
		if !hasMaterials {
			t = importTypeNew
		}
	case importTypeNew, importTypeExisting:
	default:
		return "", fmt.Errorf("%w: ImportType must be %s or %s", ErrValidation, importTypeNew, importTypeExisting)
	}

	if t == importTypeNew && input.KeyMaterialID != "" {
		return "", fmt.Errorf("%w: KeyMaterialId cannot be specified with ImportType %s", ErrValidation, importTypeNew)
	}

	return t, nil
}

// importNewMaterialLocked adds new key material: current when the key awaits material, otherwise
// pending rotation.
func (b *InMemoryBackend) importNewMaterialLocked(
	region string, key *Key, km *keyMaterial, id string, entry ImportedMaterial,
) (string, error) {
	if findImportedMaterial(key.ImportedMaterials, id) >= 0 {
		return "", fmt.Errorf(
			"%w: this key material is already associated with the key; use %s",
			ErrIncorrectKeyMaterial, importTypeExisting,
		)
	}

	if key.KeyState == KeyStatePendingImport {
		for i := range key.ImportedMaterials {
			if key.ImportedMaterials[i].State == keyMaterialCurrent {
				key.ImportedMaterials[i].State = keyMaterialNonCurrent
			}
		}

		entry.State = keyMaterialCurrent
		key.ImportedMaterials = append(key.ImportedMaterials, entry)
		b.keyMaterialsStore(region)[key.KeyID] = km
		key.KeyState = KeyStateEnabled
		key.Enabled = true
		syncCurrentExpiry(key)

		return id, nil
	}

	if isMultiRegionReplica(key) {
		return "", fmt.Errorf(
			"%w: new key material cannot be imported directly into a replica key; import it into the primary "+
				"key and use %s here", ErrUnsupportedOrigin, importTypeExisting,
		)
	}

	if findPendingImportedMaterial(key.ImportedMaterials) >= 0 {
		return "", fmt.Errorf(
			"%w: key %q already has key material pending rotation; rotate or delete it first",
			ErrKeyInvalidState, key.KeyID,
		)
	}

	entry.State = b.pendingStateForNewMaterialLocked(key)
	key.ImportedMaterials = append(key.ImportedMaterials, entry)
	b.pendingMaterialsStore(region)[key.KeyID] = km

	return id, nil
}

// reimportMaterialLocked restores previously imported material identified by id.
func (b *InMemoryBackend) reimportMaterialLocked(
	region string, key *Key, km *keyMaterial, id string, input *ImportKeyMaterialInput, entry ImportedMaterial,
) (string, error) {
	idx := findImportedMaterial(key.ImportedMaterials, id)
	if idx < 0 || (input.KeyMaterialID != "" && input.KeyMaterialID != id) {
		return "", fmt.Errorf(
			"%w: the supplied key material does not match key material already associated with the key",
			ErrIncorrectKeyMaterial,
		)
	}

	e := &key.ImportedMaterials[idx]
	e.Imported = true
	e.ExpirationModel = entry.ExpirationModel
	e.ValidTo = entry.ValidTo

	if input.KeyMaterialDescription != "" {
		e.Description = input.KeyMaterialDescription
	}

	switch e.State {
	case keyMaterialCurrent:
		b.keyMaterialsStore(region)[key.KeyID] = km
		key.KeyState = KeyStateEnabled
		key.Enabled = true
		syncCurrentExpiry(key)
	case keyMaterialNonCurrent:
		hist := b.keyMaterialHistoryStore(region)
		if !slices.ContainsFunc(hist[key.KeyID], func(h *keyMaterial) bool {
			return importedMaterialID(b.materialIDKey(key), h.symmetricKey) == id
		}) {
			hist[key.KeyID] = append(hist[key.KeyID], km)
		}
	default:
		b.pendingMaterialsStore(region)[key.KeyID] = km
	}

	return id, nil
}

// rotateImportedLocked promotes the pending imported material to current; on a multi-Region primary it
// rotates every replica too.
func (b *InMemoryBackend) rotateImportedLocked(region string, key *Key) error {
	if key.KeyState != KeyStateEnabled {
		return keyStateError(key)
	}

	if isMultiRegionReplica(key) {
		return fmt.Errorf(
			"%w: on-demand rotation of a multi-Region key set is invoked on the primary key",
			ErrUnsupportedOrigin,
		)
	}

	if key.MultiRegion && len(b.replicaKeysLocked(key)) > 0 {
		return b.rotateMultiRegionLocked(region, key)
	}

	key.ImportedMaterials = b.importedMaterialsView(key, b.keyMaterialsStore(region)[key.KeyID])

	pIdx := findImportedMaterialByState(key.ImportedMaterials, keyMaterialPendingRotation)
	if pIdx < 0 || b.pendingMaterialsStore(region)[key.KeyID] == nil {
		return fmt.Errorf(
			"%w: key %q has no imported key material pending rotation; import it with %s first",
			ErrKeyInvalidState, key.KeyID, importTypeNew,
		)
	}

	return b.promotePendingLocked(region, key, key.ImportedMaterials[pIdx].ID, UnixTimeFloat(time.Now()))
}

// deleteImportedMaterialLocked removes one generation of imported material and returns its ID.
func (b *InMemoryBackend) deleteImportedMaterialLocked(
	region string, key *Key, wantID string,
) (string, error) {
	key.ImportedMaterials = b.importedMaterialsView(key, b.keyMaterialsStore(region)[key.KeyID])

	var idx int
	if wantID != "" {
		if idx = findImportedMaterial(key.ImportedMaterials, wantID); idx < 0 {
			return "", fmt.Errorf("%w: key material %q is not associated with key %q",
				ErrKeyNotFound, wantID, key.KeyID)
		}
	} else {
		idx = findImportedMaterialByState(key.ImportedMaterials, keyMaterialCurrent)
	}

	if idx < 0 {
		b.markPendingImportLocked(region, key)

		return "", nil
	}

	e := key.ImportedMaterials[idx]

	switch e.State {
	case keyMaterialPendingRotation, keyMaterialPendingMultiRegion:
		key.ImportedMaterials = slices.Delete(key.ImportedMaterials, idx, idx+1)
		delete(b.pendingMaterialsStore(region), key.KeyID)
	case keyMaterialNonCurrent:
		key.ImportedMaterials[idx].Imported = false
		hist := b.keyMaterialHistoryStore(region)
		hist[key.KeyID] = slices.DeleteFunc(hist[key.KeyID], func(h *keyMaterial) bool {
			return importedMaterialID(b.materialIDKey(key), h.symmetricKey) == e.ID
		})
	default:
		key.ImportedMaterials[idx].Imported = false
		b.markPendingImportLocked(region, key)
	}

	return e.ID, nil
}

func (b *InMemoryBackend) markPendingImportLocked(region string, key *Key) {
	delete(b.keyMaterialsStore(region), key.KeyID)
	key.KeyState = KeyStatePendingImport
	key.Enabled = false
	key.ValidTo = 0
	key.ExpirationModel = ""
}

// ImportKeyMaterialWithResult imports key material and reports the ID KMS assigned to it.
func (b *InMemoryBackend) ImportKeyMaterialWithResult(
	ctx context.Context,
	input *ImportKeyMaterialInput,
) (*ImportKeyMaterialOutput, error) {
	b.mu.Lock("ImportKeyMaterial")
	defer b.mu.Unlock()

	key, region, err := b.resolveKeyAndRegion(ctx, input.KeyID, ErrInvalidArn)
	if err != nil {
		return nil, err
	}

	if key.Origin != KeyOriginExternal {
		return nil, fmt.Errorf(
			"%w: ImportKeyMaterial is only valid for keys with Origin=%s", ErrUnsupportedOrigin, KeyOriginExternal,
		)
	}

	if key.KeyState != KeyStatePendingImport && key.KeyState != KeyStateEnabled {
		return nil, fmt.Errorf("%w: key %q is not awaiting key material", ErrKeyInvalidState, key.KeyID)
	}

	if key.KeySpec != keySpecSymmetric {
		return nil, fmt.Errorf(
			"%w: ImportKeyMaterial only supports SYMMETRIC_DEFAULT keys; got %s",
			ErrUnsupportedParameter, key.KeySpec,
		)
	}

	if len(input.KeyMaterial) == 0 {
		return nil, fmt.Errorf("%w: KeyMaterial must not be empty", ErrIncorrectKeyMaterial)
	}

	key.ImportedMaterials = b.importedMaterialsView(key, b.keyMaterialsStore(region)[key.KeyID])

	importType, err := resolveImportType(input, len(key.ImportedMaterials) > 0)
	if err != nil {
		return nil, err
	}

	expModel, validTo, err := resolveExpirationModel(input.ExpirationModel, input.ValidTo)
	if err != nil {
		return nil, err
	}

	raw, err := b.resolveKeyMaterial(key.KeyID, input.KeyMaterial)
	if err != nil {
		return nil, err
	}

	if len(raw) != aes256Bytes {
		return nil, fmt.Errorf(
			"%w: symmetric key material must be exactly %d bytes, got %d",
			ErrIncorrectKeyMaterial, aes256Bytes, len(raw),
		)
	}

	km, err := newSymmetricKeyMaterial(slices.Clone(raw))
	if err != nil {
		return nil, fmt.Errorf("creating imported symmetric key material: %w", err)
	}

	id := importedMaterialID(b.materialIDKey(key), raw)
	entry := ImportedMaterial{
		ID: id, Description: input.KeyMaterialDescription, ExpirationModel: expModel,
		ValidTo: validTo, Imported: true,
	}

	id, err = b.dispatchImportLocked(region, key, km, id, importType, input, entry)

	if err != nil {
		return nil, err
	}

	return &ImportKeyMaterialOutput{KeyID: key.Arn, KeyMaterialID: id}, nil
}

// DeleteImportedKeyMaterialWithResult deletes imported key material and reports the deleted ID.
func (b *InMemoryBackend) DeleteImportedKeyMaterialWithResult(
	ctx context.Context,
	input *DeleteImportedKeyMaterialInput,
) (*DeleteImportedKeyMaterialOutput, error) {
	b.mu.Lock("DeleteImportedKeyMaterial")
	defer b.mu.Unlock()

	key, region, err := b.resolveKeyAndRegion(ctx, input.KeyID, ErrInvalidArn)
	if err != nil {
		return nil, err
	}

	if key.Origin != KeyOriginExternal {
		return nil, fmt.Errorf(
			"%w: DeleteImportedKeyMaterial is only valid for keys with Origin=%s",
			ErrUnsupportedOrigin, KeyOriginExternal,
		)
	}

	id, err := b.deleteImportedMaterialLocked(region, key, input.KeyMaterialID)
	if err != nil {
		return nil, err
	}

	return &DeleteImportedKeyMaterialOutput{KeyID: key.Arn, KeyMaterialID: id}, nil
}

// dispatchImportLocked routes an import to the replica, new-material or re-import path.
func (b *InMemoryBackend) dispatchImportLocked(
	region string, key *Key, km *keyMaterial, id, importType string,
	input *ImportKeyMaterialInput, entry ImportedMaterial,
) (string, error) {
	if importType == importTypeExisting {
		handled, err := b.importExistingIntoReplicaLocked(region, key, km, id, entry)
		if err != nil || handled {
			return id, err
		}
	}

	if importType == importTypeNew {
		return b.importNewMaterialLocked(region, key, km, id, entry)
	}

	return b.reimportMaterialLocked(region, key, km, id, input, entry)
}
