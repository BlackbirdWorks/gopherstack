package ocivault

import (
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	modeHSM      = "HSM"
	modeSoftware = "SOFTWARE"

	originInternal = "INTERNAL"
	originExternal = "EXTERNAL"

	minRotationDays = 60
	maxRotationDays = 1095
	day             = 24 * time.Hour
)

// CreateKeyIn is the CreateKey request body.
type CreateKeyIn struct {
	FreeformTags           map[string]string         `json:"freeformTags"`
	DefinedTags            map[string]map[string]any `json:"definedTags"`
	AutoKeyRotationDetails *RotationDetails          `json:"autoKeyRotationDetails"`
	CompartmentID          string                    `json:"compartmentId"`
	DisplayName            string                    `json:"displayName"`
	ProtectionMode         string                    `json:"protectionMode"`
	KeyShape               KeyShape                  `json:"keyShape"`
	IsAutoRotationEnabled  bool                      `json:"isAutoRotationEnabled"`
}

// UpdateKeyIn is the UpdateKey request body.
type UpdateKeyIn struct {
	DisplayName            *string                   `json:"displayName"`
	IsAutoRotationEnabled  *bool                     `json:"isAutoRotationEnabled"`
	AutoKeyRotationDetails *RotationDetails          `json:"autoKeyRotationDetails"`
	FreeformTags           map[string]string         `json:"freeformTags"`
	DefinedTags            map[string]map[string]any `json:"definedTags"`
}

// KeyFilter narrows ListKeys.
type KeyFilter struct {
	ProtectionMode string
	Algorithm      string
	CurveID        string
	Length         int
}

func validateCreateKey(in *CreateKeyIn) *APIError {
	if in.CompartmentID == "" {
		return errMissing("compartmentId is required")
	}

	if aerr := validateDisplayName(in.DisplayName); aerr != nil {
		return aerr
	}

	if aerr := validateShape(in.KeyShape); aerr != nil {
		return aerr
	}

	switch in.ProtectionMode {
	case "":
		in.ProtectionMode = modeHSM
	case modeHSM, modeSoftware:
	default:
		return errParam("protectionMode must be HSM or SOFTWARE")
	}

	return nil
}

func (b *InMemoryBackend) newRotation(in *RotationDetails) (*RotationDetails, *APIError) {
	if in == nil || in.IntervalDays < minRotationDays || in.IntervalDays > maxRotationDays {
		return nil, errParam("autoKeyRotationDetails.rotationIntervalInDays must be between 60 and 1095")
	}

	next := b.nowUTC().Add(time.Duration(in.IntervalDays) * day)
	if in.ScheduleStart != nil && in.ScheduleStart.After(b.nowUTC()) {
		next = in.ScheduleStart.UTC()
	}

	return &RotationDetails{IntervalDays: in.IntervalDays, ScheduleStart: in.ScheduleStart, NextRotation: &next}, nil
}

func (b *InMemoryBackend) addVersion(k *Key, vaultPrefix, origin string, material []byte) *KeyVersion {
	kv := &KeyVersion{
		ID: newOCID("keyversion", vaultPrefix), State: stateEnabled, Origin: origin,
		Material: material, TimeCreated: b.nowUTC(),
	}
	k.Versions = append(k.Versions, kv)
	k.CurrentVersion = kv.ID

	return kv
}

func (b *InMemoryBackend) storeKey(v *Vault, in CreateKeyIn, material []byte, origin string) (*Key, *APIError) {
	k := &Key{
		ID: newOCID("key", v.Prefix), VaultID: v.ID, CompartmentID: in.CompartmentID, DisplayName: in.DisplayName,
		State: stateEnabled, ProtectionMode: in.ProtectionMode, Shape: in.KeyShape, TimeCreated: b.nowUTC(),
		FreeformTags: in.FreeformTags, DefinedTags: in.DefinedTags, Etag: newEtag(), Seq: b.nextSeq(),
	}

	if in.IsAutoRotationEnabled {
		rot, aerr := b.newRotation(in.AutoKeyRotationDetails)
		if aerr != nil {
			return nil, aerr
		}

		k.AutoRotation, k.Rotation = true, rot
	}

	b.addVersion(k, v.Prefix, origin, material)
	b.keys[k.ID] = k

	return k, nil
}

// CreateKey creates a key with its first version; the response reports ENABLING like OCI.
func (b *InMemoryBackend) CreateKey(vaultID string, in CreateKeyIn) (keyWire, string, error) {
	if aerr := validateCreateKey(&in); aerr != nil {
		return keyWire{}, "", aerr
	}

	material, err := generateMaterial(in.KeyShape)
	if err != nil {
		return keyWire{}, "", err
	}

	b.mu.Lock("CreateKey")
	defer b.mu.Unlock()

	v, aerr := b.activeVault(vaultID)
	if aerr != nil {
		return keyWire{}, "", aerr
	}

	k, aerr := b.storeKey(v, in, material, originInternal)
	if aerr != nil {
		return keyWire{}, "", aerr
	}

	return keyView(k, stateEnabling), k.Etag, nil
}

// keyFor resolves a key inside an optional vault scope.
func (b *InMemoryBackend) keyFor(vaultID, keyID string) (*Key, *Vault, *APIError) {
	k, ok := b.keys[keyID]
	if !ok || (vaultID != "" && k.VaultID != vaultID) {
		return nil, nil, errNotFound(keyID)
	}

	v, ok := b.vaults[k.VaultID]
	if !ok || v.State == stateDeleted {
		return nil, nil, errNotFound(keyID)
	}

	return k, v, nil
}

func (b *InMemoryBackend) writableKey(vaultID, keyID, ifMatch string) (*Key, *Vault, *APIError) {
	k, v, aerr := b.keyFor(vaultID, keyID)
	if aerr != nil {
		return nil, nil, aerr
	}

	if v.State != stateActive {
		return nil, nil, errState("Vault " + v.ID + " is in state " + v.State + "; the operation requires ACTIVE")
	}

	return k, v, checkEtag(k.Etag, ifMatch)
}

// GetKey returns one key.
func (b *InMemoryBackend) GetKey(vaultID, keyID string) (keyWire, string, error) {
	b.mu.RLock("GetKey")
	defer b.mu.RUnlock()

	k, _, aerr := b.keyFor(vaultID, keyID)
	if aerr != nil {
		return keyWire{}, "", aerr
	}

	return keyView(k, ""), k.Etag, nil
}

func (f KeyFilter) match(k *Key) bool {
	return (f.ProtectionMode == "" || f.ProtectionMode == k.ProtectionMode) &&
		(f.Algorithm == "" || f.Algorithm == k.Shape.Algorithm) &&
		(f.CurveID == "" || f.CurveID == k.Shape.CurveID) &&
		(f.Length == 0 || f.Length == k.Shape.Length)
}

// ListKeys lists a vault's keys in a compartment.
func (b *InMemoryBackend) ListKeys(
	vaultID, compartment string,
	f KeyFilter,
	o ListOpts,
) (page.Page[keySummaryWire], error) {
	if compartment == "" {
		return page.Page[keySummaryWire]{}, errMissing("compartmentId is required")
	}

	b.mu.RLock("ListKeys")
	defer b.mu.RUnlock()

	if _, aerr := b.vaultByID(vaultID); aerr != nil {
		return page.Page[keySummaryWire]{}, aerr
	}

	var all []*Key

	for _, k := range b.keys {
		if k.VaultID == vaultID && k.CompartmentID == compartment && f.match(k) {
			all = append(all, k)
		}
	}

	pg := paginate(all, o, func(k *Key) string { return k.DisplayName },
		func(k *Key) (time.Time, int64) { return k.TimeCreated, k.Seq })

	out := make([]keySummaryWire, 0, len(pg.Data))
	for _, k := range pg.Data {
		out = append(out, keySummary(k))
	}

	return page.Page[keySummaryWire]{Data: out, Next: pg.Next}, nil
}

// UpdateKey changes display name, tags and the auto-rotation schedule.
func (b *InMemoryBackend) UpdateKey(vaultID, keyID, ifMatch string, in UpdateKeyIn) (keyWire, string, error) {
	b.mu.Lock("UpdateKey")
	defer b.mu.Unlock()

	k, _, aerr := b.writableKey(vaultID, keyID, ifMatch)
	if aerr != nil {
		return keyWire{}, "", aerr
	}

	if in.DisplayName != nil {
		if aerr = validateDisplayName(*in.DisplayName); aerr != nil {
			return keyWire{}, "", aerr
		}

		k.DisplayName = *in.DisplayName
	}

	if aerr = b.applyRotationUpdate(k, in); aerr != nil {
		return keyWire{}, "", aerr
	}

	if in.FreeformTags != nil {
		k.FreeformTags = in.FreeformTags
	}

	if in.DefinedTags != nil {
		k.DefinedTags = in.DefinedTags
	}

	k.Etag = newEtag()

	return keyView(k, ""), k.Etag, nil
}

func (b *InMemoryBackend) applyRotationUpdate(k *Key, in UpdateKeyIn) *APIError {
	if in.IsAutoRotationEnabled != nil && !*in.IsAutoRotationEnabled {
		k.AutoRotation, k.Rotation = false, nil

		return nil
	}

	enable := in.IsAutoRotationEnabled != nil && *in.IsAutoRotationEnabled

	if !enable && in.AutoKeyRotationDetails == nil {
		return nil
	}

	rot, aerr := b.newRotation(in.AutoKeyRotationDetails)
	if aerr != nil {
		return aerr
	}

	if k.Rotation != nil {
		prev := k.Rotation
		rot.LastRotation, rot.LastStatus, rot.LastMessage = prev.LastRotation, prev.LastStatus, prev.LastMessage
	}

	k.AutoRotation, k.Rotation = true, rot

	return nil
}

func (b *InMemoryBackend) setKeyState(vaultID, keyID, ifMatch, from, to, shown string) (keyWire, string, error) {
	b.mu.Lock("SetKeyState")
	defer b.mu.Unlock()

	k, _, aerr := b.writableKey(vaultID, keyID, ifMatch)
	if aerr != nil {
		return keyWire{}, "", aerr
	}

	if k.State != from {
		return keyWire{}, "", errState("Key " + keyID + " is in state " + k.State + ", expected " + from)
	}

	k.State, k.Etag = to, newEtag()

	return keyView(k, shown), k.Etag, nil
}

// EnableKey moves a DISABLED key to ENABLED.
func (b *InMemoryBackend) EnableKey(vaultID, keyID, ifMatch string) (keyWire, string, error) {
	return b.setKeyState(vaultID, keyID, ifMatch, stateDisabled, stateEnabled, stateEnabling)
}

// DisableKey moves an ENABLED key to DISABLED.
func (b *InMemoryBackend) DisableKey(vaultID, keyID, ifMatch string) (keyWire, string, error) {
	return b.setKeyState(vaultID, keyID, ifMatch, stateEnabled, stateDisabled, stateDisabling)
}

// ScheduleKeyDeletion moves an ENABLED or DISABLED key to PENDING_DELETION.
func (b *InMemoryBackend) ScheduleKeyDeletion(vaultID, keyID, ifMatch string, at *time.Time) (keyWire, string, error) {
	b.mu.Lock("ScheduleKeyDeletion")
	defer b.mu.Unlock()

	k, _, aerr := b.writableKey(vaultID, keyID, ifMatch)
	if aerr != nil {
		return keyWire{}, "", aerr
	}

	if k.State != stateEnabled && k.State != stateDisabled {
		return keyWire{}, "", errState("Key " + keyID + " is in state " + k.State + "; deletion cannot be scheduled")
	}

	t, aerr := b.deletionTime(at, kmsMinDeletion)
	if aerr != nil {
		return keyWire{}, "", aerr
	}

	k.PrevState, k.State, k.TimeOfDeletion, k.Etag = k.State, statePendingDeletion, &t, newEtag()

	return keyView(k, stateSchedulingDelete), k.Etag, nil
}

// CancelKeyDeletion restores the state a key had before deletion was scheduled.
func (b *InMemoryBackend) CancelKeyDeletion(vaultID, keyID, ifMatch string) (keyWire, string, error) {
	b.mu.Lock("CancelKeyDeletion")
	defer b.mu.Unlock()

	k, _, aerr := b.writableKey(vaultID, keyID, ifMatch)
	if aerr != nil {
		return keyWire{}, "", aerr
	}

	if k.State != statePendingDeletion {
		return keyWire{}, "", errState("Key " + keyID + " is not pending deletion")
	}

	k.State, k.PrevState, k.TimeOfDeletion, k.Etag = cmpOr(k.PrevState, stateEnabled), "", nil, newEtag()

	return keyView(k, stateCancellingDelete), k.Etag, nil
}

func cmpOr(a, def string) string {
	if a == "" {
		return def
	}

	return a
}

// ChangeKeyCompartment moves a key.
func (b *InMemoryBackend) ChangeKeyCompartment(vaultID, keyID, ifMatch, compartment string) error {
	if compartment == "" {
		return errMissing("compartmentId is required")
	}

	b.mu.Lock("ChangeKeyCompartment")
	defer b.mu.Unlock()

	k, _, aerr := b.writableKey(vaultID, keyID, ifMatch)
	if aerr != nil {
		return aerr
	}

	k.CompartmentID, k.Etag = compartment, newEtag()

	return nil
}

func (k *Key) version(id string) (*KeyVersion, *APIError) {
	for _, kv := range k.Versions {
		if kv.ID == id {
			return kv, nil
		}
	}

	return nil, errNotFound(id)
}

// CreateKeyVersion rotates the key: new material becomes the current version.
func (b *InMemoryBackend) CreateKeyVersion(vaultID, keyID string) (keyVersionWire, string, error) {
	b.mu.RLock("CreateKeyVersion")
	k, _, aerr := b.keyFor(vaultID, keyID)

	var shape KeyShape
	if aerr == nil {
		shape = k.Shape
	}

	b.mu.RUnlock()

	if aerr != nil {
		return keyVersionWire{}, "", aerr
	}

	material, err := generateMaterial(shape)
	if err != nil {
		return keyVersionWire{}, "", err
	}

	b.mu.Lock("CreateKeyVersion")
	defer b.mu.Unlock()

	k, v, aerr := b.writableKey(vaultID, keyID, "")
	if aerr != nil {
		return keyVersionWire{}, "", aerr
	}

	if k.State != stateEnabled {
		return keyVersionWire{}, "", errState("Key " + keyID + " must be ENABLED to create a version")
	}

	kv := b.addVersion(k, v.Prefix, originInternal, material)
	k.Etag = newEtag()

	return keyVersionFull(k, kv, stateEnabling), k.Etag, nil
}

// GetKeyVersion returns one key version including its public key.
func (b *InMemoryBackend) GetKeyVersion(vaultID, keyID, versionID string) (keyVersionWire, string, error) {
	b.mu.RLock("GetKeyVersion")
	defer b.mu.RUnlock()

	k, _, aerr := b.keyFor(vaultID, keyID)
	if aerr != nil {
		return keyVersionWire{}, "", aerr
	}

	kv, aerr := k.version(versionID)
	if aerr != nil {
		return keyVersionWire{}, "", aerr
	}

	return keyVersionFull(k, kv, ""), k.Etag, nil
}

// ListKeyVersions lists a key's versions.
func (b *InMemoryBackend) ListKeyVersions(vaultID, keyID string, o ListOpts) (page.Page[keyVersionSummaryWire], error) {
	b.mu.RLock("ListKeyVersions")
	defer b.mu.RUnlock()

	k, _, aerr := b.keyFor(vaultID, keyID)
	if aerr != nil {
		return page.Page[keyVersionSummaryWire]{}, aerr
	}

	all := slices.Clone(k.Versions)
	pg := paginate(all, o, func(kv *KeyVersion) string { return kv.ID },
		func(kv *KeyVersion) (time.Time, int64) { return kv.TimeCreated, 0 })

	out := make([]keyVersionSummaryWire, 0, len(pg.Data))
	for _, kv := range pg.Data {
		out = append(out, keyVersionSummary(k, kv))
	}

	return page.Page[keyVersionSummaryWire]{Data: out, Next: pg.Next}, nil
}

// ScheduleKeyVersionDeletion schedules deletion of a non-current key version.
func (b *InMemoryBackend) ScheduleKeyVersionDeletion(
	vaultID, keyID, versionID string,
	at *time.Time,
) (keyVersionWire, string, error) {
	b.mu.Lock("ScheduleKeyVersionDeletion")
	defer b.mu.Unlock()

	k, _, aerr := b.writableKey(vaultID, keyID, "")
	if aerr != nil {
		return keyVersionWire{}, "", aerr
	}

	kv, aerr := k.version(versionID)
	if aerr != nil {
		return keyVersionWire{}, "", aerr
	}

	if kv.State != stateEnabled && kv.State != stateDisabled || versionID == k.CurrentVersion {
		return keyVersionWire{}, "", errState("Key version " + versionID + " cannot be scheduled for deletion")
	}

	t, aerr := b.deletionTime(at, kmsMinDeletion)
	if aerr != nil {
		return keyVersionWire{}, "", aerr
	}

	kv.State, kv.TimeOfDeletion = statePendingDeletion, &t
	k.Etag = newEtag()

	return keyVersionView(k, kv, stateSchedulingDelete), k.Etag, nil
}

// CancelKeyVersionDeletion returns a PENDING_DELETION version to ENABLED.
func (b *InMemoryBackend) CancelKeyVersionDeletion(vaultID, keyID, versionID string) (keyVersionWire, string, error) {
	b.mu.Lock("CancelKeyVersionDeletion")
	defer b.mu.Unlock()

	k, _, aerr := b.writableKey(vaultID, keyID, "")
	if aerr != nil {
		return keyVersionWire{}, "", aerr
	}

	kv, aerr := k.version(versionID)
	if aerr != nil {
		return keyVersionWire{}, "", aerr
	}

	if kv.State != statePendingDeletion {
		return keyVersionWire{}, "", errState("Key version " + versionID + " is not pending deletion")
	}

	kv.State, kv.TimeOfDeletion = stateEnabled, nil
	k.Etag = newEtag()

	return keyVersionView(k, kv, stateCancellingDelete), k.Etag, nil
}
