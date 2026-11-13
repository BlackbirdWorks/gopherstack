package ocivault

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/json/v2"
	"net/http"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const (
	backupMagic    = "OCIVBK1\n"
	backupKeyBytes = 32
	kindVault      = "VAULT"
	kindKey        = "KEY"

	destBucket = "BUCKET"
	destURI    = "PRE_AUTHENTICATED_REQUEST_URI"
)

// BlobStore is the Object Storage surface backups are written to and read from.
type BlobStore interface {
	PutBlob(ns, bucket, object string, data []byte) error
	GetBlob(ns, bucket, object string) ([]byte, error)
	PutBlobPAR(uri string, data []byte) error
	GetBlobPAR(uri string) ([]byte, error)
}

// SetBlobStore wires the Object Storage backend that BackupVault and BackupKey write to.
func (b *InMemoryBackend) SetBlobStore(s BlobStore) {
	b.mu.Lock("SetBlobStore")
	b.blobs = s
	b.mu.Unlock()
}

// BackupLocation is the polymorphic backupLocation body (BUCKET or PRE_AUTHENTICATED_REQUEST_URI).
type BackupLocation struct {
	Destination string `json:"destination"`
	Namespace   string `json:"namespace"`
	BucketName  string `json:"bucketName"`
	ObjectName  string `json:"objectName"`
	URI         string `json:"uri"`
}

// BackupIn is the BackupVault, BackupKey, RestoreVaultFromObjectStore and RestoreKeyFromObjectStore body.
type BackupIn struct {
	BackupLocation *BackupLocation `json:"backupLocation"`
}

func (in BackupIn) location() (BackupLocation, *APIError) {
	l := in.BackupLocation
	if l == nil {
		return BackupLocation{}, errMissing("backupLocation is required")
	}

	switch l.Destination {
	case destBucket:
		if l.Namespace == "" || l.BucketName == "" || l.ObjectName == "" {
			return *l, errMissing("backupLocation namespace, bucketName and objectName are required")
		}
	case destURI:
		if l.URI == "" {
			return *l, errMissing("backupLocation uri is required")
		}
	default:
		return *l, errParam("backupLocation.destination must be BUCKET or PRE_AUTHENTICATED_REQUEST_URI")
	}

	return *l, nil
}

func (b *InMemoryBackend) store() (BlobStore, *APIError) {
	b.mu.RLock("store")
	defer b.mu.RUnlock()

	if b.blobs == nil {
		return nil, errParam("No Object Storage service is available for backup locations")
	}

	return b.blobs, nil
}

func writeBlob(s BlobStore, l BackupLocation, data []byte) *APIError {
	var err error

	if l.Destination == destBucket {
		err = s.PutBlob(l.Namespace, l.BucketName, l.ObjectName, data)
	} else {
		err = s.PutBlobPAR(l.URI, data)
	}

	if err != nil {
		return errParam("The backup location is not writable: " + err.Error())
	}

	return nil
}

func readBlob(s BlobStore, l BackupLocation) ([]byte, *APIError) {
	var (
		data []byte
		err  error
	)

	if l.Destination == destBucket {
		data, err = s.GetBlob(l.Namespace, l.BucketName, l.ObjectName)
	} else {
		data, err = s.GetBlobPAR(l.URI)
	}

	if err != nil {
		return nil, newErr(http.StatusNotFound, "NotAuthorizedOrNotFound",
			"The backup location could not be read: "+err.Error())
	}

	return data, nil
}

type vaultBackup struct {
	Vault Vault `json:"vault"`
}

type keyBackup struct {
	Vault Vault `json:"vault"`
	Key   Key   `json:"key"`
}

func newBackupKey() []byte {
	k := make([]byte, backupKeyBytes)
	_, _ = rand.Read(k)

	return k
}

func (b *InMemoryBackend) backupGCM() (cipher.AEAD, error) {
	blk, err := aes.NewCipher(b.backupKey)
	if err != nil {
		return nil, err //nolint:wrapcheck // internal
	}

	return cipher.NewGCM(blk)
}

// sealBackup encrypts a payload so key material never appears in a backup file in the clear.
func (b *InMemoryBackend) sealBackup(kind string, payload any) ([]byte, *APIError) {
	plain, err := json.Marshal(payload, jsonx.V1())
	if err != nil {
		return nil, newErr(http.StatusInternalServerError, "InternalServerError", "backup encoding failed")
	}

	gcm, err := b.backupGCM()
	if err != nil {
		return nil, newErr(http.StatusInternalServerError, "InternalServerError", "backup encryption failed")
	}

	nonce := make([]byte, gcm.NonceSize())
	_, _ = rand.Read(nonce)

	out := append([]byte(backupMagic), nonce...)

	return gcm.Seal(out, nonce, plain, []byte(backupMagic+kind)), nil
}

func (b *InMemoryBackend) openBackup(kind string, blob []byte, dst any) *APIError {
	bad := errParam("The backup file is corrupt, was not produced by this emulator, or is not a " + kind + " backup")

	gcm, err := b.backupGCM()
	if err != nil || !bytes.HasPrefix(blob, []byte(backupMagic)) {
		return bad
	}

	rest := blob[len(backupMagic):]
	if len(rest) < gcm.NonceSize() {
		return bad
	}

	plain, err := gcm.Open(nil, rest[:gcm.NonceSize()], rest[gcm.NonceSize():], []byte(backupMagic+kind))
	if err != nil || json.Unmarshal(plain, dst, jsonx.V1()) != nil {
		return bad
	}

	return nil
}

func (b *InMemoryBackend) abortBackup(kind, id string, restore string) {
	b.mu.Lock("abortBackup")
	defer b.mu.Unlock()

	if kind == kindVault {
		if v, ok := b.vaults[id]; ok {
			v.State, v.transition = restore, transition{}
		}

		return
	}

	if k, ok := b.keys[id]; ok {
		k.State, k.transition = restore, transition{}
	}
}

// BackupVault writes an encrypted copy of the vault metadata to the backup location.
func (b *InMemoryBackend) BackupVault(id, ifMatch, hostPort string, in BackupIn) (vaultWire, string, error) {
	loc, aerr := in.location()
	if aerr != nil {
		return vaultWire{}, "", aerr
	}

	store, aerr := b.store()
	if aerr != nil {
		return vaultWire{}, "", aerr
	}

	b.mu.Lock("BackupVault")

	v, aerr := b.activeVault(id)
	if aerr == nil && v.VaultType != vaultTypePrivate {
		aerr = errParam("Only a virtual private vault can be backed up")
	}

	if aerr == nil {
		aerr = checkEtag(v.Etag, ifMatch)
	}

	if aerr != nil {
		b.mu.Unlock()

		return vaultWire{}, "", aerr
	}

	cp := *v
	cp.transition, cp.Etag = transition{}, ""

	blob, aerr := b.sealBackup(kindVault, vaultBackup{Vault: cp})
	if aerr != nil {
		b.mu.Unlock()

		return vaultWire{}, "", aerr
	}

	b.begin(&v.State, &v.transition, stateBackupInProgress, stateActive)
	view, etag := vaultView(v, hostPort), v.Etag
	b.mu.Unlock()

	if aerr = writeBlob(store, loc, blob); aerr != nil {
		b.abortBackup(kindVault, id, stateActive)

		return vaultWire{}, "", aerr
	}

	return view, etag, nil
}

// BackupKey writes an encrypted copy of the key, all its versions and its vault metadata.
func (b *InMemoryBackend) BackupKey(vaultID, keyID, ifMatch string, in BackupIn) (keyWire, string, error) {
	loc, aerr := in.location()
	if aerr != nil {
		return keyWire{}, "", aerr
	}

	store, aerr := b.store()
	if aerr != nil {
		return keyWire{}, "", aerr
	}

	b.mu.Lock("BackupKey")

	k, v, aerr := b.writableKey(vaultID, keyID, ifMatch)
	if aerr == nil && k.State != stateEnabled && k.State != stateDisabled {
		aerr = errState("Key " + keyID + " is in state " + k.State + "; a backup requires ENABLED or DISABLED")
	}

	if aerr == nil && k.ProtectionMode != modeHSM {
		aerr = errParam("Only an HSM-protected key can be backed up")
	}

	if aerr != nil {
		b.mu.Unlock()

		return keyWire{}, "", aerr
	}

	kc, vc := *k, *v
	kc.Versions = slices.DeleteFunc(slices.Clone(k.Versions), func(kv *KeyVersion) bool {
		return kv.State == statePendingDeletion || kv.State == stateSchedulingDelete || kv.State == stateDeleted
	})
	kc.transition, kc.Etag, vc.transition, vc.Etag = transition{}, "", transition{}, ""
	prev := k.State

	blob, aerr := b.sealBackup(kindKey, keyBackup{Key: kc, Vault: vc})
	if aerr != nil {
		b.mu.Unlock()

		return keyWire{}, "", aerr
	}

	b.begin(&k.State, &k.transition, stateBackupInProgress, prev)
	view, etag := keyView(k), k.Etag
	b.mu.Unlock()

	if aerr = writeBlob(store, loc, blob); aerr != nil {
		b.abortBackup(kindKey, keyID, prev)

		return keyWire{}, "", aerr
	}

	return view, etag, nil
}

// RestoreVault recreates a vault from a backup file; the vault keeps its OCID and lands in compartment.
func (b *InMemoryBackend) RestoreVault(compartment, hostPort string, blob []byte) (vaultWire, string, error) {
	if compartment == "" {
		return vaultWire{}, "", errMissing("compartmentId is required")
	}

	var bk vaultBackup
	if aerr := b.openBackup(kindVault, blob, &bk); aerr != nil {
		return vaultWire{}, "", aerr
	}

	b.mu.Lock("RestoreVault")
	defer b.mu.Unlock()

	v := bk.Vault
	if cur, ok := b.vaults[v.ID]; ok && cur.State != stateDeleted {
		return vaultWire{}, "", errExists("Vault " + v.ID + " already exists")
	}

	v.CompartmentID, v.RestoredFrom, v.TimeOfDeletion = compartment, v.ID, nil
	v.Etag, v.Seq = newEtag(), b.nextSeq()
	b.begin(&v.State, &v.transition, stateRestoring, stateActive)
	b.vaults[v.ID] = &v

	return vaultView(&v, hostPort), v.Etag, nil
}

// RestoreKey recreates a key and its versions in its original vault from a backup file.
func (b *InMemoryBackend) RestoreKey(vaultScope string, blob []byte) (keyWire, string, error) {
	var bk keyBackup
	if aerr := b.openBackup(kindKey, blob, &bk); aerr != nil {
		return keyWire{}, "", aerr
	}

	b.mu.Lock("RestoreKey")
	defer b.mu.Unlock()

	k := bk.Key

	if vaultScope != "" && vaultScope != k.VaultID {
		return keyWire{}, "", errParam("The backup belongs to a different vault than the addressed endpoint")
	}

	if _, aerr := b.activeVault(k.VaultID); aerr != nil {
		return keyWire{}, "", aerr
	}

	if cur, ok := b.keys[k.ID]; ok && cur.State != stateDeleted {
		return keyWire{}, "", errExists("Key " + k.ID + " already exists")
	}

	final := cmpOr(k.State, stateEnabled)
	k.RestoredFrom, k.TimeOfDeletion, k.PrevState = k.ID, nil, ""
	k.Etag, k.Seq = newEtag(), b.nextSeq()
	b.begin(&k.State, &k.transition, stateRestoring, final)
	b.keys[k.ID] = &k
	b.stampKey(&k)

	return keyView(&k), k.Etag, nil
}
