package azurekeyvault

import (
	"encoding/json"
	"net/http"
)

const backupPrefix = "gopherstack-keyvault-backup:"

type backupBlob struct {
	Secret *Entity[*SecretVersion] `json:"secret,omitempty"`
	Key    *Entity[*KeyVersion]    `json:"key,omitempty"`
	Cert   *Entity[*CertVersion]   `json:"cert,omitempty"`
	Kind   string                  `json:"kind"`
}

// BackupResult is the backup response; Value is an opaque blob.
type BackupResult struct {
	Value string `json:"value"`
}

// RestoreRequest is the restore request body.
type RestoreRequest struct {
	Value string `json:"value"`
}

func encodeBackup(bl *backupBlob) (BackupResult, error) {
	raw, err := json.Marshal(bl)
	if err != nil {
		return BackupResult{}, err
	}

	return BackupResult{Value: b64u(append([]byte(backupPrefix), raw...))}, nil
}

func decodeBackup(value, kind string) (*backupBlob, error) {
	raw, err := unb64u(value)
	if err != nil || len(raw) < len(backupPrefix) || string(raw[:len(backupPrefix)]) != backupPrefix {
		return nil, errBadParameter("The backup blob is not valid.")
	}

	var bl backupBlob
	if err = json.Unmarshal(raw[len(backupPrefix):], &bl); err != nil || bl.Kind != kind {
		return nil, errBadParameter("The backup blob is not a %s backup.", lowerKind(kind))
	}

	return &bl, nil
}

func conflictExists(kind, name string) *kvError {
	return newErr(http.StatusConflict, "Conflict", "%s %s already exists in this vault.", kind, name)
}

// BackupSecret serialises every version of a secret.
func (b *InMemoryBackend) BackupSecret(vaultName, name string) (BackupResult, error) {
	var out BackupResult

	err := b.do(vaultName, func(v *vault, _ int64) error {
		e, ok := v.Secrets.active(name)
		if !ok {
			return errNotFound(kindSecret, name)
		}

		var err error

		out, err = encodeBackup(&backupBlob{Kind: kindSecret, Secret: e})

		return err
	})

	return out, err
}

// RestoreSecret recreates a secret from a backup blob.
func (b *InMemoryBackend) RestoreSecret(vaultName, base, value string) (SecretWire, error) {
	var out SecretWire

	err := b.do(vaultName, func(v *vault, _ int64) error {
		bl, err := decodeBackup(value, kindSecret)
		if err != nil {
			return err
		}

		if bl.Secret == nil || len(bl.Secret.Versions) == 0 {
			return errBadParameter("The backup blob is empty.")
		}

		if _, exists := v.Secrets[bl.Secret.Name]; exists {
			return conflictExists(kindSecret, bl.Secret.Name)
		}

		bl.Secret.Deleted = nil
		v.Secrets[bl.Secret.Name] = bl.Secret
		out = bl.Secret.latest().wire(base, bl.Secret.Name, false, nil, true)

		return nil
	})

	return out, err
}

// BackupKey serialises every version of a key.
func (b *InMemoryBackend) BackupKey(vaultName, name string) (BackupResult, error) {
	var out BackupResult

	err := b.do(vaultName, func(v *vault, _ int64) error {
		e, ok := v.Keys.active(name)
		if !ok {
			return errNotFound(kindKey, name)
		}

		var err error

		out, err = encodeBackup(&backupBlob{Kind: kindKey, Key: e})

		return err
	})

	return out, err
}

// RestoreKey recreates a key from a backup blob.
func (b *InMemoryBackend) RestoreKey(vaultName, base, value string) (KeyWire, error) {
	var out KeyWire

	err := b.do(vaultName, func(v *vault, now int64) error {
		bl, err := decodeBackup(value, kindKey)
		if err != nil {
			return err
		}

		if bl.Key == nil || len(bl.Key.Versions) == 0 {
			return errBadParameter("The backup blob is empty.")
		}

		if _, exists := v.Keys[bl.Key.Name]; exists {
			return conflictExists(kindKey, bl.Key.Name)
		}

		bl.Key.Deleted = nil
		if bl.Key.Rotation == nil {
			bl.Key.Rotation = defaultRotation(now)
		}

		v.Keys[bl.Key.Name] = bl.Key
		out = bl.Key.latest().wire(base, bl.Key.Name, true, nil)

		return nil
	})

	return out, err
}

// BackupCertificate serialises a certificate with its backing key and secret.
func (b *InMemoryBackend) BackupCertificate(vaultName, name string) (BackupResult, error) {
	var out BackupResult

	err := b.do(vaultName, func(v *vault, _ int64) error {
		e, ok := v.Certs.active(name)
		if !ok {
			return errNotFound(kindCertificate, name)
		}

		var err error

		out, err = encodeBackup(&backupBlob{Kind: kindCertificate, Cert: e, Key: v.Keys[name], Secret: v.Secrets[name]})

		return err
	})

	return out, err
}

// RestoreCertificate recreates a certificate from a backup blob.
func (b *InMemoryBackend) RestoreCertificate(vaultName, base, value string) (CertWire, error) {
	var out CertWire

	err := b.do(vaultName, func(v *vault, _ int64) error {
		bl, err := decodeBackup(value, kindCertificate)
		if err != nil {
			return err
		}

		if bl.Cert == nil || len(bl.Cert.Versions) == 0 || bl.Key == nil || bl.Secret == nil {
			return errBadParameter("The backup blob is empty.")
		}

		n := bl.Cert.Name
		if _, c := v.Certs[n]; c {
			return conflictExists(kindCertificate, n)
		}

		if _, k := v.Keys[n]; k {
			return conflictExists(kindKey, n)
		}

		if _, s := v.Secrets[n]; s {
			return conflictExists(kindSecret, n)
		}

		bl.Cert.Deleted, bl.Key.Deleted, bl.Secret.Deleted = nil, nil, nil
		v.Certs[n], v.Keys[n], v.Secrets[n] = bl.Cert, bl.Key, bl.Secret
		out = bl.Cert.latest().wire(base, n, bl.Cert, true)

		return nil
	})

	return out, err
}
