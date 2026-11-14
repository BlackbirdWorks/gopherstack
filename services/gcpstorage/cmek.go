package gcpstorage

import (
	"errors"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/cmek"
)

const (
	keyKMSKeyName     = "kmsKeyName"
	keyEncryptionConf = "encryption"
)

// SetKMSChecker validates Cloud KMS keys on buckets and objects; nil disables validation.
func (b *InMemoryBackend) SetKMSChecker(c cmek.Checker) {
	b.mu.Lock("SetKMSChecker")
	defer b.mu.Unlock()

	b.kms = c
}

// defaultKMSKey returns the bucket's encryption.defaultKmsKeyName.
func (b *Bucket) defaultKMSKey() string {
	m, ok := configMap(b, keyEncryptionConf)
	if !ok {
		return ""
	}

	s, _ := m["defaultKmsKeyName"].(string)

	return s
}

// checkKMSKey must be called with the backend lock held; the key has to be in the bucket's location.
func (b *InMemoryBackend) checkKMSKey(bk *Bucket, key string) *APIError {
	err := cmek.Check(b.kms, key, strings.ToLower(bk.Location))

	switch {
	case err == nil:
		return nil
	case errors.Is(err, cmek.ErrNotFound):
		return errInvalid("Cloud KMS key " + key + " was not found.")
	case errors.Is(err, cmek.ErrLocation):
		return errInvalid("Cloud KMS key " + key + " must be in the same location as the bucket (" + bk.Location + ").")
	case errors.Is(err, cmek.ErrPurpose):
		return errInvalid("Cloud KMS key " + key + " must be a symmetric ENCRYPT_DECRYPT key.")
	case errors.Is(err, cmek.ErrDisabled):
		return errInvalid("Cloud KMS key " + key + " has no enabled primary version.")
	}

	return errInvalid("Cloud KMS key " + key + " is unusable: " + err.Error())
}

// checkBucketKMS validates the default key of a new or changed bucket configuration.
func (b *InMemoryBackend) checkBucketKMS(next, prev *Bucket) *APIError {
	key := next.defaultKMSKey()
	if key == "" || (prev != nil && prev.defaultKMSKey() == key) {
		return nil
	}

	return b.checkKMSKey(next, key)
}

// applyObjectKMS records the key encrypting a new object: the request's, else the bucket default.
func (b *InMemoryBackend) applyObjectKMS(bk *Bucket, o *Object, p Patch) *APIError {
	key := p.str(keyKMSKeyName)
	if key == "" && o.CustomerKeySHA256 == "" {
		key = bk.defaultKMSKey()
	}

	if key == "" {
		return nil
	}

	if apiErr := b.checkKMSKey(bk, key); apiErr != nil {
		return apiErr
	}

	o.KMSKeyName = cmek.VersionName(b.kms, key)

	return nil
}
