package codegurureviewer

import (
	"context"
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/cmek"
)

// SetKeyChecker resolves KMSKeyDetails.KMSKeyId against AWS KMS; nil disables the lookup.
func (b *InMemoryBackend) SetKeyChecker(c cmek.KeyChecker) {
	b.mu.Lock("SetKeyChecker")
	defer b.mu.Unlock()

	b.keys = c
}

func (b *InMemoryBackend) keyChecker() cmek.KeyChecker {
	b.mu.RLock("keyChecker")
	defer b.mu.RUnlock()

	return b.keys
}

// checkCustomerKey validates a CUSTOMER_MANAGED_CMK key; a bad one is a ValidationException.
func (b *InMemoryBackend) checkCustomerKey(ctx context.Context, k *KMSKeyDetails) error {
	c := b.keyChecker()
	if c == nil || k == nil || k.EncryptionOption != encryptionCMK {
		return nil
	}

	err := c.CheckKey(ctx, k.KMSKeyID)

	switch {
	case err == nil:
		return nil
	case errors.Is(err, cmek.ErrNotFound):
		return validationErr("KMSKeyId %q does not exist", k.KMSKeyID)
	case errors.Is(err, cmek.ErrDisabled):
		return validationErr("KMSKeyId %q is disabled", k.KMSKeyID)
	case errors.Is(err, cmek.ErrState):
		return validationErr("KMSKeyId %q is in a state that cannot be used for encryption", k.KMSKeyID)
	case errors.Is(err, cmek.ErrPurpose):
		return validationErr("KMSKeyId %q must be a symmetric ENCRYPT_DECRYPT key", k.KMSKeyID)
	}

	return err
}
