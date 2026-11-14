package appintegrations

import (
	"context"
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/cmek"
)

// SetKeyChecker resolves KmsKey against AWS KMS; nil disables the lookup.
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

// checkKmsKey rejects a KmsKey that does not exist or cannot encrypt; every case is an InvalidRequestException,
// the only client error the CreateDataIntegration reference lists for a bad argument.
func (b *InMemoryBackend) checkKmsKey(ctx context.Context, key string) error {
	c := b.keyChecker()
	if c == nil {
		return nil
	}

	err := c.CheckKey(ctx, key)

	switch {
	case err == nil:
		return nil
	case errors.Is(err, cmek.ErrNotFound):
		return invalidErr("KmsKey %q does not exist", key)
	case errors.Is(err, cmek.ErrDisabled):
		return invalidErr("KmsKey %q is disabled", key)
	case errors.Is(err, cmek.ErrState):
		return invalidErr("KmsKey %q is in a state that cannot be used for encryption", key)
	case errors.Is(err, cmek.ErrPurpose):
		return invalidErr("KmsKey %q must be a symmetric ENCRYPT_DECRYPT key", key)
	}

	return err
}
