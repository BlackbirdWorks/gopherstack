package gcpworkflows

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/cmek"
)

// SetKeyChecker validates crypto_key_name against Cloud KMS; nil disables validation.
func (b *InMemoryBackend) SetKeyChecker(c cmek.Checker) {
	b.mu.Lock("SetKeyChecker")
	defer b.mu.Unlock()

	b.keys = c
}

func (b *InMemoryBackend) keyChecker() cmek.Checker {
	b.mu.RLock("keyChecker")
	defer b.mu.RUnlock()

	return b.keys
}

// checkKey maps a key problem to the gRPC error; disabled uses the message documented on the CMEK page.
func (b *InMemoryBackend) checkKey(key, location string) error {
	return keyError(cmek.Check(b.keyChecker(), key, location), key)
}

// cmekProblem checks key with c (the caller holds the backend lock).
func cmekProblem(c cmek.Checker, key string) error {
	return keyError(cmek.Check(c, key, ""), key)
}

func keyError(err error, key string) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, cmek.ErrNotFound):
		return errInvalid("The Cloud KMS key %s was not found.", key)
	case errors.Is(err, cmek.ErrLocation):
		return errInvalid("The Cloud KMS key ring and the protected workflow must be in the same region.")
	case errors.Is(err, cmek.ErrPurpose):
		return errInvalid("The Cloud KMS key %s must be a symmetric ENCRYPT_DECRYPT key.", key)
	case errors.Is(err, cmek.ErrDisabled):
		return errPrecondition("The provided Cloud KMS key version has been disabled.")
	}

	return err
}
