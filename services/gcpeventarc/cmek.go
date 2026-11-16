package gcpeventarc

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

// keyLocation is the Cloud KMS location that protects a resource in an Eventarc location: eu maps to europe.
func keyLocation(location string) string {
	if location == "eu" {
		return "europe"
	}

	return location
}

// checkKeyUnlocked checks a key at delivery time, without the backend lock held.
func (b *InMemoryBackend) checkKeyUnlocked(key, location string) error {
	if key == "" {
		return nil
	}

	b.mu.RLock("checkKeyUnlocked")
	keys := b.keys
	b.mu.RUnlock()

	return keyError(keys, key, location)
}

// checkKey must be called with the backend lock held.
func (b *InMemoryBackend) checkKey(key, location string) error {
	return keyError(b.keys, key, location)
}

func keyError(keys cmek.Checker, key, location string) error {
	err := cmek.Check(keys, key, keyLocation(location))

	switch {
	case err == nil:
		return nil
	case errors.Is(err, cmek.ErrNotFound):
		return errInvalid("The Cloud KMS key %q was not found.", key)
	case errors.Is(err, cmek.ErrLocation):
		return errInvalid("The region for the Cloud KMS key and the protected Eventarc resource must be the same.")
	case errors.Is(err, cmek.ErrPurpose):
		return errInvalid("The Cloud KMS key %q must be a symmetric ENCRYPT_DECRYPT key.", key)
	case errors.Is(err, cmek.ErrDisabled):
		return errPrecondition("The Cloud KMS key %q has no enabled primary version.", key)
	}

	return err
}
