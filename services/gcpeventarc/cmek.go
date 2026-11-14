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

// checkKey must be called with the backend lock held.
func (b *InMemoryBackend) checkKey(key, location string) error {
	err := cmek.Check(b.keys, key, keyLocation(location))

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
