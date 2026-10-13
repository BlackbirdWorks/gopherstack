package ec2

import (
	"fmt"
	"regexp"

	tagspkg "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// validatePinnedEC2ID checks a gopherstack:id value is prefix followed by 8 or 17 lowercase hex digits.
func validatePinnedEC2ID(id, prefix string) error {
	re := regexp.MustCompile("^" + regexp.QuoteMeta(prefix) + "[0-9a-f]{8}(?:[0-9a-f]{9})?$")
	if err := tagspkg.ValidatePinnedID(id, re, prefix+"<8 or 17 lowercase hex digits>"); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidParameter, err)
	}

	return nil
}

func errPinnedIDInUse(id string) error {
	return fmt.Errorf("%w: resource %q already exists", ErrResourceIDInUse, id)
}

func validatePinnedInstanceID(id string, count int) error {
	if id == "" {
		return nil
	}

	if count != 1 {
		return fmt.Errorf("%w: %s can only pin the ID of a single instance", ErrInvalidParameter, tagspkg.PinnedIDKey)
	}

	return validatePinnedEC2ID(id, "i-")
}

func (b *InMemoryBackend) checkPinnedInstanceLocked(id string, count int) error {
	if err := validatePinnedInstanceID(id, count); err != nil || id == "" {
		return err
	}

	if b.instances.Has(id) {
		return errPinnedIDInUse(id)
	}

	return nil
}
