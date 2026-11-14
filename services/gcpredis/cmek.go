package gcpredis

import (
	"errors"
	"regexp"

	"cloud.google.com/go/redis/apiv1/redispb"

	"github.com/blackbirdworks/gopherstack/pkgs/cmek"
)

var customerKeyRe = regexp.MustCompile(`^projects/[^/]+/locations/[^/]+/keyRings/[^/]+/cryptoKeys/[^/]+$`)

// SetKeyChecker validates customer_managed_key against Cloud KMS; nil disables validation.
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

// checkCreateKey requires the key to exist in the instance's region with an enabled primary version; a
// global or multi-region key does not match any region.
func (b *InMemoryBackend) checkCreateKey(key, region string) error {
	c := b.keyChecker()
	if key == "" || c == nil {
		return nil
	}

	if !customerKeyRe.MatchString(key) {
		return errInvalid("customer_managed_key %q must be projects/*/locations/*/keyRings/*/cryptoKeys/*.", key)
	}

	err := cmek.Check(c, key, region)

	switch {
	case err == nil:
		return nil
	case errors.Is(err, cmek.ErrNotFound):
		return errInvalid("The Cloud KMS key %q was not found.", key)
	case errors.Is(err, cmek.ErrLocation):
		return errInvalid("The Cloud KMS key must be in the same region as the instance (%s).", region)
	case errors.Is(err, cmek.ErrPurpose):
		return errInvalid("The Cloud KMS key %q must be a symmetric ENCRYPT_DECRYPT key.", key)
	case errors.Is(err, cmek.ErrDisabled):
		return errPrecondition("The Cloud KMS key %q is not available; the instance was not created.", key)
	}

	return err
}

// suspendedLocked reports whether the instance's key is unusable, which suspends it until the key returns.
func (b *InMemoryBackend) suspendedLocked(pb *redispb.Instance) bool {
	key := pb.GetCustomerManagedKey()
	if key == "" || b.keys == nil {
		return false
	}

	return cmek.Check(b.keys, key, "") != nil
}

// viewLocked clones an instance; one with an unusable key shows no state and its suspension reason.
func (b *InMemoryBackend) viewLocked(pb *redispb.Instance) *redispb.Instance {
	out := cloneInstance(pb)
	if b.suspendedLocked(pb) {
		out.State = redispb.Instance_STATE_UNSPECIFIED
		out.SuspensionReasons = []redispb.Instance_SuspensionReason{redispb.Instance_CUSTOMER_MANAGED_KEY_ISSUE}
	}

	return out
}
