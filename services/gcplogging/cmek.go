package gcplogging

import (
	"errors"

	"cloud.google.com/go/logging/apiv2/loggingpb"

	"github.com/blackbirdworks/gopherstack/pkgs/cmek"
)

// SetKeyChecker validates CMEK key names against Cloud KMS; nil disables validation.
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

// checkCmekKey maps a key problem to the gRPC error; location is the bucket or storage location, "" for any.
func checkCmekKey(c cmek.Checker, key, location string) error {
	err := cmek.Check(c, key, location)

	switch {
	case err == nil:
		return nil
	case errors.Is(err, cmek.ErrNotFound):
		return errInvalid("Cloud KMS key [%s] was not found.", key)
	case errors.Is(err, cmek.ErrLocation):
		return errInvalid("The location of the Cloud KMS key [%s] must match the location [%s] of the resource.",
			key, location)
	case errors.Is(err, cmek.ErrPurpose):
		return errInvalid("Cloud KMS key [%s] must be a symmetric ENCRYPT_DECRYPT key.", key)
	case errors.Is(err, cmek.ErrDisabled):
		return errPrecondition("Cloud KMS key [%s] has no enabled primary version.", key)
	}

	return err
}

// checkBucketKey requires the bucket's CMEK key to be usable and in the bucket's location; CMEK is not
// available in the global location.
func (b *InMemoryBackend) checkBucketKey(bucket *loggingpb.LogBucket) error {
	key := bucket.GetCmekSettings().GetKmsKeyName()
	if key == "" {
		return nil
	}

	var loc string
	if m := bucketRe.FindStringSubmatch(bucket.GetName()); m != nil {
		loc = m[2]
	}

	if b.keys != nil && loc == "global" {
		return errInvalid("Log buckets in the global location cannot use a customer-managed encryption key.")
	}

	return checkCmekKey(b.keys, key, loc)
}
