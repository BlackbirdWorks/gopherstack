package gcplogging

import (
	"errors"
	"strings"

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

// checkReadable fails a read when the Cloud KMS key protecting a source bucket, or the Log Router key of an
// organization, is unavailable (docs.cloud.google.com/logging/docs/routing/managed-encryption: queries stop
// functioning; the page names no error code).
func (b *InMemoryBackend) checkReadable(srcs []source) error {
	keys := map[string]string{}

	b.mu.RLock("checkReadable")
	checker := b.keys

	for _, s := range srcs {
		if bd, ok := b.buckets[s.bucket]; ok && bd.pb.GetCmekSettings().GetKmsKeyName() != "" {
			keys[bd.pb.GetCmekSettings().GetKmsKeyName()] = s.bucket
		}

		if p := parentOf(s.bucket); strings.HasPrefix(p, "organizations/") && b.cmek[p].GetKmsKeyName() != "" {
			keys[b.cmek[p].GetKmsKeyName()] = p
		}
	}

	b.mu.RUnlock()

	for key, owner := range keys {
		if err := cmek.Check(checker, key, ""); err != nil {
			return errPrecondition("%s cannot be read: its Cloud KMS key [%s] is unavailable.", owner, key)
		}
	}

	return nil
}
