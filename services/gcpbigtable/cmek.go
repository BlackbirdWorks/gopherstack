package gcpbigtable

import (
	"errors"
	"regexp"

	"cloud.google.com/go/bigtable/admin/apiv2/adminpb"

	"github.com/blackbirdworks/gopherstack/pkgs/cmek"
)

var zoneRe = regexp.MustCompile(`^(.+)-[a-z]$`)

// SetKeyChecker validates cluster encryption keys against Cloud KMS; without one, keys are accepted unchecked.
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

// clusterRegion returns the region of a cluster zone location, e.g. us-central1 for .../locations/us-central1-b.
func clusterRegion(c *adminpb.Cluster) string {
	m := locationRe.FindStringSubmatch(c.GetLocation())
	if m == nil {
		return ""
	}

	if z := zoneRe.FindStringSubmatch(m[2]); z != nil {
		return z[1]
	}

	return m[2]
}

// checkClusterKey requires a cluster's kms_key_name to be an enabled symmetric key in the cluster's region
// (the Cluster.EncryptionConfig field docs). The caller must not hold the lock: Cloud KMS is consulted.
func (b *InMemoryBackend) checkClusterKey(c *adminpb.Cluster) error {
	key := c.GetEncryptionConfig().GetKmsKeyName()
	if key == "" {
		return nil
	}

	region := clusterRegion(c)

	err := cmek.Check(b.keyChecker(), key, region)

	switch {
	case err == nil:
		return nil
	case errors.Is(err, cmek.ErrNotFound):
		return errInvalid("Cloud KMS key %s was not found.", key)
	case errors.Is(err, cmek.ErrLocation):
		return errInvalid("Cloud KMS key %s must be a regional key in the cluster's region (%s).", key, region)
	case errors.Is(err, cmek.ErrPurpose):
		return errInvalid("Cloud KMS key %s must be a symmetric ENCRYPT_DECRYPT key.", key)
	case errors.Is(err, cmek.ErrDisabled), errors.Is(err, cmek.ErrState):
		return errPrecondition("Cloud KMS key %s has no enabled primary version.", key)
	default:
		return err
	}
}
