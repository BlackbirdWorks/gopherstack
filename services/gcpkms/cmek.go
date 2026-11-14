package gcpkms

import (
	"strconv"

	"cloud.google.com/go/kms/apiv1/kmspb"

	"github.com/blackbirdworks/gopherstack/pkgs/cmek"
)

// CheckKey implements cmek.Checker: the key must exist, be ENCRYPT_DECRYPT with an enabled primary version
// and, when location is set, live in that location.
func (b *InMemoryBackend) CheckKey(keyName, location string) error {
	b.mu.Lock("CheckKey")
	defer b.mu.Unlock()

	b.sweepLocked()

	k, ok := b.keys[keyName]
	if !ok {
		return cmek.ErrNotFound
	}

	if location != "" && cmek.Location(keyName) != location {
		return cmek.ErrLocation
	}

	if k.pb.GetPurpose() != kmspb.CryptoKey_ENCRYPT_DECRYPT {
		return cmek.ErrPurpose
	}

	if v := k.version(k.primary); v == nil || v.pb.GetState() != kmspb.CryptoKeyVersion_ENABLED {
		return cmek.ErrDisabled
	}

	return nil
}

// PrimaryVersion implements cmek.Versioner; it returns "" for an unknown key.
func (b *InMemoryBackend) PrimaryVersion(keyName string) string {
	b.mu.Lock("PrimaryVersion")
	defer b.mu.Unlock()

	b.sweepLocked()

	k, ok := b.keys[keyName]
	if !ok {
		return ""
	}

	return keyName + "/cryptoKeyVersions/" + strconv.FormatInt(k.primary, 10)
}
