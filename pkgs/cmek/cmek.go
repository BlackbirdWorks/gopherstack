// Package cmek holds the seam GCP services use to validate a customer-managed encryption key against Cloud KMS.
package cmek

import (
	"context"
	"errors"
	"strings"
)

// The Err* values are returned by a Checker; callers map them to their service's error.
var (
	ErrNotFound = errors.New("crypto key not found")
	ErrDisabled = errors.New("crypto key has no enabled primary version")
	ErrPurpose  = errors.New("crypto key purpose is not ENCRYPT_DECRYPT")
	ErrLocation = errors.New("crypto key location does not match")
	ErrState    = errors.New("crypto key is not in a usable state")
)

// Checker validates a Cloud KMS crypto key name; a non-empty location must equal the key's location.
type Checker interface {
	CheckKey(keyName, location string) error
}

// Location returns the location segment of projects/*/locations/*/keyRings/*/cryptoKeys/*.
func Location(keyName string) string {
	parts := strings.Split(keyName, "/")
	if len(parts) < 4 || parts[2] != "locations" {
		return ""
	}

	return parts[3]
}

// Check runs c when it is set; an unset checker accepts every key.
func Check(c Checker, keyName, location string) error {
	if c == nil || keyName == "" {
		return nil
	}

	return c.CheckKey(keyName, location)
}

// Versioner is implemented by checkers that can name a key's primary version.
type Versioner interface {
	PrimaryVersion(keyName string) string
}

// VersionName returns the primary version of keyName when c can name it, else keyName itself.
func VersionName(c Checker, keyName string) string {
	if v, ok := c.(Versioner); ok {
		if name := v.PrimaryVersion(keyName); name != "" {
			return name
		}
	}

	return keyName
}

// KeyChecker validates an AWS KMS key reference (key ID, ARN or alias); it returns ErrNotFound, ErrDisabled,
// ErrState or ErrPurpose when the key cannot encrypt.
type KeyChecker interface {
	CheckKey(ctx context.Context, keyRef string) error
}

// KeyResolver is implemented by KeyCheckers that can turn a key reference (ID, alias or ARN) into its ARN.
type KeyResolver interface {
	ResolveKeyARN(ctx context.Context, keyRef string) (string, error)
}
