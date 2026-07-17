package xray

import (
	"fmt"
	"regexp"
)

// validKMSKeyID checks whether a KMS KeyId is in an acceptable format:
// alias/... | arn:aws:kms:... | UUID (hex with dashes, 36 chars).
//
//nolint:lll // regex is intentionally long; splitting would harm readability
var validKMSKeyID = regexp.MustCompile(
	`^(alias/[a-zA-Z0-9/_-]+|arn:aws:kms:[a-z0-9-]+:\d+:key/[a-zA-Z0-9/_-]+|[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})$`,
)

// GetEncryptionConfig returns the current X-Ray encryption configuration.
// If the current status is UPDATING, this call advances it to ACTIVE.
func (b *InMemoryBackend) GetEncryptionConfig() *EncryptionConfig {
	b.mu.Lock("GetEncryptionConfig")
	defer b.mu.Unlock()

	if b.encryptionConfig.Status == statusUpdating {
		b.encryptionConfig.Status = statusActive
	}

	cp := *b.encryptionConfig

	return &cp
}

// PutEncryptionConfig updates the X-Ray encryption configuration.
// encType must be one of "NONE" or "KMS". keyID is only used when encType is "KMS".
// When encType is KMS the keyID must match alias/..., ARN, or UUID format.
// The status is initially set to UPDATING; the next GET will advance it to ACTIVE.
func (b *InMemoryBackend) PutEncryptionConfig(encType, keyID string) (*EncryptionConfig, error) {
	if encType != encTypeNone && encType != encTypeKMS {
		return nil, fmt.Errorf("%w: Type must be NONE or KMS", ErrValidation)
	}

	if encType == encTypeKMS {
		if keyID == "" {
			return nil, fmt.Errorf("%w: KeyId is required when Type is KMS", ErrValidation)
		}

		if !validKMSKeyID.MatchString(keyID) {
			return nil, fmt.Errorf("%w: KeyId must be alias/..., key ARN, or UUID", ErrValidation)
		}
	}

	b.mu.Lock("PutEncryptionConfig")
	defer b.mu.Unlock()

	status := statusActive

	if encType == encTypeKMS {
		status = statusUpdating
	}

	b.encryptionConfig = &EncryptionConfig{
		Type:   encType,
		KeyID:  keyID,
		Status: status,
	}

	cp := *b.encryptionConfig

	return &cp, nil
}

const (
	// encTypeNone is the X-Ray encryption type for no encryption.
	encTypeNone = "NONE"
)

const (
	// encTypeKMS is the X-Ray encryption type for KMS-managed encryption.
	encTypeKMS = "KMS"
)
