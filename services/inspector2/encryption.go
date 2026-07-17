package inspector2

import "fmt"

// GetEncryptionKey returns encryption key info for the given resource type and scan type.
func (b *InMemoryBackend) GetEncryptionKey(resourceType, scanType string) (*EncryptionKey, error) {
	b.mu.RLock("GetEncryptionKey")
	defer b.mu.RUnlock()

	key := resourceType + "/" + scanType
	if k, ok := b.encryptionKeys.Get(key); ok {
		cp := *k

		return &cp, nil
	}

	// Return default AWS-managed key info.
	return &EncryptionKey{
		KmsKeyID:     "AWS_OWNED_KEY",
		ResourceType: resourceType,
		ScanType:     scanType,
	}, nil
}

// ResetEncryptionKey resets the encryption key to the AWS-managed default.
func (b *InMemoryBackend) ResetEncryptionKey(resourceType, scanType string) error {
	b.mu.Lock("ResetEncryptionKey")
	defer b.mu.Unlock()

	key := resourceType + "/" + scanType
	b.encryptionKeys.Delete(key)

	return nil
}

// UpdateEncryptionKey sets a customer-managed KMS key for the given resource and scan type.
func (b *InMemoryBackend) UpdateEncryptionKey(kmsKeyID, resourceType, scanType string) error {
	b.mu.Lock("UpdateEncryptionKey")
	defer b.mu.Unlock()

	if kmsKeyID == "" || resourceType == "" || scanType == "" {
		return fmt.Errorf("%w: kmsKeyId, resourceType, and scanType are required", ErrValidation)
	}

	b.encryptionKeys.Put(&EncryptionKey{
		KmsKeyID:     kmsKeyID,
		ResourceType: resourceType,
		ScanType:     scanType,
	})

	return nil
}
