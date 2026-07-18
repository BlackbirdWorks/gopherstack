package glacier

import (
	"fmt"
	"maps"
	"strings"
)

const (
	// maxVaultTags is the maximum number of tags allowed on a single vault.
	maxVaultTags = 10
	// maxTagKeyLen is the maximum byte length of a tag key.
	maxTagKeyLen = 128
	// maxTagValueLen is the maximum byte length of a tag value.
	maxTagValueLen = 256
)

// isValidTagChar reports whether b is an allowed tag character.
// AWS allows Unicode letters (L), spaces (Z), numbers (N), and _.:/=+\-@.
// For the ASCII subset used in practice this check is sufficient.
func isValidTagChar(b byte) bool {
	if b >= 'a' && b <= 'z' {
		return true
	}

	if b >= 'A' && b <= 'Z' {
		return true
	}

	if b >= '0' && b <= '9' {
		return true
	}

	switch b {
	case ' ', '_', '.', ':', '/', '=', '+', '-', '@':
		return true
	}

	return false
}

// validateTagKey returns an error if the tag key is invalid.
func validateTagKey(k string) error {
	if len(k) == 0 || len(k) > maxTagKeyLen {
		return fmt.Errorf("%w: tag key length must be 1-%d", ErrInvalidTag, maxTagKeyLen)
	}

	if strings.HasPrefix(k, "aws:") {
		return fmt.Errorf("%w: tag key prefix \"aws:\" is reserved", ErrInvalidTag)
	}

	for i := range len(k) {
		if !isValidTagChar(k[i]) {
			return fmt.Errorf("%w: tag key contains invalid character 0x%02x", ErrInvalidTag, k[i])
		}
	}

	return nil
}

// validateTagValue returns an error if the tag value is invalid.
func validateTagValue(v string) error {
	if len(v) > maxTagValueLen {
		return fmt.Errorf("%w: tag value exceeds %d characters", ErrInvalidTag, maxTagValueLen)
	}

	for i := range len(v) {
		if !isValidTagChar(v[i]) {
			return fmt.Errorf("%w: tag value contains invalid character 0x%02x", ErrInvalidTag, v[i])
		}
	}

	return nil
}

// AddTagsToVault adds or updates tags on a vault.
func (b *InMemoryBackend) AddTagsToVault(accountID, region, vaultName string, tags map[string]string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	v, ok := b.vaults.Get(vaultARN(accountID, region, vaultName))
	if !ok {
		return ErrVaultNotFound
	}

	// Validate individual key/value chars, length, and reserved prefix before applying.
	for k, val := range tags {
		if err := validateTagKey(k); err != nil {
			return err
		}

		if err := validateTagValue(val); err != nil {
			return err
		}
	}

	if v.Tags == nil {
		v.Tags = make(map[string]string)
	}

	// Check that adding these tags would not exceed the per-vault limit.
	merged := len(v.Tags)

	for k := range tags {
		if _, exists := v.Tags[k]; !exists {
			merged++
		}
	}

	if merged > maxVaultTags {
		return ErrTooManyTags
	}

	maps.Copy(v.Tags, tags)

	return nil
}

// ListTagsForVault returns all tags for a vault.
func (b *InMemoryBackend) ListTagsForVault(accountID, region, vaultName string) (map[string]string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	v, ok := b.vaults.Get(vaultARN(accountID, region, vaultName))
	if !ok {
		return nil, ErrVaultNotFound
	}

	result := make(map[string]string, len(v.Tags))

	maps.Copy(result, v.Tags)

	return result, nil
}

// RemoveTagsFromVault removes tags from a vault.
func (b *InMemoryBackend) RemoveTagsFromVault(accountID, region, vaultName string, tagKeys []string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	v, ok := b.vaults.Get(vaultARN(accountID, region, vaultName))
	if !ok {
		return ErrVaultNotFound
	}

	for _, k := range tagKeys {
		delete(v.Tags, k)
	}

	return nil
}
