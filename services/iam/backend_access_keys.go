package iam

import (
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// CreateAccessKey creates a new access key for an IAM user.
func (b *InMemoryBackend) CreateAccessKey(userName string) (*AccessKey, error) {
	b.mu.Lock("CreateAccessKey")
	defer b.mu.Unlock()

	if _, exists := b.users.Get(userName); !exists {
		return nil, fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	// AWS allows at most 2 access keys per user.
	const maxAccessKeysPerUser = 2
	existingKeys := b.userAccessKeys[userName]
	existingCount := len(existingKeys)

	if existingCount >= maxAccessKeysPerUser {
		return nil, fmt.Errorf(
			"%w: user %q already has %d access keys (maximum %d)",
			ErrLimitExceeded, userName, existingCount, maxAccessKeysPerUser,
		)
	}

	secret, err := newSecretAccessKey()
	if err != nil {
		return nil, fmt.Errorf("creating access key: %w", err)
	}

	ak := AccessKey{
		AccessKeyID:     newAccessKeyID(),
		SecretAccessKey: secret,
		UserName:        userName,
		Status:          "Active",
		CreateDate:      time.Now().UTC(),
	}
	b.accessKeys.Put(&ak)
	b.userAccessKeys[userName] = append(b.userAccessKeys[userName], ak.AccessKeyID)

	return &ak, nil
}

// DeleteAccessKey deletes an access key by ID.
func (b *InMemoryBackend) DeleteAccessKey(userName, accessKeyID string) error {
	b.mu.Lock("DeleteAccessKey")
	defer b.mu.Unlock()

	ak, exists := b.accessKeys.Get(accessKeyID)
	if !exists || ak.UserName != userName {
		return fmt.Errorf(
			"%w: access key %q not found for user %q",
			ErrAccessKeyNotFound,
			accessKeyID,
			userName,
		)
	}

	b.accessKeys.Delete(accessKeyID)

	keys := b.userAccessKeys[userName]
	for i, id := range keys {
		if id == accessKeyID {
			b.userAccessKeys[userName] = append(keys[:i], keys[i+1:]...)

			break
		}
	}

	return nil
}

// ListAccessKeys returns a paginated list of access keys for an IAM user.
func (b *InMemoryBackend) ListAccessKeys(
	userName, marker string,
	maxItems int,
) (page.Page[AccessKey], error) {
	b.mu.RLock("ListAccessKeys")
	defer b.mu.RUnlock()

	if _, exists := b.users.Get(userName); !exists {
		return page.Page[AccessKey]{}, fmt.Errorf(
			"%w: user %q not found",
			ErrUserNotFound,
			userName,
		)
	}

	userKeys := b.userAccessKeys[userName]
	keys := make([]AccessKey, 0, len(userKeys))
	for _, id := range userKeys {
		if ak, ok := b.accessKeys.Get(id); ok {
			keys = append(keys, *ak)
		}
	}

	sort.Slice(keys, func(i, j int) bool { return keys[i].AccessKeyID < keys[j].AccessKeyID })

	return page.New(keys, marker, maxItems, iamDefaultMaxItems), nil
}

// ListAllAccessKeys returns all access keys (for dashboard).
func (b *InMemoryBackend) ListAllAccessKeys() []AccessKey {
	b.mu.RLock("ListAllAccessKeys")
	defer b.mu.RUnlock()

	keys := make([]AccessKey, 0, b.accessKeys.Len())
	for _, ak := range b.accessKeys.All() {
		keys = append(keys, *ak)
	}

	sort.Slice(keys, func(i, j int) bool { return keys[i].AccessKeyID < keys[j].AccessKeyID })

	return keys
}

// purgeAccessKeysLocked removes access keys created before cutoff.
// Caller must hold b.mu.
func (b *InMemoryBackend) purgeAccessKeysLocked(cutoff time.Time) {
	for _, ak := range b.accessKeys.All() {
		if ak.CreateDate.Before(cutoff) {
			b.accessKeys.Delete(ak.AccessKeyID)
		}
	}
}

// UpdateAccessKey updates the status of an access key (Active or Inactive).
func (b *InMemoryBackend) UpdateAccessKey(userName, accessKeyID, status string) error {
	if status != accessKeyStatusActive && status != "Inactive" {
		return fmt.Errorf("%w: status must be Active or Inactive", ErrInvalidAction)
	}

	b.mu.Lock("UpdateAccessKey")
	defer b.mu.Unlock()

	ak, exists := b.accessKeys.Get(accessKeyID)
	if !exists || ak.UserName != userName {
		return fmt.Errorf("%w: access key %q not found for user %q", ErrAccessKeyNotFound, accessKeyID, userName)
	}

	ak.Status = status
	b.accessKeys.Put(ak)

	return nil
}

// GetAccessKeyLastUsed returns last-use information for an access key.
// Returns real data if the key has been used (via RecordAccessKeyUsage); otherwise returns N/A.
func (b *InMemoryBackend) GetAccessKeyLastUsed(accessKeyID string) (*AccessKeyLastUsed, error) {
	b.mu.RLock("GetAccessKeyLastUsed")
	defer b.mu.RUnlock()

	ak, exists := b.accessKeys.Get(accessKeyID)
	if !exists {
		return nil, fmt.Errorf("%w: access key %q not found", ErrAccessKeyNotFound, accessKeyID)
	}

	result := &AccessKeyLastUsed{
		UserName:    ak.UserName,
		AccessKeyID: accessKeyID,
	}

	if ak.LastUsedDate != nil {
		result.LastUsedDate = ak.LastUsedDate.UTC().Format("2006-01-02T15:04:05Z")
		result.ServiceName = ak.LastUsedServiceName
		result.Region = ak.LastUsedRegion
	} else {
		result.LastUsedDate = notApplicable
		result.ServiceName = notApplicable
		result.Region = notApplicable
	}

	return result, nil
}

// RecordAccessKeyUsage updates the LastUsedDate, LastUsedRegion, and LastUsedServiceName
// for the given access key. Called from the auth layer on each authenticated request.
func (b *InMemoryBackend) RecordAccessKeyUsage(accessKeyID, region, serviceName string) {
	b.mu.Lock("RecordAccessKeyUsage")
	defer b.mu.Unlock()

	ak, exists := b.accessKeys.Get(accessKeyID)
	if !exists {
		return
	}

	now := time.Now().UTC()
	ak.LastUsedDate = &now
	ak.LastUsedRegion = region
	ak.LastUsedServiceName = serviceName
	b.accessKeys.Put(ak)
}
