package appsync

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"slices"
	"strings"
	"time"
)

// apiKeyIDChars is the character set used for API key IDs.
const apiKeyIDChars = "abcdefghijklmnopqrstuvwxyz0123456789"

// apiKeyIDPrefix is the prefix used in AppSync API key IDs.
const apiKeyIDPrefix = "da2-"

// defaultAPIKeyExpiryDays is the default expiry in days when not specified.
const defaultAPIKeyExpiryDays = 365

// maxAPIKeysPerAPI is the AWS-enforced limit of API keys per GraphQL API.
// AWS default quota is 50 API keys per GraphQL API.
const maxAPIKeysPerAPI = 50

// randomAPIKeyID generates a random API key ID with the "da2-" prefix,
// matching the format of real AWS AppSync API key IDs.
func randomAPIKeyID() string {
	const length = 13

	b := make([]byte, length)
	charCount := uint64(len(apiKeyIDChars))

	for i := range b {
		var v [8]byte
		_, _ = rand.Read(v[:])
		b[i] = apiKeyIDChars[binary.BigEndian.Uint64(v[:])%charCount]
	}

	return apiKeyIDPrefix + string(b)
}

// CreateAPIKey creates an API key for a GraphQL API.
func (b *InMemoryBackend) CreateAPIKey(apiID, description string, expires int64) (*APIKey, error) {
	b.mu.Lock("CreateAPIKey")
	defer b.mu.Unlock()

	if !b.apis.Has(apiID) {
		return nil, fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	if b.apiKeys[apiID] == nil {
		b.apiKeys[apiID] = make(map[string]*APIKey)
	}

	if len(b.apiKeys[apiID]) >= maxAPIKeysPerAPI {
		return nil, fmt.Errorf(
			"%w: api %s already has the maximum of %d API keys",
			ErrValidation,
			apiID,
			maxAPIKeysPerAPI,
		)
	}

	// Default expiry to 365 days from now if not specified.
	if expires <= 0 {
		expires = time.Now().AddDate(0, 0, defaultAPIKeyExpiryDays).Unix()
	}

	// Cap expiry at 365 days from now (AWS enforces this).
	maxExpires := time.Now().AddDate(0, 0, defaultAPIKeyExpiryDays).Unix()
	if expires > maxExpires {
		expires = maxExpires
	}

	keyID := randomAPIKeyID()

	key := &APIKey{
		ID:          keyID,
		Description: description,
		Expires:     expires,
	}

	b.apiKeys[apiID][keyID] = key

	cp := *key

	return &cp, nil
}

// ListAPIKeys returns all non-expired API keys for a GraphQL API.
func (b *InMemoryBackend) ListAPIKeys(apiID string) ([]*APIKey, error) {
	b.mu.RLock("ListApiKeys")
	defer b.mu.RUnlock()

	if !b.apis.Has(apiID) {
		return nil, fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	now := time.Now().Unix()
	keys := b.apiKeys[apiID]
	out := make([]*APIKey, 0, len(keys))

	for _, k := range keys {
		// Skip expired keys — AWS does not return expired keys in ListApiKeys.
		if k.Expires > 0 && k.Expires <= now {
			continue
		}

		cp := *k
		out = append(out, &cp)
	}

	slices.SortFunc(out, func(a, b *APIKey) int {
		return strings.Compare(a.ID, b.ID)
	})

	return out, nil
}

// DeleteAPIKey deletes an API key from a GraphQL API.
func (b *InMemoryBackend) DeleteAPIKey(apiID, keyID string) error {
	b.mu.Lock("DeleteApiKey")
	defer b.mu.Unlock()

	if !b.apis.Has(apiID) {
		return fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	keys := b.apiKeys[apiID]
	if keys == nil || keys[keyID] == nil {
		return fmt.Errorf("%w: api key %s not found", ErrNotFound, keyID)
	}

	delete(keys, keyID)

	return nil
}

// UpdateAPIKey updates an existing API key's description and/or expiry.
func (b *InMemoryBackend) UpdateAPIKey(apiID, keyID, description string, expires int64) (*APIKey, error) {
	b.mu.Lock("UpdateApiKey")
	defer b.mu.Unlock()

	if !b.apis.Has(apiID) {
		return nil, fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	keys := b.apiKeys[apiID]
	if keys == nil || keys[keyID] == nil {
		return nil, fmt.Errorf("%w: api key %s not found", ErrNotFound, keyID)
	}

	existing := keys[keyID]

	if description != "" {
		existing.Description = description
	}

	if expires > 0 {
		// Cap expiry at 365 days from now (AWS enforces this on update too).
		maxExpires := time.Now().AddDate(0, 0, defaultAPIKeyExpiryDays).Unix()
		if expires > maxExpires {
			expires = maxExpires
		}

		existing.Expires = expires
	}

	cp := *existing

	return &cp, nil
}

// SweepExpiredAPIKeys removes all expired API keys across all GraphQL APIs.
func (b *InMemoryBackend) SweepExpiredAPIKeys() int {
	b.mu.Lock("SweepExpiredAPIKeys")
	defer b.mu.Unlock()

	now := time.Now().Unix()
	totalEvicted := 0

	for apiID, keys := range b.apiKeys {
		for keyID, k := range keys {
			if k.Expires > 0 && k.Expires <= now {
				delete(keys, keyID)
				totalEvicted++
			}
		}
		if len(keys) == 0 {
			delete(b.apiKeys, apiID)
		}
	}

	return totalEvicted
}
