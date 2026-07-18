package apigateway

import (
	"fmt"
	"sort"
	"time"
)

// CreateAPIKey creates a new API key with an optional auto-generated value.
func (b *InMemoryBackend) CreateAPIKey(input CreateAPIKeyInput) (*APIKey, error) {
	if input.Name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateAPIKey")
	defer b.mu.Unlock()

	for _, k := range b.apiKeys.All() {
		if k.Name == input.Name {
			return nil, fmt.Errorf("%w: API key with name %q already exists", ErrAlreadyExists, input.Name)
		}
	}

	now := unixEpochTime{time.Now()}
	id := randomID(apiIDLength)

	backendTags := initTagsFromInput("apigw.apikey."+id+".tags", input.Tags)

	value := input.Value
	if value == "" {
		// AWS generates a 40-character alphanumeric key value when none is provided.
		value = randomID(apiKeyValueLength)
	}

	key := &APIKey{
		ID:              id,
		Name:            input.Name,
		Description:     input.Description,
		Value:           value,
		CustomerID:      input.CustomerID,
		Enabled:         input.Enabled,
		Tags:            backendTags,
		CreatedDate:     now,
		LastUpdatedDate: now,
	}
	b.apiKeys.Put(key)
	b.apiKeysByValue[value] = id

	cp := *key

	return &cp, nil
}

// GetAPIKey retrieves an API key by ID.
func (b *InMemoryBackend) GetAPIKey(id string) (*APIKey, error) {
	b.mu.RLock("GetAPIKey")
	defer b.mu.RUnlock()
	key, ok := b.apiKeys.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: API key %s not found", ErrAPIKeyNotFound, id)
	}
	cp := *key

	return &cp, nil
}

// GetAPIKeyByValue retrieves an API key by its value (the secret string sent in
// x-api-key). It resolves the key in O(1) via the value→ID index instead of a
// linear scan, because it runs on the hot data-plane path for every apiKey-required
// request.
func (b *InMemoryBackend) GetAPIKeyByValue(value string) (*APIKey, error) {
	b.mu.RLock("GetAPIKeyByValue")
	defer b.mu.RUnlock()
	id, ok := b.apiKeysByValue[value]
	if !ok {
		return nil, fmt.Errorf("%w: API key with value not found", ErrAPIKeyNotFound)
	}
	k, ok := b.apiKeys.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: API key with value not found", ErrAPIKeyNotFound)
	}
	cp := *k

	return &cp, nil
}

// GetAPIKeys returns all API keys sorted by ID.
func (b *InMemoryBackend) GetAPIKeys() ([]APIKey, error) {
	b.mu.RLock("GetAPIKeys")
	defer b.mu.RUnlock()
	all := make([]APIKey, 0, b.apiKeys.Len())
	for _, k := range b.apiKeys.All() {
		all = append(all, *k)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	return all, nil
}

// DeleteAPIKey removes an API key by ID.
func (b *InMemoryBackend) DeleteAPIKey(id string) error {
	b.mu.Lock("DeleteAPIKey")
	defer b.mu.Unlock()
	key, ok := b.apiKeys.Get(id)
	if !ok {
		return fmt.Errorf("%w: API key %s not found", ErrAPIKeyNotFound, id)
	}
	delete(b.apiKeysByValue, key.Value)
	b.apiKeys.Delete(id)

	return nil
}

// UpdateAPIKey updates mutable fields on an existing API key.
func (b *InMemoryBackend) UpdateAPIKey(id string, input UpdateAPIKeyInput) (*APIKey, error) {
	b.mu.Lock("UpdateAPIKey")
	defer b.mu.Unlock()
	key, ok := b.apiKeys.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: API key %s not found", ErrAPIKeyNotFound, id)
	}
	if input.Name != "" {
		key.Name = input.Name
	}
	if input.Description != "" {
		key.Description = input.Description
	}
	if input.CustomerID != "" {
		key.CustomerID = input.CustomerID
	}
	if input.Enabled != nil {
		key.Enabled = *input.Enabled
	}
	key.LastUpdatedDate = unixEpochTime{time.Now()}
	cp := *key

	return &cp, nil
}

// GetAPIKeysPage returns API keys with cursor-based pagination.
func (b *InMemoryBackend) GetAPIKeysPage(limit int, position string) ([]APIKey, string, error) {
	b.mu.RLock("GetAPIKeysPage")
	defer b.mu.RUnlock()

	all := make([]APIKey, 0, b.apiKeys.Len())
	for _, k := range b.apiKeys.All() {
		all = append(all, *k)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })
	page, pos := paginatePageByKey(all, limit, position, func(k APIKey) string { return k.ID })

	return page, pos, nil
}
