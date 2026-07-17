package appsync

import (
	"fmt"
)

// isValidAPICacheType returns true if the given cache type is a valid AppSync API cache type.
func isValidAPICacheType(t string) bool {
	switch t {
	case "SMALL", "MEDIUM", "LARGE", "XLARGE",
		"LARGE_2X", "LARGE_4X", "LARGE_8X", "LARGE_12X",
		"T2_SMALL", "T2_MEDIUM",
		"R4_1XLARGE", "R4_2XLARGE", "R4_4XLARGE", "R4_8XLARGE":
		return true
	default:
		return false
	}
}

// isValidAPICachingBehavior returns true if the given caching behavior is valid.
func isValidAPICachingBehavior(behavior string) bool {
	switch behavior {
	case "FULL_REQUEST_CACHING", "PER_RESOLVER_CACHING", "FULL_REQUEST_DATA_CACHING":
		return true
	default:
		return false
	}
}

// CreateAPICache creates a cache configuration for a GraphQL API.
func (b *InMemoryBackend) CreateAPICache(apiID string, cache *APICache) (*APICache, error) {
	b.mu.Lock("CreateAPICache")
	defer b.mu.Unlock()

	if !b.apis.Has(apiID) {
		return nil, fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	if cache.TTL <= 0 {
		return nil, fmt.Errorf("%w: ttl must be greater than 0", ErrValidation)
	}

	if cache.Type == "" {
		return nil, fmt.Errorf("%w: type is required", ErrValidation)
	}

	if !isValidAPICacheType(cache.Type) {
		return nil, fmt.Errorf("%w: invalid cache type %q", ErrValidation, cache.Type)
	}

	if cache.APICachingBehavior == "" {
		return nil, fmt.Errorf("%w: apiCachingBehavior is required", ErrValidation)
	}

	if !isValidAPICachingBehavior(cache.APICachingBehavior) {
		return nil, fmt.Errorf("%w: invalid apiCachingBehavior %q", ErrValidation, cache.APICachingBehavior)
	}

	if b.apiCaches.Has(apiID) {
		return nil, fmt.Errorf("%w: api cache already exists for api %s", ErrAlreadyExists, apiID)
	}

	cache.APIID = apiID
	if cache.Status == "" {
		cache.Status = "AVAILABLE"
	}

	b.apiCaches.Put(cache)

	cp := *cache

	return &cp, nil
}

// GetAPICache returns the cache configuration for a GraphQL API.
func (b *InMemoryBackend) GetAPICache(apiID string) (*APICache, error) {
	b.mu.RLock("GetApiCache")
	defer b.mu.RUnlock()

	if !b.apis.Has(apiID) {
		return nil, fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	cache, ok := b.apiCaches.Get(apiID)
	if !ok {
		return nil, fmt.Errorf("%w: api cache not found for api %s", ErrNotFound, apiID)
	}

	cp := *cache

	return &cp, nil
}

// DeleteAPICache deletes the cache configuration for a GraphQL API.
func (b *InMemoryBackend) DeleteAPICache(apiID string) error {
	b.mu.Lock("DeleteApiCache")
	defer b.mu.Unlock()

	if !b.apis.Has(apiID) {
		return fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	if !b.apiCaches.Has(apiID) {
		return fmt.Errorf("%w: api cache not found for api %s", ErrNotFound, apiID)
	}

	b.apiCaches.Delete(apiID)

	return nil
}

// UpdateAPICache updates the cache configuration for a GraphQL API.
func (b *InMemoryBackend) UpdateAPICache(apiID string, cache *APICache) (*APICache, error) {
	b.mu.Lock("UpdateApiCache")
	defer b.mu.Unlock()

	if !b.apis.Has(apiID) {
		return nil, fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	existing, ok := b.apiCaches.Get(apiID)
	if !ok {
		return nil, fmt.Errorf("%w: api cache not found for api %s", ErrNotFound, apiID)
	}

	if cache.TTL > 0 {
		existing.TTL = cache.TTL
	}

	if cache.Type != "" {
		if !isValidAPICacheType(cache.Type) {
			return nil, fmt.Errorf("%w: invalid cache type %q", ErrValidation, cache.Type)
		}

		existing.Type = cache.Type
	}

	if cache.APICachingBehavior != "" {
		if !isValidAPICachingBehavior(cache.APICachingBehavior) {
			return nil, fmt.Errorf("%w: invalid apiCachingBehavior %q", ErrValidation, cache.APICachingBehavior)
		}

		existing.APICachingBehavior = cache.APICachingBehavior
	}

	cp := *existing

	return &cp, nil
}

// FlushAPICache flushes the cache for a GraphQL API.
func (b *InMemoryBackend) FlushAPICache(apiID string) error {
	b.mu.Lock("FlushApiCache")
	defer b.mu.Unlock()

	if !b.apis.Has(apiID) {
		return fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	if !b.apiCaches.Has(apiID) {
		return fmt.Errorf("%w: api cache not found for api %s", ErrNotFound, apiID)
	}

	return nil
}
