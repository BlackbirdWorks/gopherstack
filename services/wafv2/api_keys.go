package wafv2

import (
	"context"
	"fmt"
	"sort"

	"github.com/google/uuid"
)

func apiKeyMapKey(scope, apiKey string) string {
	return scope + ":" + apiKey
}

// CreateAPIKey creates a new API key for the given scope and token domains.
func (b *InMemoryBackend) CreateAPIKey(ctx context.Context, scope string, tokenDomains []string) (*APIKey, error) {
	b.mu.Lock("CreateAPIKey")
	defer b.mu.Unlock()

	region := storeRegion(scope, getRegion(ctx, b.region))
	key := uuid.NewString()
	a := &APIKey{
		APIKeyValue:  key,
		Scope:        scope,
		TokenDomains: cloneAddresses(tokenDomains),
		Region:       region,
	}
	b.apiKeys.Put(a)

	return &APIKey{
		APIKeyValue:  a.APIKeyValue,
		Scope:        a.Scope,
		TokenDomains: cloneAddresses(a.TokenDomains),
	}, nil
}

// DeleteAPIKey deletes the API key identified by scope and key value.
func (b *InMemoryBackend) DeleteAPIKey(ctx context.Context, scope, apiKey string) error {
	b.mu.Lock("DeleteAPIKey")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	compositeKey := regionKey(region, apiKeyMapKey(scope, apiKey))

	if !b.apiKeys.Delete(compositeKey) {
		return fmt.Errorf("%w: API key not found", ErrAPIKeyNotFound)
	}

	return nil
}

// ListAPIKeys returns all API keys, optionally filtered by scope.
func (b *InMemoryBackend) ListAPIKeys(ctx context.Context, scope string) []*APIKey {
	b.mu.RLock("ListAPIKeys")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	regionKeys := b.apiKeysByRegion.Get(region)
	list := make([]*APIKey, 0, len(regionKeys))

	for _, a := range regionKeys {
		if scope == "" || a.Scope == scope {
			list = append(list, &APIKey{
				APIKeyValue:  a.APIKeyValue,
				Scope:        a.Scope,
				TokenDomains: cloneAddresses(a.TokenDomains),
			})
		}
	}

	sort.Slice(list, func(i, j int) bool { return list[i].APIKeyValue < list[j].APIKeyValue })

	return list
}

// GetDecryptedAPIKey returns the API key identified by scope and key value.
func (b *InMemoryBackend) GetDecryptedAPIKey(ctx context.Context, scope, apiKey string) (*APIKey, error) {
	b.mu.RLock("GetDecryptedAPIKey")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	a, ok := b.apiKeys.Get(regionKey(region, apiKeyMapKey(scope, apiKey)))
	if !ok {
		return nil, fmt.Errorf("%w: API key not found", ErrAPIKeyNotFound)
	}

	return &APIKey{
		APIKeyValue:  a.APIKeyValue,
		Scope:        a.Scope,
		TokenDomains: cloneAddresses(a.TokenDomains),
	}, nil
}
