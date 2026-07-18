package apigateway

import (
	"fmt"
	"sort"
	"strings"
)

// CreateBasePathMapping creates a new base path mapping for a domain name.
func (b *InMemoryBackend) CreateBasePathMapping(input CreateBasePathMappingInput) (*BasePathMapping, error) {
	if input.DomainName == "" {
		return nil, fmt.Errorf("%w: domainName is required", ErrInvalidParameter)
	}

	if input.RestAPIID == "" {
		return nil, fmt.Errorf("%w: restApiId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateBasePathMapping")
	defer b.mu.Unlock()

	mapKey := basePathMappingKey(input.DomainName, input.BasePath)
	if b.basePathMappings.Has(mapKey) {
		return nil, fmt.Errorf("%w: base path mapping already exists for domain %q path %q",
			ErrAlreadyExists, input.DomainName, input.BasePath)
	}

	bpm := &BasePathMapping{
		DomainName: input.DomainName,
		BasePath:   input.BasePath,
		RestAPIID:  input.RestAPIID,
		Stage:      input.Stage,
	}
	b.basePathMappings.Put(bpm)

	cp := *bpm

	return &cp, nil
}

// GetBasePathMapping retrieves a base path mapping by domain + path.
func (b *InMemoryBackend) GetBasePathMapping(domainName, basePath string) (*BasePathMapping, error) {
	b.mu.RLock("GetBasePathMapping")
	defer b.mu.RUnlock()
	bpm, ok := b.basePathMappings.Get(basePathMappingKey(domainName, basePath))
	if !ok {
		return nil, fmt.Errorf(
			"%w: base path mapping not found for domain %q path %q",
			ErrBasePathMappingNotFound,
			domainName,
			basePath,
		)
	}
	cp := *bpm

	return &cp, nil
}

// GetBasePathMappings returns all base path mappings for a domain name.
func (b *InMemoryBackend) GetBasePathMappings(domainName string) ([]BasePathMapping, error) {
	b.mu.RLock("GetBasePathMappings")
	defer b.mu.RUnlock()
	var all []BasePathMapping
	prefix := domainName + "#"
	for _, bpm := range b.basePathMappings.All() {
		if strings.HasPrefix(basePathMappingKeyFn(bpm), prefix) {
			all = append(all, *bpm)
		}
	}
	sort.Slice(all, func(i, j int) bool { return all[i].BasePath < all[j].BasePath })

	return all, nil
}

// DeleteBasePathMapping removes a base path mapping by domain + path.
func (b *InMemoryBackend) DeleteBasePathMapping(domainName, basePath string) error {
	b.mu.Lock("DeleteBasePathMapping")
	defer b.mu.Unlock()
	if !b.basePathMappings.Delete(basePathMappingKey(domainName, basePath)) {
		return fmt.Errorf(
			"%w: base path mapping not found for domain %q path %q",
			ErrBasePathMappingNotFound,
			domainName,
			basePath,
		)
	}

	return nil
}

// UpdateBasePathMapping updates an existing base path mapping.
func (b *InMemoryBackend) UpdateBasePathMapping(input UpdateBasePathMappingInput) (*BasePathMapping, error) {
	b.mu.Lock("UpdateBasePathMapping")
	defer b.mu.Unlock()

	key := basePathMappingKey(input.DomainName, input.BasePath)
	m, ok := b.basePathMappings.Get(key)
	if !ok {
		return nil, fmt.Errorf("%w: base path mapping %s/%s not found", ErrNotFound, input.DomainName, input.BasePath)
	}

	if input.RestAPIID != "" {
		m.RestAPIID = input.RestAPIID
	}

	if input.Stage != "" {
		m.Stage = input.Stage
	}

	return m, nil
}
