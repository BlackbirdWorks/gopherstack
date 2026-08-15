package opensearch

import (
	"fmt"
)

// CreateIndex creates an index for a domain. indexSchema is the opaque
// smithy-document body of the real AWS CreateIndex request (see
// DomainIndex.IndexSchema); it is nil for the emulator's separate
// OpenSearch-data-plane-style index creation route, which supplies
// mappings/settings/aliases directly instead.
func (b *InMemoryBackend) CreateIndex(
	domainName, indexName string,
	mappings, settings, aliases map[string]any,
	indexSchema any,
) (*DomainIndex, error) {
	b.mu.Lock("CreateIndex")
	defer b.mu.Unlock()

	if !b.domains.Has(domainName) {
		return nil, fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	idx := &DomainIndex{
		IndexName:     indexName,
		IndexStatus:   pkgStateActive,
		Mappings:      mappings,
		Settings:      settings,
		Aliases:       aliases,
		IndexSchema:   indexSchema,
		Documents:     make(map[string]map[string]any),
		DomainName:    domainName,
		DocumentCount: 0,
	}
	b.domainIndexes.Put(idx)

	cp := *idx

	return &cp, nil
}

// DeleteIndex removes an index from a domain.
func (b *InMemoryBackend) DeleteIndex(domainName, indexName string) (*DomainIndex, error) {
	b.mu.Lock("DeleteIndex")
	defer b.mu.Unlock()

	idx, exists := b.domainIndexes.Get(domainIndexKey(domainName, indexName))
	if !exists {
		return nil, fmt.Errorf(
			"%w: index %s not found on domain %s",
			ErrConnectionNotFound,
			indexName,
			domainName,
		)
	}

	cp := *idx
	b.domainIndexes.Delete(domainIndexKey(domainName, indexName))

	return &cp, nil
}

// GetIndex returns an index by domain and name.
func (b *InMemoryBackend) GetIndex(domainName, indexName string) (*DomainIndex, error) {
	b.mu.RLock("GetIndex")
	defer b.mu.RUnlock()

	idx, exists := b.domainIndexes.Get(domainIndexKey(domainName, indexName))
	if !exists {
		return nil, fmt.Errorf(
			"%w: index %s not found on domain %s",
			ErrConnectionNotFound,
			indexName,
			domainName,
		)
	}

	cp := *idx

	return &cp, nil
}

// UpdateIndex updates the mappings and settings of an index. indexSchema is
// the opaque smithy-document body of the real AWS UpdateIndex request (see
// DomainIndex.IndexSchema); it is nil for the emulator's separate
// OpenSearch-data-plane-style index update route.
func (b *InMemoryBackend) UpdateIndex(
	domainName, indexName string,
	mappings, settings map[string]any,
	indexSchema any,
) (*DomainIndex, error) {
	b.mu.Lock("UpdateIndex")
	defer b.mu.Unlock()

	idx, exists := b.domainIndexes.Get(domainIndexKey(domainName, indexName))
	if !exists {
		return nil, fmt.Errorf(
			"%w: index %s not found on domain %s",
			ErrConnectionNotFound,
			indexName,
			domainName,
		)
	}

	idx.Mappings = mappings
	idx.Settings = settings
	if indexSchema != nil {
		idx.IndexSchema = indexSchema
	}
	cp := *idx

	return &cp, nil
}
