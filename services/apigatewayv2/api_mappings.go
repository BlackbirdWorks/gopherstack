package apigatewayv2

import (
	"fmt"
	"sort"
)

// CreateAPIMapping creates a new API mapping for a custom domain name.
func (b *InMemoryBackend) CreateAPIMapping(domainName string, input CreateAPIMappingInput) (*APIMapping, error) {
	if input.APIID == "" {
		return nil, fmt.Errorf("%w: apiId is required", ErrBadRequest)
	}

	if input.Stage == "" {
		return nil, fmt.Errorf("%w: stage is required", ErrBadRequest)
	}

	b.mu.Lock("CreateAPIMapping")
	defer b.mu.Unlock()

	if !b.domainNames.Has(domainName) {
		return nil, ErrDomainNameNotFound
	}

	if !b.apis.Has(input.APIID) {
		return nil, ErrAPINotFound
	}

	if !b.stages.Has(stageKey(input.APIID, input.Stage)) {
		return nil, ErrStageNotFound
	}

	// AWS allows only one mapping per (domain, apiMappingKey). The empty key is
	// the domain's default (base-path) mapping and is itself unique. Reject a
	// duplicate key with a ConflictException, matching real API Gateway v2.
	for _, existing := range b.apiMappingsByDomain.Get(domainName) {
		if existing.APIMappingKey == input.APIMappingKey {
			return nil, fmt.Errorf(
				"%w: an api mapping already exists for the mapping key %q on domain %q",
				ErrAlreadyExists, input.APIMappingKey, domainName,
			)
		}
	}

	id := randomID()
	mapping := &APIMapping{
		APIMappingID:  id,
		DomainName:    domainName,
		APIID:         input.APIID,
		Stage:         input.Stage,
		APIMappingKey: input.APIMappingKey,
	}

	b.apiMappings.Put(mapping)

	cp := *mapping

	return &cp, nil
}

// GetAPIMapping retrieves a specific API mapping.
func (b *InMemoryBackend) GetAPIMapping(domainName, mappingID string) (*APIMapping, error) {
	b.mu.RLock("GetAPIMapping")
	defer b.mu.RUnlock()

	if !b.domainNames.Has(domainName) {
		return nil, ErrDomainNameNotFound
	}

	m, ok := b.apiMappings.Get(apiMappingKey(domainName, mappingID))
	if !ok {
		return nil, ErrAPIMappingNotFound
	}

	cp := *m

	return &cp, nil
}

// GetAPIMappings retrieves all API mappings for a domain name.
func (b *InMemoryBackend) GetAPIMappings(domainName string) ([]APIMapping, error) {
	b.mu.RLock("GetAPIMappings")
	defer b.mu.RUnlock()

	if !b.domainNames.Has(domainName) {
		return nil, ErrDomainNameNotFound
	}

	mappings := b.apiMappingsByDomain.Get(domainName)
	result := make([]APIMapping, 0, len(mappings))

	for _, m := range mappings {
		result = append(result, *m)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].APIMappingID < result[j].APIMappingID
	})

	return result, nil
}

// DeleteAPIMapping removes an API mapping from a domain name.
func (b *InMemoryBackend) DeleteAPIMapping(domainName, mappingID string) error {
	b.mu.Lock("DeleteAPIMapping")
	defer b.mu.Unlock()

	if !b.domainNames.Has(domainName) {
		return ErrDomainNameNotFound
	}

	if !b.apiMappings.Delete(apiMappingKey(domainName, mappingID)) {
		return ErrAPIMappingNotFound
	}

	return nil
}

// UpdateAPIMapping updates fields on an existing API mapping.
func (b *InMemoryBackend) UpdateAPIMapping(
	domainName, mappingID string,
	input UpdateAPIMappingInput,
) (*APIMapping, error) {
	b.mu.Lock("UpdateAPIMapping")
	defer b.mu.Unlock()

	if !b.domainNames.Has(domainName) {
		return nil, ErrDomainNameNotFound
	}

	m, ok := b.apiMappings.Get(apiMappingKey(domainName, mappingID))
	if !ok {
		return nil, ErrAPIMappingNotFound
	}

	if input.APIID != "" {
		if !b.apis.Has(input.APIID) {
			return nil, ErrAPINotFound
		}
		stageToCheck := m.Stage
		if input.Stage != "" {
			stageToCheck = input.Stage
		}
		if !b.stages.Has(stageKey(input.APIID, stageToCheck)) {
			return nil, ErrStageNotFound
		}
		m.APIID = input.APIID
	}

	if input.Stage != "" {
		m.Stage = input.Stage
	}

	if input.APIMappingKey != "" {
		m.APIMappingKey = input.APIMappingKey
	}

	cp := *m

	return &cp, nil
}
