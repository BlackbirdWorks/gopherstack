package apigateway

import (
	"fmt"
	"sort"
	"time"
)

// CreateDocumentationPart creates a documentation part for a REST API.
func (b *InMemoryBackend) CreateDocumentationPart(input CreateDocumentationPartInput) (*DocumentationPart, error) {
	if input.RestAPIID == "" {
		return nil, fmt.Errorf("%w: restApiId is required", ErrInvalidParameter)
	}

	if input.Location.Type == "" {
		return nil, fmt.Errorf("%w: location.type is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateDocumentationPart")
	defer b.mu.Unlock()

	if !b.restApis.Has(input.RestAPIID) {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, input.RestAPIID)
	}

	id := randomID(resourceIDLength)
	part := &DocumentationPart{
		ID:         id,
		RestAPIID:  input.RestAPIID,
		Location:   input.Location,
		Properties: input.Properties,
	}
	b.documentationParts.Put(part)

	cp := *part

	return &cp, nil
}

// CreateDocumentationVersion creates a documentation version snapshot for a REST API.
func (b *InMemoryBackend) CreateDocumentationVersion(
	input CreateDocumentationVersionInput,
) (*DocumentationVersion, error) {
	if input.RestAPIID == "" {
		return nil, fmt.Errorf("%w: restApiId is required", ErrInvalidParameter)
	}

	if input.Version == "" {
		return nil, fmt.Errorf("%w: documentationVersion is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateDocumentationVersion")
	defer b.mu.Unlock()

	if !b.restApis.Has(input.RestAPIID) {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, input.RestAPIID)
	}

	if b.documentationVersions.Has(documentationVersionKey(input.RestAPIID, input.Version)) {
		return nil, fmt.Errorf("%w: documentation version %q already exists", ErrAlreadyExists, input.Version)
	}

	ver := &DocumentationVersion{
		RestAPIID:   input.RestAPIID,
		Version:     input.Version,
		Description: input.Description,
		CreatedDate: unixEpochTime{time.Now()},
	}
	b.documentationVersions.Put(ver)

	cp := *ver

	return &cp, nil
}

// GetDocumentationPart retrieves a documentation part by ID.
func (b *InMemoryBackend) GetDocumentationPart(restAPIID, docPartID string) (*DocumentationPart, error) {
	b.mu.RLock("GetDocumentationPart")
	defer b.mu.RUnlock()
	if !b.restApis.Has(restAPIID) {
		return nil, fmt.Errorf("%w: %s", ErrRestAPINotFound, restAPIID)
	}
	p, ok := b.documentationParts.Get(documentationPartKey(restAPIID, docPartID))
	if !ok {
		return nil, fmt.Errorf("%w: documentation part %s not found", ErrDocumentationPartNotFound, docPartID)
	}
	cp := *p

	return &cp, nil
}

// GetDocumentationParts returns all documentation parts for a REST API sorted by ID.
func (b *InMemoryBackend) GetDocumentationParts(restAPIID string) ([]DocumentationPart, error) {
	b.mu.RLock("GetDocumentationParts")
	defer b.mu.RUnlock()
	if !b.restApis.Has(restAPIID) {
		return nil, fmt.Errorf("%w: %s", ErrRestAPINotFound, restAPIID)
	}
	group := b.documentationPartsByAPI.Get(restAPIID)
	all := make([]DocumentationPart, 0, len(group))
	for _, p := range group {
		all = append(all, *p)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	return all, nil
}

// DeleteDocumentationPart removes a documentation part by ID.
func (b *InMemoryBackend) DeleteDocumentationPart(restAPIID, docPartID string) error {
	b.mu.Lock("DeleteDocumentationPart")
	defer b.mu.Unlock()
	if !b.restApis.Has(restAPIID) {
		return fmt.Errorf("%w: %s", ErrRestAPINotFound, restAPIID)
	}
	if !b.documentationParts.Delete(documentationPartKey(restAPIID, docPartID)) {
		return fmt.Errorf("%w: documentation part %s not found", ErrDocumentationPartNotFound, docPartID)
	}

	return nil
}

// GetDocumentationVersion retrieves a documentation version by version string.
func (b *InMemoryBackend) GetDocumentationVersion(restAPIID, version string) (*DocumentationVersion, error) {
	b.mu.RLock("GetDocumentationVersion")
	defer b.mu.RUnlock()
	if !b.restApis.Has(restAPIID) {
		return nil, fmt.Errorf("%w: %s", ErrRestAPINotFound, restAPIID)
	}
	v, ok := b.documentationVersions.Get(documentationVersionKey(restAPIID, version))
	if !ok {
		return nil, fmt.Errorf("%w: documentation version %q not found", ErrDocumentationVersionNotFound, version)
	}
	cp := *v

	return &cp, nil
}

// GetDocumentationVersions returns all documentation versions for a REST API sorted by version.
func (b *InMemoryBackend) GetDocumentationVersions(restAPIID string) ([]DocumentationVersion, error) {
	b.mu.RLock("GetDocumentationVersions")
	defer b.mu.RUnlock()
	if !b.restApis.Has(restAPIID) {
		return nil, fmt.Errorf("%w: %s", ErrRestAPINotFound, restAPIID)
	}
	group := b.documentationVersionsByAPI.Get(restAPIID)
	all := make([]DocumentationVersion, 0, len(group))
	for _, v := range group {
		all = append(all, *v)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].Version < all[j].Version })

	return all, nil
}

// DeleteDocumentationVersion removes a documentation version by version string.
func (b *InMemoryBackend) DeleteDocumentationVersion(restAPIID, version string) error {
	b.mu.Lock("DeleteDocumentationVersion")
	defer b.mu.Unlock()
	if !b.restApis.Has(restAPIID) {
		return fmt.Errorf("%w: %s", ErrRestAPINotFound, restAPIID)
	}
	if !b.documentationVersions.Delete(documentationVersionKey(restAPIID, version)) {
		return fmt.Errorf("%w: documentation version %q not found", ErrDocumentationVersionNotFound, version)
	}

	return nil
}

// UpdateDocumentationPart updates the properties of a documentation part.
func (b *InMemoryBackend) UpdateDocumentationPart(input UpdateDocumentationPartInput) (*DocumentationPart, error) {
	b.mu.Lock("UpdateDocumentationPart")
	defer b.mu.Unlock()

	if !b.restApis.Has(input.RestAPIID) {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, input.RestAPIID)
	}

	part, ok := b.documentationParts.Get(documentationPartKey(input.RestAPIID, input.DocPartID))
	if !ok {
		return nil, fmt.Errorf("%w: documentation part %s not found", ErrNotFound, input.DocPartID)
	}

	if input.Properties != "" {
		part.Properties = input.Properties
	}

	return part, nil
}

// UpdateDocumentationVersion updates a documentation version's description.
func (b *InMemoryBackend) UpdateDocumentationVersion(
	input UpdateDocumentationVersionInput,
) (*DocumentationVersion, error) {
	b.mu.Lock("UpdateDocumentationVersion")
	defer b.mu.Unlock()

	if !b.restApis.Has(input.RestAPIID) {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, input.RestAPIID)
	}

	ver, ok := b.documentationVersions.Get(documentationVersionKey(input.RestAPIID, input.DocumentationVersion))
	if !ok {
		return nil, fmt.Errorf("%w: documentation version %s not found", ErrNotFound, input.DocumentationVersion)
	}

	if input.Description != "" {
		ver.Description = input.Description
	}

	return ver, nil
}
