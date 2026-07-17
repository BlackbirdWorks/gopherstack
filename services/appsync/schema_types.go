package appsync

import (
	"fmt"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// isValidTypeFormat returns true if the given type format is valid.
func isValidTypeFormat(f TypeDefinitionFormat) bool {
	return f == TypeFormatSDL || f == TypeFormatJSON
}

// CreateType creates a GraphQL type for an API.
func (b *InMemoryBackend) CreateType(apiID, definition string, format TypeDefinitionFormat) (*APIType, error) {
	b.mu.Lock("CreateType")
	defer b.mu.Unlock()

	if !b.apis.Has(apiID) {
		return nil, fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	if format != "" && !isValidTypeFormat(format) {
		return nil, fmt.Errorf("%w: invalid format %q, must be SDL or JSON", ErrValidation, format)
	}

	// Extract type name from definition (assumes SDL format: "type TypeName { ... }").
	name := extractTypeName(definition)
	if name == "" {
		name = randomAPIID()
	}

	if b.types.Has(apiTypeKey(apiID, name)) {
		return nil, fmt.Errorf("%w: type %s already exists for api %s", ErrAlreadyExists, name, apiID)
	}

	typeARN := arn.Build("appsync", b.region, b.accountID,
		fmt.Sprintf("apis/%s/types/%s", apiID, name))

	t := &APIType{
		ARN:        typeARN,
		Name:       name,
		Definition: definition,
		Format:     format,
		APIID:      apiID,
	}

	b.types.Put(t)

	cp := *t

	return &cp, nil
}

// extractTypeName extracts the type name from a SDL type definition.
func extractTypeName(definition string) string {
	definition = strings.TrimSpace(definition)
	for _, prefix := range []string{"type ", "input ", "enum ", "interface ", "union ", "scalar "} {
		if after, ok := strings.CutPrefix(definition, prefix); ok {
			rest := after
			// Take the first word (the type name).
			end := strings.IndexAny(rest, " \t\n\r{")
			if end > 0 {
				return rest[:end]
			}

			return rest
		}
	}

	return ""
}

// GetType returns a GraphQL type by name.
func (b *InMemoryBackend) GetType(apiID, typeName string) (*APIType, error) {
	b.mu.RLock("GetType")
	defer b.mu.RUnlock()

	t, ok := b.types.Get(apiTypeKey(apiID, typeName))
	if !ok {
		return nil, fmt.Errorf("%w: type %s not found", ErrNotFound, typeName)
	}

	cp := *t

	return &cp, nil
}

// ListTypes returns all GraphQL types for an API.
func (b *InMemoryBackend) ListTypes(apiID string) ([]*APIType, error) {
	b.mu.RLock("ListTypes")
	defer b.mu.RUnlock()

	if !b.apis.Has(apiID) {
		return nil, fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	types := b.typesByAPI.Get(apiID)
	out := make([]*APIType, 0, len(types))

	for _, t := range types {
		cp := *t
		out = append(out, &cp)
	}

	slices.SortFunc(out, func(a, b *APIType) int {
		return strings.Compare(a.Name, b.Name)
	})

	return out, nil
}

// DeleteType deletes a GraphQL type.
func (b *InMemoryBackend) DeleteType(apiID, typeName string) error {
	b.mu.Lock("DeleteType")
	defer b.mu.Unlock()

	key := apiTypeKey(apiID, typeName)
	if !b.types.Has(key) {
		return fmt.Errorf("%w: type %s not found", ErrNotFound, typeName)
	}

	b.types.Delete(key)

	return nil
}

// UpdateType updates an existing GraphQL type definition.
func (b *InMemoryBackend) UpdateType(
	apiID, typeName, definition string,
	format TypeDefinitionFormat,
) (*APIType, error) {
	b.mu.Lock("UpdateType")
	defer b.mu.Unlock()

	existing, ok := b.types.Get(apiTypeKey(apiID, typeName))
	if !ok {
		return nil, fmt.Errorf("%w: type %s not found", ErrNotFound, typeName)
	}

	if definition != "" {
		existing.Definition = definition
	}

	if format != "" {
		existing.Format = format
	}

	cp := *existing

	return &cp, nil
}

// ListTypesByAssociation lists types associated with a given merged API source association.
// Since types are stored per-API and not per-association in the in-memory backend,
// this returns types from the merged API.
func (b *InMemoryBackend) ListTypesByAssociation(mergedAPIID, associationID, _ string) ([]*APIType, error) {
	b.mu.RLock("ListTypesByAssociation")
	defer b.mu.RUnlock()

	if !b.apis.Has(mergedAPIID) {
		return nil, fmt.Errorf("%w: api %s not found", ErrNotFound, mergedAPIID)
	}

	if !b.sourceAssocs.Has(associationID) {
		return nil, fmt.Errorf("%w: source api association %s not found", ErrNotFound, associationID)
	}

	ts := b.typesByAPI.Get(mergedAPIID)
	out := make([]*APIType, 0, len(ts))

	for _, t := range ts {
		cp := *t
		out = append(out, &cp)
	}

	slices.SortFunc(out, func(a, b *APIType) int {
		return strings.Compare(a.Name, b.Name)
	})

	return out, nil
}
