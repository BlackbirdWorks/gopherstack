package appsync

import (
	"fmt"

	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"
)

// StartSchemaCreation parses and stores the schema SDL for an API.
func (b *InMemoryBackend) StartSchemaCreation(apiID, sdl string) (*Schema, error) {
	b.mu.Lock("StartSchemaCreation")
	defer b.mu.Unlock()

	if !b.apis.Has(apiID) {
		return nil, fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	// Validate and parse the schema.
	parsed, gqlErr := gqlparser.LoadSchema(&ast.Source{
		Name:  "schema.graphql",
		Input: sdl,
	})
	if gqlErr != nil {
		schema := &Schema{
			APIID:   apiID,
			SDL:     sdl,
			Status:  SchemaStatusFailed,
			Details: gqlErr.Error(),
		}
		b.schemas.Put(schema)

		return nil, fmt.Errorf("%w: %s", ErrInvalidSchema, gqlErr.Error())
	}

	schema := &Schema{
		APIID:        apiID,
		SDL:          sdl,
		Status:       SchemaStatusActive,
		parsedSchema: parsed,
	}
	b.schemas.Put(schema)

	cp := *schema

	return &cp, nil
}

// GetSchemaCreationStatus returns the current schema creation status for an API.
func (b *InMemoryBackend) GetSchemaCreationStatus(apiID string) (*Schema, error) {
	b.mu.RLock("GetSchemaCreationStatus")
	defer b.mu.RUnlock()

	if !b.apis.Has(apiID) {
		return nil, fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	schema, ok := b.schemas.Get(apiID)
	if !ok {
		return &Schema{
			APIID:  apiID,
			Status: SchemaStatusNotApplicable,
		}, nil
	}

	cp := *schema

	return &cp, nil
}

// GetIntrospectionSchema returns the schema SDL for an API.
func (b *InMemoryBackend) GetIntrospectionSchema(apiID, _ string) ([]byte, error) {
	b.mu.RLock("GetIntrospectionSchema")
	defer b.mu.RUnlock()

	schema, ok := b.schemas.Get(apiID)
	if !ok {
		return nil, fmt.Errorf("%w: schema not found for api %s", ErrNotFound, apiID)
	}

	return []byte(schema.SDL), nil
}
