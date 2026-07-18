package apigatewayv2

import (
	"fmt"
	"sort"
)

// CreateModel creates a new model for an API.
func (b *InMemoryBackend) CreateModel(apiID string, input CreateModelInput) (*Model, error) {
	if input.Name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrBadRequest)
	}

	b.mu.Lock("CreateModel")
	defer b.mu.Unlock()

	if !b.apis.Has(apiID) {
		return nil, ErrAPINotFound
	}

	for _, existing := range b.modelsByAPI.Get(apiID) {
		if existing.Name == input.Name {
			return nil, fmt.Errorf("%w: model name %q already exists", ErrAlreadyExists, input.Name)
		}
	}

	id := randomID()
	model := &Model{
		ModelID:     id,
		APIID:       apiID,
		Name:        input.Name,
		Schema:      input.Schema,
		ContentType: input.ContentType,
		Description: input.Description,
	}

	b.models.Put(model)

	cp := *model

	return &cp, nil
}

// GetModel retrieves a model by ID.
func (b *InMemoryBackend) GetModel(apiID, modelID string) (*Model, error) {
	b.mu.RLock("GetModel")
	defer b.mu.RUnlock()

	if !b.apis.Has(apiID) {
		return nil, ErrAPINotFound
	}

	m, ok := b.models.Get(modelKey(apiID, modelID))
	if !ok {
		return nil, ErrModelNotFound
	}

	cp := *m

	return &cp, nil
}

// GetModels retrieves all models for an API.
func (b *InMemoryBackend) GetModels(apiID string) ([]Model, error) {
	b.mu.RLock("GetModels")
	defer b.mu.RUnlock()

	if !b.apis.Has(apiID) {
		return nil, ErrAPINotFound
	}

	models := b.modelsByAPI.Get(apiID)
	result := make([]Model, 0, len(models))

	for _, m := range models {
		result = append(result, *m)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ModelID < result[j].ModelID
	})

	return result, nil
}

// DeleteModel removes a model from an API.
func (b *InMemoryBackend) DeleteModel(apiID, modelID string) error {
	b.mu.Lock("DeleteModel")
	defer b.mu.Unlock()

	if !b.apis.Has(apiID) {
		return ErrAPINotFound
	}

	if !b.models.Delete(modelKey(apiID, modelID)) {
		return ErrModelNotFound
	}

	return nil
}

// UpdateModel updates fields on an existing model.
func (b *InMemoryBackend) UpdateModel(apiID, modelID string, input UpdateModelInput) (*Model, error) {
	b.mu.Lock("UpdateModel")
	defer b.mu.Unlock()

	if !b.apis.Has(apiID) {
		return nil, ErrAPINotFound
	}

	m, ok := b.models.Get(modelKey(apiID, modelID))
	if !ok {
		return nil, ErrModelNotFound
	}

	if input.Name != "" {
		m.Name = input.Name
	}

	if input.Schema != "" {
		m.Schema = input.Schema
	}

	if input.ContentType != "" {
		m.ContentType = input.ContentType
	}

	if input.Description != "" {
		m.Description = input.Description
	}

	cp := *m

	return &cp, nil
}
