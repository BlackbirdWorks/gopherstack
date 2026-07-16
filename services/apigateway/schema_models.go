package apigateway

import (
	"fmt"
	"sort"
)

// CreateModel creates a data model for a REST API.
func (b *InMemoryBackend) CreateModel(input CreateModelInput) (*Model, error) {
	if input.RestAPIID == "" {
		return nil, fmt.Errorf("%w: restApiId is required", ErrInvalidParameter)
	}

	if input.Name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrInvalidParameter)
	}

	if input.ContentType == "" {
		return nil, fmt.Errorf("%w: contentType is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateModel")
	defer b.mu.Unlock()

	if !b.restApis.Has(input.RestAPIID) {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, input.RestAPIID)
	}

	for _, m := range b.modelsByAPI.Get(input.RestAPIID) {
		if m.Name == input.Name {
			return nil, fmt.Errorf(
				"%w: model %q already exists in REST API %s",
				ErrAlreadyExists,
				input.Name,
				input.RestAPIID,
			)
		}
	}

	id := randomID(resourceIDLength)
	model := &Model{
		ID:          id,
		RestAPIID:   input.RestAPIID,
		Name:        input.Name,
		Description: input.Description,
		ContentType: input.ContentType,
		Schema:      input.Schema,
	}
	b.models.Put(model)

	cp := *model

	return &cp, nil
}

// GetModel retrieves a model by name within a REST API.
func (b *InMemoryBackend) GetModel(restAPIID, modelName string) (*Model, error) {
	b.mu.RLock("GetModel")
	defer b.mu.RUnlock()
	if !b.restApis.Has(restAPIID) {
		return nil, fmt.Errorf("%w: %s", ErrRestAPINotFound, restAPIID)
	}
	for _, m := range b.modelsByAPI.Get(restAPIID) {
		if m.Name == modelName {
			cp := *m

			return &cp, nil
		}
	}

	return nil, fmt.Errorf("%w: model %q not found", ErrModelNotFound, modelName)
}

// GetModels returns all models for a REST API sorted by name.
func (b *InMemoryBackend) GetModels(restAPIID string) ([]Model, error) {
	b.mu.RLock("GetModels")
	defer b.mu.RUnlock()
	if !b.restApis.Has(restAPIID) {
		return nil, fmt.Errorf("%w: %s", ErrRestAPINotFound, restAPIID)
	}
	group := b.modelsByAPI.Get(restAPIID)
	all := make([]Model, 0, len(group))
	for _, m := range group {
		all = append(all, *m)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })

	return all, nil
}

// DeleteModel removes a model from a REST API by name.
func (b *InMemoryBackend) DeleteModel(restAPIID, modelName string) error {
	b.mu.Lock("DeleteModel")
	defer b.mu.Unlock()
	if !b.restApis.Has(restAPIID) {
		return fmt.Errorf("%w: %s", ErrRestAPINotFound, restAPIID)
	}
	for _, m := range b.modelsByAPI.Get(restAPIID) {
		if m.Name == modelName {
			b.models.Delete(modelKeyFn(m))

			return nil
		}
	}

	return fmt.Errorf("%w: model %q not found", ErrModelNotFound, modelName)
}

// UpdateModel updates description and schema on a model.
func (b *InMemoryBackend) UpdateModel(restAPIID, modelName string, input UpdateModelInput) (*Model, error) {
	b.mu.Lock("UpdateModel")
	defer b.mu.Unlock()
	if !b.restApis.Has(restAPIID) {
		return nil, fmt.Errorf("%w: %s", ErrRestAPINotFound, restAPIID)
	}
	for _, m := range b.modelsByAPI.Get(restAPIID) {
		if m.Name == modelName {
			if input.Description != "" {
				m.Description = input.Description
			}
			if input.Schema != "" {
				m.Schema = input.Schema
			}
			cp := *m

			return &cp, nil
		}
	}

	return nil, fmt.Errorf("%w: model %q not found", ErrModelNotFound, modelName)
}

// GetModelTemplate returns the default template for a model.
func (b *InMemoryBackend) GetModelTemplate(restAPIID, modelName string) (string, error) {
	b.mu.RLock("GetModelTemplate")
	defer b.mu.RUnlock()

	if !b.restApis.Has(restAPIID) {
		return "", fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}

	var model *Model
	for _, m := range b.modelsByAPI.Get(restAPIID) {
		if m.Name == modelName {
			model = m

			break
		}
	}

	if model == nil {
		return "", fmt.Errorf("%w: model %s not found", ErrNotFound, modelName)
	}

	if model.Schema != "" {
		return model.Schema, nil
	}

	return "#set($inputRoot = $input.path('$'))\n{}", nil
}
