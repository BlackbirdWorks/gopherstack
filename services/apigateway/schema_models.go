package apigateway

import (
	"encoding/json"
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

// GetModel retrieves a model by name within a REST API. When flatten is true
// (api_op_GetModel.go's Flatten httpQuery param, "resolve all external model
// references and returns a flattened model schema"), any $ref pointing at
// another model in the same REST API is inlined recursively.
func (b *InMemoryBackend) GetModel(restAPIID, modelName string, flatten bool) (*Model, error) {
	b.mu.RLock("GetModel")
	defer b.mu.RUnlock()
	if !b.restApis.Has(restAPIID) {
		return nil, fmt.Errorf("%w: %s", ErrRestAPINotFound, restAPIID)
	}
	for _, m := range b.modelsByAPI.Get(restAPIID) {
		if m.Name == modelName {
			cp := *m
			if flatten {
				cp.Schema = flattenModelSchema(b, restAPIID, cp.Schema, map[string]bool{modelName: true})
			}

			return &cp, nil
		}
	}

	return nil, fmt.Errorf("%w: model %q not found", ErrModelNotFound, modelName)
}

// flattenModelSchema inlines every {"$ref": "..."} object in schema that
// names another model in the same REST API, recursively. visiting guards
// against a $ref cycle (a model, directly or transitively, referencing
// itself) by refusing to expand a name already being expanded, leaving that
// $ref object as-is rather than looping forever. A schema that isn't valid
// JSON, or a $ref naming no model in this REST API, is left untouched.
func flattenModelSchema(b *InMemoryBackend, restAPIID, schema string, visiting map[string]bool) string {
	var node any
	if err := json.Unmarshal([]byte(schema), &node); err != nil {
		return schema
	}

	resolved := resolveModelRefNode(b, restAPIID, node, visiting)

	out, err := json.Marshal(resolved)
	if err != nil {
		return schema
	}

	return string(out)
}

// resolveModelRefNode walks a decoded JSON-schema value, replacing any
// {"$ref": "<...>/models/<name>"}-shaped object with the referenced model's
// own (recursively flattened) schema.
func resolveModelRefNode(b *InMemoryBackend, restAPIID string, node any, visiting map[string]bool) any {
	switch v := node.(type) {
	case map[string]any:
		if ref, ok := v["$ref"].(string); ok && len(v) == 1 {
			if inlined, resolved := resolveModelRef(b, restAPIID, ref, visiting); resolved {
				return inlined
			}

			return v
		}
		out := make(map[string]any, len(v))
		for k, val := range v {
			out[k] = resolveModelRefNode(b, restAPIID, val, visiting)
		}

		return out
	case []any:
		out := make([]any, len(v))
		for i, val := range v {
			out[i] = resolveModelRefNode(b, restAPIID, val, visiting)
		}

		return out
	default:
		return node
	}
}

// resolveModelRef looks up the model named by a $ref's trailing path segment
// (matching schemaRefName's convention, which also covers the real AWS
// model-cross-reference URI shape
// "https://apigateway.amazonaws.com/restapis/{restApiId}/models/{model}"
// alongside the OpenAPI-internal "#/definitions/{model}" and
// "#/components/schemas/{model}" forms this backend's own OpenAPI importer
// produces) and returns its recursively flattened, decoded schema.
func resolveModelRef(b *InMemoryBackend, restAPIID, ref string, visiting map[string]bool) (any, bool) {
	refJSON, err := json.Marshal(struct {
		Ref string `json:"$ref"`
	}{Ref: ref})
	if err != nil {
		return nil, false
	}

	name := schemaRefName(refJSON)
	if name == "" || visiting[name] {
		return nil, false
	}

	for _, m := range b.modelsByAPI.Get(restAPIID) {
		if m.Name != name {
			continue
		}

		nested := make(map[string]bool, len(visiting)+1)
		for k := range visiting {
			nested[k] = true
		}
		nested[name] = true

		var decoded any
		if unmarshalErr := json.Unmarshal([]byte(m.Schema), &decoded); unmarshalErr != nil {
			return nil, false
		}

		return resolveModelRefNode(b, restAPIID, decoded, nested), true
	}

	return nil, false
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
