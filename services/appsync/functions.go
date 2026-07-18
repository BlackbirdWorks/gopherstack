package appsync

import (
	"fmt"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// defaultFunctionVersion is the default AppSync function runtime version.
const defaultFunctionVersion = "2018-05-29"

// CreateFunction creates an AppSync pipeline function.
func (b *InMemoryBackend) CreateFunction(apiID string, f *Function) (*Function, error) {
	b.mu.Lock("CreateFunction")
	defer b.mu.Unlock()

	if !b.apis.Has(apiID) {
		return nil, fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	if f.Name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}

	// Enforce name uniqueness across all functions in the API.
	for _, existing := range b.functionsByAPI.Get(apiID) {
		if existing.Name == f.Name {
			return nil, fmt.Errorf("%w: function with name %s already exists", ErrAlreadyExists, f.Name)
		}
	}

	funcID := randomAPIID()
	funcARN := arn.Build("appsync", b.region, b.accountID,
		fmt.Sprintf("apis/%s/functions/%s", apiID, funcID))

	f.APIID = apiID
	f.FunctionID = funcID
	f.FunctionARN = funcARN

	// Default to the well-known function runtime version.
	if f.FunctionVersion == "" {
		f.FunctionVersion = defaultFunctionVersion
	}

	b.functions.Put(f)

	cp := *f

	return &cp, nil
}

// GetFunction returns a pipeline function by ID.
func (b *InMemoryBackend) GetFunction(apiID, functionID string) (*Function, error) {
	b.mu.RLock("GetFunction")
	defer b.mu.RUnlock()

	if !b.apis.Has(apiID) {
		return nil, fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	fn, ok := b.functions.Get(functionKey(apiID, functionID))
	if !ok {
		return nil, fmt.Errorf("%w: function %s not found", ErrNotFound, functionID)
	}

	cp := *fn

	return &cp, nil
}

// ListFunctions returns all pipeline functions for a GraphQL API.
func (b *InMemoryBackend) ListFunctions(apiID string) ([]*Function, error) {
	b.mu.RLock("ListFunctions")
	defer b.mu.RUnlock()

	if !b.apis.Has(apiID) {
		return nil, fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	fns := b.functionsByAPI.Get(apiID)
	out := make([]*Function, 0, len(fns))

	for _, fn := range fns {
		cp := *fn
		out = append(out, &cp)
	}

	slices.SortFunc(out, func(a, b *Function) int {
		return strings.Compare(a.Name, b.Name)
	})

	return out, nil
}

// DeleteFunction deletes a pipeline function.
// Returns an error if any resolver's pipeline config still references this function.
func (b *InMemoryBackend) DeleteFunction(apiID, functionID string) error {
	b.mu.Lock("DeleteFunction")
	defer b.mu.Unlock()

	if !b.apis.Has(apiID) {
		return fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	key := functionKey(apiID, functionID)
	if !b.functions.Has(key) {
		return fmt.Errorf("%w: function %s not found", ErrNotFound, functionID)
	}

	// Prevent deletion if any resolver still references this function.
	for _, r := range b.resolversByAPI.Get(apiID) {
		if slices.Contains(r.PipelineConfig, functionID) {
			return fmt.Errorf(
				"%w: function %s is still referenced by resolver %s.%s",
				ErrValidation,
				functionID,
				r.TypeName,
				r.FieldName,
			)
		}
	}

	b.functions.Delete(key)

	return nil
}

// UpdateFunction updates an existing pipeline function.
func (b *InMemoryBackend) UpdateFunction(apiID, functionID string, f *Function) (*Function, error) {
	b.mu.Lock("UpdateFunction")
	defer b.mu.Unlock()

	existing, ok := b.functions.Get(functionKey(apiID, functionID))
	if !ok {
		return nil, fmt.Errorf("%w: function %s not found", ErrNotFound, functionID)
	}

	if f.Name != "" {
		existing.Name = f.Name
	}

	if f.Description != "" {
		existing.Description = f.Description
	}

	if f.DataSourceName != "" {
		existing.DataSourceName = f.DataSourceName
	}

	if f.RequestMappingTemplate != "" {
		existing.RequestMappingTemplate = f.RequestMappingTemplate
	}

	if f.ResponseMappingTemplate != "" {
		existing.ResponseMappingTemplate = f.ResponseMappingTemplate
	}

	if f.Code != "" {
		existing.Code = f.Code
	}

	if f.Runtime != nil {
		existing.Runtime = f.Runtime
	}

	if f.SyncConfig != nil {
		existing.SyncConfig = f.SyncConfig
	}

	if f.MaxBatchSize != 0 {
		existing.MaxBatchSize = f.MaxBatchSize
	}

	cp := *existing

	return &cp, nil
}
