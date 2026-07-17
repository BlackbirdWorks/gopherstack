package appsync

import (
	"fmt"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// resolverKindUnit is the "UNIT" resolver kind.
const resolverKindUnit = "UNIT"

// resolverKindPipeline is the "PIPELINE" resolver kind.
const resolverKindPipeline = "PIPELINE"

// isValidResolverKind returns true if the given resolver kind is valid.
func isValidResolverKind(kind string) bool {
	return kind == "" || kind == resolverKindUnit || kind == resolverKindPipeline
}

// resolverKey builds the map key for a resolver.
func resolverKey(typeName, fieldName string) string {
	return typeName + "." + fieldName
}

// CreateResolver creates a resolver for an API type field.
func (b *InMemoryBackend) CreateResolver(apiID, typeName string, r *Resolver) (*Resolver, error) {
	b.mu.Lock("CreateResolver")
	defer b.mu.Unlock()

	if !b.apis.Has(apiID) {
		return nil, fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	if r.FieldName == "" {
		return nil, fmt.Errorf("%w: fieldName is required", ErrValidation)
	}

	if !isValidResolverKind(r.Kind) {
		return nil, fmt.Errorf("%w: invalid kind %q, must be UNIT or PIPELINE", ErrValidation, r.Kind)
	}

	if r.Kind == resolverKindPipeline && len(r.PipelineConfig) == 0 {
		return nil, fmt.Errorf("%w: pipelineConfig is required for PIPELINE resolvers", ErrValidation)
	}

	if (r.Kind == "" || r.Kind == resolverKindUnit) && r.DataSourceName == "" {
		return nil, fmt.Errorf("%w: dataSourceName is required for UNIT resolvers", ErrValidation)
	}

	key := resolverTableKey(apiID, typeName, r.FieldName)
	if b.resolvers.Has(key) {
		return nil, fmt.Errorf("%w: resolver %s.%s already exists", ErrAlreadyExists, typeName, r.FieldName)
	}

	r.APIID = apiID
	r.TypeName = typeName
	r.ResolverARN = arn.Build("appsync", b.region, b.accountID,
		fmt.Sprintf("apis/%s/types/%s/resolvers/%s", apiID, typeName, r.FieldName))

	if r.Kind == "" {
		r.Kind = resolverKindUnit
	}

	b.resolvers.Put(r)

	cp := *r

	return &cp, nil
}

// GetResolver returns a resolver by API ID, type, and field name.
func (b *InMemoryBackend) GetResolver(apiID, typeName, fieldName string) (*Resolver, error) {
	b.mu.RLock("GetResolver")
	defer b.mu.RUnlock()

	if !b.apis.Has(apiID) {
		return nil, fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	r, ok := b.resolvers.Get(resolverTableKey(apiID, typeName, fieldName))
	if !ok {
		return nil, fmt.Errorf("%w: resolver %s.%s not found", ErrNotFound, typeName, fieldName)
	}

	cp := *r

	return &cp, nil
}

// ListResolvers returns all resolvers for an API type.
func (b *InMemoryBackend) ListResolvers(apiID, typeName string) ([]*Resolver, error) {
	b.mu.RLock("ListResolvers")
	defer b.mu.RUnlock()

	if !b.apis.Has(apiID) {
		return nil, fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	res := b.resolversByAPI.Get(apiID)
	out := make([]*Resolver, 0, len(res))

	for _, r := range res {
		if r.TypeName == typeName {
			cp := *r
			out = append(out, &cp)
		}
	}

	slices.SortFunc(out, func(a, b *Resolver) int {
		return strings.Compare(a.FieldName, b.FieldName)
	})

	return out, nil
}

// DeleteResolver deletes a resolver.
func (b *InMemoryBackend) DeleteResolver(apiID, typeName, fieldName string) error {
	b.mu.Lock("DeleteResolver")
	defer b.mu.Unlock()

	if !b.apis.Has(apiID) {
		return fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	key := resolverTableKey(apiID, typeName, fieldName)
	if !b.resolvers.Has(key) {
		return fmt.Errorf("%w: resolver %s.%s not found", ErrNotFound, typeName, fieldName)
	}

	b.resolvers.Delete(key)

	return nil
}

// UpdateResolver updates an existing resolver.
func (b *InMemoryBackend) UpdateResolver(apiID, typeName string, r *Resolver) (*Resolver, error) {
	b.mu.Lock("UpdateResolver")
	defer b.mu.Unlock()

	key := typeName + "." + r.FieldName

	existing, ok := b.resolvers.Get(resolverTableKey(apiID, typeName, r.FieldName))
	if !ok {
		return nil, fmt.Errorf("%w: resolver %s not found", ErrNotFound, key)
	}

	if r.RequestMappingTemplate != "" {
		existing.RequestMappingTemplate = r.RequestMappingTemplate
	}

	if r.ResponseMappingTemplate != "" {
		existing.ResponseMappingTemplate = r.ResponseMappingTemplate
	}

	if r.DataSourceName != "" {
		existing.DataSourceName = r.DataSourceName
	}

	if r.Kind != "" {
		existing.Kind = r.Kind
	}

	if len(r.PipelineConfig) > 0 {
		existing.PipelineConfig = r.PipelineConfig
	}

	if r.CachingConfig != nil {
		existing.CachingConfig = r.CachingConfig
	}

	if r.SyncConfig != nil {
		existing.SyncConfig = r.SyncConfig
	}

	if r.MaxBatchSize != 0 {
		existing.MaxBatchSize = r.MaxBatchSize
	}

	if r.Code != "" {
		existing.Code = r.Code
	}

	if r.Runtime != nil {
		existing.Runtime = r.Runtime
	}

	cp := *existing

	return &cp, nil
}

// ListResolversByFunction returns all resolvers that use a given function.
func (b *InMemoryBackend) ListResolversByFunction(apiID, functionID string) ([]*Resolver, error) {
	b.mu.RLock("ListResolversByFunction")
	defer b.mu.RUnlock()

	if !b.apis.Has(apiID) {
		return nil, fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	var out []*Resolver

	for _, r := range b.resolversByAPI.Get(apiID) {
		if slices.Contains(r.PipelineConfig, functionID) {
			cp := *r
			out = append(out, &cp)
		}
	}

	slices.SortFunc(out, func(a, b *Resolver) int {
		key := func(r *Resolver) string { return r.TypeName + "." + r.FieldName }

		return strings.Compare(key(a), key(b))
	})

	return out, nil
}
