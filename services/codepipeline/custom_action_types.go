package codepipeline

import (
	"context"
	"fmt"
	"maps"
	"sort"
)

// CreateCustomActionType stores a new custom action type.
func (b *InMemoryBackend) CreateCustomActionType(
	ctx context.Context,
	cat *CustomActionType,
) (*CustomActionType, error) {
	b.mu.Lock("CreateCustomActionType")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	catKey := customActionTypeKey{Category: cat.Category, Provider: cat.Provider, Version: cat.Version}
	key := regionKey(region, catKey.String())

	if b.customActionTypes.Has(key) {
		return nil, fmt.Errorf("%w: custom action type %q/%q/%q already exists",
			ErrAlreadyExists, cat.Category, cat.Provider, cat.Version)
	}

	if cat.Owner == "" {
		cat.Owner = keyOwnerCustom
	}

	cp := copyCustomActionType(cat)
	cp.region = region
	b.customActionTypes.Put(cp)

	return copyCustomActionType(cp), nil
}

// DeleteCustomActionType removes a custom action type.
// Returns ResourceInUseException if any pipeline references the type.
func (b *InMemoryBackend) DeleteCustomActionType(ctx context.Context, category, provider, version string) error {
	b.mu.Lock("DeleteCustomActionType")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	key := regionKey(region, customActionTypeKey{Category: category, Provider: provider, Version: version}.String())

	if !b.customActionTypes.Has(key) {
		return fmt.Errorf("%w: custom action type %q/%q/%q", ErrActionTypeNotFound, category, provider, version)
	}

	// Check that no pipeline references this action type.
	for _, p := range b.pipelinesByRegion.Get(region) {
		for _, stage := range p.Declaration.Stages {
			for _, action := range stage.Actions {
				at := action.ActionTypeID
				if at.Category == category && at.Provider == provider && at.Version == version {
					return fmt.Errorf("%w: action type %q/%q/%q is in use by pipeline %q",
						ErrResourceInUse, category, provider, version, p.Declaration.Name)
				}
			}
		}
	}

	b.customActionTypes.Delete(key)

	return nil
}

// GetActionType retrieves a custom action type.
func (b *InMemoryBackend) GetActionType(
	ctx context.Context,
	category, owner, provider, version string,
) (*CustomActionType, error) {
	b.mu.RLock("GetActionType")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	key := regionKey(region, customActionTypeKey{Category: category, Provider: provider, Version: version}.String())

	cat, ok := b.customActionTypes.Get(key)
	if !ok {
		return nil, fmt.Errorf("%w: action type %q/%q/%q/%q", ErrActionTypeNotFound, category, owner, provider, version)
	}

	return copyCustomActionType(cat), nil
}

// AddCustomActionTypeInternal seeds a custom action type into the backend's default region (for testing).
func (b *InMemoryBackend) AddCustomActionTypeInternal(cat *CustomActionType) {
	b.mu.Lock("AddCustomActionTypeInternal")
	defer b.mu.Unlock()

	cp := copyCustomActionType(cat)
	cp.region = b.region
	b.customActionTypes.Put(cp)
}

func copyCustomActionType(c *CustomActionType) *CustomActionType {
	cp := *c

	if c.Tags != nil {
		cp.Tags = make(map[string]string, len(c.Tags))
		maps.Copy(cp.Tags, c.Tags)
	}

	if c.ConfigurationProperties != nil {
		cp.ConfigurationProperties = make([]ActionConfigurationProperty, len(c.ConfigurationProperties))
		copy(cp.ConfigurationProperties, c.ConfigurationProperties)
	}

	if c.Settings != nil {
		s := *c.Settings
		cp.Settings = &s
	}

	return &cp
}

// UpdateActionType updates an action type definition with full fields.
func (b *InMemoryBackend) UpdateActionType(ctx context.Context, cat *CustomActionType) error {
	b.mu.Lock("UpdateActionType")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	key := regionKey(region, customActionTypeKey{
		Category: cat.Category,
		Provider: cat.Provider,
		Version:  cat.Version,
	}.String())

	if !b.customActionTypes.Has(key) {
		return ErrActionTypeNotFound
	}

	cp := copyCustomActionType(cat)
	cp.region = region
	b.customActionTypes.Put(cp)

	return nil
}

// ListActionTypes returns all registered action types in the request region.
func (b *InMemoryBackend) ListActionTypes(ctx context.Context) []*CustomActionType {
	b.mu.RLock("ListActionTypes")
	defer b.mu.RUnlock()

	entries := b.customActionTypesByRegion.Get(getRegion(ctx, b.region))

	result := make([]*CustomActionType, 0, len(entries))

	for _, cat := range entries {
		result = append(result, copyCustomActionType(cat))
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Provider < result[j].Provider
	})

	return result
}
