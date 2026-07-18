package fis

import (
	"fmt"
	"slices"
	"strings"
)

// ----------------------------------------
// Target resource type discovery
// ----------------------------------------

// ListTargetResourceTypes returns all known target resource types.
func (b *InMemoryBackend) ListTargetResourceTypes() []TargetResourceTypeSummary {
	b.mu.RLock("ListTargetResourceTypes")
	providers := b.actionProviders
	b.mu.RUnlock()

	seen := make(map[string]TargetResourceTypeSummary)

	// Built-in types.
	for _, rt := range builtinTargetResourceTypes() {
		seen[rt.ResourceType] = rt
	}

	// From action providers.
	for _, p := range providers {
		for _, def := range p.FISActions() {
			if def.TargetType == "" {
				continue
			}

			if _, exists := seen[def.TargetType]; !exists {
				seen[def.TargetType] = TargetResourceTypeSummary{
					ResourceType: def.TargetType,
				}
			}
		}
	}

	result := make([]TargetResourceTypeSummary, 0, len(seen))
	for _, rt := range seen {
		result = append(result, rt)
	}

	slices.SortFunc(
		result,
		func(a, b TargetResourceTypeSummary) int { return strings.Compare(a.ResourceType, b.ResourceType) },
	)

	return result
}

// GetTargetResourceType returns a single target resource type by resource type string.
func (b *InMemoryBackend) GetTargetResourceType(resourceType string) (*TargetResourceTypeSummary, error) {
	all := b.ListTargetResourceTypes()

	for _, rt := range all {
		if rt.ResourceType == resourceType {
			cp := rt

			return &cp, nil
		}
	}

	return nil, fmt.Errorf("%w: %s", ErrTargetResourceTypeNotFound, resourceType)
}
