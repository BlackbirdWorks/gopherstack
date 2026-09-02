package eventbridge

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

// builtinRegistryAWSEvents and builtinRegistryDiscoveredSchemas are AWS-managed registries
// that cannot be created or deleted by users.
const (
	builtinRegistryAWSEvents         = "aws.events"
	builtinRegistryDiscoveredSchemas = "discovered-schemas"
)

// isBuiltinRegistry reports whether name is an AWS-managed registry.
func isBuiltinRegistry(name string) bool {
	return name == builtinRegistryAWSEvents || name == builtinRegistryDiscoveredSchemas
}

// CreateRegistry creates a new schema registry.
func (b *InMemoryBackend) CreateRegistry(
	ctx context.Context, //nolint:revive // existing issue.
	input CreateRegistryInput,
) (*SchemaRegistry, error) {
	if input.RegistryName == "" {
		return nil, fmt.Errorf("%w: RegistryName is required", ErrInvalidParameter)
	}

	if isBuiltinRegistry(input.RegistryName) {
		return nil, fmt.Errorf(
			"%w: cannot create registry with reserved name %s",
			ErrForbiddenOperation,
			input.RegistryName,
		)
	}

	b.mu.Lock("CreateRegistry")
	defer b.mu.Unlock()

	if b.registriesTable().Has(input.RegistryName) {
		return nil, fmt.Errorf(
			"%w: registry %s already exists",
			ErrAlreadyExists,
			input.RegistryName,
		)
	}

	reg := &SchemaRegistry{
		RegistryArn:  b.registryARN(input.RegistryName),
		RegistryName: input.RegistryName,
		Description:  input.Description,
		Tags:         input.Tags,
	}
	b.registriesTable().Put(reg)

	cp := *reg

	return &cp, nil
}

// DeleteRegistry deletes a registry and all its schemas and versions.
func (b *InMemoryBackend) DeleteRegistry(
	ctx context.Context, //nolint:revive // existing issue.
	registryName string,
) error {
	if registryName == "" {
		return fmt.Errorf("%w: RegistryName is required", ErrInvalidParameter)
	}

	if isBuiltinRegistry(registryName) {
		return fmt.Errorf(
			"%w: cannot delete built-in registry %s",
			ErrForbiddenOperation,
			registryName,
		)
	}

	b.mu.Lock("DeleteRegistry")
	defer b.mu.Unlock()

	if !b.registriesTable().Has(registryName) {
		return fmt.Errorf("%w: registry %s not found", ErrNotFound, registryName)
	}

	b.registriesTable().Delete(registryName)
	delete(b.schemas, registryName)

	// Remove all version and code binding records for this registry's schemas.
	for key := range b.schemaVersions {
		if strings.HasPrefix(key, registryName+"/") {
			delete(b.schemaVersions, key)
		}
	}

	for key := range b.codeBindings {
		if strings.HasPrefix(key, registryName+"/") {
			delete(b.codeBindings, key)
		}
	}

	return nil
}

// DescribeRegistry returns a single schema registry.
func (b *InMemoryBackend) DescribeRegistry(
	ctx context.Context, //nolint:revive // existing issue.
	registryName string,
) (*SchemaRegistry, error) {
	if registryName == "" {
		return nil, fmt.Errorf("%w: RegistryName is required", ErrInvalidParameter)
	}

	b.mu.RLock("DescribeRegistry")
	defer b.mu.RUnlock()

	reg, exists := b.registriesTable().Get(registryName)
	if !exists {
		return nil, fmt.Errorf("%w: registry %s not found", ErrNotFound, registryName)
	}

	cp := *reg

	return &cp, nil
}

// ListRegistries returns schema registries optionally filtered by name prefix.
func (b *InMemoryBackend) ListRegistries(ctx context.Context, //nolint:revive // existing issue.
	namePrefix, nextToken string, limit int,
) ([]SchemaRegistry, string, error) {
	b.mu.RLock("ListRegistries")
	defer b.mu.RUnlock()

	all := make([]SchemaRegistry, 0, b.registriesTable().Len())
	for _, reg := range b.registriesTable().All() {
		if namePrefix == "" || strings.HasPrefix(reg.RegistryName, namePrefix) {
			all = append(all, *reg)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].RegistryName < all[j].RegistryName })

	page, outToken := paginateN(all, nextToken, limit)

	return page, outToken, nil
}

// UpdateRegistry updates an existing schema registry description.
func (b *InMemoryBackend) UpdateRegistry(
	ctx context.Context, //nolint:revive // existing issue.
	input UpdateRegistryInput,
) (*SchemaRegistry, error) {
	if input.RegistryName == "" {
		return nil, fmt.Errorf("%w: RegistryName is required", ErrInvalidParameter)
	}

	b.mu.Lock("UpdateRegistry")
	defer b.mu.Unlock()

	reg, exists := b.registriesTable().Get(input.RegistryName)
	if !exists {
		return nil, fmt.Errorf("%w: registry %s not found", ErrNotFound, input.RegistryName)
	}

	reg.Description = input.Description

	cp := *reg

	return &cp, nil
}
