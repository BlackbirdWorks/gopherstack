package eventbridge

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	defaultSchemaVersion = "1"

	// schemaTypeOpenAPI3 and schemaTypeJSONSchemaDraft4 are the only valid schema types AWS accepts.
	schemaTypeOpenAPI3         = "OpenApi3"
	schemaTypeJSONSchemaDraft4 = "JSONSchemaDraft4"
)

// CreateSchema creates a new schema (version "1") within a registry.
func (b *InMemoryBackend) CreateSchema(
	ctx context.Context, //nolint:revive // existing issue.
	input CreateSchemaInput,
) (*Schema, error) {
	if input.RegistryName == "" {
		return nil, fmt.Errorf("%w: RegistryName is required", ErrInvalidParameter)
	}

	if input.SchemaName == "" {
		return nil, fmt.Errorf("%w: SchemaName is required", ErrInvalidParameter)
	}

	if input.Type == "" {
		return nil, fmt.Errorf("%w: Type is required", ErrInvalidParameter)
	}

	if input.Type != schemaTypeOpenAPI3 && input.Type != schemaTypeJSONSchemaDraft4 {
		return nil, fmt.Errorf(
			"%w: Type must be %s or %s, got %s",
			ErrInvalidParameter,
			schemaTypeOpenAPI3,
			schemaTypeJSONSchemaDraft4,
			input.Type,
		)
	}

	if input.Content == "" {
		return nil, fmt.Errorf("%w: Content is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateSchema")
	defer b.mu.Unlock()

	if !b.registriesTable().Has(input.RegistryName) {
		return nil, fmt.Errorf("%w: registry %s not found", ErrNotFound, input.RegistryName)
	}

	schemaTable := b.schemasTableFor(input.RegistryName)

	if schemaTable.Has(input.SchemaName) {
		return nil, fmt.Errorf(
			"%w: schema %s already exists in registry %s",
			ErrAlreadyExists,
			input.SchemaName,
			input.RegistryName,
		)
	}

	now := time.Now()
	schema := &Schema{
		SchemaArn:          b.schemaARN(input.RegistryName, input.SchemaName),
		SchemaName:         input.SchemaName,
		SchemaVersion:      defaultSchemaVersion,
		RegistryName:       input.RegistryName,
		Description:        input.Description,
		Type:               input.Type,
		Content:            input.Content,
		LastModified:       now,
		VersionCreatedDate: now,
		Tags:               input.Tags,
	}
	schemaTable.Put(schema)

	// Record version 1.
	versionKey := b.schemaVersionKey(input.RegistryName, input.SchemaName)
	sv := &SchemaVersion{
		SchemaArn:     schema.SchemaArn,
		SchemaName:    input.SchemaName,
		SchemaVersion: defaultSchemaVersion,
		RegistryName:  input.RegistryName,
		Type:          input.Type,
		Content:       input.Content,
		CreatedDate:   now,
	}
	b.schemaVersions[versionKey] = []*SchemaVersion{sv}

	cp := *schema

	return &cp, nil
}

// DeleteSchema deletes a schema and all its versions.
func (b *InMemoryBackend) DeleteSchema(
	ctx context.Context, //nolint:revive // existing issue.
	registryName, schemaName string,
) error {
	if registryName == "" {
		return fmt.Errorf("%w: RegistryName is required", ErrInvalidParameter)
	}

	if schemaName == "" {
		return fmt.Errorf("%w: SchemaName is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteSchema")
	defer b.mu.Unlock()

	if !b.registriesTable().Has(registryName) {
		return fmt.Errorf("%w: registry %s not found", ErrNotFound, registryName)
	}

	if _, ok := b.getSchema(registryName, schemaName); !ok {
		return fmt.Errorf(
			"%w: schema %s not found in registry %s",
			ErrNotFound,
			schemaName,
			registryName,
		)
	}

	if t := b.schemas[registryName]; t != nil {
		t.Delete(schemaName)
	}

	versionKey := b.schemaVersionKey(registryName, schemaName)
	delete(b.schemaVersions, versionKey)

	// Remove all code bindings for this schema.
	for key := range b.codeBindings {
		if strings.HasPrefix(key, registryName+"/"+schemaName+"/") {
			delete(b.codeBindings, key)
		}
	}

	return nil
}

// DescribeSchema returns the current (or requested version of) a schema.
func (b *InMemoryBackend) DescribeSchema(ctx context.Context, //nolint:revive // existing issue.
	registryName, schemaName, schemaVersion string,
) (*Schema, error) {
	if registryName == "" {
		return nil, fmt.Errorf("%w: RegistryName is required", ErrInvalidParameter)
	}

	if schemaName == "" {
		return nil, fmt.Errorf("%w: SchemaName is required", ErrInvalidParameter)
	}

	b.mu.RLock("DescribeSchema")
	defer b.mu.RUnlock()

	if !b.registriesTable().Has(registryName) {
		return nil, fmt.Errorf("%w: registry %s not found", ErrNotFound, registryName)
	}

	schema, ok := b.getSchema(registryName, schemaName)
	if !ok {
		return nil, fmt.Errorf(
			"%w: schema %s not found in registry %s",
			ErrNotFound,
			schemaName,
			registryName,
		)
	}

	// If a specific version is requested, fetch that version's content.
	if schemaVersion != "" && schemaVersion != schema.SchemaVersion {
		versionKey := b.schemaVersionKey(registryName, schemaName)
		for _, sv := range b.schemaVersions[versionKey] {
			if sv.SchemaVersion == schemaVersion {
				cp := *schema
				cp.SchemaVersion = sv.SchemaVersion
				cp.Content = sv.Content
				cp.Type = sv.Type
				cp.VersionCreatedDate = sv.CreatedDate

				return &cp, nil
			}
		}

		return nil, fmt.Errorf("%w: schema version %s not found", ErrNotFound, schemaVersion)
	}

	cp := *schema

	return &cp, nil
}

// ListSchemas returns schemas in a registry optionally filtered by name prefix.
func (b *InMemoryBackend) ListSchemas(ctx context.Context, //nolint:revive // existing issue.
	registryName, namePrefix, nextToken string,
) ([]Schema, string, error) {
	if registryName == "" {
		return nil, "", fmt.Errorf("%w: RegistryName is required", ErrInvalidParameter)
	}

	b.mu.RLock("ListSchemas")
	defer b.mu.RUnlock()

	if !b.registriesTable().Has(registryName) {
		return nil, "", fmt.Errorf("%w: registry %s not found", ErrNotFound, registryName)
	}

	schemaTable := b.schemas[registryName]
	var schemaAll []*Schema
	if schemaTable != nil {
		schemaAll = schemaTable.All()
	}
	all := make([]Schema, 0, len(schemaAll))
	for _, s := range schemaAll {
		if namePrefix == "" || strings.HasPrefix(s.SchemaName, namePrefix) {
			all = append(all, *s)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].SchemaName < all[j].SchemaName })

	page, outToken := paginate(all, nextToken)

	return page, outToken, nil
}

// SearchSchemas searches schemas in a registry by keyword match against schema name or content.
func (b *InMemoryBackend) SearchSchemas(ctx context.Context, //nolint:revive // existing issue.
	registryName, keywords, nextToken string,
) ([]Schema, string, error) {
	if registryName == "" {
		return nil, "", fmt.Errorf("%w: RegistryName is required", ErrInvalidParameter)
	}

	b.mu.RLock("SearchSchemas")
	defer b.mu.RUnlock()

	if !b.registriesTable().Has(registryName) {
		return nil, "", fmt.Errorf("%w: registry %s not found", ErrNotFound, registryName)
	}

	all := make([]Schema, 0)
	lower := strings.ToLower(keywords)

	var schemaAll []*Schema
	if schemaTable := b.schemas[registryName]; schemaTable != nil {
		schemaAll = schemaTable.All()
	}
	for _, s := range schemaAll {
		if keywords == "" ||
			strings.Contains(strings.ToLower(s.SchemaName), lower) ||
			strings.Contains(strings.ToLower(s.Content), lower) {
			all = append(all, *s)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].SchemaName < all[j].SchemaName })

	page, outToken := paginate(all, nextToken)

	return page, outToken, nil
}

// UpdateSchema creates a new version of an existing schema.
func (b *InMemoryBackend) UpdateSchema(
	ctx context.Context, //nolint:revive // existing issue.
	input UpdateSchemaInput,
) (*Schema, error) {
	if input.RegistryName == "" {
		return nil, fmt.Errorf("%w: RegistryName is required", ErrInvalidParameter)
	}

	if input.SchemaName == "" {
		return nil, fmt.Errorf("%w: SchemaName is required", ErrInvalidParameter)
	}

	b.mu.Lock("UpdateSchema")
	defer b.mu.Unlock()

	if !b.registriesTable().Has(input.RegistryName) {
		return nil, fmt.Errorf("%w: registry %s not found", ErrNotFound, input.RegistryName)
	}

	schema, ok := b.getSchema(input.RegistryName, input.SchemaName)
	if !ok {
		return nil, fmt.Errorf(
			"%w: schema %s not found in registry %s",
			ErrNotFound,
			input.SchemaName,
			input.RegistryName,
		)
	}

	now := time.Now()

	versionKey := b.schemaVersionKey(input.RegistryName, input.SchemaName)
	currentVersions := b.schemaVersions[versionKey]
	newVersionNum := strconv.Itoa(len(currentVersions) + 1)

	// Apply updates.
	if input.Content != "" {
		schema.Content = input.Content
	}

	if input.Type != "" {
		schema.Type = input.Type
	}

	if input.Description != "" {
		schema.Description = input.Description
	}

	schema.SchemaVersion = newVersionNum
	schema.LastModified = now
	schema.VersionCreatedDate = now

	sv := &SchemaVersion{
		SchemaArn:     schema.SchemaArn,
		SchemaName:    input.SchemaName,
		SchemaVersion: newVersionNum,
		RegistryName:  input.RegistryName,
		Type:          schema.Type,
		Content:       schema.Content,
		CreatedDate:   now,
	}
	b.schemaVersions[versionKey] = append(currentVersions, sv)

	cp := *schema

	return &cp, nil
}

// ListSchemaVersions returns all versions of a schema.
func (b *InMemoryBackend) ListSchemaVersions(ctx context.Context, //nolint:revive // existing issue.
	registryName, schemaName, nextToken string,
) ([]SchemaVersion, string, error) {
	if registryName == "" {
		return nil, "", fmt.Errorf("%w: RegistryName is required", ErrInvalidParameter)
	}

	if schemaName == "" {
		return nil, "", fmt.Errorf("%w: SchemaName is required", ErrInvalidParameter)
	}

	b.mu.RLock("ListSchemaVersions")
	defer b.mu.RUnlock()

	if !b.registriesTable().Has(registryName) {
		return nil, "", fmt.Errorf("%w: registry %s not found", ErrNotFound, registryName)
	}

	if _, ok := b.getSchema(registryName, schemaName); !ok {
		return nil, "", fmt.Errorf(
			"%w: schema %s not found in registry %s",
			ErrNotFound,
			schemaName,
			registryName,
		)
	}

	versionKey := b.schemaVersionKey(registryName, schemaName)
	raw := b.schemaVersions[versionKey]
	all := make([]SchemaVersion, len(raw))
	for i, sv := range raw {
		all[i] = *sv
	}

	// Versions are stored in insertion order (ascending version number).
	page, outToken := paginate(all, nextToken)

	return page, outToken, nil
}

// DescribeSchemaVersion returns a specific schema version.
func (b *InMemoryBackend) DescribeSchemaVersion(ctx context.Context, //nolint:revive // existing issue.
	registryName, schemaName, schemaVersion string,
) (*SchemaVersion, error) {
	if registryName == "" {
		return nil, fmt.Errorf("%w: RegistryName is required", ErrInvalidParameter)
	}

	if schemaName == "" {
		return nil, fmt.Errorf("%w: SchemaName is required", ErrInvalidParameter)
	}

	if schemaVersion == "" {
		return nil, fmt.Errorf("%w: SchemaVersion is required", ErrInvalidParameter)
	}

	b.mu.RLock("DescribeSchemaVersion")
	defer b.mu.RUnlock()

	versionKey := b.schemaVersionKey(registryName, schemaName)
	for _, sv := range b.schemaVersions[versionKey] {
		if sv.SchemaVersion == schemaVersion {
			cp := *sv

			return &cp, nil
		}
	}

	return nil, fmt.Errorf(
		"%w: schema version %s not found for %s/%s",
		ErrNotFound,
		schemaVersion,
		registryName,
		schemaName,
	)
}

// DeleteSchemaVersion deletes a specific version of a schema.
// AWS rejects deletion of the last remaining version (BadRequestException).
func (b *InMemoryBackend) DeleteSchemaVersion(ctx context.Context, //nolint:revive // existing issue.
	registryName, schemaName, schemaVersion string,
) error {
	if registryName == "" {
		return fmt.Errorf("%w: RegistryName is required", ErrInvalidParameter)
	}

	if schemaName == "" {
		return fmt.Errorf("%w: SchemaName is required", ErrInvalidParameter)
	}

	if schemaVersion == "" {
		return fmt.Errorf("%w: SchemaVersion is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteSchemaVersion")
	defer b.mu.Unlock()

	versionKey := b.schemaVersionKey(registryName, schemaName)
	versions := b.schemaVersions[versionKey]

	idx := -1
	for i, sv := range versions {
		if sv.SchemaVersion == schemaVersion {
			idx = i

			break
		}
	}

	if idx < 0 {
		return fmt.Errorf(
			"%w: schema version %s not found for %s/%s",
			ErrNotFound,
			schemaVersion,
			registryName,
			schemaName,
		)
	}

	// AWS rejects deletion of the last remaining schema version.
	if len(versions) == 1 {
		return fmt.Errorf(
			"%w: cannot delete the last remaining version of schema %s",
			ErrInvalidParameter,
			schemaName,
		)
	}

	b.schemaVersions[versionKey] = append(versions[:idx], versions[idx+1:]...)

	// If the deleted version was the latest, update the parent schema pointer.
	b.maybeUpdateSchemaAfterVersionDelete(registryName, schemaName, schemaVersion, versionKey)

	return nil
}

// maybeUpdateSchemaAfterVersionDelete updates the parent schema's version pointer when
// the deleted version was the schema's current latest.
func (b *InMemoryBackend) maybeUpdateSchemaAfterVersionDelete(
	registryName, schemaName, schemaVersion, versionKey string,
) {
	schema, ok := b.getSchema(registryName, schemaName)
	if !ok || schema.SchemaVersion != schemaVersion {
		return
	}

	remaining := b.schemaVersions[versionKey]
	if len(remaining) == 0 {
		return
	}

	latest := remaining[len(remaining)-1]
	schema.SchemaVersion = latest.SchemaVersion
	schema.Content = latest.Content
	schema.Type = latest.Type
	schema.VersionCreatedDate = latest.CreatedDate
}

// GetDiscoveredSchema generates a schema skeleton from one or more event JSON strings.
// Returns a minimal OpenApi3 schema template (real schema inference is out of scope).
func (b *InMemoryBackend) GetDiscoveredSchema(
	ctx context.Context, //nolint:revive // existing issue.
	input GetDiscoveredSchemaInput,
) (string, error) {
	if len(input.Events) == 0 {
		return "", fmt.Errorf("%w: at least one event is required", ErrInvalidParameter)
	}

	if input.Type == "" {
		return "", fmt.Errorf("%w: Type is required", ErrInvalidParameter)
	}

	// Return a minimal discoverable schema stub. AWS generates a full schema from
	// the event payload; for in-process emulation a minimal valid skeleton suffices.
	stub := `{"openapi":"3.0.0","info":{"title":"DiscoveredSchema","version":"1.0"},"paths":{}}`

	return stub, nil
}

// PutCodeBinding triggers code binding generation for a schema version.
func (b *InMemoryBackend) PutCodeBinding(
	ctx context.Context, //nolint:revive // existing issue.
	input PutCodeBindingInput,
) (*CodeBinding, error) {
	if input.RegistryName == "" {
		return nil, fmt.Errorf("%w: RegistryName is required", ErrInvalidParameter)
	}

	if input.SchemaName == "" {
		return nil, fmt.Errorf("%w: SchemaName is required", ErrInvalidParameter)
	}

	if input.Language == "" {
		return nil, fmt.Errorf("%w: Language is required", ErrInvalidParameter)
	}

	b.mu.Lock("PutCodeBinding")
	defer b.mu.Unlock()

	if !b.registriesTable().Has(input.RegistryName) {
		return nil, fmt.Errorf("%w: registry %s not found", ErrNotFound, input.RegistryName)
	}

	schema, ok := b.getSchema(input.RegistryName, input.SchemaName)
	if !ok {
		return nil, fmt.Errorf(
			"%w: schema %s not found in registry %s",
			ErrNotFound,
			input.SchemaName,
			input.RegistryName,
		)
	}

	schemaVer := input.SchemaVersion
	if schemaVer == "" {
		schemaVer = schema.SchemaVersion
	}

	now := time.Now()
	binding := &CodeBinding{
		CreationDate:  now,
		LastModified:  now,
		Language:      input.Language,
		SchemaVersion: schemaVer,
		Status:        "CREATE_COMPLETE",
	}

	key := b.codeBindingKey(input.RegistryName, input.SchemaName, input.Language)
	b.codeBindings[key] = binding

	cp := *binding

	return &cp, nil
}

// DescribeCodeBinding returns the status of a code binding.
func (b *InMemoryBackend) DescribeCodeBinding(ctx context.Context, //nolint:revive // existing issue.
	input DescribeCodeBindingInput,
) (*CodeBinding, error) {
	if input.RegistryName == "" {
		return nil, fmt.Errorf("%w: RegistryName is required", ErrInvalidParameter)
	}

	if input.SchemaName == "" {
		return nil, fmt.Errorf("%w: SchemaName is required", ErrInvalidParameter)
	}

	if input.Language == "" {
		return nil, fmt.Errorf("%w: Language is required", ErrInvalidParameter)
	}

	b.mu.RLock("DescribeCodeBinding")
	defer b.mu.RUnlock()

	key := b.codeBindingKey(input.RegistryName, input.SchemaName, input.Language)
	binding, exists := b.codeBindings[key]
	if !exists {
		return nil, fmt.Errorf("%w: code binding for %s/%s language=%s not found",
			ErrNotFound, input.RegistryName, input.SchemaName, input.Language)
	}

	cp := *binding

	return &cp, nil
}

// ListCodeBindings returns all code bindings for a given schema (optionally filtered by version).
func (b *InMemoryBackend) ListCodeBindings(ctx context.Context, //nolint:revive // existing issue.
	input ListCodeBindingsInput,
) ([]CodeBinding, string, error) {
	if input.RegistryName == "" {
		return nil, "", fmt.Errorf("%w: RegistryName is required", ErrInvalidParameter)
	}

	if input.SchemaName == "" {
		return nil, "", fmt.Errorf("%w: SchemaName is required", ErrInvalidParameter)
	}

	b.mu.RLock("ListCodeBindings")
	defer b.mu.RUnlock()

	prefix := input.RegistryName + "/" + input.SchemaName + "/"
	all := make([]CodeBinding, 0)

	for key, cb := range b.codeBindings {
		if !strings.HasPrefix(key, prefix) {
			continue
		}

		if input.SchemaVersion != "" && cb.SchemaVersion != input.SchemaVersion {
			continue
		}

		all = append(all, *cb)
	}

	sort.Slice(all, func(i, j int) bool { return all[i].Language < all[j].Language })

	page, outToken := paginate(all, input.NextToken)

	return page, outToken, nil
}

// GetCodeBindingSource returns placeholder source code for a generated code binding.
// Real source generation is out of scope for in-process emulation.
func (b *InMemoryBackend) GetCodeBindingSource(ctx context.Context, //nolint:revive // existing issue.
	registryName, schemaName, language, schemaVersion string,
) (string, error) {
	if registryName == "" {
		return "", fmt.Errorf("%w: RegistryName is required", ErrInvalidParameter)
	}

	if schemaName == "" {
		return "", fmt.Errorf("%w: SchemaName is required", ErrInvalidParameter)
	}

	if language == "" {
		return "", fmt.Errorf("%w: Language is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetCodeBindingSource")
	defer b.mu.RUnlock()

	key := b.codeBindingKey(registryName, schemaName, language)
	if _, exists := b.codeBindings[key]; !exists {
		return "", fmt.Errorf("%w: code binding for %s/%s language=%s not found",
			ErrNotFound, registryName, schemaName, language)
	}

	// Return a minimal placeholder; real codegen is AWS-side only.
	src := fmt.Sprintf("// Generated code binding for %s/%s (%s)\n// Schema version: %s\n",
		registryName, schemaName, language, schemaVersion)

	return src, nil
}
