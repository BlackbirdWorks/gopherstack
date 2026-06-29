package glue

import "fmt"

// DeleteSchemaVersion removes a single schema version from the store.
// Returns ErrNotFound if the schema or version does not exist.
func (b *InMemoryBackend) DeleteSchemaVersion(
	registryName, schemaName string,
	versionNumber int64,
) error {
	b.mu.Lock("DeleteSchemaVersion")
	defer b.mu.Unlock()

	s, ok := b.schemas[schemaKey(registryName, schemaName)]
	if !ok {
		return fmt.Errorf("schema %q/%q not found: %w", registryName, schemaName, ErrNotFound)
	}

	listKey := schemaVersionListKey(s.SchemaARN)
	versions := b.schemaVersions[listKey]

	for i, sv := range versions {
		if sv.VersionNumber == versionNumber {
			b.schemaVersions[listKey] = append(versions[:i], versions[i+1:]...)

			return nil
		}
	}

	return fmt.Errorf("schema version %d not found: %w", versionNumber, ErrNotFound)
}

// DeleteIntegrationResourceProperty removes stored integration resource
// properties for resourceArn.  Returns ErrNotFound if none exist.
func (b *InMemoryBackend) DeleteIntegrationResourceProperty(resourceArn string) error {
	if resourceArn == "" {
		return fmt.Errorf("%w: ResourceArn is required", ErrValidation)
	}

	b.mu.Lock("DeleteIntegrationResourceProperty")
	defer b.mu.Unlock()

	if _, ok := b.integrationResourceProps[resourceArn]; !ok {
		return fmt.Errorf("resource property for %q not found: %w", resourceArn, ErrNotFound)
	}

	delete(b.integrationResourceProps, resourceArn)

	return nil
}

// DeleteIntegrationTableProperties removes stored integration table properties
// for the given resource ARN and table name.  Returns ErrNotFound if none exist.
func (b *InMemoryBackend) DeleteIntegrationTableProperties(resourceArn, tableName string) error {
	if resourceArn == "" || tableName == "" {
		return fmt.Errorf("%w: ResourceArn and TableName are required", ErrValidation)
	}

	b.mu.Lock("DeleteIntegrationTableProperties")
	defer b.mu.Unlock()

	key := resourceArn + "|" + tableName
	if _, ok := b.integrationTableProps[key]; !ok {
		return fmt.Errorf(
			"table property for %q/%q not found: %w",
			resourceArn,
			tableName,
			ErrNotFound,
		)
	}

	delete(b.integrationTableProps, key)

	return nil
}
