package appsync

import (
	"fmt"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// isValidDataSourceType returns true if the given data source type is valid.
func isValidDataSourceType(t DataSourceType) bool {
	switch t {
	case DataSourceTypeNone, DataSourceTypeLambda, DataSourceTypeDynamoDB,
		DataSourceTypeHTTP, DataSourceTypeOpenSearch, DataSourceTypeRelational,
		DataSourceTypeEventBridge:
		return true
	default:
		return false
	}
}

// CreateDataSource creates a data source for an API.
func (b *InMemoryBackend) CreateDataSource(apiID string, ds *DataSource) (*DataSource, error) {
	b.mu.Lock("CreateDataSource")
	defer b.mu.Unlock()

	if !b.apis.Has(apiID) {
		return nil, fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	if ds.Name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}

	if ds.Type == "" {
		return nil, fmt.Errorf("%w: type is required", ErrValidation)
	}

	if !isValidDataSourceType(ds.Type) {
		return nil, fmt.Errorf("%w: invalid type %q", ErrValidation, ds.Type)
	}

	if ds.Type == DataSourceTypeHTTP && (ds.HTTPConfig == nil || ds.HTTPConfig.Endpoint == "") {
		return nil, fmt.Errorf("%w: httpConfig.endpoint is required for HTTP data sources", ErrValidation)
	}

	if b.datasources.Has(datasourceKey(apiID, ds.Name)) {
		return nil, fmt.Errorf("%w: datasource %s already exists", ErrAlreadyExists, ds.Name)
	}

	ds.APIID = apiID
	ds.DataSourceARN = arn.Build(
		"appsync",
		b.region,
		b.accountID,
		fmt.Sprintf("apis/%s/datasources/%s", apiID, ds.Name),
	)

	if ds.Tags == nil {
		ds.Tags = tags.New("appsync.ds." + apiID + "." + ds.Name + ".tags")
	}

	b.datasources.Put(ds)

	cp := *ds

	return &cp, nil
}

// GetDataSource returns a data source by API ID and name.
func (b *InMemoryBackend) GetDataSource(apiID, name string) (*DataSource, error) {
	b.mu.RLock("GetDataSource")
	defer b.mu.RUnlock()

	if !b.apis.Has(apiID) {
		return nil, fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	ds, ok := b.datasources.Get(datasourceKey(apiID, name))
	if !ok {
		return nil, fmt.Errorf("%w: datasource %s not found", ErrNotFound, name)
	}

	cp := *ds

	return &cp, nil
}

// ListDataSources returns all data sources for an API.
func (b *InMemoryBackend) ListDataSources(apiID string) ([]*DataSource, error) {
	b.mu.RLock("ListDataSources")
	defer b.mu.RUnlock()

	if !b.apis.Has(apiID) {
		return nil, fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	dss := b.datasourcesByAPI.Get(apiID)
	out := make([]*DataSource, 0, len(dss))

	for _, ds := range dss {
		cp := *ds
		out = append(out, &cp)
	}

	slices.SortFunc(out, func(a, b *DataSource) int {
		return strings.Compare(a.Name, b.Name)
	})

	return out, nil
}

// DeleteDataSource deletes a data source.
// Returns an error if any resolver in the API still references this data source.
func (b *InMemoryBackend) DeleteDataSource(apiID, name string) error {
	b.mu.Lock("DeleteDataSource")
	defer b.mu.Unlock()

	ds, ok := b.datasources.Get(datasourceKey(apiID, name))
	if !ok {
		return fmt.Errorf("%w: datasource %s not found", ErrNotFound, name)
	}

	// Prevent deletion if any UNIT resolver references this data source.
	for _, r := range b.resolversByAPI.Get(apiID) {
		if r.DataSourceName == name {
			return fmt.Errorf(
				"%w: data source %s is still referenced by resolver %s.%s",
				ErrValidation,
				name,
				r.TypeName,
				r.FieldName,
			)
		}
	}

	// Prevent deletion if any function references this data source.
	for _, fn := range b.functionsByAPI.Get(apiID) {
		if fn.DataSourceName == name {
			return fmt.Errorf(
				"%w: data source %s is still referenced by function %s",
				ErrValidation,
				name,
				fn.Name,
			)
		}
	}

	b.datasources.Delete(datasourceKey(apiID, name))

	if ds.Tags != nil {
		ds.Tags.Close()
	}

	return nil
}

// UpdateDataSource updates an existing data source.
func (b *InMemoryBackend) UpdateDataSource(apiID, name string, ds *DataSource) (*DataSource, error) {
	b.mu.Lock("UpdateDataSource")
	defer b.mu.Unlock()

	existing, ok := b.datasources.Get(datasourceKey(apiID, name))
	if !ok {
		return nil, fmt.Errorf("%w: data source %s not found", ErrNotFound, name)
	}

	if ds.Description != "" {
		existing.Description = ds.Description
	}

	if ds.Type != "" {
		existing.Type = ds.Type
	}

	if ds.ServiceRoleARN != "" {
		existing.ServiceRoleARN = ds.ServiceRoleARN
	}

	if ds.LambdaConfig != nil {
		existing.LambdaConfig = ds.LambdaConfig
	}

	if ds.DynamoDBConfig != nil {
		existing.DynamoDBConfig = ds.DynamoDBConfig
	}

	if ds.HTTPConfig != nil {
		existing.HTTPConfig = ds.HTTPConfig
	}

	if ds.OpenSearchConfig != nil {
		existing.OpenSearchConfig = ds.OpenSearchConfig
	}

	if ds.EventBridgeConfig != nil {
		existing.EventBridgeConfig = ds.EventBridgeConfig
	}

	if ds.RelationalDatabaseConfig != nil {
		existing.RelationalDatabaseConfig = ds.RelationalDatabaseConfig
	}

	cp := *existing

	return &cp, nil
}

// StartDataSourceIntrospection starts an introspection job for a data source.
// Returns an introspection ID that can be polled via GetDataSourceIntrospection.
func (b *InMemoryBackend) StartDataSourceIntrospection(apiID, dataSourceName string) (string, error) {
	b.mu.RLock("StartDataSourceIntrospection")
	defer b.mu.RUnlock()

	if !b.apis.Has(apiID) {
		return "", fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	if !b.datasources.Has(datasourceKey(apiID, dataSourceName)) {
		return "", fmt.Errorf("%w: datasource %s not found", ErrNotFound, dataSourceName)
	}

	id := randomAPIID()

	return id, nil
}

// GetDataSourceIntrospection returns the result of a data source introspection job.
// Since introspection is a no-op stub, this always returns a COMPLETED result.
func (b *InMemoryBackend) GetDataSourceIntrospection(introspectionID string) (*DataSourceIntrospectionResult, error) {
	if introspectionID == "" {
		return nil, fmt.Errorf("%w: introspectionId is required", ErrValidation)
	}

	return &DataSourceIntrospectionResult{
		IntrospectionID: introspectionID,
		Status:          "SUCCESS",
		Models:          []any{},
	}, nil
}
