package appsync

import (
	"cmp"
	"context"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// maxIntrospectionJobs bounds the stored introspection jobs; the oldest are dropped first.
const maxIntrospectionJobs = 100

// isValidDataSourceType returns true if the given data source type is valid.
func isValidDataSourceType(t DataSourceType) bool {
	switch t {
	case DataSourceTypeNone, DataSourceTypeLambda, DataSourceTypeDynamoDB,
		DataSourceTypeHTTP, DataSourceTypeOpenSearch, DataSourceTypeRelational,
		DataSourceTypeEventBridge, DataSourceTypeElasticsearch:
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

	if !dataSourceNamePattern.MatchString(ds.Name) {
		return nil, fmt.Errorf("%w: data source name must match [_A-Za-z][_0-9A-Za-z]* (max 65 characters)",
			ErrValidation)
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
	release, err := b.removeDataSource(apiID, name)
	if err != nil || release == "" {
		return err
	}

	b.mu.RLock("DeleteDataSource")
	requester := b.osRequester
	b.mu.RUnlock()

	if r, ok := requester.(OpenSearchReleaser); ok {
		r.ReleaseOpenSearch(context.Background(), release)
	}

	return nil
}

func (b *InMemoryBackend) removeDataSource(apiID, name string) (string, error) {
	b.mu.Lock("DeleteDataSource")
	defer b.mu.Unlock()

	ds, ok := b.datasources.Get(datasourceKey(apiID, name))
	if !ok {
		return "", fmt.Errorf("%w: datasource %s not found", ErrNotFound, name)
	}

	// Prevent deletion if any UNIT resolver references this data source.
	for _, r := range b.resolversByAPI.Get(apiID) {
		if r.DataSourceName == name {
			return "", fmt.Errorf(
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
			return "", fmt.Errorf(
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

	return b.unusedSearchEndpoint(ds), nil
}

// unusedSearchEndpoint returns the search endpoint of ds when no remaining data source uses it.
func (b *InMemoryBackend) unusedSearchEndpoint(ds *DataSource) string {
	endpoint := searchEndpoint(ds)
	if endpoint == "" {
		return ""
	}

	for _, other := range b.datasources.All() {
		if searchEndpoint(other) == endpoint {
			return ""
		}
	}

	return endpoint
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

	if ds.ElasticsearchConfig != nil {
		existing.ElasticsearchConfig = ds.ElasticsearchConfig
	}

	if ds.EventBridgeConfig != nil {
		existing.EventBridgeConfig = ds.EventBridgeConfig
	}

	if ds.RelationalDatabaseConfig != nil {
		existing.RelationalDatabaseConfig = ds.RelationalDatabaseConfig
	}

	if ds.MetricsConfig != "" {
		existing.MetricsConfig = ds.MetricsConfig
	}

	cp := *existing

	return &cp, nil
}

// StartDataSourceIntrospection introspects the RDS cluster named by cfg through the RDS Data API and records the
// outcome; unlike other Create/Start operations it is not scoped to an API or data source. The job's final status
// is read back with GetDataSourceIntrospection.
func (b *InMemoryBackend) StartDataSourceIntrospection(
	ctx context.Context, cfg *RDSDataAPIConfig,
) (*DataSourceIntrospection, error) {
	if cfg == nil {
		return nil, fmt.Errorf("%w: rdsDataApiConfig is required", ErrValidation)
	}

	if cfg.ResourceARN == "" || cfg.SecretARN == "" || cfg.DatabaseName == "" {
		return nil, fmt.Errorf(
			"%w: rdsDataApiConfig.resourceArn, secretArn and databaseName are required", ErrValidation,
		)
	}

	rec := &DataSourceIntrospection{
		IntrospectionID:     randomAPIID(),
		IntrospectionStatus: DataSourceIntrospectionStatusSuccess,
		RDSDataAPIConfig:    cfg,
	}

	models, err := b.introspect(withRegion(ctx, b.region), cfg)
	if err != nil {
		rec.IntrospectionStatus, rec.IntrospectionStatusDetail = DataSourceIntrospectionStatusFailed, err.Error()
	} else {
		rec.IntrospectionResult = &DataSourceIntrospectionResult{Models: models}
	}

	b.mu.Lock("StartDataSourceIntrospection")
	rec.CreatedAt = b.clock().UnixNano()
	b.introspections.Put(rec)
	b.pruneIntrospections()
	b.mu.Unlock()

	cp := *rec
	cp.IntrospectionStatus = DataSourceIntrospectionStatusProcessing
	cp.IntrospectionStatusDetail = ""
	cp.IntrospectionResult = nil

	return &cp, nil
}

func (b *InMemoryBackend) clock() time.Time {
	if b.now != nil {
		return b.now()
	}

	return time.Now()
}

// pruneIntrospections drops the oldest jobs beyond maxIntrospectionJobs; the caller holds b.mu.
func (b *InMemoryBackend) pruneIntrospections() {
	excess := b.introspections.Len() - maxIntrospectionJobs
	if excess <= 0 {
		return
	}

	jobs := b.introspections.All()
	slices.SortFunc(jobs, func(x, y *DataSourceIntrospection) int {
		return cmp.Or(cmp.Compare(x.CreatedAt, y.CreatedAt), cmp.Compare(x.IntrospectionID, y.IntrospectionID))
	})

	for _, j := range jobs[:excess] {
		b.introspections.Delete(j.IntrospectionID)
	}
}

func (b *InMemoryBackend) introspect(
	ctx context.Context, cfg *RDSDataAPIConfig,
) ([]*DataSourceIntrospectionModel, error) {
	b.mu.RLock("introspect")
	exec := b.rdsExec
	b.mu.RUnlock()

	if exec == nil {
		return nil, ErrRDSNotConfigured
	}

	return introspectRDS(ctx, exec, cfg)
}

// GetDataSourceIntrospection returns the persisted record of a previously started
// introspection job.
func (b *InMemoryBackend) GetDataSourceIntrospection(introspectionID string) (*DataSourceIntrospection, error) {
	if introspectionID == "" {
		return nil, fmt.Errorf("%w: introspectionId is required", ErrValidation)
	}

	b.mu.RLock("GetDataSourceIntrospection")
	defer b.mu.RUnlock()

	rec, ok := b.introspections.Get(introspectionID)
	if !ok {
		return nil, fmt.Errorf("%w: introspection %s not found", ErrNotFound, introspectionID)
	}

	cp := *rec

	return &cp, nil
}

var dataSourceNamePattern = regexp.MustCompile(`^[_A-Za-z][_0-9A-Za-z]{0,64}$`)
