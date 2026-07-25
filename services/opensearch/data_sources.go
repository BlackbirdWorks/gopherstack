package opensearch

import (
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// dataSourceStatusActive matches the AWS DataSourceStatus enum's ACTIVE
// value, the status a newly-added data source starts in.
const dataSourceStatusActive = "ACTIVE"

// AddDataSource adds a data source to a domain.
func (b *InMemoryBackend) AddDataSource(
	domainName, name, description string,
	dataSourceType json.RawMessage,
) (string, error) {
	if domainName == "" {
		return "", fmt.Errorf("%w: DomainName is required", ErrInvalidParameter)
	}

	if name == "" {
		return "", fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	b.mu.Lock("AddDataSource")
	defer b.mu.Unlock()

	if !b.domains.Has(domainName) {
		return "", fmt.Errorf("%w: domain %s not found", ErrDomainNotFound, domainName)
	}

	if b.domainDataSources.Has(dataSourceKey(domainName, name)) {
		return "", fmt.Errorf(
			"%w: data source %s already exists on domain %s",
			ErrDataSourceAlreadyExists,
			name,
			domainName,
		)
	}

	b.domainDataSources.Put(&DataSource{
		Name:           name,
		Description:    description,
		DataSourceType: dataSourceType,
		Status:         dataSourceStatusActive,
		DomainName:     domainName,
	})

	return "Data source created successfully", nil
}

// AddDirectQueryDataSource adds a direct-query data source.
func (b *InMemoryBackend) AddDirectQueryDataSource(
	name, description string,
	dataSourceType json.RawMessage,
	openSearchArns []string,
) (string, error) {
	if name == "" {
		return "", fmt.Errorf("%w: DataSourceName is required", ErrInvalidParameter)
	}

	b.mu.Lock("AddDirectQueryDataSource")
	defer b.mu.Unlock()

	if b.directQueryDataSources.Has(name) {
		return "", fmt.Errorf(
			"%w: direct query data source %s already exists",
			ErrDataSourceAlreadyExists,
			name,
		)
	}

	dsARN := arn.Build("opensearch", b.region, b.accountID, "directQueryDataSource/"+name)
	b.directQueryDataSources.Put(&DirectQueryDataSource{
		Name:           name,
		Description:    description,
		DataSourceType: dataSourceType,
		OpenSearchArns: openSearchArns,
		DataSourceArn:  dsARN,
	})

	return dsARN, nil
}

// GetDataSource returns a data source by domain and name.
func (b *InMemoryBackend) GetDataSource(domainName, name string) (*DataSource, error) {
	b.mu.RLock("GetDataSource")
	defer b.mu.RUnlock()

	ds, exists := b.domainDataSources.Get(dataSourceKey(domainName, name))
	if !exists {
		return nil, fmt.Errorf(
			"%w: data source %s not found on domain %s",
			ErrDataSourceNotFound,
			name,
			domainName,
		)
	}

	cp := *ds

	return &cp, nil
}

// ListDataSources returns all data sources for a domain.
func (b *InMemoryBackend) ListDataSources(domainName string) ([]*DataSource, error) {
	b.mu.RLock("ListDataSources")
	defer b.mu.RUnlock()

	group := b.domainDataSourcesByDomain.Get(domainName)
	out := make([]*DataSource, 0, len(group))

	for _, ds := range group {
		cp := *ds
		out = append(out, &cp)
	}

	return out, nil
}

// UpdateDataSource updates a data source's description, type, and/or status.
// Real AWS requires DataSourceType on every update call; Description and
// Status are optional and left unchanged when omitted.
func (b *InMemoryBackend) UpdateDataSource(
	domainName, name, description string,
	dataSourceType json.RawMessage,
	status string,
) error {
	b.mu.Lock("UpdateDataSource")
	defer b.mu.Unlock()

	ds, exists := b.domainDataSources.Get(dataSourceKey(domainName, name))
	if !exists {
		return fmt.Errorf(
			"%w: data source %s not found on domain %s",
			ErrDataSourceNotFound,
			name,
			domainName,
		)
	}

	ds.Description = description

	if len(dataSourceType) > 0 {
		ds.DataSourceType = dataSourceType
	}

	if status != "" {
		ds.Status = status
	}

	return nil
}

// DeleteDataSource removes a data source from a domain.
func (b *InMemoryBackend) DeleteDataSource(domainName, name string) error {
	b.mu.Lock("DeleteDataSource")
	defer b.mu.Unlock()

	b.domainDataSources.Delete(dataSourceKey(domainName, name))

	return nil
}

// ListDirectQueryDataSources returns all direct-query data sources.
func (b *InMemoryBackend) ListDirectQueryDataSources() []*DirectQueryDataSource {
	b.mu.RLock("ListDirectQueryDataSources")
	defer b.mu.RUnlock()

	out := make([]*DirectQueryDataSource, 0, b.directQueryDataSources.Len())
	for _, ds := range b.directQueryDataSources.All() {
		cp := *ds
		out = append(out, &cp)
	}

	return out
}

// GetDirectQueryDataSource returns a direct-query data source by name.
func (b *InMemoryBackend) GetDirectQueryDataSource(name string) (*DirectQueryDataSource, error) {
	b.mu.RLock("GetDirectQueryDataSource")
	defer b.mu.RUnlock()

	ds, exists := b.directQueryDataSources.Get(name)
	if !exists {
		return nil, fmt.Errorf(
			"%w: direct query data source %s not found",
			ErrDataSourceNotFound,
			name,
		)
	}

	cp := *ds

	return &cp, nil
}

// UpdateDirectQueryDataSource updates a direct-query data source's
// description, type, and OpenSearch ARNs. Real AWS requires DataSourceType
// and OpenSearchArns on every update call.
func (b *InMemoryBackend) UpdateDirectQueryDataSource(
	name, description string,
	dataSourceType json.RawMessage,
	openSearchArns []string,
) (*DirectQueryDataSource, error) {
	b.mu.Lock("UpdateDirectQueryDataSource")
	defer b.mu.Unlock()

	ds, exists := b.directQueryDataSources.Get(name)
	if !exists {
		return nil, fmt.Errorf(
			"%w: direct query data source %s not found",
			ErrDataSourceNotFound,
			name,
		)
	}

	ds.Description = description
	ds.OpenSearchArns = openSearchArns

	if len(dataSourceType) > 0 {
		ds.DataSourceType = dataSourceType
	}

	cp := *ds

	return &cp, nil
}

// DeleteDirectQueryDataSource removes a direct-query data source by name.
func (b *InMemoryBackend) DeleteDirectQueryDataSource(name string) error {
	b.mu.Lock("DeleteDirectQueryDataSource")
	defer b.mu.Unlock()

	b.directQueryDataSources.Delete(name)

	return nil
}
