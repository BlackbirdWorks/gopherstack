package quicksight

import (
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// ---- DataSources ----

func (b *InMemoryBackend) CreateDataSource(
	accountID, dataSourceID, name, dsType string,
	permissions []ResourcePermission,
	tags map[string]string,
) (*DataSource, error) {
	if dataSourceID == "" || name == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateDataSource")
	defer b.mu.Unlock()

	key := dataSourceKey(accountID, dataSourceID)
	if b.dataSources.Has(key) {
		return nil, ErrDataSourceAlreadyExists
	}

	now := time.Now().UTC()
	ds := &storedDataSource{
		CreatedTime:     now,
		LastUpdatedTime: now,
		DataSourceID:    dataSourceID,
		Arn:             arn.Build("quicksight", b.region, accountID, fmt.Sprintf("datasource/%s", dataSourceID)),
		Name:            name,
		Type:            dsType,
		Status:          statusCreationSuccessful,
		Permissions:     clonePermissions(permissions),
	}
	b.dataSources.Put(ds)

	if len(tags) > 0 {
		b.tags[ds.Arn] = maps.Clone(tags)
	}

	return ds.toDataSource(), nil
}

func (b *InMemoryBackend) DescribeDataSource(accountID, dataSourceID string) (*DataSource, error) {
	b.mu.RLock("DescribeDataSource")
	defer b.mu.RUnlock()

	ds, ok := b.dataSources.Get(dataSourceKey(accountID, dataSourceID))
	if !ok {
		return nil, ErrDataSourceNotFound
	}

	return ds.toDataSource(), nil
}

func (b *InMemoryBackend) UpdateDataSource(accountID, dataSourceID, name string) (*DataSource, error) {
	b.mu.Lock("UpdateDataSource")
	defer b.mu.Unlock()

	key := dataSourceKey(accountID, dataSourceID)
	ds, ok := b.dataSources.Get(key)
	if !ok {
		return nil, ErrDataSourceNotFound
	}

	if name != "" {
		ds.Name = name
	}
	ds.LastUpdatedTime = time.Now().UTC()
	ds.Status = statusUpdateSuccessful

	return ds.toDataSource(), nil
}

func (b *InMemoryBackend) DeleteDataSource(accountID, dataSourceID string) error {
	b.mu.Lock("DeleteDataSource")
	defer b.mu.Unlock()

	key := dataSourceKey(accountID, dataSourceID)
	ds, ok := b.dataSources.Get(key)
	if !ok {
		return ErrDataSourceNotFound
	}

	delete(b.tags, ds.Arn)
	b.dataSources.Delete(key)

	return nil
}

//nolint:dupl // list functions share structure but operate on different stored types
func (b *InMemoryBackend) ListDataSources(
	_ string,
	maxResults int32,
	nextToken string,
) ([]*DataSource, string, error) {
	b.mu.RLock("ListDataSources")
	defer b.mu.RUnlock()

	all := b.dataSources.All()

	if maxResults <= 0 || maxResults > defaultMaxResults {
		maxResults = defaultMaxResults
	}

	start := 0
	if nextToken != "" {
		for i, ds := range all {
			if ds.DataSourceID == nextToken {
				start = i

				break
			}
		}
	}

	end := start + int(maxResults)
	var next string
	if end < len(all) {
		next = all[end].DataSourceID
	} else {
		end = len(all)
	}

	result := make([]*DataSource, 0, end-start)
	for _, ds := range all[start:end] {
		result = append(result, ds.toDataSource())
	}

	return result, next, nil
}

// SearchDataSources searches data sources by name (filter Name ==
// filterDataSourceName); any other filter Name is an ownership-related filter
// that this in-memory backend doesn't track and is treated as a pass-through
// match, mirroring folderMatchesFilter's permissive default.
//
//nolint:dupl // search functions share structure but operate on different stored types
func (b *InMemoryBackend) SearchDataSources(
	_ string,
	filters []SearchFilter,
	maxResults int32,
	nextToken string,
) ([]*DataSource, string, error) {
	b.mu.RLock("SearchDataSources")
	defer b.mu.RUnlock()

	var filtered []*storedDataSource
	for _, ds := range b.dataSources.All() {
		if matchesAllNameFilters(ds.Name, filters, filterDataSourceName) {
			filtered = append(filtered, ds)
		}
	}
	sort.Slice(filtered, func(i, j int) bool { return filtered[i].DataSourceID < filtered[j].DataSourceID })

	if maxResults <= 0 || maxResults > defaultMaxResults {
		maxResults = defaultMaxResults
	}

	start := 0
	if nextToken != "" {
		for i, ds := range filtered {
			if ds.DataSourceID == nextToken {
				start = i

				break
			}
		}
	}

	end := start + int(maxResults)
	var next string
	if end < len(filtered) {
		next = filtered[end].DataSourceID
	} else {
		end = len(filtered)
	}

	result := make([]*DataSource, 0, end-start)
	for _, ds := range filtered[start:end] {
		result = append(result, ds.toDataSource())
	}

	return result, next, nil
}

// ---- DataSource permissions ----

func (b *InMemoryBackend) DescribeDataSourcePermissions(
	accountID, dataSourceID string,
) (*DataSource, []ResourcePermission, error) {
	b.mu.RLock("DescribeDataSourcePermissions")
	defer b.mu.RUnlock()

	ds, ok := b.dataSources.Get(dataSourceKey(accountID, dataSourceID))
	if !ok {
		return nil, nil, ErrDataSourceNotFound
	}

	return ds.toDataSource(), clonePermissions(ds.Permissions), nil
}

func (b *InMemoryBackend) UpdateDataSourcePermissions(
	accountID, dataSourceID string,
	grant, revoke []ResourcePermission,
) (*DataSource, []ResourcePermission, error) {
	b.mu.Lock("UpdateDataSourcePermissions")
	defer b.mu.Unlock()

	ds, ok := b.dataSources.Get(dataSourceKey(accountID, dataSourceID))
	if !ok {
		return nil, nil, ErrDataSourceNotFound
	}

	ds.Permissions = applyGrantRevoke(ds.Permissions, grant, revoke)
	ds.LastUpdatedTime = time.Now().UTC()

	return ds.toDataSource(), clonePermissions(ds.Permissions), nil
}
