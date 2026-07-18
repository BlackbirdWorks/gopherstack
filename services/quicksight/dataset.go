package quicksight

import (
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/google/uuid"
)

// ---- DataSets ----

// CreateDataSet creates a dataset. When importMode is SPICE, AWS triggers an
// ingestion as a side effect of dataset creation and returns its ARN/ID in
// the CreateDataSet response; this backend mirrors that by creating a real
// Ingestion record (so a subsequent DescribeIngestion/ListIngestions call
// finds it) instead of fabricating an ARN/ID that names a resource that was
// never persisted. For DIRECT_QUERY datasets, no ingestion is triggered and
// the returned *Ingestion is nil.
func (b *InMemoryBackend) CreateDataSet(
	accountID, dataSetID, name, importMode string,
	permissions []ResourcePermission,
	tags map[string]string,
) (*DataSet, *Ingestion, error) {
	if dataSetID == "" || name == "" {
		return nil, nil, ErrValidation
	}

	b.mu.Lock("CreateDataSet")
	defer b.mu.Unlock()

	key := dataSetKey(accountID, dataSetID)
	if b.dataSets.Has(key) {
		return nil, nil, ErrDataSetAlreadyExists
	}

	if importMode == "" {
		importMode = "SPICE"
	}

	now := time.Now().UTC()
	ds := &storedDataSet{
		CreatedTime:      now,
		LastUpdatedTime:  now,
		DataSetID:        dataSetID,
		Arn:              arn.Build("quicksight", b.region, accountID, fmt.Sprintf("dataset/%s", dataSetID)),
		Name:             name,
		ImportMode:       importMode,
		RefreshSchedules: make(map[string]*storedRefreshSchedule),
		Permissions:      clonePermissions(permissions),
	}
	b.dataSets.Put(ds)

	if len(tags) > 0 {
		b.tags[ds.Arn] = maps.Clone(tags)
	}

	var ingestion *Ingestion
	if importMode == "SPICE" {
		ing := &storedIngestion{
			CreatedTime:     now,
			IngestionID:     uuid.NewString(),
			DataSetID:       dataSetID,
			IngestionStatus: statusCompleted,
		}
		ing.Arn = arn.Build(
			"quicksight",
			b.region,
			accountID,
			fmt.Sprintf("dataset/%s/ingestion/%s", dataSetID, ing.IngestionID),
		)
		b.ingestions.Put(ing)
		ingestion = ing.toIngestion()
	}

	return ds.toDataSet(), ingestion, nil
}

func (b *InMemoryBackend) DescribeDataSet(accountID, dataSetID string) (*DataSet, error) {
	b.mu.RLock("DescribeDataSet")
	defer b.mu.RUnlock()

	ds, ok := b.dataSets.Get(dataSetKey(accountID, dataSetID))
	if !ok {
		return nil, ErrDataSetNotFound
	}

	return ds.toDataSet(), nil
}

func (b *InMemoryBackend) UpdateDataSet(accountID, dataSetID, name, importMode string) (*DataSet, error) {
	b.mu.Lock("UpdateDataSet")
	defer b.mu.Unlock()

	key := dataSetKey(accountID, dataSetID)
	ds, ok := b.dataSets.Get(key)
	if !ok {
		return nil, ErrDataSetNotFound
	}

	if name != "" {
		ds.Name = name
	}
	if importMode != "" {
		ds.ImportMode = importMode
	}
	ds.LastUpdatedTime = time.Now().UTC()

	return ds.toDataSet(), nil
}

func (b *InMemoryBackend) DeleteDataSet(accountID, dataSetID string) error {
	b.mu.Lock("DeleteDataSet")
	defer b.mu.Unlock()

	key := dataSetKey(accountID, dataSetID)
	ds, ok := b.dataSets.Get(key)
	if !ok {
		return ErrDataSetNotFound
	}

	delete(b.tags, ds.Arn)
	b.dataSets.Delete(key)

	return nil
}

//nolint:dupl // list functions share structure but operate on different stored types
func (b *InMemoryBackend) ListDataSets(
	_ string,
	maxResults int32,
	nextToken string,
) ([]*DataSet, string, error) {
	b.mu.RLock("ListDataSets")
	defer b.mu.RUnlock()

	all := b.dataSets.All()

	if maxResults <= 0 || maxResults > defaultMaxResults {
		maxResults = defaultMaxResults
	}

	start := 0
	if nextToken != "" {
		for i, ds := range all {
			if ds.DataSetID == nextToken {
				start = i

				break
			}
		}
	}

	end := start + int(maxResults)
	var next string
	if end < len(all) {
		next = all[end].DataSetID
	} else {
		end = len(all)
	}

	result := make([]*DataSet, 0, end-start)
	for _, ds := range all[start:end] {
		result = append(result, ds.toDataSet())
	}

	return result, next, nil
}

// SearchDataSets searches data sets by name (filter Name == filterDataSetName);
// any other filter Name is an ownership-related filter that this in-memory
// backend doesn't track and is treated as a pass-through match.
//
//nolint:dupl // search functions share structure but operate on different stored types
func (b *InMemoryBackend) SearchDataSets(
	_ string,
	filters []SearchFilter,
	maxResults int32,
	nextToken string,
) ([]*DataSet, string, error) {
	b.mu.RLock("SearchDataSets")
	defer b.mu.RUnlock()

	var filtered []*storedDataSet
	for _, ds := range b.dataSets.All() {
		if matchesAllNameFilters(ds.Name, filters, filterDataSetName) {
			filtered = append(filtered, ds)
		}
	}
	sort.Slice(filtered, func(i, j int) bool { return filtered[i].DataSetID < filtered[j].DataSetID })

	if maxResults <= 0 || maxResults > defaultMaxResults {
		maxResults = defaultMaxResults
	}

	start := 0
	if nextToken != "" {
		for i, ds := range filtered {
			if ds.DataSetID == nextToken {
				start = i

				break
			}
		}
	}

	end := start + int(maxResults)
	var next string
	if end < len(filtered) {
		next = filtered[end].DataSetID
	} else {
		end = len(filtered)
	}

	result := make([]*DataSet, 0, end-start)
	for _, ds := range filtered[start:end] {
		result = append(result, ds.toDataSet())
	}

	return result, next, nil
}

// ---- DataSet permissions ----

func (b *InMemoryBackend) DescribeDataSetPermissions(
	accountID, dataSetID string,
) (*DataSet, []ResourcePermission, error) {
	b.mu.RLock("DescribeDataSetPermissions")
	defer b.mu.RUnlock()

	ds, ok := b.dataSets.Get(dataSetKey(accountID, dataSetID))
	if !ok {
		return nil, nil, ErrDataSetNotFound
	}

	return ds.toDataSet(), clonePermissions(ds.Permissions), nil
}

func (b *InMemoryBackend) UpdateDataSetPermissions(
	accountID, dataSetID string,
	grant, revoke []ResourcePermission,
) (*DataSet, []ResourcePermission, error) {
	b.mu.Lock("UpdateDataSetPermissions")
	defer b.mu.Unlock()

	ds, ok := b.dataSets.Get(dataSetKey(accountID, dataSetID))
	if !ok {
		return nil, nil, ErrDataSetNotFound
	}

	ds.Permissions = applyGrantRevoke(ds.Permissions, grant, revoke)
	ds.LastUpdatedTime = time.Now().UTC()

	return ds.toDataSet(), clonePermissions(ds.Permissions), nil
}

// ---- Ingestions ----

func (b *InMemoryBackend) CreateIngestion(accountID, dataSetID, ingestionID string) (*Ingestion, error) {
	b.mu.Lock("CreateIngestion")
	defer b.mu.Unlock()

	if !b.dataSets.Has(dataSetKey(accountID, dataSetID)) {
		return nil, ErrDataSetNotFound
	}

	key := ingestionKey(accountID, dataSetID, ingestionID)
	if b.ingestions.Has(key) {
		return nil, ErrIngestionAlreadyExists
	}

	ing := &storedIngestion{
		CreatedTime: time.Now().UTC(),
		IngestionID: ingestionID,
		Arn: arn.Build(
			"quicksight",
			b.region,
			accountID,
			fmt.Sprintf("dataset/%s/ingestion/%s", dataSetID, ingestionID),
		),
		DataSetID:       dataSetID,
		IngestionStatus: statusRunning,
	}
	b.ingestions.Put(ing)

	return ing.toIngestion(), nil
}

func (b *InMemoryBackend) DescribeIngestion(accountID, dataSetID, ingestionID string) (*Ingestion, error) {
	b.mu.RLock("DescribeIngestion")
	defer b.mu.RUnlock()

	ing, ok := b.ingestions.Get(ingestionKey(accountID, dataSetID, ingestionID))
	if !ok {
		return nil, ErrIngestionNotFound
	}

	return ing.toIngestion(), nil
}

func (b *InMemoryBackend) CancelIngestion(accountID, dataSetID, ingestionID string) error {
	b.mu.Lock("CancelIngestion")
	defer b.mu.Unlock()

	key := ingestionKey(accountID, dataSetID, ingestionID)
	ing, ok := b.ingestions.Get(key)
	if !ok {
		return ErrIngestionNotFound
	}

	ing.IngestionStatus = statusCancelled

	return nil
}

//nolint:dupl // list functions share structure but operate on different stored types
func (b *InMemoryBackend) ListIngestions(
	_, dataSetID string,
	maxResults int32,
	nextToken string,
) ([]*Ingestion, string, error) {
	b.mu.RLock("ListIngestions")
	defer b.mu.RUnlock()

	var all []*storedIngestion
	for _, ing := range b.ingestions.All() {
		if ing.DataSetID == dataSetID {
			all = append(all, ing)
		}
	}

	if maxResults <= 0 || maxResults > defaultMaxResults {
		maxResults = defaultMaxResults
	}

	start := 0
	if nextToken != "" {
		for i, ing := range all {
			if ing.IngestionID == nextToken {
				start = i

				break
			}
		}
	}

	end := start + int(maxResults)
	var next string
	if end < len(all) {
		next = all[end].IngestionID
	} else {
		end = len(all)
	}

	result := make([]*Ingestion, 0, end-start)
	for _, ing := range all[start:end] {
		result = append(result, ing.toIngestion())
	}

	return result, next, nil
}
