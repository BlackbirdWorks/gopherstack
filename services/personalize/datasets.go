package personalize

import (
	"fmt"
	"strings"
	"time"
)

// --- Dataset ---

// validDatasetTypes are the case-insensitive DatasetType values the real API
// accepts (documented on CreateDatasetInput.DatasetType; the field is a
// plain *string in the SDK, not a typed smithy enum, but AWS still rejects
// anything outside this set server-side).
var validDatasetTypes = map[string]bool{ //nolint:gochecknoglobals // fixed lookup table, mirrors errCodeLookup style
	"INTERACTIONS":        true,
	"ITEMS":               true,
	"USERS":               true,
	"ACTIONS":             true,
	"ACTION_INTERACTIONS": true,
}

// CreateDataset creates a new dataset.
func (b *InMemoryBackend) CreateDataset(
	name, datasetGroupArn, datasetType, schemaArn string,
	tags map[string]string,
) (*Dataset, error) {
	b.mu.Lock("CreateDataset")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}
	if b.datasets.Has(name) {
		return nil, fmt.Errorf("%w: dataset %q already exists", ErrAlreadyExists, name)
	}
	if !validDatasetTypes[strings.ToUpper(datasetType)] {
		return nil, fmt.Errorf("%w: datasetType %q is invalid", ErrValidation, datasetType)
	}
	if b.findDatasetGroup(datasetGroupArn) == nil {
		return nil, fmt.Errorf("%w: dataset group %q not found", ErrNotFound, datasetGroupArn)
	}
	if b.findSchema(schemaArn) == nil {
		return nil, fmt.Errorf("%w: schema %q not found", ErrNotFound, schemaArn)
	}

	now := time.Now().UTC()
	ds := &Dataset{
		DatasetArn:          b.personalizeARN("dataset", name),
		Name:                name,
		DatasetGroupArn:     datasetGroupArn,
		DatasetType:         datasetType,
		SchemaArn:           schemaArn,
		Status:              statusActive,
		CreationDateTime:    now,
		LastUpdatedDateTime: now,
	}
	b.datasets.Put(ds)
	if len(tags) > 0 {
		b.tags[ds.DatasetArn] = copyStringMap(tags)
	}

	return ds, nil
}

// DescribeDataset returns a dataset by name or ARN.
func (b *InMemoryBackend) DescribeDataset(nameOrArn string) (*Dataset, error) {
	b.mu.RLock("DescribeDataset")
	defer b.mu.RUnlock()

	if ds := b.findDataset(nameOrArn); ds != nil {
		return ds, nil
	}

	return nil, fmt.Errorf("%w: dataset %q not found", ErrNotFound, nameOrArn)
}

// UpdateDataset updates a dataset's schema.
func (b *InMemoryBackend) UpdateDataset(nameOrArn, schemaArn string) (*Dataset, error) {
	b.mu.Lock("UpdateDataset")
	defer b.mu.Unlock()

	ds := b.findDataset(nameOrArn)
	if ds == nil {
		return nil, fmt.Errorf("%w: dataset %q not found", ErrNotFound, nameOrArn)
	}
	if schemaArn != "" {
		ds.SchemaArn = schemaArn
	}
	ds.LastUpdatedDateTime = time.Now().UTC()

	return ds, nil
}

// DeleteDataset removes a dataset.
func (b *InMemoryBackend) DeleteDataset(nameOrArn string) error {
	b.mu.Lock("DeleteDataset")
	defer b.mu.Unlock()

	ds := b.findDataset(nameOrArn)
	if ds == nil {
		return fmt.Errorf("%w: dataset %q not found", ErrNotFound, nameOrArn)
	}
	b.datasets.Delete(ds.Name)
	delete(b.tags, ds.DatasetArn)

	return nil
}

// ListDatasets returns datasets, optionally filtered by dataset group ARN.
func (b *InMemoryBackend) ListDatasets(datasetGroupArn string, maxResults int, nextToken string) ([]*Dataset, string) {
	b.mu.RLock("ListDatasets")
	defer b.mu.RUnlock()

	all := b.datasets.Snapshot()
	filtered := make([]*Dataset, 0, len(all))
	for _, ds := range all {
		if datasetGroupArn == "" || ds.DatasetGroupArn == datasetGroupArn {
			filtered = append(filtered, ds)
		}
	}

	return paginateItems(filtered, datasetKeyFn, maxResults, nextToken)
}

func (b *InMemoryBackend) findDataset(nameOrArn string) *Dataset {
	if ds, ok := b.datasets.Get(nameOrArn); ok {
		return ds
	}
	for _, ds := range b.datasets.All() {
		if ds.DatasetArn == nameOrArn {
			return ds
		}
	}

	return nil
}

// requireDataset FK-validates that datasetArn resolves to a real dataset,
// shared by CreateDatasetImportJob and CreateDatasetExportJob (both key
// their source data off a datasetArn).
func (b *InMemoryBackend) requireDataset(datasetArn string) error {
	if b.findDataset(datasetArn) == nil {
		return fmt.Errorf("%w: dataset %q not found", ErrNotFound, datasetArn)
	}

	return nil
}

// --- DatasetImportJob ---

// CreateDatasetImportJob creates a new dataset import job.
func (b *InMemoryBackend) CreateDatasetImportJob(
	jobName, datasetArn, roleArn string,
	dataSource map[string]any,
	tags map[string]string,
) (*DatasetImportJob, error) {
	b.mu.Lock("CreateDatasetImportJob")
	defer b.mu.Unlock()

	if err := requireJobName(jobName); err != nil {
		return nil, err
	}
	if err := b.requireDataset(datasetArn); err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	jobArn := b.personalizeARN("dataset-import-job", jobName)
	job := &DatasetImportJob{
		DatasetImportJobArn: jobArn,
		JobName:             jobName,
		DatasetArn:          datasetArn,
		RoleArn:             roleArn,
		DataSource:          dataSource,
		Status:              statusActive,
		CreationDateTime:    now,
		LastUpdatedDateTime: now,
	}
	b.datasetImportJobs.Put(job)
	if len(tags) > 0 {
		b.tags[jobArn] = copyStringMap(tags)
	}

	return job, nil
}

// DescribeDatasetImportJob returns a dataset import job by ARN.
func (b *InMemoryBackend) DescribeDatasetImportJob(jobArn string) (*DatasetImportJob, error) {
	b.mu.RLock("DescribeDatasetImportJob")
	defer b.mu.RUnlock()

	job, ok := b.datasetImportJobs.Get(jobArn)
	if !ok {
		return nil, fmt.Errorf("%w: dataset import job %q not found", ErrNotFound, jobArn)
	}

	return job, nil
}

// ListDatasetImportJobs returns import jobs, optionally filtered by dataset ARN.
func (b *InMemoryBackend) ListDatasetImportJobs(
	datasetArn string,
	maxResults int,
	nextToken string,
) ([]*DatasetImportJob, string) {
	b.mu.RLock("ListDatasetImportJobs")
	defer b.mu.RUnlock()

	all := b.datasetImportJobs.Snapshot()
	filtered := make([]*DatasetImportJob, 0, len(all))
	for _, job := range all {
		if datasetArn == "" || job.DatasetArn == datasetArn {
			filtered = append(filtered, job)
		}
	}

	return paginateItems(filtered, datasetImportJobKeyFn, maxResults, nextToken)
}

// --- DatasetExportJob ---

// CreateDatasetExportJob creates a new dataset export job.
func (b *InMemoryBackend) CreateDatasetExportJob(
	jobName, datasetArn, roleArn string,
	jobOutput map[string]any,
	tags map[string]string,
) (*DatasetExportJob, error) {
	b.mu.Lock("CreateDatasetExportJob")
	defer b.mu.Unlock()

	if err := requireJobName(jobName); err != nil {
		return nil, err
	}
	if err := b.requireDataset(datasetArn); err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	jobArn := b.personalizeARN("dataset-export-job", jobName)
	job := &DatasetExportJob{
		DatasetExportJobArn: jobArn,
		JobName:             jobName,
		DatasetArn:          datasetArn,
		RoleArn:             roleArn,
		JobOutput:           jobOutput,
		Status:              statusActive,
		CreationDateTime:    now,
		LastUpdatedDateTime: now,
	}
	b.datasetExportJobs.Put(job)
	if len(tags) > 0 {
		b.tags[jobArn] = copyStringMap(tags)
	}

	return job, nil
}

// DescribeDatasetExportJob returns a dataset export job by ARN.
func (b *InMemoryBackend) DescribeDatasetExportJob(jobArn string) (*DatasetExportJob, error) {
	b.mu.RLock("DescribeDatasetExportJob")
	defer b.mu.RUnlock()

	job, ok := b.datasetExportJobs.Get(jobArn)
	if !ok {
		return nil, fmt.Errorf("%w: dataset export job %q not found", ErrNotFound, jobArn)
	}

	return job, nil
}

// ListDatasetExportJobs returns export jobs, optionally filtered by dataset ARN.
func (b *InMemoryBackend) ListDatasetExportJobs(
	datasetArn string,
	maxResults int,
	nextToken string,
) ([]*DatasetExportJob, string) {
	b.mu.RLock("ListDatasetExportJobs")
	defer b.mu.RUnlock()

	all := b.datasetExportJobs.Snapshot()
	filtered := make([]*DatasetExportJob, 0, len(all))
	for _, job := range all {
		if datasetArn == "" || job.DatasetArn == datasetArn {
			filtered = append(filtered, job)
		}
	}

	return paginateItems(filtered, datasetExportJobKeyFn, maxResults, nextToken)
}
