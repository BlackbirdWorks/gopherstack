package personalize

import (
	"fmt"
	"time"
)

// --- DataDeletionJob ---

// CreateDataDeletionJob creates a new data deletion job.
func (b *InMemoryBackend) CreateDataDeletionJob(
	jobName, datasetGroupArn, roleArn string,
	dataSource map[string]any,
	tags map[string]string,
) (*DataDeletionJob, error) {
	b.mu.Lock("CreateDataDeletionJob")
	defer b.mu.Unlock()

	if jobName == "" {
		return nil, fmt.Errorf("%w: jobName is required", ErrValidation)
	}

	now := time.Now().UTC()
	jobArn := b.personalizeARN("data-deletion-job", jobName)
	job := &DataDeletionJob{
		DataDeletionJobArn:  jobArn,
		JobName:             jobName,
		DatasetGroupArn:     datasetGroupArn,
		RoleArn:             roleArn,
		DataSource:          dataSource,
		Status:              statusActive,
		NumDeleted:          0,
		CreationDateTime:    now,
		LastUpdatedDateTime: now,
	}
	b.dataDeletionJobs.Put(job)
	if len(tags) > 0 {
		b.tags[jobArn] = copyStringMap(tags)
	}

	return job, nil
}

// DescribeDataDeletionJob returns a data deletion job by ARN.
func (b *InMemoryBackend) DescribeDataDeletionJob(jobArn string) (*DataDeletionJob, error) {
	b.mu.RLock("DescribeDataDeletionJob")
	defer b.mu.RUnlock()

	job, ok := b.dataDeletionJobs.Get(jobArn)
	if !ok {
		return nil, fmt.Errorf("%w: data deletion job %q not found", ErrNotFound, jobArn)
	}

	return job, nil
}

// ListDataDeletionJobs returns data deletion jobs, optionally filtered by dataset group ARN.
func (b *InMemoryBackend) ListDataDeletionJobs(
	datasetGroupArn string,
	maxResults int,
	nextToken string,
) ([]*DataDeletionJob, string) {
	b.mu.RLock("ListDataDeletionJobs")
	defer b.mu.RUnlock()

	all := b.dataDeletionJobs.Snapshot()
	filtered := make([]*DataDeletionJob, 0, len(all))
	for _, job := range all {
		if datasetGroupArn == "" || job.DatasetGroupArn == datasetGroupArn {
			filtered = append(filtered, job)
		}
	}

	return paginateItems(filtered, dataDeletionJobKeyFn, maxResults, nextToken)
}
