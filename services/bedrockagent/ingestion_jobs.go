package bedrockagent

import (
	"context"
	"fmt"
	"time"
)

// ---------------------------------------------------------------------------
// Ingestion job CRUD
// ---------------------------------------------------------------------------

func jobKey(kbID, dsID, jobID string) string { return kbID + "/" + dsID + "/" + jobID }

// StartIngestionJob creates and starts a new ingestion job.
func (b *InMemoryBackend) StartIngestionJob(
	_ context.Context, kbID, dsID, description string,
) (*IngestionJob, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.dataSources.Has(dsKey(kbID, dsID)) {
		return nil, fmt.Errorf("%w: data source %q not found", ErrNotFound, dsID)
	}

	id := b.nextID("job", &b.jobCounter)
	now := time.Now().UTC()

	job := &IngestionJob{
		IngestionJobID:  id,
		KnowledgeBaseID: kbID,
		DataSourceID:    dsID,
		Status:          ingestionJobComplete,
		Description:     description,
		StartedAt:       now,
		UpdatedAt:       now,
		Statistics:      b.ingestionStatisticsLocked(kbID, dsID),
	}

	b.ingestionJobs.Put(job)

	return jobCopy(job), nil
}

// ingestionStatisticsLocked computes document-count statistics for a data
// source's ingestion job from the real KnowledgeBaseDocument store (b.mu
// must already be held). This service has no crawler backing a data
// source's real external content (S3/web/etc.), so the only documents it
// can honestly account for are ones a client explicitly pushed via
// IngestKnowledgeBaseDocuments; every one of those is counted as scanned
// and newly indexed. Deleted/failed/modified counts stay zero since
// gopherstack does not track a prior-job document snapshot to diff
// against -- reporting non-zero there would be fabricated, not read from
// real state.
func (b *InMemoryBackend) ingestionStatisticsLocked(kbID, dsID string) *IngestionJobStatistics {
	group := b.kbDocumentsByDataSource.Get(dsKey(kbID, dsID))
	count := int64(len(group))

	return &IngestionJobStatistics{
		NumberOfDocumentsScanned:    count,
		NumberOfNewDocumentsIndexed: count,
	}
}

// GetIngestionJob returns an ingestion job.
func (b *InMemoryBackend) GetIngestionJob(
	_ context.Context, kbID, dsID, jobID string,
) (*IngestionJob, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	job, ok := b.ingestionJobs.Get(jobKey(kbID, dsID, jobID))
	if !ok {
		return nil, fmt.Errorf("%w: ingestion job %q not found", ErrNotFound, jobID)
	}

	return jobCopy(job), nil
}

// StopIngestionJob stops an ingestion job.
func (b *InMemoryBackend) StopIngestionJob(
	_ context.Context, kbID, dsID, jobID string,
) (*IngestionJob, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	job, ok := b.ingestionJobs.Get(jobKey(kbID, dsID, jobID))
	if !ok {
		return nil, fmt.Errorf("%w: ingestion job %q not found", ErrNotFound, jobID)
	}

	job.Status = "STOPPED"
	job.UpdatedAt = time.Now().UTC()

	return jobCopy(job), nil
}

// ListIngestionJobs returns paginated ingestion job summaries.
func (b *InMemoryBackend) ListIngestionJobs(
	_ context.Context, kbID, dsID string, maxResults int, nextToken string,
) ([]*IngestionJob, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	group := b.ingestionJobsByDataSource.Get(dsKey(kbID, dsID))
	ids := tableIDs(group, func(j *IngestionJob) string { return j.IngestionJobID })
	ids, outToken := paginate(ids, nextToken, maxResults)

	out := make([]*IngestionJob, 0, len(ids))

	for _, id := range ids {
		job, _ := b.ingestionJobs.Get(jobKey(kbID, dsID, id))
		out = append(out, jobCopy(job))
	}

	return out, outToken, nil
}

func jobCopy(j *IngestionJob) *IngestionJob {
	cp := *j

	return &cp
}
