package bedrock

import (
	"fmt"
	"sort"
	"time"
)

const (
	ingestionCompleteDelay = 200 * time.Millisecond // delay for ingestion job STARTING → COMPLETE
)

const (
	jobStatusStarting = "STARTING"
	jobStatusComplete = "COMPLETE"
)

func ingestionJobKey(
	kbID, dsID, jobID string,
) string {
	return kbID + "/" + dsID + "/" + jobID
}

// StartIngestionJob starts an ingestion job for a data source.
// AWS rejects starting a new job if one is already in STARTING state for the same data source (ConflictException).
func (b *InMemoryBackend) StartIngestionJob(kbID, dsID, description string) (*IngestionJob, error) {
	b.mu.Lock("StartIngestionJob")
	defer b.mu.Unlock()

	if _, ok := b.knowledgeBases.Get(kbID); !ok {
		return nil, fmt.Errorf("%w: knowledge base %q not found", ErrNotFound, kbID)
	}

	if _, ok := b.dataSources.Get(kbID + "/" + dsID); !ok {
		return nil, fmt.Errorf("%w: data source %q not found", ErrNotFound, dsID)
	}

	for _, job := range b.ingestionJobs.All() {
		if job.KnowledgeBaseID == kbID && job.DataSourceID == dsID && job.Status == jobStatusStarting {
			return nil, fmt.Errorf("%w: data source %q already has a running ingestion job", ErrAlreadyExists, dsID)
		}
	}

	b.ingestionJobCounter++
	jobID := fmt.Sprintf("ij-%08d", b.ingestionJobCounter)
	now := time.Now()

	job := &IngestionJob{
		StartedAt:       now,
		UpdatedAt:       now,
		IngestionJobID:  jobID,
		KnowledgeBaseID: kbID,
		DataSourceID:    dsID,
		Status:          jobStatusStarting,
		Description:     description,
	}
	b.ingestionJobs.Put(job)

	job.completionDueAt = now.Add(ingestionCompleteDelay)

	cp := *job

	return &cp, nil
}

// GetIngestionJob returns an ingestion job by ID.
func (b *InMemoryBackend) GetIngestionJob(kbID, dsID, jobID string) (*IngestionJob, error) {
	b.mu.Lock("GetIngestionJob")
	defer b.mu.Unlock()

	job, ok := b.ingestionJobs.Get(ingestionJobKey(kbID, dsID, jobID))
	if !ok {
		return nil, fmt.Errorf("%w: ingestion job %q not found", ErrNotFound, jobID)
	}

	advanceIngestionJob(job)
	cp := *job

	return &cp, nil
}

// ListIngestionJobs lists ingestion jobs for a data source.
func (b *InMemoryBackend) ListIngestionJobs(
	kbID, dsID string,
	maxResults int,
	nextToken string,
) ([]*IngestionJob, string) {
	b.mu.Lock("ListIngestionJobs")
	defer b.mu.Unlock()

	list := make([]*IngestionJob, 0, b.ingestionJobs.Len())

	for _, job := range b.ingestionJobs.All() {
		if job.KnowledgeBaseID == kbID && job.DataSourceID == dsID {
			advanceIngestionJob(job)
			cp := *job
			list = append(list, &cp)
		}
	}

	sort.Slice(list, func(i, j int) bool { return list[i].IngestionJobID < list[j].IngestionJobID })

	return paginate(list, maxResults, nextToken)
}

func advanceIngestionJob(job *IngestionJob) {
	if job.Status == jobStatusStarting && !time.Now().Before(job.completionDueAt) {
		job.Status = jobStatusComplete
		job.UpdatedAt = time.Now()
	}
}

const (
	jobStatusStopped = "STOPPED"
)

// StopIngestionJob stops a running ingestion job.
// AWS only allows stopping jobs in STARTING state; other states return ValidationException.
func (b *InMemoryBackend) StopIngestionJob(kbID, dsID, jobID string) (*IngestionJob, error) {
	b.mu.Lock("StopIngestionJob")
	defer b.mu.Unlock()

	job, ok := b.ingestionJobs.Get(ingestionJobKey(kbID, dsID, jobID))
	if !ok {
		return nil, fmt.Errorf("%w: ingestion job %q not found", ErrNotFound, jobID)
	}

	if job.Status != jobStatusStarting {
		return nil, fmt.Errorf(
			"%w: ingestion job %q cannot be stopped in status %s",
			ErrValidation,
			jobID,
			job.Status,
		)
	}

	job.Status = jobStatusStopped
	job.UpdatedAt = time.Now()
	cp := *job

	return &cp, nil
}
