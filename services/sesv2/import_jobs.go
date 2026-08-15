package sesv2

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// ImportDestination mirrors types.ImportDestination's two mutually exclusive
// branches (exactly one is set, enforced by handleCreateImportJob).
type ImportDestination struct {
	ContactListName             string `json:"contactListName,omitempty"`
	ContactListImportAction     string `json:"contactListImportAction,omitempty"`
	SuppressionListImportAction string `json:"suppressionListImportAction,omitempty"`
}

// ImportJob stores an import job.
type ImportJob struct {
	CreatedAt         time.Time         `json:"createdAt"`
	JobID             string            `json:"jobId"`
	JobStatus         string            `json:"jobStatus"`
	ImportDestination ImportDestination `json:"importDestination"`
}

// CreateImportJob creates an import job. gopherstack has no S3 fetcher to read
// the import file itself, so the job never actually applies any records to a
// contact list or the suppression list -- it only records which destination
// the (unfetchable) import targeted, readable back via GetImportJob/
// ListImportJobs.
func (b *InMemoryBackend) CreateImportJob(destination ImportDestination) (*ImportJob, error) {
	jobID := uuid.New().String()

	job := &ImportJob{
		JobID:             jobID,
		JobStatus:         "CREATED",
		CreatedAt:         time.Now(),
		ImportDestination: destination,
	}

	b.mu.Lock("CreateImportJob")
	b.importJobs.Put(job)
	b.mu.Unlock()

	cp := *job

	return &cp, nil
}

// GetImportJob retrieves an import job.
func (b *InMemoryBackend) GetImportJob(jobID string) (*ImportJob, error) {
	b.mu.RLock("GetImportJob")
	defer b.mu.RUnlock()

	job, ok := b.importJobs.Get(jobID)
	if !ok {
		return nil, fmt.Errorf("%w: import job %s not found", ErrNotFound, jobID)
	}

	cp := *job

	return &cp, nil
}

// ListImportJobs returns all import jobs.
func (b *InMemoryBackend) ListImportJobs(nextToken string, pageSize int) page.Page[*ImportJob] {
	b.mu.RLock("ListImportJobs")
	defer b.mu.RUnlock()

	snap := b.importJobs.Snapshot()

	items := make([]*ImportJob, 0, len(snap))
	for _, j := range snap {
		cp := *j
		items = append(items, &cp)
	}

	return page.New(items, nextToken, pageSize, sesv2DefaultMaxItems)
}
