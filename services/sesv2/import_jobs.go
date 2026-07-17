package sesv2

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// ImportJob stores an import job.
type ImportJob struct {
	CreatedAt time.Time `json:"createdAt"`
	JobID     string    `json:"jobId"`
	JobStatus string    `json:"jobStatus"`
}

// CreateImportJob creates an import job.
func (b *InMemoryBackend) CreateImportJob(dataSource string) (*ImportJob, error) {
	jobID := uuid.New().String()

	job := &ImportJob{
		JobID:     jobID,
		JobStatus: "CREATED",
		CreatedAt: time.Now(),
	}

	_ = dataSource

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
