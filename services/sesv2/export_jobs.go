package sesv2

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const exportJobStatusCancelled = "CANCELLED"

// Real ExportSourceType enum values (aws-sdk-go-v2/service/sesv2/types/enums.go).
const (
	ExportSourceTypeMetricsData     = "METRICS_DATA"
	ExportSourceTypeMessageInsights = "MESSAGE_INSIGHTS"
)

// ExportJob represents a SES v2 export job (minimal model for CancelExportJob).
type ExportJob struct {
	CreatedAt        time.Time `json:"createdAt"`
	JobID            string    `json:"jobId"`
	JobStatus        string    `json:"jobStatus"`
	ExportSourceType string    `json:"exportSourceType"`
}

// CancelExportJob sets an export job status to CANCELLED.
func (b *InMemoryBackend) CancelExportJob(jobID string) error {
	b.mu.Lock("CancelExportJob")
	defer b.mu.Unlock()

	job, ok := b.exportJobs.Get(jobID)
	if !ok {
		return fmt.Errorf("%w: export job %s not found", ErrNotFound, jobID)
	}

	job.JobStatus = exportJobStatusCancelled

	return nil
}

// AddExportJobInternal creates an export job directly (for tests).
func (b *InMemoryBackend) AddExportJobInternal(jobID, status string) *ExportJob {
	b.mu.Lock("AddExportJobInternal")
	defer b.mu.Unlock()

	job := &ExportJob{
		JobID:     jobID,
		JobStatus: status,
		CreatedAt: time.Now(),
	}
	b.exportJobs.Put(job)

	return job
}

// ---- export / import jobs ----

// CreateExportJob creates a new export job. gopherstack has no metrics-
// aggregation or message-log engine behind an export job, so it never
// actually produces export file contents -- it only records which of the two
// mutually exclusive ExportDataSource branches the caller selected (see
// ExportSourceType), readable back via GetExportJob/ListExportJobs.
func (b *InMemoryBackend) CreateExportJob(sourceType string) (*ExportJob, error) {
	jobID := uuid.New().String()

	job := &ExportJob{
		JobID:            jobID,
		JobStatus:        "CREATED",
		CreatedAt:        time.Now(),
		ExportSourceType: sourceType,
	}

	b.mu.Lock("CreateExportJob")
	b.exportJobs.Put(job)
	b.mu.Unlock()

	cp := *job

	return &cp, nil
}

// GetExportJob retrieves an export job.
func (b *InMemoryBackend) GetExportJob(jobID string) (*ExportJob, error) {
	b.mu.RLock("GetExportJob")
	defer b.mu.RUnlock()

	job, ok := b.exportJobs.Get(jobID)
	if !ok {
		return nil, fmt.Errorf("%w: export job %s not found", ErrNotFound, jobID)
	}

	cp := *job

	return &cp, nil
}

// ListExportJobs returns all export jobs.
func (b *InMemoryBackend) ListExportJobs(nextToken string, pageSize int) page.Page[*ExportJob] {
	b.mu.RLock("ListExportJobs")
	defer b.mu.RUnlock()

	snap := b.exportJobs.Snapshot()

	items := make([]*ExportJob, 0, len(snap))
	for _, j := range snap {
		cp := *j
		items = append(items, &cp)
	}

	return page.New(items, nextToken, pageSize, sesv2DefaultMaxItems)
}
