package quicksight

import (
	"fmt"
	"time"
)

const (
	snapshotJobStatusQueued    = "QUEUED"
	snapshotJobStatusCompleted = "COMPLETED"
)

// storedDashboardSnapshotJob is the persisted representation of one dashboard
// snapshot ("export to PDF/CSV/Excel") job, keyed by (DashboardId, JobId).
type storedDashboardSnapshotJob struct {
	CreatedTime     time.Time      `json:"createdTime"`
	LastUpdatedTime time.Time      `json:"lastUpdatedTime"`
	SnapshotConfig  map[string]any `json:"snapshotConfig,omitempty"`
	JobID           string         `json:"jobId"`
	Arn             string         `json:"arn"`
	DashboardID     string         `json:"dashboardId"`
	Status          string         `json:"status"`
	S3URI           string         `json:"s3Uri,omitempty"`
}

func (j *storedDashboardSnapshotJob) toDashboardSnapshotJob() *DashboardSnapshotJob {
	return &DashboardSnapshotJob{
		CreatedTime:     j.CreatedTime,
		LastUpdatedTime: j.LastUpdatedTime,
		SnapshotConfig:  j.SnapshotConfig,
		JobID:           j.JobID,
		Arn:             j.Arn,
		DashboardID:     j.DashboardID,
		Status:          j.Status,
		S3URI:           j.S3URI,
	}
}

// ---- Dashboard snapshot jobs ----

func (b *InMemoryBackend) StartDashboardSnapshotJob(
	accountID, dashboardID, jobID string,
	snapshotConfiguration map[string]any,
) (*DashboardSnapshotJob, error) {
	if jobID == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("StartDashboardSnapshotJob")
	defer b.mu.Unlock()

	if _, ok := b.dashboards[dashboardKey(accountID, dashboardID)]; !ok {
		return nil, ErrDashboardNotFound
	}

	now := time.Now().UTC()
	job := &storedDashboardSnapshotJob{
		CreatedTime:     now,
		LastUpdatedTime: now,
		SnapshotConfig:  snapshotConfiguration,
		JobID:           jobID,
		Arn:             b.buildARN("dashboard", dashboardID) + "/snapshot-job/" + jobID,
		DashboardID:     dashboardID,
		Status:          snapshotJobStatusQueued,
	}
	b.dashboardSnapshotJobs[dashboardSnapshotJobKey(accountID, dashboardID, jobID)] = job

	return job.toDashboardSnapshotJob(), nil
}

func (b *InMemoryBackend) settleDashboardSnapshotJobLocked(job *storedDashboardSnapshotJob, accountID string) {
	if job.Status != snapshotJobStatusQueued {
		return
	}

	job.Status = snapshotJobStatusCompleted
	job.LastUpdatedTime = time.Now().UTC()
	job.S3URI = fmt.Sprintf(
		"s3://quicksight-snapshots-%s/%s/%s/%s.pdf",
		b.region, accountID, job.DashboardID, job.JobID,
	)
}

func (b *InMemoryBackend) DescribeDashboardSnapshotJob(
	accountID, dashboardID, jobID string,
) (*DashboardSnapshotJob, error) {
	b.mu.Lock("DescribeDashboardSnapshotJob")
	defer b.mu.Unlock()

	job, ok := b.dashboardSnapshotJobs[dashboardSnapshotJobKey(accountID, dashboardID, jobID)]
	if !ok {
		return nil, ErrDashboardSnapshotJobNotFound
	}

	// Settle the job deterministically on first poll, mirroring how a real
	// snapshot job finishes shortly after being queued.
	b.settleDashboardSnapshotJobLocked(job, accountID)

	return job.toDashboardSnapshotJob(), nil
}

func (b *InMemoryBackend) DescribeDashboardSnapshotJobResult(
	accountID, dashboardID, jobID string,
) (*DashboardSnapshotJob, error) {
	b.mu.Lock("DescribeDashboardSnapshotJobResult")
	defer b.mu.Unlock()

	job, ok := b.dashboardSnapshotJobs[dashboardSnapshotJobKey(accountID, dashboardID, jobID)]
	if !ok {
		return nil, ErrDashboardSnapshotJobNotFound
	}

	b.settleDashboardSnapshotJobLocked(job, accountID)

	return job.toDashboardSnapshotJob(), nil
}
