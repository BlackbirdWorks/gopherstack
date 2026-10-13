package pinpoint

import "time"

const (
	jobStatusProcessing = "PROCESSING"
	jobStatusCompleted  = "COMPLETED"
	jobHalfDivisor      = 2
	jobSinglePiece      = 1
)

// SetLifecycleDelay sets how long an export or import job takes to reach COMPLETED; zero completes at once.
func (b *InMemoryBackend) SetLifecycleDelay(d time.Duration) {
	b.mu.Lock("SetLifecycleDelay")
	defer b.mu.Unlock()
	b.lifecycleDelay = d
}

// jobProgress derives a job's status and completion time from its creation time. Caller must hold b.mu.
func (b *InMemoryBackend) jobProgress(creationDate string, now time.Time) (string, string) {
	created, err := time.Parse(time.RFC3339, creationDate)
	if err != nil {
		return jobStatusCompleted, creationDate
	}

	age := now.Sub(created)

	switch {
	case age >= b.lifecycleDelay:
		return jobStatusCompleted, created.Add(b.lifecycleDelay).UTC().Format(time.RFC3339)
	case age >= b.lifecycleDelay/jobHalfDivisor:
		return jobStatusProcessing, ""
	default:
		return jobStatusCreated, ""
	}
}

// settleExportJob returns a copy of j with status and progress derived for now. Caller must hold b.mu.
func (b *InMemoryBackend) settleExportJob(j *ExportJob, now time.Time) *ExportJob {
	cp := *j
	cp.JobStatus, cp.CompletionDate = b.jobProgress(j.CreationDate, now)

	if cp.JobStatus == jobStatusCompleted {
		cp.TotalPieces = jobSinglePiece
		cp.CompletedPieces = jobSinglePiece
		cp.TotalProcessed = b.countExportedEndpoints(j)
	}

	return &cp
}

// settleImportJob is the import counterpart of settleExportJob; no S3 object is read, so nothing is processed.
func (b *InMemoryBackend) settleImportJob(j *ImportJob, now time.Time) *ImportJob {
	cp := *j
	cp.JobStatus, cp.CompletionDate = b.jobProgress(j.CreationDate, now)

	if cp.JobStatus == jobStatusCompleted {
		cp.TotalPieces = jobSinglePiece
		cp.CompletedPieces = jobSinglePiece
	}

	return &cp
}

// countExportedEndpoints counts the endpoints an export of the job's scope would contain. Caller must hold b.mu.
func (b *InMemoryBackend) countExportedEndpoints(j *ExportJob) int {
	if j.SegmentID != "" {
		return 0
	}

	n := 0

	for _, e := range b.endpoints.All() {
		if e.ApplicationID == j.ApplicationID {
			n++
		}
	}

	return n
}
