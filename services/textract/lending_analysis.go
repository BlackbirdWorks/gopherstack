package textract

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// syntheticLendingResults builds realistic lending results.
func syntheticLendingResults() []LendingResult {
	return []LendingResult{
		{
			Page: 1,
			PageClassification: &PageClassification{
				PageType:   []LendingDetection{{Text: "PAYSTUB", Confidence: confidenceLending}},
				PageNumber: []LendingDetection{{Text: "1", Confidence: confidencePage}},
			},
			Extractions: []Extraction{
				{
					LendingDocument: &LendingDocument{
						LendingFields: []LendingField{
							{
								Type:           &LendingDetection{Text: "BORROWER_NAME", Confidence: confidenceLending},
								ValueDetection: &LendingDetection{Text: "Jane Doe", Confidence: confidenceLending},
								PageNumber:     1,
							},
							{
								Type:           &LendingDetection{Text: "GROSS_INCOME", Confidence: confidenceLending2},
								ValueDetection: &LendingDetection{Text: "$5000.00", Confidence: confidenceLending2},
								PageNumber:     1,
							},
						},
						SignatureDetections: []SignatureDetection{
							{
								Confidence: confidenceLendSig,
								Geometry: makeGeometry(
									geoLendSigLeft, geoLendSigTop,
									geoLendSigWidth, geoLendSigHeight,
								),
							},
						},
					},
				},
			},
		},
	}
}

// syntheticLendingSummary builds a realistic lending summary.
func syntheticLendingSummary() *LendingSummary {
	return &LendingSummary{
		DocumentGroups: []DocumentGroup{
			{
				Type: "PAYSTUB",
				SplitDocuments: []SplitDocument{
					{Index: 1, Pages: []int{1}},
				},
				DetectedSignatures: []DetectedSignature{
					{Page: 1},
				},
			},
		},
		UndetectedDocumentTypes: []string{},
	}
}

// StartLendingAnalysis creates an async lending analysis job.
func (b *InMemoryBackend) StartLendingAnalysis(ctx context.Context, documentURI string) (*LendingJob, error) {
	return b.StartLendingAnalysisWithOptions(ctx, documentURI, nil, nil, "", "")
}

// StartLendingAnalysisWithOptions creates an async lending analysis job with
// full options, including ClientRequestToken dedup: real AWS returns the same
// JobId when the same token is reused (see the docs on
// StartLendingAnalysisInput.ClientRequestToken).
func (b *InMemoryBackend) StartLendingAnalysisWithOptions(
	ctx context.Context,
	_ string,
	outputConfig *OutputConfig,
	notificationChannel *NotificationChannel,
	jobTag, clientRequestToken string,
) (*LendingJob, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("StartLendingAnalysis")

	// Idempotency: if token already seen, return existing job.
	if clientRequestToken != "" {
		if existingID, ok := b.lendingClientTokenToJobIDStore(region)[clientRequestToken]; ok {
			if existing, ok2 := b.lendingJobs.Get(regionKey(region, existingID)); ok2 {
				result := cloneLendingJob(existing)
				b.mu.Unlock()

				return result, nil
			}
		}
	}

	jobID := uuid.NewString()
	job := &LendingJob{
		Region:              region,
		JobID:               jobID,
		JobStatus:           jobStatusInProgress,
		CreationTime:        time.Now(),
		Results:             syntheticLendingResults(),
		Summary:             syntheticLendingSummary(),
		OutputConfig:        outputConfig,
		NotificationChannel: notificationChannel,
		JobTag:              jobTag,
		ClientRequestToken:  clientRequestToken,
	}
	b.lendingJobs.Put(job)
	trimLendingJobsIfNeeded(b.lendingJobs, b.lendingJobsByRegion, region, b.maxJobs)

	if clientRequestToken != "" {
		b.lendingClientTokenToJobIDStore(region)[clientRequestToken] = jobID
	}

	if b.asyncJobDelay == 0 {
		job.JobStatus = jobStatusSucceeded
		result := cloneLendingJob(job)
		b.mu.Unlock()

		return result, nil
	}

	b.mu.Unlock()

	key := lendingJobKey(job)

	b.runDelayed(b.asyncJobDelay, func() {
		b.mu.Lock("StartLendingAnalysis-complete")
		defer b.mu.Unlock()

		if j, ok := b.lendingJobs.Get(key); ok {
			j.JobStatus = jobStatusSucceeded
		}
	})

	b.mu.RLock("StartLendingAnalysis-read")
	stored, _ := b.lendingJobs.Get(key)
	result := cloneLendingJob(stored)
	b.mu.RUnlock()

	return result, nil
}

// GetLendingAnalysis retrieves the results of a lending analysis job.
// Returns a clone of the stored job.
func (b *InMemoryBackend) GetLendingAnalysis(ctx context.Context, jobID string) (*LendingJob, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("GetLendingAnalysis")
	defer b.mu.RUnlock()

	job, ok := b.lendingJobs.Get(regionKey(region, jobID))
	if !ok {
		return nil, fmt.Errorf("%w: lending job %s not found", ErrJobNotFound, jobID)
	}

	return cloneLendingJob(job), nil
}

// GetLendingAnalysisSummary returns a summary of a lending analysis job.
func (b *InMemoryBackend) GetLendingAnalysisSummary(ctx context.Context, jobID string) (*LendingJob, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("GetLendingAnalysisSummary")
	defer b.mu.RUnlock()

	job, ok := b.lendingJobs.Get(regionKey(region, jobID))
	if !ok {
		return nil, fmt.Errorf("%w: lending job %s not found", ErrJobNotFound, jobID)
	}

	// Return a summary: same job status and summary, without per-page lending results.
	cp := *job
	cp.Results = nil

	return &cp, nil
}

// cloneLendingJob returns a deep copy of a LendingJob.
func cloneLendingJob(j *LendingJob) *LendingJob {
	cp := *j
	cp.Results = make([]LendingResult, len(j.Results))
	copy(cp.Results, j.Results)

	if j.Warnings != nil {
		cp.Warnings = make([]WarningBlock, len(j.Warnings))
		copy(cp.Warnings, j.Warnings)
	}

	return &cp
}

func trimLendingJobsIfNeeded(
	t *store.Table[LendingJob], byRegion *store.Index[LendingJob], region string, maxJobs int,
) {
	entries := slices.Clone(byRegion.Get(region))
	if len(entries) <= maxJobs {
		return
	}

	sort.Slice(entries, func(i, k int) bool { return entries[i].CreationTime.Before(entries[k].CreationTime) })

	excess := len(entries) - maxJobs
	for i := range excess {
		t.Delete(lendingJobKey(entries[i]))
	}
}
